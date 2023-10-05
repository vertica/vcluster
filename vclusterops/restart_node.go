/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package vclusterops

import (
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// Normal strings are easier and safer to use in Go.
type VRestartNodesOptions struct {
	// basic db info
	DatabaseOptions
	// A set of nodes(nodename - host) that we want to restart in the database
	Nodes map[string]string
	// timeout for polling nodes that we want to restart in HTTPSPollNodeStateOp
	StatePollingTimeout int
}

type VRestartNodesInfo struct {
	// The IP address that we intend to re-IP can be obtained from a set of nodes provided as input
	// within VRestartNodesOptions struct
	ReIPList []string
	// The node names that we intend to restart can be acquired from a set of nodes provided as input
	// within the VRestartNodesOptions struct
	NodeNamesToRestart []string
	// the hosts that we want to restart
	HostsToRestart []string
}

func VRestartNodesOptionsFactory() VRestartNodesOptions {
	opt := VRestartNodesOptions{}

	// set default values to the params
	opt.setDefaultValues()
	return opt
}

func (options *VRestartNodesOptions) setDefaultValues() {
	options.DatabaseOptions.SetDefaultValues()
}

func (options *VRestartNodesOptions) validateRequiredOptions() error {
	err := options.ValidateBaseOptions("restart_node")
	if err != nil {
		return err
	}
	if len(options.Nodes) == 0 {
		return fmt.Errorf("--restart option is required")
	}

	return nil
}

func (options *VRestartNodesOptions) validateParseOptions() error {
	return options.validateRequiredOptions()
}

// analyzeOptions will modify some options based on what is chosen
func (options *VRestartNodesOptions) analyzeOptions() (err error) {
	// we analyze host names when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
	}
	return nil
}

// ParseNodesList builds and returns a map from a comma-separated list of nodes.
// Ex: vnodeName1=host1,vnodeName2=host2 ---> map[string]string{vnodeName1: host1, vnodeName2: host2}
func (options *VRestartNodesOptions) ParseNodesList(nodeListStr string) error {
	nodes, err := util.ParseKeyValueListStr(nodeListStr, "restart")
	if err != nil {
		return err
	}
	options.Nodes = make(map[string]string)
	for k, v := range nodes {
		ip, err := util.ResolveToOneIP(v, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
		options.Nodes[k] = ip
	}
	return nil
}

func (options *VRestartNodesOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// VRestartNodes will restart the given nodes for a cluster that hasn't yet lost
// cluster quorum. This will handle updating of the nodes IP in the vertica
// catalog if necessary. Use VStartDatabase if cluster quorum is lost.
func (vcc *VClusterCommands) VRestartNodes(options *VRestartNodesOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// validate and analyze options
	err := options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	// get db name and hosts from config file and options
	dbName, hosts := options.GetNameAndHosts(options.Config)
	options.DBName = &dbName
	options.Hosts = hosts

	// set default value to StatePollingTimeout
	if options.StatePollingTimeout == 0 {
		options.StatePollingTimeout = util.DefaultStatePollingTimeout
	}

	// retrieve database information to execute the command so we do not always rely on some user input
	vdb := MakeVCoordinationDatabase()
	err = getVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return err
	}

	var hostsNoNeedToReIP []string
	hostNodeNameMap := make(map[string]string)
	restartNodeInfo := new(VRestartNodesInfo)
	for _, vnode := range vdb.HostNodeMap {
		hostNodeNameMap[vnode.Name] = vnode.Address
	}
	for nodename, newIP := range options.Nodes {
		oldIP, ok := hostNodeNameMap[nodename]
		if !ok {
			vlog.LogPrintError("fail to provide a non-existent node name %s", nodename)
			return fmt.Errorf("the node with the provided name %s does not exist", nodename)
		}
		// if the IP that is given is different than the IP in the catalog, a re-ip is necessary
		if oldIP != newIP {
			restartNodeInfo.ReIPList = append(restartNodeInfo.ReIPList, newIP)
			restartNodeInfo.NodeNamesToRestart = append(restartNodeInfo.NodeNamesToRestart, nodename)
			vlog.LogInfo("the node with the name %s needs to be re-IP %s", restartNodeInfo.NodeNamesToRestart, restartNodeInfo.ReIPList)
		} else {
			// otherwise, we don't need to re-ip
			hostsNoNeedToReIP = append(hostsNoNeedToReIP, newIP)
		}
	}

	// we can proceed to restart both nodes with and without IP changes
	restartNodeInfo.HostsToRestart = append(restartNodeInfo.HostsToRestart, restartNodeInfo.ReIPList...)
	restartNodeInfo.HostsToRestart = append(restartNodeInfo.HostsToRestart, hostsNoNeedToReIP...)

	// produce restart_node instructions
	instructions, err := vcc.produceRestartNodesInstructions(restartNodeInfo, options, &vdb)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %s", err)
		return err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to restart node, %s", err)
		return err
	}
	return nil
}

// produceRestartNodesInstructions will build a list of instructions to execute for
// the restart_node command.
//
// The generated instructions will later perform the following operations necessary
// for a successful restart_node:
//   - Check NMA connectivity
//   - Check Vertica versions
//   - Call network profile
//   - Call https re-ip endpoint
//   - Reload spread
//   - Get UP nodes through HTTPS call, if any node is UP then the DB is UP and ready for starting nodes
//   - Use any UP primary nodes as source host for syncing spread.conf and vertica.conf, source host can be picked
//     by a HTTPS /v1/nodes call for finding UP primary nodes
//   - Sync the confs to the nodes to be restarted
//   - Call https /v1/startup/command to get restart command of the nodes to be restarted
//   - restart nodes
//   - Poll node start up
//   - sync catalog
func (vcc *VClusterCommands) produceRestartNodesInstructions(restartNodeInfo *VRestartNodesInfo, options *VRestartNodesOptions,
	vdb *VCoordinationDatabase) ([]ClusterOp, error) {
	var instructions []ClusterOp

	nmaHealthOp := makeNMAHealthOp(options.Hosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(vcc.Log, options.Hosts, true)
	// need username for https operations
	err := options.SetUsePassword()
	if err != nil {
		return instructions, err
	}

	httpsGetUpNodesOp, err := makeHTTPSGetUpNodesOp(*options.DBName, options.Hosts,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&httpsGetUpNodesOp,
	)

	// If we identify any nodes that need re-IP, HostsToRestart will contain the nodes that need re-IP.
	// Otherwise, HostsToRestart will consist of all hosts with IPs recorded in the catalog, which are provided by user input.
	if len(restartNodeInfo.ReIPList) != 0 {
		nmaNetworkProfileOp := makeNMANetworkProfileOp(restartNodeInfo.ReIPList)
		httpsReIPOp, e := makeHTTPSReIPOp(restartNodeInfo.NodeNamesToRestart, restartNodeInfo.ReIPList,
			options.usePassword, *options.UserName, options.Password)
		if e != nil {
			return instructions, e
		}
		// host is set to nil value in the reload spread step
		// we use information from node information to find the up host later
		httpsReloadSpreadOp, e := makeHTTPSReloadSpreadOp(true, *options.UserName, options.Password)
		if e != nil {
			return instructions, e
		}
		// update new vdb information after re-ip
		httpsGetNodesInfoOp, e := makeHTTPSGetNodesInfoOp(*options.DBName, options.Hosts,
			options.usePassword, *options.UserName, options.Password, vdb)
		if err != nil {
			return instructions, e
		}
		instructions = append(instructions,
			&nmaNetworkProfileOp,
			&httpsReIPOp,
			&httpsReloadSpreadOp,
			&httpsGetNodesInfoOp,
		)
	}

	// The second parameter (sourceConfHost) in produceTransferConfigOps is set to a nil value in the upload and download step
	// we use information from v1/nodes endpoint to get all node information to update the sourceConfHost value
	// after we find any UP primary nodes as source host for syncing spread.conf and vertica.conf
	// we will remove the nil parameters in VER-88401 by adding them in execContext
	produceTransferConfigOps(&instructions,
		nil, /*source hosts for transferring configuration files*/
		restartNodeInfo.HostsToRestart,
		vdb)

	httpsRestartUpCommandOp, err := makeHTTPSStartUpCommandOp(options.usePassword, *options.UserName, options.Password, vdb)
	if err != nil {
		return instructions, err
	}
	nmaRestartNewNodesOp := makeNMAStartNodeOpWithVDB(restartNodeInfo.HostsToRestart, vdb)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOpWithTimeoutAndCommand(restartNodeInfo.HostsToRestart,
		options.usePassword, *options.UserName, options.Password, options.StatePollingTimeout, RestartNodeCmd)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsRestartUpCommandOp,
		&nmaRestartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(options.Hosts, true, *options.UserName, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}
