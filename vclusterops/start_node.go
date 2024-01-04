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

// VStartNodesOptions represents the available options when you start one or more nodes
// with VStartNodes.
type VStartNodesOptions struct {
	// basic db info
	DatabaseOptions
	// A set of nodes(nodename - host) that we want to start in the database
	Nodes map[string]string
	// timeout for polling nodes that we want to start in httpsPollNodeStateOp
	StatePollingTimeout int
	// If the path is set, the NMA will store the Vertica start command at the path
	// instead of executing it. This is useful in containerized environments where
	// you may not want to have both the NMA and Vertica server in the same container.
	// This feature requires version 24.2.0+.
	StartUpConf *string
}

type VStartNodesInfo struct {
	// The IP address that we intend to re-IP can be obtained from a set of nodes provided as input
	// within VStartNodesOptions struct
	ReIPList []string
	// The node names that we intend to start can be acquired from a set of nodes provided as input
	// within the VStartNodesOptions struct
	NodeNamesToStart []string
	// the hosts that we want to start
	HostsToStart []string
}

func VStartNodesOptionsFactory() VStartNodesOptions {
	opt := VStartNodesOptions{}

	// set default values to the params
	opt.setDefaultValues()
	return opt
}

func (options *VStartNodesOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()
	options.StartUpConf = new(string)
}

func (options *VStartNodesOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions("restart_node", logger)
	if err != nil {
		return err
	}
	if len(options.Nodes) == 0 {
		return fmt.Errorf("--restart option is required")
	}

	return nil
}

func (options *VStartNodesOptions) validateParseOptions(logger vlog.Printer) error {
	return options.validateRequiredOptions(logger)
}

// analyzeOptions will modify some options based on what is chosen
func (options *VStartNodesOptions) analyzeOptions() (err error) {
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

// ParseNodesList builds and returns a map of nodes from a comma-separated list of nodes.
// For example, vnodeName1=host1,vnodeName2=host2 is converted to map[string]string{vnodeName1: host1, vnodeName2: host2}
func (options *VStartNodesOptions) ParseNodesList(nodeListStr string) error {
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

func (options *VStartNodesOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := options.validateParseOptions(logger); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// VStartNodes starts the given nodes for a cluster that has not yet lost
// cluster quorum. Returns any error encountered. If necessary, it updates the
// node's IP in the Vertica catalog. If cluster quorum is already lost, use
// VStartDatabase. It will skip any nodes given that no longer exist in the
// catalog.
func (vcc *VClusterCommands) VStartNodes(options *VStartNodesOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// validate and analyze options
	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return err
	}

	// get db name and hosts from config file and options
	dbName, hosts, err := options.getNameAndHosts(options.Config)
	if err != nil {
		return err
	}

	options.DBName = &dbName
	options.Hosts = hosts

	// set default value to StatePollingTimeout
	if options.StatePollingTimeout == 0 {
		options.StatePollingTimeout = util.DefaultStatePollingTimeout
	}

	// retrieve database information to execute the command so we do not always rely on some user input
	vdb := makeVCoordinationDatabase()
	err = vcc.getVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return err
	}

	var hostsNoNeedToReIP []string
	hostNodeNameMap := make(map[string]string)
	restartNodeInfo := new(VStartNodesInfo)
	for _, vnode := range vdb.HostNodeMap {
		hostNodeNameMap[vnode.Name] = vnode.Address
	}
	for nodename, newIP := range options.Nodes {
		oldIP, ok := hostNodeNameMap[nodename]
		if !ok {
			// We can get here if the caller requests a node that we were in the
			// middle of removing. Log a warning and continue without starting
			// that node.
			vcc.Log.Info("skipping start of node that doesn't exist in the catalog",
				"nodename", nodename, "newIP", newIP)
			continue
		}
		// if the IP that is given is different than the IP in the catalog, a re-ip is necessary
		if oldIP != newIP {
			restartNodeInfo.ReIPList = append(restartNodeInfo.ReIPList, newIP)
			restartNodeInfo.NodeNamesToStart = append(restartNodeInfo.NodeNamesToStart, nodename)
			vcc.Log.Info("the nodes need to be re-IP", "nodeNames", restartNodeInfo.NodeNamesToStart, "IPs", restartNodeInfo.ReIPList)
		} else {
			// otherwise, we don't need to re-ip
			hostsNoNeedToReIP = append(hostsNoNeedToReIP, newIP)
		}
	}

	// we can proceed to restart both nodes with and without IP changes
	restartNodeInfo.HostsToStart = append(restartNodeInfo.HostsToStart, restartNodeInfo.ReIPList...)
	restartNodeInfo.HostsToStart = append(restartNodeInfo.HostsToStart, hostsNoNeedToReIP...)

	// If no nodes found to start. We can simply exit here. This can happen if
	// given a list of nodes that aren't in the catalog any longer.
	if len(restartNodeInfo.HostsToStart) == 0 {
		vcc.Log.Info("None of the nodes provided are in the catalog. There is nothing to start.")
		return nil
	}

	// produce restart_node instructions
	instructions, err := vcc.produceStartNodesInstructions(restartNodeInfo, options, &vdb)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		return fmt.Errorf("fail to restart node, %w", err)
	}
	return nil
}

// produceStartNodesInstructions will build a list of instructions to execute for
// the restart_node command.
//
// The generated instructions will later perform the following operations necessary
// for a successful restart_node:
//   - Check NMA connectivity
//   - Get UP nodes through HTTPS call, if any node is UP then the DB is UP and ready for starting nodes
//   - If need to do re-ip:
//     1. Call network profile
//     2. Call https re-ip endpoint
//     3. Reload spread
//     4. Call https /v1/nodes to update nodes' info
//   - Check Vertica versions
//   - Use any UP primary nodes as source host for syncing spread.conf and vertica.conf
//   - Sync the confs to the nodes to be restarted
//   - Call https /v1/startup/command to get restart command of the nodes to be restarted
//   - restart nodes
//   - Poll node start up
//   - sync catalog
func (vcc *VClusterCommands) produceStartNodesInstructions(startNodeInfo *VStartNodesInfo, options *VStartNodesOptions,
	vdb *VCoordinationDatabase) ([]clusterOp, error) {
	var instructions []clusterOp

	nmaHealthOp := makeNMAHealthOp(vcc.Log, options.Hosts)
	// need username for https operations
	err := options.setUsePassword(vcc.Log)
	if err != nil {
		return instructions, err
	}

	httpsGetUpNodesOp, err := makeHTTPSGetUpNodesOp(vcc.Log, *options.DBName, options.Hosts,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&httpsGetUpNodesOp,
	)

	// If we identify any nodes that need re-IP, HostsToRestart will contain the nodes that need re-IP.
	// Otherwise, HostsToRestart will consist of all hosts with IPs recorded in the catalog, which are provided by user input.
	if len(startNodeInfo.ReIPList) != 0 {
		nmaNetworkProfileOp := makeNMANetworkProfileOp(vcc.Log, startNodeInfo.ReIPList)
		httpsReIPOp, e := makeHTTPSReIPOp(startNodeInfo.NodeNamesToStart, startNodeInfo.ReIPList,
			options.usePassword, *options.UserName, options.Password)
		if e != nil {
			return instructions, e
		}
		// host is set to nil value in the reload spread step
		// we use information from node information to find the up host later
		httpsReloadSpreadOp, e := makeHTTPSReloadSpreadOp(vcc.Log, true, *options.UserName, options.Password)
		if e != nil {
			return instructions, e
		}
		// update new vdb information after re-ip
		httpsGetNodesInfoOp, e := makeHTTPSGetNodesInfoOp(vcc.Log, *options.DBName, options.Hosts,
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

	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOpWithVDB(vcc.Log, true /*hosts need to have the same Vertica version*/, vdb)
	instructions = append(instructions, &nmaVerticaVersionOp)

	// The second parameter (sourceConfHost) in produceTransferConfigOps is set to a nil value in the upload and download step
	// we use information from v1/nodes endpoint to get all node information to update the sourceConfHost value
	// after we find any UP primary nodes as source host for syncing spread.conf and vertica.conf
	// we will remove the nil parameters in VER-88401 by adding them in execContext
	produceTransferConfigOps(vcc.Log,
		&instructions,
		nil, /*source hosts for transferring configuration files*/
		startNodeInfo.HostsToStart,
		vdb)

	httpsRestartUpCommandOp, err := makeHTTPSStartUpCommandOp(vcc.Log, options.usePassword, *options.UserName, options.Password, vdb)
	if err != nil {
		return instructions, err
	}
	nmaRestartNewNodesOp := makeNMAStartNodeOpWithVDB(vcc.Log, startNodeInfo.HostsToStart, *options.StartUpConf, vdb)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOpWithTimeoutAndCommand(vcc.Log, startNodeInfo.HostsToStart,
		options.usePassword, *options.UserName, options.Password, options.StatePollingTimeout, StartNodeCmd)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsRestartUpCommandOp,
		&nmaRestartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(vcc.Log, options.Hosts, true, *options.UserName, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}
