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

// VAddNodeOptions are the option arguments for the VAddNode API
type VAddNodeOptions struct {
	DatabaseOptions
	// Hosts to add to database
	NewHosts []string
	// A map of VCoordinationNode built from new hosts
	NewHostNodeMap map[string]VCoordinationNode
	// Name of the subcluster that the new nodes will be added to
	SCName *string
	// A set of nodes(vnode - host) in the database
	Nodes map[string]string
	// A (must be up) host that will be used to execute
	// add_node operations.
	InputHost string
	DepotSize *string // like 10G
	// Skip rebalance shards if true
	SkipRebalanceShards *bool
}

func VAddNodeOptionsFactory() VAddNodeOptions {
	opt := VAddNodeOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (o *VAddNodeOptions) SetDefaultValues() {
	o.DatabaseOptions.SetDefaultValues()

	o.SCName = new(string)
	o.NewHostNodeMap = make(map[string]VCoordinationNode)
	o.SkipRebalanceShards = new(bool)
	// true by default for the operator
	*o.SkipRebalanceShards = true
	o.DepotSize = new(string)
}

func (o *VAddNodeOptions) validateRequiredOptions() error {
	err := o.ValidateBaseOptions("db_add_node")
	if err != nil {
		return err
	}
	if len(o.NewHosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}
	return nil
}

func (o *VAddNodeOptions) validateEonOptions(config *ClusterConfig) error {
	if !o.IsEonMode(config) {
		return nil
	}

	if *o.SCName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}

	if *o.HonorUserInput {
		err := util.ValidateRequiredAbsPath(o.DepotPrefix, "depot path")
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *VAddNodeOptions) validateExtraOptions() error {
	if !*o.HonorUserInput {
		return nil
	}
	if len(o.Nodes) == 0 {
		return fmt.Errorf("must specify a non-empty set of nodes(vnode - host)")
	}
	// catalog prefix path
	err := util.ValidateRequiredAbsPath(o.CatalogPrefix, "catalog path")
	if err != nil {
		return err
	}
	// data prefix
	err = util.ValidateRequiredAbsPath(o.DataPrefix, "data path")
	return err
}

// ParseNodeList builds and returns a map from a comma-separated list of nodes.
// Ex: vnodeName1=host1,vnodeName2=host2 ---> map[string]string{vnodeName1: host1, vnodeName2: host2}
func (o *VAddNodeOptions) ParseNodeList(nodeListStr string) error {
	nodes, err := util.ParseKeyValueListStr(nodeListStr, "vnodes")
	if err != nil {
		return err
	}
	o.Nodes = make(map[string]string)
	for k, v := range nodes {
		ip, err := util.ResolveToOneIP(v, o.Ipv6.ToBool())
		if err != nil {
			return err
		}
		o.Nodes[k] = ip
		// We also get the existing hosts ip
		o.Hosts = append(o.Hosts, ip)
	}
	return nil
}

func (o *VAddNodeOptions) validateParseOptions(config *ClusterConfig) error {
	// batch 1: validate required parameters
	err := o.validateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = o.validateEonOptions(config)
	if err != nil {
		return err
	}
	// batch 3: validate all other params
	err = o.validateExtraOptions()
	return err
}

// analyzeOptions will modify some options based on what is chosen
func (o *VAddNodeOptions) analyzeOptions() (err error) {
	o.NewHosts, err = util.ResolveRawHostsToAddresses(o.NewHosts, o.Ipv6.ToBool())
	if err != nil {
		return err
	}

	if o.InputHost != "" {
		o.InputHost, err = util.ResolveToOneIP(o.InputHost, o.Ipv6.ToBool())
		if err != nil {
			return err
		}
	}

	o.normalizePaths()
	return nil
}

// ParseNewHostList converts the string list of hosts, to add, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
func (o *VAddNodeOptions) ParseNewHostList(hosts string) error {
	inputHostList, err := util.SplitHosts(hosts)
	if err != nil {
		return err
	}

	o.NewHosts = inputHostList
	return nil
}

func (o *VAddNodeOptions) validateAnalyzeOptions(config *ClusterConfig) error {
	err := o.validateParseOptions(config)
	if err != nil {
		return err
	}

	err = o.analyzeOptions()
	if err != nil {
		return err
	}

	return o.SetUsePassword()
}

// VAddNode is the top-level API for adding node(s) to an existing database.
func (vcc *VClusterCommands) VAddNode(options *VAddNodeOptions) (VCoordinationDatabase, error) {
	vdb := MakeVCoordinationDatabase()

	// get config from vertica_cluster.yaml
	clusterConfig, err := options.GetDBConfig()
	if err != nil {
		return vdb, err
	}

	if clusterConfig == nil && !*options.HonorUserInput {
		return vdb, fmt.Errorf("could not find %s, aborting add node", ConfigFileName)
	}

	err = options.validateAnalyzeOptions(clusterConfig)
	if err != nil {
		return vdb, err
	}

	err = vdb.SetVCDatabaseForAddNode(options, clusterConfig)
	if err != nil {
		return vdb, err
	}

	instructions, err := produceAddNodeInstructions(&vdb, options)
	if err != nil {
		vlog.LogPrintError("fail to produce add node instructions, %s", err)
		return vdb, err
	}

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.Run(); runError != nil {
		vlog.LogPrintError("fail to complete add node operation, %s", runError)
		return vdb, runError
	}
	return vdb, nil
}

// produceAddNodeInstructions will build a list of instructions to execute for
// the add node operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful add_node:
//   - Check NMA connectivity
//   - Check NMA versions
//   - Check if the DB exists
//   - Check that none of the hosts already exists in the db
//   - If we have subcluster in the input, check if the subcluster exists. If not, we stop.
//     If we do not have a subcluster in the input, fetch the current default subcluster name
//   - Prepare directories
//   - Get network profiles
//   - Create the new node
//   - Reload spread
//   - Transfer config files to the new node
//   - Start the new node
//   - Poll node startup
//   - Create depot on the new node (Eon mode only)
//   - Sync catalog
//   - Rebalance shards on subcluster (Eon mode only)
func produceAddNodeInstructions(vdb *VCoordinationDatabase,
	options *VAddNodeOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	inputHost, newNodeHosts, allHosts := getAddNodeHosts(vdb, options)
	usePassword := options.usePassword
	username := *options.UserName

	nmaHealthOp := makeNMAHealthOp(allHosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(allHosts, true)

	httpCheckNodesExistOp, err := makeHTTPCheckNodesExistOp(inputHost,
		newNodeHosts, usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&httpCheckNodesExistOp)
	if vdb.IsEon {
		httpsFindSubclusterOrDefaultOp, e := makeHTTPSFindSubclusterOrDefaultOp(
			inputHost, usePassword, username, options.Password, *options.SCName)
		if e != nil {
			return instructions, e
		}
		instructions = append(instructions, &httpsFindSubclusterOrDefaultOp)
	}
	nmaPrepareDirectoriesOp, err := makeNMAPrepareDirectoriesOp(options.NewHostNodeMap)
	if err != nil {
		return instructions, err
	}
	nmaNetworkProfileOp := makeNMANetworkProfileOp(allHosts)
	httpsCreateNodeOp, err := makeHTTPSCreateNodeOp(newNodeHosts, inputHost,
		usePassword, *options.UserName, options.Password, vdb, *options.SCName)
	if err != nil {
		return instructions, err
	}
	httpsReloadSpreadOp, err := makeHTTPSReloadSpreadOp(inputHost, true, username, options.Password)
	if err != nil {
		return instructions, err
	}

	nmaReadCatalogEditorOp, err := makeNMAReadCatalogEditorOp(inputHost, vdb)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&httpsCreateNodeOp,
		&httpsReloadSpreadOp,
		&nmaReadCatalogEditorOp,
	)

	// we will remove the nil parameters in VER-88401 by adding them in execContext
	produceTransferConfigOps(&instructions,
		inputHost,
		nil, /*all existing nodes*/
		newNodeHosts,
		nil /*db configurations retrieved from a running db*/)
	nmaStartNewNodesOp := makeNMAStartNodeOp(newNodeHosts)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOp(allHosts, usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaStartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	return prepareAdditionalEonInstructions(vdb, options, instructions,
		username, usePassword, inputHost, newNodeHosts)
}

func getAddNodeHosts(vdb *VCoordinationDatabase,
	options *VAddNodeOptions) (inputHost, newNodeHosts, allHosts []string) {
	if options.InputHost != "" {
		// If the user specified an input host, we use it to execute the ops.
		// It must be up.
		inputHost = append(inputHost, options.InputHost)
	} else {
		// If an input host is not specified we use one from user/config host list.
		// There is already a Jira(VER-88096) to improve how we get all nodes
		// information of a running database.
		inputHost = append(inputHost, util.SliceDiff(vdb.HostList, options.NewHosts)[0])
	}

	newNodeHosts = options.NewHosts

	// Some operations need all of the new hosts, plus the initiator host.
	// allHosts includes them all.
	allHosts = inputHost
	allHosts = append(allHosts, newNodeHosts...)

	return inputHost, newNodeHosts, allHosts
}

func prepareAdditionalEonInstructions(vdb *VCoordinationDatabase,
	options *VAddNodeOptions,
	instructions []ClusterOp,
	username string, usePassword bool,
	inputHost, newNodeHosts []string) ([]ClusterOp, error) {
	if vdb.UseDepot {
		httpsCreateNodesDepotOp, err := makeHTTPSCreateNodesDepotOp(vdb,
			newNodeHosts, usePassword, username, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsCreateNodesDepotOp)
	}
	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(inputHost, true, username, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
		if !*options.SkipRebalanceShards {
			httpsRBSCShardsOp, err := makeHTTPSRebalanceSubclusterShardsOp(
				inputHost, usePassword, username, options.Password, *options.SCName)
			if err != nil {
				return instructions, err
			}
			instructions = append(instructions, &httpsRBSCShardsOp)
		}
	}

	return instructions, nil
}
