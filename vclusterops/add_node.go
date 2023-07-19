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
	"github.com/vertica/vcluster/vclusterops/vstruct"
)

// VAddNodeOptions are the option arguments for the VAddNode API
type VAddNodeOptions struct {
	DatabaseOptions
	// Hosts to add to database
	NewHosts []string
	// Hostname or IP of an existing node in the cluster that is known to be UP
	InitiatorHost *string
	// A map of VCoordinationNode built from new hosts
	NewHostNodeMap map[string]VCoordinationNode
	// Name of the subcluster that the new nodes will be added to
	SCName *string
	// A set of nodes(vnode - host) in the database
	Nodes              map[string]string
	SkipStartupPolling vstruct.NullableBool
	DepotSize          *string // like 10G
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
	o.InitiatorHost = new(string)
	o.SkipStartupPolling = vstruct.NotSet
	o.NewHostNodeMap = make(map[string]VCoordinationNode)
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
	err := util.ValidateName(*o.SCName, "subcluster")
	if err != nil {
		return err
	}
	if *o.HonorUserInput {
		err = util.ValidateRequiredAbsPath(o.DepotPrefix, "depot path")
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
	if len(o.Nodes) != len(o.RawHosts) {
		return fmt.Errorf("number of VNODE=HOST pairs should match the number of hosts")
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
func (o *VAddNodeOptions) analyzeOptions(config *ClusterConfig) error {
	o.setInitiatorFromConfig(config)
	err := o.resolveAllHosts()
	if err != nil {
		return err
	}

	// process correct catalog path, data path and depot path prefixes
	*o.CatalogPrefix = util.GetCleanPath(*o.CatalogPrefix)
	*o.DataPrefix = util.GetCleanPath(*o.DataPrefix)
	*o.DepotPrefix = util.GetCleanPath(*o.DepotPrefix)
	return nil
}

// setInitiatorFromConfig will set the initiator host, to the first of the hosts
// in the config file, if needed.
func (o *VAddNodeOptions) setInitiatorFromConfig(config *ClusterConfig) {
	if config == nil {
		return
	}
	o.InitiatorHost = &config.Hosts[0]
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

// resolveHosts resolve all hosts(new hosts and existing hosts) to IP addresses
func (o *VAddNodeOptions) resolveAllHosts() (err error) {
	ipv6 := o.Ipv6.ToBool()
	// we analyze hostnames when HonorUserInput is set, otherwise we use hosts in yaml config
	if *o.HonorUserInput {
		// resolve RawHosts to be IP addresses
		o.Hosts, err = util.ResolveRawHostsToAddresses(o.RawHosts, ipv6)
		if err != nil {
			return err
		}
		*o.InitiatorHost = o.Hosts[0]
	}
	// resolve NewHosts to be IP addresses
	o.NewHosts, err = util.ResolveRawHostsToAddresses(o.NewHosts, ipv6)
	return err
}

func (o *VAddNodeOptions) validateAnalyzeOptions(config *ClusterConfig) error {
	if err := o.validateParseOptions(config); err != nil {
		return err
	}
	err := o.analyzeOptions(config)
	return err
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
		vlog.LogPrintError("fail to produce add node instructions, %w", err)
		return vdb, err
	}

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	if runError := clusterOpEngine.Run(); runError != nil {
		vlog.LogPrintError("fail to complete add node operation, %w", runError)
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
//   - Prepare directories
//   - Get network profiles
//   - Create the new node
//   - Reload spread
//   - Transfer config files to the new node
//   - Start the new node
//   - Poll node startup
//   - Create depot on the new node (Eon mode only)
//   - Sync catalog
func produceAddNodeInstructions(vdb *VCoordinationDatabase, options *VAddNodeOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	initiatorHost := []string{
		*options.InitiatorHost,
	}
	newNodeHosts := options.NewHosts
	// Some operations need all of the new hosts, plus the initiator host.
	// allHosts includes them all.
	allHosts := initiatorHost
	allHosts = append(allHosts, newNodeHosts...)

	nmaHealthOp := MakeNMAHealthOp("NMAHealthOp", allHosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp("NMAVerticaVersionOp", allHosts, true)

	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.ValidateUserName()
		if err != nil {
			return instructions, err
		}
	}
	username := *options.UserName
	httpsGetUpNodesOp := MakeHTTPSGetUpNodesOp("HTTPSGetUpNodesOp", *options.Name, initiatorHost,
		usePassword, username, options.Password)

	nmaPrepareDirectoriesOp, err := MakeNMAPrepareDirectoriesOp("NMAPrepareDirectoriesOp", options.NewHostNodeMap)
	if err != nil {
		return instructions, err
	}

	nmaNetworkProfileOp := MakeNMANetworkProfileOp("NMANetworkProfileOp", allHosts)

	httpCreateNodeOp := MakeHTTPCreateNodeOp("HTTPCreateNodeOp", newNodeHosts, initiatorHost,
		true, *options.UserName, options.Password, vdb)

	httpsReloadSpreadOp := MakeHTTPSReloadSpreadOp("HTTPSReloadSpreadOp", initiatorHost, true, username, options.Password)

	nmaFetchVdbFromCatEdOp, err := MakeNMAFetchVdbFromCatalogEditorOp(
		"NMAFetchVdbFromCatalogEditorOp", vdb.HostNodeMap, initiatorHost)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&httpsGetUpNodesOp,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&httpCreateNodeOp,
		&httpsReloadSpreadOp,
		&nmaFetchVdbFromCatEdOp,
	)

	produceTransferConfigOps(&instructions, initiatorHost, nil, newNodeHosts, make(map[string]string))
	nmaStartNewNodesOp := MakeNMAStartNodeOp("NMAStartNodeOp", newNodeHosts)
	httpsPollNodeStateOp := MakeHTTPSPollNodeStateOp("HTTPSPollNodeStateOp", allHosts, true, username, options.Password)
	instructions = append(instructions,
		&nmaStartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	if vdb.UseDepot {
		httpsCreateDepotOp := MakeHTTPSCreateDepotOp("HTTPSCreateDepotOp", vdb, initiatorHost, true, username, options.Password)
		instructions = append(instructions, &httpsCreateDepotOp)
	}

	if vdb.IsEon {
		httpsSyncCatalogOp := MakeHTTPSSyncCatalogOp("HTTPSyncCatalogOp", initiatorHost, true, username, options.Password)
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}
