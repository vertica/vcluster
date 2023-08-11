package vclusterops

import (
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// Normal strings are easier and safer to use in Go.
type VRestartNodesOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: hidden info
	NodeNamesToRestart []string // the node names that we want to restart
	HostsToRestart     []string // the hosts that we want to restart
	RawHostsToRestart  []string // expected to be IP addresses or hostnames to restart
	IPList             []string // the ip address that we want to re-ip
	RawIPList          []string // expected to be IP addresses or hostnames to re-ip
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

	return nil
}

func (options *VRestartNodesOptions) validateParseOptions() error {
	return options.validateRequiredOptions()
}

// analyzeOptions will modify some options based on what is chosen
func (options *VRestartNodesOptions) analyzeOptions() (err error) {
	options.HostsToRestart, err = util.ResolveRawHostsToAddresses(options.RawHostsToRestart, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	options.IPList, err = util.ResolveRawHostsToAddresses(options.RawIPList, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

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

// ParseNodeNamesListToRestart converts the string list of node names, to restart, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
func (options *VRestartNodesOptions) ParseNodeNamesListToRestart(nodeNames string) error {
	nodeNamesToRestart, err := util.SplitHosts(nodeNames)
	if err != nil {
		return err
	}

	options.NodeNamesToRestart = nodeNamesToRestart
	return nil
}

// ParseHostListToRestart converts the string list of hosts, to restart, into a slice of strings.
// The hosts should be separated by comma, and will be converted to lower case.
func (options *VRestartNodesOptions) ParseHostListToRestart(hosts string) error {
	hostsToRestart, err := util.SplitHosts(hosts)
	if err != nil {
		return err
	}

	options.RawHostsToRestart = hostsToRestart
	return nil
}

// ParseIpsToRestart converts the string list of Ips, to re-ip, into a slice of strings.
// The ips should be separated by comma, and will be converted to lower case.
func (options *VRestartNodesOptions) ParseIpsToRestart(ips string) error {
	ipList, err := util.SplitHosts(ips)
	if err != nil {
		return err
	}

	options.RawIPList = ipList
	return nil
}

func (options *VRestartNodesOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	err := options.analyzeOptions()
	return err
}

// GetNodeNames can choose the right node names from user input and config file
func (options *VRestartNodesOptions) getNodeNames(config *ClusterConfig) []string {
	var nodeNames []string
	if config == nil {
		// when config file is not available, we use node names option from the user input
		if len(options.NodeNamesToRestart) > 0 && *options.HonorUserInput {
			nodeNames = options.NodeNamesToRestart
		}
	} else {
		// Use node names option in the config file
		if len(options.NodeNamesToRestart) > 0 {
			return options.NodeNamesToRestart
		}
		// otherwise, use vnodes option in the config file
		nodeNamesHostMap := make(map[string]string)
		for _, node := range config.Nodes {
			nodeNamesHostMap[node.Address] = node.Name
		}
		for _, host := range options.HostsToRestart {
			nodeName, ok := nodeNamesHostMap[host]
			if ok {
				nodeNames = append(nodeNames, nodeName)
			}
		}
	}
	return nodeNames
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

	// TODO: library users won't have vertica_cluster.yaml, remove GetDBConfig() when VER-88442 is closed.
	// load vdb info from the YAML config file
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig()
	if err != nil {
		return err
	}

	// validate and analyze options
	err = options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	// get db name and hosts from config file and options
	dbName, hosts := options.GetNameAndHosts(config)
	options.Name = &dbName
	options.Hosts = hosts
	// get node name from config file and options
	nodeNames := options.getNodeNames(config)
	options.NodeNamesToRestart = nodeNames

	// retrieve database information to execute the command so we do not always rely on some user input
	vdb := MakeVCoordinationDatabase()
	err = GetVDBFromRunningDB(&vdb, &options.DatabaseOptions)
	if err != nil {
		return err
	}

	// produce restart_node instructions
	instructions, err := produceRestartNodesInstructions(options, &vdb)
	if err != nil {
		vlog.LogPrintError("fail to production instructions, %s", err)
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
//   - Sync the confs to the to the nodes to be restarted
//   - Call https /v1/startup/command to get restart command of the nodes to be restarted
//   - restart nodes
//   - Poll node start up
//   - sync catalog
func produceRestartNodesInstructions(options *VRestartNodesOptions, vdb *VCoordinationDatabase) ([]ClusterOp, error) {
	var instructions []ClusterOp

	nmaHealthOp := makeNMAHealthOp(options.Hosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(options.Hosts, true)
	// need username for https operations
	err := options.SetUsePassword()
	if err != nil {
		return instructions, err
	}

	httpsGetUpNodesOp, err := makeHTTPSGetUpNodesOp(*options.Name, options.Hosts,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&httpsGetUpNodesOp,
	)

	if options.IPList != nil {
		nmaNetworkProfileOp := makeNMANetworkProfileOp(options.IPList)
		httpsReIPOp, e := makeHTTPSReIPOp(options.NodeNamesToRestart, options.IPList,
			options.usePassword, *options.UserName, options.Password)
		if e != nil {
			return instructions, e
		}
		// host is set to nil value in the reload spread step
		// we use information from node information to find the up host later
		httpsReloadSpreadOp, e := makeHTTPSReloadSpreadOp(nil /*hosts*/, true, *options.UserName, options.Password)
		if e != nil {
			return instructions, e
		}
		// update new vdb information after re-ip
		httpsGetNodesInfoOp, e := makeHTTPSGetNodesInfoOp(*options.Name, options.Hosts,
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
		options.Hosts,
		options.NodeNamesToRestart,
		vdb)

	httpsRestartUpCommandOp, err := makeHTTPSRestartUpCommandOp(options.usePassword, *options.UserName, options.Password, vdb)
	if err != nil {
		return instructions, err
	}
	nmaRestartNewNodesOp := makeNMAStartNodeOpWithVDB(options.NodeNamesToRestart, vdb)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOp(options.Hosts,
		options.usePassword, *options.UserName, options.Password)
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
