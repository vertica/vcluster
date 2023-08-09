package vclusterops

import (
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// Normal strings are easier and safer to use in Go.
type VRestartNodesOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: hidden info
	HostsToRestart    []string // the hosts that we want to restart
	RawHostsToRestart []string // expected to be IP addresses or hostnames to restart
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
	if len(options.RawHostsToRestart) == 0 {
		return fmt.Errorf("must specify a host or host list")
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

func (options *VRestartNodesOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	err := options.analyzeOptions()
	return err
}

// PrepareRestartNodes invokes /cluster endpoint to determine whether this is an EON database
func VPrepareRestartNodes(options *VRestartNodesOptions) (bool, error) {
	config, err := options.GetDBConfig()
	if err != nil {
		return false, err
	}

	// get db name and hosts from config file and options
	dbName, hosts := options.GetNameAndHosts(config)
	options.Name = &dbName
	options.Hosts = hosts

	// validate and analyze options
	err = options.ValidateAnalyzeOptions()
	if err != nil {
		return false, err
	}

	instructions, err := producePreRestartNodesInstructions(options)
	if err != nil {
		vlog.LogPrintError("fail to produce restart_node pre-instructions, %s", err)
		return false, err
	}

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to pre-restart node, %s", err)
		return false, err
	}
	if clusterOpEngine.execContext.isEon {
		vlog.LogPrintInfo("The database is eon")
	}
	return clusterOpEngine.execContext.isEon, nil
}

func producePreRestartNodesInstructions(options *VRestartNodesOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp
	err := options.SetUsePassword()
	if err != nil {
		return instructions, err
	}
	// invoking the /cluster endpoint.
	httpsGetClusterInfoOp, err := makeHTTPSGetClusterInfoOp(options.Hosts,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &httpsGetClusterInfoOp)
	return instructions, nil
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

	// load vdb info from the YAML config file
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig()
	if err != nil {
		return err
	}

	// get db name and hosts from config file and options
	dbName, hosts := options.GetNameAndHosts(config)
	options.Name = &dbName
	options.Hosts = hosts

	// validate and analyze options
	err = options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	// produce restart_node instructions
	instructions, err := produceRestartNodesInstructions(options)
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
//   - Get UP nodes through HTTPS call, if any node is UP then the DB is UP and ready for starting nodes
//   - Use any UP primary nodes as source host for syncing spread.conf and vertica.conf, source host can be picked
//     by a HTTPS /v1/nodes call for finding UP primary nodes
//   - Sync the confs to the to the nodes to be restarted
//   - Call https /v1/startup/command to get restart command of the nodes to be restarted
//   - restart nodes
//   - Poll node start up
//   - sync catalog
func produceRestartNodesInstructions(options *VRestartNodesOptions) ([]ClusterOp, error) {
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
	httpsGetNodesInfoOp, err := makeHTTPSGetNodesInfoOp(options.Hosts,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&httpsGetUpNodesOp,
		&httpsGetNodesInfoOp,
	)

	// The second parameter (sourceConfHost) in produceTransferConfigOps is set to a nil value in the upload and download step
	// we use information from v1/nodes endpoint to get all node information to update the sourceConfHost value
	// after we find any UP primary nodes as source host for syncing spread.conf and vertica.conf
	produceTransferConfigOps(&instructions, nil, options.Hosts, options.HostsToRestart)

	HTTPSRestartUpCommandOp, err := makeHTTPSRestartUpCommandOp(options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}
	nmaRestartNewNodesOp := makeNMAStartNodeOp(options.HostsToRestart)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOp(options.HostsToRestart,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&HTTPSRestartUpCommandOp,
		&nmaRestartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	if options.IsEon.ToBool() {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(options.Hosts, true, *options.UserName, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}
