package vclusterops

import (
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
)

// Normal strings are easier and safer to use in Go.
type VRestartNodesOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: hidden info
	UsePassword       bool
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

func (options *VRestartNodesOptions) validateParseOptions(config *ClusterConfig) error {
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

func (options *VRestartNodesOptions) ValidateAnalyzeOptions(config *ClusterConfig) error {
	if err := options.validateParseOptions(config); err != nil {
		return err
	}
	if err := options.analyzeOptions(); err != nil {
		return err
	}
	return nil
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
	err = options.ValidateAnalyzeOptions(config)
	if err != nil {
		return err
	}

	// produce restart_node instructions
	instructions, err := produceRestartNodesInstructions(options)
	if err != nil {
		err = fmt.Errorf("fail to production instructions: %w", err)
		return err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	err = clusterOpEngine.Run()
	if err != nil {
		err = fmt.Errorf("fail to restart node: %w", err)
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

	nmaHealthOp := MakeNMAHealthOp(options.Hosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp(options.Hosts, true)
	// need username for https operations
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.ValidateUserName()
		if err != nil {
			return instructions, err
		}
	}

	httpsGetUpNodesOp := MakeHTTPSGetUpNodesOp("HTTPSGetUpNodesOp", *options.Name, options.Hosts,
		usePassword, *options.UserName, options.Password)

	httpsGetNodesInfoOp := makeHTTPSGetNodesInfoOp(options.Hosts,
		usePassword, *options.UserName, options.Password)

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

	// restartUpCommandFileContent stores Vertica restart command information
	// that can be used to restart a specific node.
	var restartUpCommandFileContent string
	HTTPSRestartUpCommandOp := makeHTTPSRestartUpCommandOp(usePassword, *options.UserName, options.Password, &restartUpCommandFileContent)
	nmaRestartNewNodesOp := makeNMAStartNodeOp(options.HostsToRestart, &restartUpCommandFileContent)
	instructions = append(instructions,
		&HTTPSRestartUpCommandOp,
		&nmaRestartNewNodesOp,
	)

	httpsPollNodeStateOp := MakeHTTPSPollNodeStateOp(options.HostsToRestart,
		usePassword, *options.UserName, options.Password)

	// clusterFileContent stores cluster information that can determine whether this is an EON database
	// by invoking the /cluster endpoint.
	var clusterFileContent string
	httpsGetClusterInfoOp := makeHTTPSGetClusterInfoOp(options.Hosts,
		usePassword, *options.UserName, options.Password, &clusterFileContent)
	httpsSyncCatalogOp := MakeHTTPSSyncCatalogOp(options.Hosts, true, *options.UserName, options.Password, &clusterFileContent)
	instructions = append(instructions,
		&httpsPollNodeStateOp,
		&httpsGetClusterInfoOp,
		&httpsSyncCatalogOp)

	return instructions, nil
}
