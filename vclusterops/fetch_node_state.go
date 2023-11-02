package vclusterops

import (
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
)

type VFetchNodeStateOptions struct {
	DatabaseOptions
}

func VFetchNodeStateOptionsFactory() VFetchNodeStateOptions {
	opt := VFetchNodeStateOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (options *VFetchNodeStateOptions) validateParseOptions(vcc *VClusterCommands) error {
	if len(options.RawHosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}

	if options.Password == nil {
		vcc.Log.PrintInfo("no password specified, using none")
	}

	return nil
}

func (options *VFetchNodeStateOptions) analyzeOptions() error {
	hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	options.Hosts = hostAddresses
	return nil
}

func (options *VFetchNodeStateOptions) ValidateAnalyzeOptions(vcc *VClusterCommands) error {
	if err := options.validateParseOptions(vcc); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// VFetchNodeState fetches node states (e.g., up or down) in the cluster
func (vcc *VClusterCommands) VFetchNodeState(options *VFetchNodeStateOptions) ([]NodeInfo, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.ValidateAnalyzeOptions(vcc)
	if err != nil {
		return nil, err
	}

	// TODO: we need to support reading hosts from config for Go client

	// produce list_allnodes instructions
	instructions, err := vcc.produceListAllNodesInstructions(options)
	if err != nil {
		return nil, fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run(vcc.Log)
	nodeStates := clusterOpEngine.execContext.nodesInfo

	return nodeStates, runError
}

// produceListAllNodesInstructions will build a list of instructions to execute for
// the fetch node state operation.
func (vcc *VClusterCommands) produceListAllNodesInstructions(options *VFetchNodeStateOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	// get hosts
	hosts := options.Hosts

	// validate user name
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.ValidateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	httpsCheckNodeStateOp, err := makeHTTPCheckNodeStateOp(vcc.Log, hosts,
		usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions, &httpsCheckNodeStateOp)

	return instructions, nil
}
