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
	opt.setDefaultValues()

	return opt
}

func (options *VFetchNodeStateOptions) validateParseOptions(vcc VClusterCommands) error {
	if len(options.RawHosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}

	if options.Password == nil {
		vcc.Log.PrintInfo("no password specified, using none")
	}

	return nil
}

func (options *VFetchNodeStateOptions) analyzeOptions() error {
	if len(options.RawHosts) > 0 {
		hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.OldIpv6.ToBool())
		if err != nil {
			return err
		}
		options.Hosts = hostAddresses
	}
	return nil
}

func (options *VFetchNodeStateOptions) validateAnalyzeOptions(vcc VClusterCommands) error {
	if err := options.validateParseOptions(vcc); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// VFetchNodeState returns the node state (e.g., up or down) for each node in the cluster and any
// error encountered.
func (vcc VClusterCommands) VFetchNodeState(options *VFetchNodeStateOptions) ([]NodeInfo, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.validateAnalyzeOptions(vcc)
	if err != nil {
		return nil, err
	}

	// produce list_allnodes instructions
	instructions, err := vcc.produceListAllNodesInstructions(options)
	if err != nil {
		return nil, fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	nodeStates := clusterOpEngine.execContext.nodesInfo

	return nodeStates, runError
}

// produceListAllNodesInstructions will build a list of instructions to execute for
// the fetch node state operation.
func (vcc VClusterCommands) produceListAllNodesInstructions(options *VFetchNodeStateOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	// get hosts
	hosts := options.Hosts

	// validate user name
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	httpsCheckNodeStateOp, err := makeHTTPSCheckNodeStateOp(hosts,
		usePassword, *options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions, &httpsCheckNodeStateOp)

	return instructions, nil
}
