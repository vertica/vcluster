package vclusterops

import (
	"errors"
	"fmt"

	"github.com/vertica/vcluster/rfc7807"
	"github.com/vertica/vcluster/vclusterops/util"
)

type VFetchNodeStateOptions struct {
	DatabaseOptions
	// retrieve the version for down nodes by invoking two additional
	// operations: NMAHealth and NMA readCatalogEditor. This is useful
	// when we cannot get the version for down nodes from a running database
	GetVersion bool

	SkipDownDatabase bool
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
		hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.IPv6)
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

	// this vdb is used to fetch node version
	var vdb VCoordinationDatabase
	err = vcc.getVDBFromRunningDBIncludeSandbox(&vdb, &options.DatabaseOptions, util.MainClusterSandbox)
	if err != nil {
		vcc.Log.PrintInfo("Error from vdb build: %s", err.Error())

		rfcError := &rfc7807.VProblem{}
		ok := errors.As(err, &rfcError)
		if ok {
			if rfcError.ProblemID == rfc7807.AuthenticationError {
				return nil, err
			}
		}

		if options.SkipDownDatabase {
			return []NodeInfo{}, rfc7807.New(rfc7807.FetchDownDatabase)
		}

		return vcc.fetchNodeStateFromDownDB(options)
	}

	// produce list_all_nodes instructions
	instructions, err := vcc.produceListAllNodesInstructions(options, &vdb)
	if err != nil {
		return nil, fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	clusterOpEngine := makeClusterOpEngine(instructions, options)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	nodeStates := clusterOpEngine.execContext.nodesInfo
	if runError == nil {
		// fill node version
		for i, nodeInfo := range nodeStates {
			vnode, ok := vdb.HostNodeMap[nodeInfo.Address]
			if ok {
				nodeStates[i].Version = vnode.Version
			} else {
				// we do not let this fail
				// but the version for this node will be empty
				vcc.Log.PrintWarning("Cannot find host %s in fetched node versions",
					nodeInfo.Address)
			}
		}

		return nodeStates, nil
	}

	// error out in case of wrong certificate or password
	if len(clusterOpEngine.execContext.hostsWithWrongAuth) > 0 {
		return nodeStates,
			fmt.Errorf("wrong certificate or password on hosts %v", clusterOpEngine.execContext.hostsWithWrongAuth)
	}

	// if failed to get node info from a running database,
	// we will try to get it by reading catalog editor
	upNodeCount := 0
	for _, n := range nodeStates {
		if n.State == util.NodeUpState {
			upNodeCount++
		}
	}

	if upNodeCount == 0 {
		if options.SkipDownDatabase {
			return []NodeInfo{}, rfc7807.New(rfc7807.FetchDownDatabase)
		}

		return vcc.fetchNodeStateFromDownDB(options)
	}

	return nodeStates, runError
}

func (vcc VClusterCommands) fetchNodeStateFromDownDB(options *VFetchNodeStateOptions) ([]NodeInfo, error) {
	const msg = "Cannot get node information from running database. " +
		"Try to get node information by reading catalog editor.\n" +
		"The states of the nodes are shown as DOWN because we failed to fetch the node states."
	fmt.Println(msg)
	vcc.Log.PrintInfo(msg)

	var nodeStates []NodeInfo

	var fetchDatabaseOptions VFetchCoordinationDatabaseOptions
	fetchDatabaseOptions.DatabaseOptions = options.DatabaseOptions
	fetchDatabaseOptions.readOnly = true
	vdb, err := vcc.VFetchCoordinationDatabase(&fetchDatabaseOptions)
	if err != nil {
		return nodeStates, err
	}

	for _, h := range vdb.HostList {
		var nodeInfo NodeInfo
		n := vdb.HostNodeMap[h]
		nodeInfo.Address = n.Address
		nodeInfo.Name = n.Name
		nodeInfo.CatalogPath = n.CatalogPath
		nodeInfo.Subcluster = n.Subcluster
		nodeInfo.IsPrimary = n.IsPrimary
		nodeInfo.Version = n.Version
		nodeInfo.State = util.NodeDownState
		nodeStates = append(nodeStates, nodeInfo)
	}

	return nodeStates, nil
}

// produceListAllNodesInstructions will build a list of instructions to execute for
// the fetch node state operation.
func (vcc VClusterCommands) produceListAllNodesInstructions(
	options *VFetchNodeStateOptions,
	vdb *VCoordinationDatabase) ([]clusterOp, error) {
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

	nmaHealthOp := makeNMAHealthOpSkipUnreachable(options.Hosts)
	nmaReadVerticaVersionOp := makeNMAReadVerticaVersionOp(vdb)

	// Trim host list
	hosts = options.updateHostlist(vcc, vdb, hosts)

	httpsCheckNodeStateOp, err := makeHTTPSCheckNodeStateOp(hosts,
		usePassword, options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}

	if options.GetVersion {
		instructions = append(instructions,
			&nmaHealthOp,
			&nmaReadVerticaVersionOp)
	}

	instructions = append(instructions,
		&httpsCheckNodeStateOp,
	)

	return instructions, nil
}

// Update and limit the hostlist based on status and sandbox info
// Note: if we have any UP main cluster host in the input list, the trimmed hostlist would always contain
//
//	only main cluster UP hosts.
func (options *VFetchNodeStateOptions) updateHostlist(vcc VClusterCommands, vdb *VCoordinationDatabase, inputHosts []string) []string {
	var mainClusterHosts []string
	var upSandboxHosts []string

	for _, h := range inputHosts {
		vnode, ok := vdb.HostNodeMap[h]
		if !ok {
			// host address not found in vdb, skip it
			continue
		}
		if vnode.Sandbox == "" && (vnode.State == util.NodeUpState || vnode.State == util.NodeUnknownState) {
			mainClusterHosts = append(mainClusterHosts, vnode.Address)
		} else if vnode.State == util.NodeUpState {
			upSandboxHosts = append(upSandboxHosts, vnode.Address)
		}
	}
	if len(mainClusterHosts) > 0 {
		vcc.Log.PrintWarning("Main cluster UP node found in host list. The status would be fetched from a main cluster host!")
		return mainClusterHosts
	}
	if len(upSandboxHosts) > 0 {
		vcc.Log.PrintWarning("Only sandboxed UP nodes found in host list. The status would be fetched from a sandbox host!")
		return upSandboxHosts
	}

	// We do not have an up host, so better try with complete input hostlist
	return inputHosts
}
