/*
 (c) Copyright [2023-2024] Open Text.
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

	"github.com/vertica/vcluster/rfc7807"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VUnsandboxOptions struct {
	DatabaseOptions
	SCName     string
	SCHosts    []string
	SCRawHosts []string
	// if restart the subcluster after unsandboxing it, the default value of it is true
	RestartSC bool
	// if any node in the target subcluster is up. This is for internal use only.
	hasUpNodeInSC bool
	// The expected node names with their IPs in the subcluster, the user of vclusterOps need
	// to make sure the provided values are correct. This option will be used to do re-ip in
	// the main cluster.
	NodeNameAddressMap map[string]string
	// A primary up host in the main cluster. This option will be used to do re-ip in
	// the main cluster.
	PrimaryUpHost string
}

func VUnsandboxOptionsFactory() VUnsandboxOptions {
	options := VUnsandboxOptions{}
	options.setDefaultValues()
	return options
}

func (options *VUnsandboxOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()
	options.RestartSC = true
}

func (options *VUnsandboxOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions(UnsandboxSCCmd, logger)
	if err != nil {
		return err
	}

	if options.SCName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}

	err = util.ValidateScName(options.SCName)
	if err != nil {
		return err
	}
	return nil
}

func (options *VUnsandboxOptions) validateParseOptions(logger vlog.Printer) error {
	// batch 1: validate required parameters
	err := options.validateRequiredOptions(logger)
	if err != nil {
		return err
	}
	return nil
}

// resolve hostnames to be IPs
func (options *VUnsandboxOptions) analyzeOptions() (err error) {
	// we analyze hostnames when it is set in user input, otherwise we use hosts in yaml config
	if len(options.RawHosts) > 0 {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.IPv6)
		if err != nil {
			return err
		}
	}

	// resolve SCRawHosts to be IP addresses
	if len(options.SCRawHosts) > 0 {
		options.SCHosts, err = util.ResolveRawHostsToAddresses(options.SCRawHosts, options.IPv6)
		if err != nil {
			return err
		}
	}

	return nil
}

func (options *VUnsandboxOptions) ValidateAnalyzeOptions(logger vlog.Printer) error {
	if err := options.validateParseOptions(logger); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// SubclusterNotSandboxedError is the error that is returned when
// the subcluster does not need unsandbox operation
type SubclusterNotSandboxedError struct {
	SCName string
}

func (e *SubclusterNotSandboxedError) Error() string {
	return fmt.Sprintf(`cannot unsandbox a regular subcluster [%s]`, e.SCName)
}

// unsandboxPreCheck will build a list of instructions to perform
// unsandbox_subcluster pre-checks
//
// The generated instructions will later perform the following operations necessary
// for a successful unsandbox_subcluster
// - Get cluster and nodes info (check if the DB is Eon)
// - Get the subcluster info (check if the target subcluster is sandboxed)
func (vcc *VClusterCommands) unsandboxPreCheck(vdb *VCoordinationDatabase, options *VUnsandboxOptions) error {
	err := vcc.getVDBFromMainRunningDBContainsSandbox(vdb, &options.DatabaseOptions)
	if err != nil {
		return err
	}
	if !vdb.IsEon {
		return fmt.Errorf(`cannot unsandbox subclusters for an enterprise database '%s'`,
			options.DBName)
	}

	scFound := false
	var sandboxedHosts []string

	upHosts := []string{}
	for _, vnode := range vdb.HostNodeMap {
		if !scFound && vnode.Subcluster == options.SCName {
			scFound = true
		}

		if vnode.State != util.NodeDownState {
			upHosts = append(upHosts, vnode.Address)
		}
		if vnode.Subcluster == options.SCName {
			// if the subcluster is not sandboxed, return error immediately
			if vnode.Sandbox == "" {
				return &SubclusterNotSandboxedError{SCName: options.SCName}
			}
			sandboxedHosts = append(sandboxedHosts, vnode.Address)
			// when the node state is not "DOWN" ("UP" or "UNKNOWN"), we consider
			// the node is running
			if vnode.State != util.NodeDownState {
				options.hasUpNodeInSC = true
			}
		}
	}
	// change hosts in options to all up hosts so the user can only provide hosts in main cluster
	options.Hosts = upHosts

	if !scFound {
		vcc.Log.PrintError(`subcluster '%s' does not exist`, options.SCName)
		rfcErr := rfc7807.New(rfc7807.SubclusterNotFound).WithHost(options.Hosts[0])
		return rfcErr
	}

	mainClusterHost := util.SliceDiff(options.Hosts, sandboxedHosts)
	if len(mainClusterHost) == 0 {
		return fmt.Errorf(`require at least one UP host outside of the sandbox subcluster '%s'in the input host list`, options.SCName)
	}
	options.SCHosts = sandboxedHosts
	return nil
}

// produceUnsandboxSCInstructions will build a list of instructions to execute for
// the unsandbox subcluster operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful unsandbox_subcluster:
//   - Get UP nodes through HTTPS call, if any node is UP then the DB is UP and ready for running unsandboxing operation
//     Also get up nodes from fellow subclusters in the same sandbox. Also get all UP nodes info in the given subcluster
//   - If the subcluster is UP
//     1. Stop the up subcluster hosts
//     2. Poll for stopped hosts to be down
//   - Run unsandboxing for the user provided subcluster using the selected initiator host(s).
//   - Remove catalog dirs from unsandboxed hosts
//   - VCluster CLI will restart the unsandboxed hosts using below instructions, but k8s operator will skip the restart process
//     1. Check Vertica versions
//     2. get start commands from UP main cluster node
//     3. run startup commands for unsandboxed nodes
//     4. Poll for started nodes to be UP
func (vcc *VClusterCommands) produceUnsandboxSCInstructions(options *VUnsandboxOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	username := options.UserName
	// Check NMA health on sandbox hosts
	nmaHealthOp := makeNMAHealthOp(options.SCHosts)
	instructions = append(instructions, &nmaHealthOp)

	// Get all up nodes
	httpsGetUpNodesOp, err := makeHTTPSGetUpScNodesOp(options.DBName, options.Hosts,
		usePassword, username, options.Password, UnsandboxSCCmd, options.SCName)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &httpsGetUpNodesOp)

	scHosts := []string{}
	scNodeNames := []string{}
	for nodeName, host := range options.NodeNameAddressMap {
		scHosts = append(scHosts, host)
		scNodeNames = append(scNodeNames, nodeName)
	}
	if options.hasUpNodeInSC {
		// Stop the nodes in the subcluster that is to be unsandboxed
		httpsStopNodeOp, e := makeHTTPSStopNodeOp(scHosts, scNodeNames, usePassword,
			username, options.Password, nil)
		if e != nil {
			return instructions, e
		}

		// Poll for nodes down
		httpsPollScDown, e := makeHTTPSPollSubclusterNodeStateDownOp(scHosts, options.SCName,
			usePassword, username, options.Password)
		if e != nil {
			return instructions, e
		}

		instructions = append(instructions,
			&httpsStopNodeOp,
			&httpsPollScDown,
		)
	}

	// Run Unsandboxing
	httpsUnsandboxSubclusterOp, err := makeHTTPSUnsandboxingOp(options.SCName,
		usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}

	// Clean catalog dirs
	nmaDeleteDirsOp, err := makeNMADeleteDirsSandboxOp(scHosts, true, true /* sandbox */)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsUnsandboxSubclusterOp,
		&nmaDeleteDirsOp,
	)

	if options.RestartSC {
		// NMA check vertica versions before restart
		nmaVersionCheck := makeNMAVerticaVersionOpAfterUnsandbox(true, options.SCName)

		// Get startup commands
		httpsStartUpCommandOp, err := makeHTTPSStartUpCommandOpAfterUnsandbox(usePassword, username, options.Password)
		if err != nil {
			return instructions, err
		}

		// Start the nodes
		nmaStartNodesOp := makeNMAStartNodeOpAfterUnsandbox("")

		// Poll for nodes UP
		httpsPollScUp, err := makeHTTPSPollSubclusterNodeStateUpOp(scHosts, options.SCName,
			usePassword, username, options.Password)
		if err != nil {
			return instructions, err
		}

		instructions = append(instructions,
			&nmaVersionCheck,
			&httpsStartUpCommandOp,
			&nmaStartNodesOp,
			&httpsPollScUp,
		)
	}

	return instructions, nil
}

func (vcc VClusterCommands) VUnsandbox(options *VUnsandboxOptions) error {
	vcc.Log.V(0).Info("VUnsandbox method called", "options", options)
	return runSandboxCmd(vcc, options)
}

// runCommand will produce instructions and run them
func (options *VUnsandboxOptions) runCommand(vcc VClusterCommands) error {
	// if the users want to do re-ip before unsandboxing, we require them
	// to provide some node information
	if options.PrimaryUpHost != "" && len(options.NodeNameAddressMap) > 0 {
		err := vcc.reIP(&options.DatabaseOptions, options.SCName, options.PrimaryUpHost,
			options.NodeNameAddressMap, true /*reload spread*/)
		if err != nil {
			return err
		}
	}

	vdb := makeVCoordinationDatabase()
	err := vcc.unsandboxPreCheck(&vdb, options)
	if err != nil {
		return err
	}
	// make instructions
	instructions, err := vcc.produceUnsandboxSCInstructions(options)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// add certs and instructions to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// run the engine
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to unsandbox subcluster %s, %w", options.SCName, runError)
	}
	return nil
}
