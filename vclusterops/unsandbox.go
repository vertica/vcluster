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

	"github.com/vertica/vcluster/rfc7807"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VUnsandboxOptions struct {
	DatabaseOptions
	SCName     *string
	SCHosts    []string
	SCRawHosts []string
}

func VUnsandboxOptionsFactory() VUnsandboxOptions {
	opt := VUnsandboxOptions{}
	opt.setDefaultValues()
	return opt
}

func (options *VUnsandboxOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()
	options.SCName = new(string)
}

func (options *VUnsandboxOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions("unsandbox_subcluster", logger)
	if err != nil {
		return err
	}

	if *options.SCName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}
	return nil
}

// resolve hostnames to be IPs
func (options *VUnsandboxOptions) analyzeOptions() (err error) {
	// we analyze hostnames when it is set in user input, otherwise we use hosts in yaml config
	if len(options.RawHosts) > 0 {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.OldIpv6.ToBool())
		if err != nil {
			return err
		}
	}

	// resolve SCRawHosts to be IP addresses
	if len(options.SCRawHosts) > 0 {
		options.SCHosts, err = util.ResolveRawHostsToAddresses(options.SCRawHosts, options.OldIpv6.ToBool())
		if err != nil {
			return err
		}
	}

	return nil
}

func (options *VUnsandboxOptions) ValidateAnalyzeOptions(vcc VClusterCommands) error {
	if err := options.validateRequiredOptions(vcc.Log); err != nil {
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
	err := vcc.getVDBFromRunningDB(vdb, &options.DatabaseOptions)
	if err != nil {
		return err
	}
	if !vdb.IsEon {
		return fmt.Errorf(`cannot unsandbox subclusters for an enterprise database '%s'`,
			*options.DBName)
	}

	scFound := false
	var sandboxedHosts []string

	for _, vnode := range vdb.HostNodeMap {
		if !scFound && vnode.Subcluster == *options.SCName {
			scFound = true
		}

		if vnode.Subcluster == *options.SCName {
			// if the subcluster is not sandboxed, return error immediately
			if vnode.Sandbox == "" {
				return &SubclusterNotSandboxedError{SCName: *options.SCName}
			}
			sandboxedHosts = append(sandboxedHosts, vnode.Address)
		}
	}

	if !scFound {
		vcc.Log.PrintError(`subcluster '%s' does not exist`, *options.SCName)
		rfcErr := rfc7807.New(rfc7807.SubclusterNotFound).WithHost(options.Hosts[0])
		return rfcErr
	}

	mainClusterHost := util.SliceDiff(options.Hosts, sandboxedHosts)
	if len(mainClusterHost) == 0 {
		return fmt.Errorf(`require at least one UP host outside of the sandbox subcluster '%s'in the input host list`, *options.SCName)
	}
	return nil
}

// produceUnsandboxSCInstructions will build a list of instructions to execute for
// the unsandbox subcluster operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful unsandbox_subcluster:
//   - Get UP nodes through HTTPS call, if any node is UP then the DB is UP and ready for running unsandboxing operation
//     Also get up nodes from fellow subclusters in the same sandbox. Also get all UP nodes info in the given subcluster
//   - Stop the up subcluster hosts
//   - Poll for stopped hosts to be down
//   - Run unsandboxing for the user provided subcluster using the selected initiator host(s).
//   - Remove catalog dirs from unsandboxed hosts
//   - Restart the unsandboxed hosts and Poll for the unsandboxed subcluster hosts to be UP.
//   - Check Vertica versions
//   - get start commands from UP main cluster node
//   - run startup commands for unsandboxed nodes
//   - Poll for started nodes to be UP
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

	username := *options.UserName

	// Get all up nodes
	httpsGetUpNodesOp, err := makeHTTPSGetUpScNodesOp(*options.DBName, options.Hosts,
		usePassword, username, options.Password, UnsandboxCmd, *options.SCName)
	if err != nil {
		return instructions, err
	}

	// Stop the nodes in the subcluster that is to be unsandboxed
	httpsStopNodeOp, err := makeHTTPSStopNodeOp(usePassword, username, options.Password,
		nil)
	if err != nil {
		return instructions, err
	}

	// Poll for nodes down
	httpsPollScDown, err := makeHTTPSPollSubclusterNodeStateDownOp(*options.SCName,
		usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}

	// Run Unsandboxing
	httpsUnsandboxSubclusterOp, err := makeHTTPSUnsandboxingOp(*options.SCName,
		usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}

	// Clean catalog dirs
	nmaDeleteDirsOp, err := makeNMADeleteDirsSandboxOp(true, true /* sandbox */)
	if err != nil {
		return instructions, err
	}

	// NMA check vertica versions before restart
	nmaVersionCheck := makeNMAVerticaVersionOpAfterUnsandbox(true, *options.SCName)

	// Get startup commands
	httpsStartUpCommandOp, err := makeHTTPSStartUpCommandOpAfterUnsandbox(usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}

	// Start the nodes
	nmaRestartNodesOp := makeNMAStartNodeOpAfterUnsandbox("")

	// Poll for nodes UP
	httpsPollScUp, err := makeHTTPSPollSubclusterNodeStateUpOp(*options.SCName,
		usePassword, username, options.Password)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsGetUpNodesOp,
		&httpsStopNodeOp,
		&httpsPollScDown,
		&httpsUnsandboxSubclusterOp,
		&nmaDeleteDirsOp,
		&nmaVersionCheck,
		&httpsStartUpCommandOp,
		&nmaRestartNodesOp,
		&httpsPollScUp,
	)

	return instructions, nil
}

func (vcc VClusterCommands) VUnsandbox(options *VUnsandboxOptions) error {
	vcc.Log.V(0).Info("VUnsandbox method called", "options", options)
	return runSandboxCmd(vcc, options)
}

// runCommand will produce instructions and run them
func (options *VUnsandboxOptions) runCommand(vcc VClusterCommands) error {
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
		return fmt.Errorf("fail to unsandbox subcluster %s, %w", *options.SCName, runError)
	}
	return nil
}
