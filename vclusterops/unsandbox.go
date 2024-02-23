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

type VUnsandboxOptions struct {
	DatabaseOptions
	SCName     *string
	SCHosts    []string
	SCRawHosts []string
}

type VUnsandboxSubclusterInfo struct {
	DBName string
	// Hosts should contain at least one primary host for performing unsandboxing,
	// as unsandboxing has to be initiated from the main cluster host.
	Hosts    []string
	SCHosts  []string
	UserName string
	Password *string
	SCName   string
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
	// we analyze hostnames when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
	}

	// resolve SCRawHosts to be IP addresses
	options.SCHosts, err = util.ResolveRawHostsToAddresses(options.SCRawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	return nil
}

func (options *VUnsandboxOptions) ValidateAnalyzeOptions(vcc *VClusterCommands) error {
	if err := options.validateRequiredOptions(vcc.Log); err != nil {
		return err
	}
	return options.analyzeOptions()
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
func (vcc *VClusterCommands) produceUnsandboxSCInstructions(unsandboxSubclusterInfo *VUnsandboxSubclusterInfo,
	options *VUnsandboxOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if unsandboxSubclusterInfo.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	username := *options.UserName

	// Get all up nodes
	httpsGetUpNodesOp, err := makeHTTPSGetUpScNodesOp(unsandboxSubclusterInfo.DBName, unsandboxSubclusterInfo.Hosts,
		usePassword, username, unsandboxSubclusterInfo.Password, UnsandboxCmd, unsandboxSubclusterInfo.SCName)
	if err != nil {
		return instructions, err
	}

	// Stop the nodes in the subcluster that is to be unsandboxed
	httpsStopNodeOp, err := makeHTTPSStopNodeOp(usePassword, username, unsandboxSubclusterInfo.Password,
		nil)
	if err != nil {
		return instructions, err
	}

	// Poll for nodes down
	httpsPollScDown, err := makeHTTPSPollSubclusterNodeStateDownOp(unsandboxSubclusterInfo.SCName,
		usePassword, username, unsandboxSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	// Run Unsandboxing
	httpsUnsandboxSubclusterOp, err := makeHTTPSUnsandboxingOp(unsandboxSubclusterInfo.SCName,
		usePassword, username, unsandboxSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	// Clean catalog dirs
	nmaDeleteDirsOp, err := makeNMADeleteDirsSandboxOp(true, true /* sandbox */)
	if err != nil {
		return instructions, err
	}

	// NMA check vertica versions before restart
	nmaVersionCheck := makeNMAVerticaVersionOpAfterUnsandbox(true, unsandboxSubclusterInfo.SCName)

	// Get startup commands
	httpsStartUpCommandOp, err := makeHTTPSStartUpCommandOpAfterUnsandbox(usePassword, username, unsandboxSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	// Start the nodes
	nmaRestartNodesOp := makeNMAStartNodeOpAfterUnsandbox("")

	// Poll for nodes UP
	httpsPollScUp, err := makeHTTPSPollSubclusterNodeStateUpOp(unsandboxSubclusterInfo.SCName,
		usePassword, username, unsandboxSubclusterInfo.Password)
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

func (vcc *VClusterCommands) VUnsandbox(options *VUnsandboxOptions) error {
	vcc.Log.V(0).Info("VUnsandbox method called with options " + fmt.Sprintf("%#v", options))
	// check required options
	err := options.ValidateAnalyzeOptions(vcc)
	if err != nil {
		vcc.Log.Error(err, "validation of unsandboxing arguments failed")
		return err
	}
	// build unsandboxSubclusterInfo from options
	unsandboxSubclusterInfo := VUnsandboxSubclusterInfo{
		UserName: *options.UserName,
		Password: options.Password,
		SCName:   *options.SCName,
	}
	unsandboxSubclusterInfo.DBName, unsandboxSubclusterInfo.Hosts, err = options.getNameAndHosts(options.Config)
	if err != nil {
		return err
	}

	instructions, err := vcc.produceUnsandboxSCInstructions(&unsandboxSubclusterInfo, options)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// Create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to unsandbox subcluster %s, %w", unsandboxSubclusterInfo.SCName, runError)
	}
	return nil
}
