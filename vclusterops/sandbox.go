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

type VSandboxOptions struct {
	DatabaseOptions
	SandboxName *string
	SCName      *string
	SCHosts     []string
	SCRawHosts  []string
}

type VSandboxSubclusterInfo struct {
	DBName string
	// Hosts should contain at least one primary host for performing sandboxing,
	// as sandboxing requires the subcluster to be secondary.
	Hosts       []string
	SCHosts     []string
	UserName    string
	Password    *string
	SCName      string
	SandboxName string
}

func VSandboxOptionsFactory() VSandboxOptions {
	opt := VSandboxOptions{}
	opt.setDefaultValues()
	return opt
}

func (options *VSandboxOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()
	options.SCName = new(string)
	options.SandboxName = new(string)
}

func (options *VSandboxOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions("sandbox_subcluster", logger)
	if err != nil {
		return err
	}

	if *options.SCName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}

	if *options.SandboxName == "" {
		return fmt.Errorf("must specify a sandbox name")
	}
	return nil
}

// resolve hostnames to be IPs
func (options *VSandboxOptions) analyzeOptions() (err error) {
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

func (options *VSandboxOptions) ValidateAnalyzeOptions(vcc *VClusterCommands) error {
	if err := options.validateRequiredOptions(vcc.Log); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// produceSandboxSubclusterInstructions will build a list of instructions to execute for
// the sandbox subcluster operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful sandbox_subcluster:
//   - Get UP nodes through HTTPS call, if any node is UP then the DB is UP and ready for running sandboxing operation
//   - Get subcluster sandbox information for the Up hosts. When we choose an initiator host for sandboxing,
//     This would help us filter out sandboxed Up hosts.
//     Also, we would want to filter out hosts from the subcluster to be sandboxed.
//   - Run Sandboxing for the user provided subcluster using the selected initiator host.
//   - Poll for the sandboxed subcluster hosts to be UP.

func (vcc *VClusterCommands) produceSandboxSubclusterInstructions(sandboxSubclusterInfo *VSandboxSubclusterInfo,
	options *VSandboxOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if sandboxSubclusterInfo.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	username := *options.UserName

	// Get all up nodes
	httpsGetUpNodesOp, err := makeHTTPSGetUpNodesOp(vcc.Log, sandboxSubclusterInfo.DBName, sandboxSubclusterInfo.Hosts,
		usePassword, username, sandboxSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	// Get subcluster sandboxing information and remove sandboxed nodes from prospective initator hosts list
	httpsCheckSubclusterSandboxOp, err := makeHTTPSCheckSubclusterSandboxOp(vcc.Log, sandboxSubclusterInfo.Hosts,
		sandboxSubclusterInfo.SCName, sandboxSubclusterInfo.SandboxName, usePassword, username, sandboxSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	// Run Sandboxing
	httpsSandboxSubclusterOp, err := makeHTTPSandboxingOp(sandboxSubclusterInfo.SCName, sandboxSubclusterInfo.SandboxName,
		usePassword, username, sandboxSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	// Poll for sandboxed nodes to be up
	httpsPollSubclusterNodeOp, err := makeHTTPSPollSubclusterNodeStateOp(vcc.Log, sandboxSubclusterInfo.SCName,
		usePassword, username, sandboxSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsGetUpNodesOp,
		&httpsCheckSubclusterSandboxOp,
		&httpsSandboxSubclusterOp,
		&httpsPollSubclusterNodeOp,
	)

	return instructions, nil
}

func (vcc *VClusterCommands) VSandbox(options *VSandboxOptions) error {
	vcc.Log.V(0).Info("VSandbox method called with options " + fmt.Sprintf("%#v", options))
	// check required options
	err := options.ValidateAnalyzeOptions(vcc)
	if err != nil {
		vcc.Log.Error(err, "validation of sandboxing arguments failed")
		return err
	}

	// build sandboxSubclusterInfo from config file and options
	sandboxSubclusterInfo := VSandboxSubclusterInfo{
		UserName:    *options.UserName,
		Password:    options.Password,
		SCName:      *options.SCName,
		SandboxName: *options.SandboxName,
	}
	sandboxSubclusterInfo.DBName, sandboxSubclusterInfo.Hosts, err = options.getNameAndHosts(options.Config)
	if err != nil {
		return err
	}

	instructions, err := vcc.produceSandboxSubclusterInstructions(&sandboxSubclusterInfo, options)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// Create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to sandbox subcluster %s, %w", sandboxSubclusterInfo.SCName, runError)
	}
	return nil
}
