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

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"golang.org/x/exp/maps"
)

// VStartScOptions represents the available options when you start a subcluster
// from a database.
type VStartScOptions struct {
	DatabaseOptions
	VStartNodesOptions
	SCName string // subcluster to start
}

func VStartScOptionsFactory() VStartScOptions {
	options := VStartScOptions{}
	// set default values to the params
	options.setDefaultValues()

	return options
}

func (options *VStartScOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()
	options.VStartNodesOptions.setDefaultValues()
}

func (options *VStartScOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions(StartSubclusterCmd, logger)
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

func (options *VStartScOptions) validateEonOptions() error {
	if !options.IsEon {
		return fmt.Errorf(`cannot start subcluster from an enterprise database '%s'`,
			options.DBName)
	}
	return nil
}

func (options *VStartScOptions) validateParseOptions(logger vlog.Printer) error {
	// batch 1: validate required parameters
	err := options.validateRequiredOptions(logger)
	if err != nil {
		return err
	}

	// batch 2: validate eon params
	err = options.validateEonOptions()
	if err != nil {
		return err
	}
	return nil
}

func (options *VStartScOptions) analyzeOptions() (err error) {
	// we analyze host names when it is set in user input, otherwise we use hosts in yaml config
	if len(options.RawHosts) > 0 {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.IPv6)
		if err != nil {
			return err
		}
		options.normalizePaths()
	}
	return nil
}

func (options *VStartScOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := options.validateParseOptions(logger); err != nil {
		return err
	}
	err := options.analyzeOptions()
	if err != nil {
		return err
	}
	return options.setUsePasswordAndValidateUsernameIfNeeded(logger)
}

// VStartSubcluster start nodes in a subcluster. It returns any error encountered.
// VStartSubcluster has two major phases:
//  1. Pre-check: check the subcluster name and get nodes for the subcluster.
//  2. Start nodes: Optional. If there are any down nodes in the subcluster, runs VStartNodes.
func (vcc VClusterCommands) VStartSubcluster(options *VStartScOptions) error {
	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return err
	}

	// retrieve database information to execute the command so we do not always rely on some user input
	vdb := makeVCoordinationDatabase()
	err = vcc.getVDBFromRunningDBIncludeSandbox(&vdb, &options.DatabaseOptions, AnySandbox)
	if err != nil {
		return err
	}

	// node name to host address map
	nodesToStart := make(map[string]string)

	// collect down nodes to start in the target subcluster
	for _, vnode := range vdb.HostNodeMap {
		if vnode.Subcluster == options.SCName && vnode.State == util.NodeDownState {
			nodesToStart[vnode.Name] = vnode.Address
		}
	}

	if len(nodesToStart) == 0 {
		return fmt.Errorf("cannot find down node to start in subcluster %s",
			options.SCName)
	}

	options.VStartNodesOptions.Nodes = nodesToStart
	options.VStartNodesOptions.DatabaseOptions = options.DatabaseOptions
	options.VStartNodesOptions.StatePollingTimeout = options.StatePollingTimeout
	options.VStartNodesOptions.vdb = &vdb

	vlog.DisplayColorInfo("Starting nodes %v in subcluster %s", maps.Keys(nodesToStart), options.SCName)

	return vcc.VStartNodes(&options.VStartNodesOptions)
}
