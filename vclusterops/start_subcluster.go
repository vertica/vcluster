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
	SubclusterToStart string // subcluster to start
}

func VStartScOptionsFactory() VStartScOptions {
	opt := VStartScOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (o *VStartScOptions) setDefaultValues() {
	o.DatabaseOptions.setDefaultValues()
	o.VStartNodesOptions.setDefaultValues()
}

func (o *VStartScOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := o.validateBaseOptions("start_subcluster", logger)
	if err != nil {
		return err
	}

	if o.SubclusterToStart == "" {
		return fmt.Errorf("must specify a subcluster name")
	}
	return nil
}

func (o *VStartScOptions) validateEonOptions() error {
	if !o.IsEon {
		return fmt.Errorf(`cannot start subcluster from an enterprise database '%s'`,
			o.DBName)
	}
	return nil
}

func (o *VStartScOptions) validateParseOptions(logger vlog.Printer) error {
	err := o.validateRequiredOptions(logger)
	if err != nil {
		return err
	}

	return o.validateEonOptions()
}

func (o *VStartScOptions) analyzeOptions() (err error) {
	// we analyze host names when it is set in user input, otherwise we use hosts in yaml config
	if len(o.RawHosts) > 0 {
		// resolve RawHosts to be IP addresses
		o.Hosts, err = util.ResolveRawHostsToAddresses(o.RawHosts, o.IPv6)
		if err != nil {
			return err
		}
		o.normalizePaths()
	}
	return nil
}

func (o *VStartScOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := o.validateParseOptions(logger); err != nil {
		return err
	}
	err := o.analyzeOptions()
	if err != nil {
		return err
	}
	return o.setUsePassword(logger)
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
		if vnode.Subcluster == options.SubclusterToStart && vnode.State == util.NodeDownState {
			nodesToStart[vnode.Name] = vnode.Address
		}
	}

	if len(nodesToStart) == 0 {
		return fmt.Errorf("cannot find down node to start in subcluster %s",
			options.SubclusterToStart)
	}

	var startNodesOptions VStartNodesOptions
	startNodesOptions.Nodes = nodesToStart
	startNodesOptions.DatabaseOptions = options.DatabaseOptions
	startNodesOptions.StatePollingTimeout = options.StatePollingTimeout
	startNodesOptions.vdb = &vdb

	fmt.Printf("Starting nodes %v in subcluster %s\n", maps.Keys(nodesToStart), options.SubclusterToStart)
	return vcc.VStartNodes(&startNodesOptions)
}
