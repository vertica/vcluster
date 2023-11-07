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
	"errors"
	"fmt"
	"time"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// const to sync cmd, options parsing, and this
const VScrutinizeTypeName = "scrutinize"

// top level handler for scrutinize operations
const scrutinizeURLPrefix = "scrutinize/"

// these could be replaced with options later
const scrutinizeLogAgeHours = 24            // copy archived logs produced in recent 24 hours
const scrutinizeLogLimitBytes = 10737418240 // 10GB in bytes

// batches are fixed, top level folders for each node's data
const scrutinizeBatchNormal = "normal"

type VScrutinizeOptions struct {
	DatabaseOptions
	ID string // generated: "VerticaScrutinize.yyyymmddhhmmss"
}

func VScrutinizOptionsFactory() VScrutinizeOptions {
	opt := VScrutinizeOptions{}
	opt.setDefaultValues()
	return opt
}

func (options *VScrutinizeOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()

	options.ID = generateScrutinizeID()
}

func generateScrutinizeID() string {
	const idPrefix = "VerticaScrutinize."
	const timeFmt = "20060102150405" // using fixed reference time from pkg 'time'
	idSuffix := time.Now().Format(timeFmt)
	return idPrefix + idSuffix
}

func (options *VScrutinizeOptions) validateRequiredOptions(log vlog.Printer) error {
	// checks for correctness, but not for presence of all flags
	err := options.validateBaseOptions(VScrutinizeTypeName, log)
	if err != nil {
		return err
	}

	// will auto-generate username if not provided
	// should be replaced by a later call to SetUsePassword() when we use
	// an embedded server endpoint for system table retrieval
	err = options.validateUserName(log)
	if err != nil {
		return err
	}

	// Password is not required

	if *options.HonorUserInput {
		// RawHosts is already required by the cmd parser, so no need to check here
		err = options.validateCatalogPath()
		if err != nil && options.Config == nil {
			// if we have cluster config, we can get the path from there
			return err
		}
	}

	return nil
}

func (options *VScrutinizeOptions) validateParseOptions(log vlog.Printer) error {
	return options.validateRequiredOptions(log)
}

// analyzeOptions will modify some options based on what is chosen
func (options *VScrutinizeOptions) analyzeOptions(log vlog.Printer) (err error) {
	// we analyze host names when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
		log.V(1).Info("Resolved host list to IPs", "Hosts", options.Hosts)
	}

	return nil
}

func (options *VScrutinizeOptions) ValidateAnalyzeOptions(log vlog.Printer) error {
	if err := options.validateParseOptions(log); err != nil {
		return err
	}
	return options.analyzeOptions(log)
}

func (vcc *VClusterCommands) VScrutinize(options *VScrutinizeOptions) error {
	// check required options (including those that can come from cluster config)
	err := options.ValidateAnalyzeOptions(vcc.Log)
	if err != nil {
		vcc.Log.Error(err, "validation of scrutinize arguments failed")
		return err
	}

	// fill in vars from cluster config if necessary
	dbName, hosts, err := options.getNameAndHosts(options.Config)
	if err != nil {
		vcc.Log.Error(err, "failed to retrieve info from cluster config for database",
			"dbname", *options.DBName)
		return err
	}
	catPrefix, err := options.getCatalogPrefix(options.Config)
	if err != nil {
		vcc.Log.Error(err, "failed to retrieve info from cluster config for database",
			"dbname", *options.DBName)
		return err
	}
	options.DBName = &dbName
	options.Hosts = hosts
	options.CatalogPrefix = catPrefix

	// populate vdb with:
	// 1. slice of nodes with NMA running
	// 2. host -> node info map
	vdb := makeVCoordinationDatabase()
	err = options.getVDBForScrutinize(vcc.Log, &vdb)
	if err != nil {
		vcc.Log.Error(err, "failed to retrieve cluster info for scrutinize")
		return err
	}
	// from now on, use hosts with healthy NMA
	options.Hosts = vdb.HostList

	// prepare main instructions
	instructions, err := vcc.produceScrutinizeInstructions(options, &vdb)
	if err != nil {
		vcc.Log.Error(err, "failed to produce instructions for scrutinize")
		return err
	}
	err = options.runClusterOpEngine(vcc.Log, instructions)
	if err != nil {
		vcc.Log.Error(err, "failed to run scrutinize operations")
		return err
	}

	// TODO tar all results (VER-89741)

	return nil
}

// getVDBForScrutinize populates an empty coordinator database with the minimum
// required information for further scrutinize operations.
func (options *VScrutinizeOptions) getVDBForScrutinize(log vlog.Printer,
	vdb *VCoordinationDatabase) error {
	// get nodes where NMA is running and only use those for NMA ops
	nmaGetHealthyNodesOp := makeNMAGetHealthyNodesOp(log, options.Hosts, vdb)
	err := options.runClusterOpEngine(log, []ClusterOp{&nmaGetHealthyNodesOp})
	if err != nil {
		return err
	}

	// get map of host to node name and fully qualified catalog path
	nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(log, vdb.HostList, *options.DBName,
		*options.CatalogPrefix, true /* ignore internal errors */, vdb)
	err = options.runClusterOpEngine(log, []ClusterOp{&nmaGetNodesInfoOp})
	if err != nil {
		return err
	}

	// remove any hosts that responded healthy, but couldn't return host info
	vdb.HostList = []string{}
	for host := range vdb.HostNodeMap {
		vdb.HostList = append(vdb.HostList, host)
	}
	if len(vdb.HostList) == 0 {
		return fmt.Errorf("no hosts successfully returned node info")
	}

	return nil
}

// produceScrutinizeInstructions will build a list of instructions to execute for
// the scrutinize operation, after preliminary configuration retrieval ops.
//
// At this point, hosts/nodes should be filtered so that all have NMA running.
//
// The generated instructions will later perform the following operations necessary
// for a successful scrutinize:
//   - TODO Get up nodes through https call
//   - TODO Initiate system table staging on the first up node, if available
//   - Stage vertica logs on all nodes
//   - TODO Stage ErrorReport.txt on all nodes
//   - TODO Stage DC tables on all nodes
//   - TODO Tar and retrieve vertica logs from all nodes (batch normal)
//   - TODO Tar and retrieve error report from all nodes (batch context)
//   - TODO Tar and retrieve DC tables from all nodes (batch context)
//   - TODO (If applicable) Poll for system table staging completion on task node
//   - TODO (If applicable) Tar and retrieve system tables from task node (batch system_tables)
func (vcc *VClusterCommands) produceScrutinizeInstructions(options *VScrutinizeOptions,
	vdb *VCoordinationDatabase) (instructions []ClusterOp, err error) {
	// extract needed info from vdb
	hostNodeNameMap, hostCatPathMap, err := getNodeInfoForScrutinize(options.Hosts, vdb)
	if err != nil {
		return nil, fmt.Errorf("failed to process retrieved node info, details %w", err)
	}

	nmaStageVerticaLogsOp, err := makeNMAStageVerticaLogsOp(vcc.Log, options.ID, options.Hosts,
		hostNodeNameMap, hostCatPathMap, scrutinizeLogLimitBytes, scrutinizeLogAgeHours)
	if err != nil {
		// map invariant assertion failure -- should not occur
		return nil, err
	}
	instructions = append(instructions, &nmaStageVerticaLogsOp)

	return instructions, nil
}

func getNodeInfoForScrutinize(hosts []string, vdb *VCoordinationDatabase,
) (hostNodeNameMap, hostCatPathMap map[string]string, err error) {
	hostNodeNameMap = make(map[string]string)
	hostCatPathMap = make(map[string]string)
	var allErrors error
	for _, host := range hosts {
		nodeInfo := vdb.HostNodeMap[host]
		if nodeInfo == nil {
			// should never occur, but assert failure is better than nullptr deref
			return hostNodeNameMap, hostCatPathMap, fmt.Errorf("host %s has no saved info", host)
		}
		nodeName := nodeInfo.Name
		catPath := nodeInfo.CatalogPath

		// actual validation here
		if nodeName == "" {
			allErrors = errors.Join(allErrors, fmt.Errorf("host %s has empty name", host))
		}
		err = util.ValidateRequiredAbsPath(&catPath, "catalog path")
		if err != nil {
			allErrors = errors.Join(allErrors, fmt.Errorf("host %s has problematic catalog path %s, details: %w", host, catPath, err))
		}
		hostNodeNameMap[host] = nodeName
		hostCatPathMap[host] = catPath
	}

	return hostNodeNameMap, hostCatPathMap, allErrors
}
