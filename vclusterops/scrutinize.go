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
	"os"
	"os/exec"
	"time"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// const to sync cmd, options parsing, and this
const VScrutinizeTypeName = "scrutinize"

// files and folders used by scrutinize
const scrutinizeOutputBasePath = "/tmp/scrutinize"
const scrutinizeRemoteOutputPath = scrutinizeOutputBasePath + "/remote"
const scrutinizeLogFileName = "vcluster.log"

// these could be replaced with options later
const scrutinizeLogAgeHours = 24                   // copy archived logs produced in recent 24 hours
const scrutinizeLogLimitBytes = 10737418240        // 10GB in bytes
const scrutinizeFileLimitBytes = 100 * 1024 * 1024 // 100 MB in bytes

// batches are fixed, top level folders for each node's data
const scrutinizeBatchNormal = "normal"
const scrutinizeBatchContext = "context"
const scrutinizeBatchSystemTables = "system_tables"
const scrutinizeSuffixSystemTables = "systables"

type VScrutinizeOptions struct {
	DatabaseOptions
	ID          string // generated: "VerticaScrutinize.yyyymmddhhmmss"
	TarballName string // final tarball name
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

func (options *VScrutinizeOptions) validateRequiredOptions(logger vlog.Printer) error {
	// checks for correctness, but not for presence of all flags
	err := options.validateBaseOptions(VScrutinizeTypeName, logger)
	if err != nil {
		return err
	}

	// will auto-generate username if not provided
	// can be removed after adding embedded server endpoint for system table retrieval
	err = options.validateUserName(logger)
	if err != nil {
		return err
	}

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

func (options *VScrutinizeOptions) validateParseOptions(logger vlog.Printer) error {
	return options.validateRequiredOptions(logger)
}

// analyzeOptions will modify some options based on what is chosen
func (options *VScrutinizeOptions) analyzeOptions(logger vlog.Printer) (err error) {
	// we analyze host names when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
		logger.V(1).Info("Resolved host list to IPs", "Hosts", options.Hosts)
	}

	err = options.setUsePassword(logger)
	return err
}

func (options *VScrutinizeOptions) ValidateAnalyzeOptions(logger vlog.Printer) error {
	if err := options.validateParseOptions(logger); err != nil {
		return err
	}
	return options.analyzeOptions(logger)
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

	// add vcluster log to output
	options.stageVclusterLog(options.ID, vcc.Log)

	// tar all results
	if err = tarAndRemoveDirectory(options.TarballName, options.ID, vcc.Log); err != nil {
		vcc.Log.Error(err, "failed to create final scrutinize output tarball")
		return err
	}

	return nil
}

// stageVclusterLog attempts to copy the vcluster log to the scrutinize tarball, as
// that will contain log entries for this scrutinize run.  Any failure shouldn't
// abort scrutinize, so just prints a warning.
func (options *VScrutinizeOptions) stageVclusterLog(id string, log vlog.Printer) {
	// if using vcluster command line, the log path will always be set
	if options.LogPath == nil {
		log.PrintWarning("Path to scrutinize log not provided. " +
			"The log for this scrutinize run will not be included.")
		return
	}

	destPath := fmt.Sprintf("%s/%s/%s", scrutinizeRemoteOutputPath, id, scrutinizeLogFileName)
	sourcePath := *options.LogPath

	// copy the log instead of symlinking to avoid issues with tar
	log.Info("Copying scrutinize log", "source", sourcePath, "dest", destPath)
	const logFilePerms = 0700
	err := util.CopyFile(sourcePath, destPath, logFilePerms)
	if err != nil {
		log.PrintWarning("Unable to copy scrutinize log: %s", err.Error())
	}
}

// tarAndRemoveDirectory packages the final scrutinize output.
func tarAndRemoveDirectory(tarballName, id string, log vlog.Printer) (err error) {
	tarballPath := scrutinizeOutputBasePath + "/" + tarballName + ".tar"
	cmd := exec.Command("tar", "cf", tarballPath, "-C", "/tmp/scrutinize/remote", id)
	log.Info("running command %s with args %v", cmd.Path, cmd.Args)
	if err = cmd.Run(); err != nil {
		return
	}
	log.PrintInfo("Scrutinize final result at %s", tarballPath)

	intermediateDirectoryPath := "/tmp/scrutinize/remote/" + id
	if err = os.RemoveAll(intermediateDirectoryPath); err != nil {
		log.PrintError("Failed to remove intermediate output directory %s: %s", intermediateDirectoryPath, err.Error())
	}

	return nil
}

// getVDBForScrutinize populates an empty coordinator database with the minimum
// required information for further scrutinize operations.
func (options *VScrutinizeOptions) getVDBForScrutinize(logger vlog.Printer,
	vdb *VCoordinationDatabase) error {
	// get nodes where NMA is running and only use those for NMA ops
	getHealthyNodesOp := makeNMAGetHealthyNodesOp(options.Hosts, vdb)
	err := options.runClusterOpEngine(logger, []clusterOp{&getHealthyNodesOp})
	if err != nil {
		return err
	}

	// get map of host to node name and fully qualified catalog path
	getNodesInfoOp := makeNMAGetNodesInfoOp(vdb.HostList, *options.DBName,
		*options.CatalogPrefix, true /* ignore internal errors */, vdb)
	err = options.runClusterOpEngine(logger, []clusterOp{&getNodesInfoOp})
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
//   - Get up nodes through https call
//   - Initiate system table staging on the first up node, if available
//   - Stage vertica logs on all nodes
//   - Stage files on all nodes
//   - Stage DC tables on all nodes
//   - Tar and retrieve vertica logs and DC tables from all nodes (batch normal)
//   - Tar and retrieve error report from all nodes (batch context)
//   - (If applicable) Poll for system table staging completion on task node
//   - (If applicable) Tar and retrieve system tables from task node (batch system_tables)
func (vcc *VClusterCommands) produceScrutinizeInstructions(options *VScrutinizeOptions,
	vdb *VCoordinationDatabase) (instructions []clusterOp, err error) {
	// extract needed info from vdb
	hostNodeNameMap, hostCatPathMap, err := getNodeInfoForScrutinize(options.Hosts, vdb)
	if err != nil {
		return nil, fmt.Errorf("failed to process retrieved node info, details %w", err)
	}

	// Get up database nodes for the system table task
	getUpNodesOp, err := makeHTTPSGetUpNodesOp(*options.DBName, options.Hosts,
		options.usePassword, *options.UserName, options.Password, ScrutinizeCmd)
	if err != nil {
		return nil, err
	}
	getUpNodesOp.allowNoUpHosts()
	instructions = append(instructions, &getUpNodesOp)

	stageSystemTablesInstructions, err := getStageSystemTablesInstructions(vcc.Log, options, hostNodeNameMap)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, stageSystemTablesInstructions...)

	// stage Vertica logs
	stageVerticaLogsOp, err := makeNMAStageVerticaLogsOp(options.ID, options.Hosts,
		hostNodeNameMap, hostCatPathMap, scrutinizeLogLimitBytes, scrutinizeLogAgeHours)
	if err != nil {
		// map invariant assertion failure -- should not occur
		return nil, err
	}
	instructions = append(instructions, &stageVerticaLogsOp)

	// stage DC Tables
	stageDCTablesOp, err := makeNMAStageDCTablesOp(options.ID, options.Hosts,
		hostNodeNameMap, hostCatPathMap)
	if err != nil {
		// map invariant assertion failure -- should not occur
		return nil, err
	}
	instructions = append(instructions, &stageDCTablesOp)

	// stage 'normal' batch files -- see NMA for what files are collected
	stageVerticaNormalFilesOp, err := makeNMAStageFilesOp(options.ID, scrutinizeBatchNormal,
		options.Hosts, hostNodeNameMap, hostCatPathMap, scrutinizeFileLimitBytes)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &stageVerticaNormalFilesOp)

	// stage 'context' batch files -- see NMA for what files are collected
	stageVerticaContextFilesOp, err := makeNMAStageFilesOp(options.ID, scrutinizeBatchContext,
		options.Hosts, hostNodeNameMap, hostCatPathMap, scrutinizeFileLimitBytes)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &stageVerticaContextFilesOp)

	// run and stage diagnostic command results -- see NMA for what commands are run
	stageCommandsOp, err := makeNMAStageCommandsOp(vcc.Log, options.ID, scrutinizeBatchContext,
		options.Hosts, hostNodeNameMap, hostCatPathMap)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &stageCommandsOp)

	// get 'normal' batch tarball (inc. Vertica logs and 'normal' batch files)
	getNormalTarballOp, err := makeNMAGetScrutinizeTarOp(options.ID, scrutinizeBatchNormal,
		options.Hosts, hostNodeNameMap)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &getNormalTarballOp)

	// get 'context' batch tarball (inc. 'context' batch files)
	getContextTarballOp, err := makeNMAGetScrutinizeTarOp(options.ID, scrutinizeBatchContext,
		options.Hosts, hostNodeNameMap)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &getContextTarballOp)

	// get 'system_tables' batch tarball last, as staging systables can take a long time
	getSystemTablesTarballOp, err := makeNMAGetScrutinizeTarOp(options.ID, scrutinizeBatchSystemTables,
		options.Hosts, hostNodeNameMap)
	if err != nil {
		return nil, err
	}
	getSystemTablesTarballOp.useSingleHost()
	instructions = append(instructions, &getSystemTablesTarballOp)

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

func getStageSystemTablesInstructions(logger vlog.Printer, options *VScrutinizeOptions, hostNodeNameMap map[string]string,
) (instructions []clusterOp, err error) {
	// Prepare directories for scrutinizer staging system tables
	var stagingDir string
	prepareScrutinizerDirsOp, err := makeNMAPrepareScrutinizerDirectoriesOp(
		logger, options.ID, hostNodeNameMap, scrutinizeBatchSystemTables, scrutinizeSuffixSystemTables, &stagingDir,
	)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &prepareScrutinizerDirsOp)

	// Get a list of existing system tables for staging system tables operation
	getSystemTablesOp, err := makeHTTPSGetSystemTablesOp(logger, options.Hosts,
		options.usePassword, *options.UserName, options.Password)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &getSystemTablesOp)

	// Stage system tables stored in execContext
	stageSystemTablesOp, err := makeHTTPSStageSystemTablesOp(logger,
		options.usePassword, *options.UserName, options.Password, options.ID, hostNodeNameMap, &stagingDir)
	if err != nil {
		return nil, err
	}
	instructions = append(instructions, &stageSystemTablesOp)

	return instructions, nil
}
