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

// Normal strings are easier and safer to use in Go.
type VStartDatabaseOptions struct {
	// basic db info
	DatabaseOptions
	// Timeout for polling the states of all nodes in the database in HTTPSPollNodeStateOp
	StatePollingTimeout int
}

func VStartDatabaseOptionsFactory() VStartDatabaseOptions {
	opt := VStartDatabaseOptions{}

	// set default values to the params
	opt.SetDefaultValues()
	return opt
}

func (options *VStartDatabaseOptions) SetDefaultValues() {
	options.DatabaseOptions.SetDefaultValues()
}

func (options *VStartDatabaseOptions) validateRequiredOptions() error {
	err := options.ValidateBaseOptions("start_db")
	if err != nil {
		return err
	}

	if *options.HonorUserInput {
		err = options.ValidateCatalogPath()
		if err != nil {
			return err
		}
	}

	return nil
}

func (options *VStartDatabaseOptions) validateEonOptions() error {
	if *options.CommunalStorageLocation != "" {
		return util.ValidateCommunalStorageLocation(*options.CommunalStorageLocation)
	}

	return nil
}

func (options *VStartDatabaseOptions) validateParseOptions() error {
	// batch 1: validate required parameters
	err := options.validateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	return options.validateEonOptions()
}

func (options *VStartDatabaseOptions) analyzeOptions() (err error) {
	// we analyze hostnames when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
	}
	return nil
}

func (options *VStartDatabaseOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	return options.analyzeOptions()
}

func (vcc *VClusterCommands) VStartDatabase(options *VStartDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	// get db name and hosts from config file and options
	dbName, hosts := options.GetNameAndHosts(options.Config)
	options.DBName = &dbName
	options.Hosts = hosts
	options.CatalogPrefix = options.GetCatalogPrefix(options.Config)

	// set default value to StatePollingTimeout
	if options.StatePollingTimeout == 0 {
		options.StatePollingTimeout = util.DefaultStatePollingTimeout
	}

	var pVDB *VCoordinationDatabase
	// retrieve database information from cluster_config.json for EON databases
	if options.IsEonMode(options.Config) {
		if *options.CommunalStorageLocation != "" {
			vdb, e := options.getVDBWhenDBIsDown()
			if e != nil {
				return e
			}
			// we want to read catalog info only from primary nodes later
			vdb.filterPrimaryNodes()
			pVDB = &vdb
		} else {
			// When communal storage location is missing, we only log a warning message
			// because fail to read cluster_config.json will not affect start_db in most of the cases.
			vlog.LogPrintWarningln("communal storage location is not specified for an eon database," +
				" first start_db after revive_db could fail because we cannot retrieve the correct database information")
		}
	}

	// produce start_db instructions
	instructions, err := vcc.produceStartDBInstructions(options, pVDB)
	if err != nil {
		err = fmt.Errorf("fail to production instructions: %w", err)
		return err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		runError = fmt.Errorf("fail to start database: %w", runError)
		return runError
	}

	return nil
}

// produceStartDBInstructions will build a list of instructions to execute for
// the start_db command.
//
// The generated instructions will later perform the following operations necessary
// for a successful start_db:
//   - Check NMA connectivity
//   - Check Vertica versions
//   - Check to see if any dbs running
//   - Use NMA /catalog/database to get the best source node for spread.conf and vertica.conf
//   - Sync the confs to the rest of nodes who have lower catalog version (results from the previous step)
//   - Start all nodes of the database
//   - Poll node startup
//   - Sync catalog (Eon mode only)
func (vcc *VClusterCommands) produceStartDBInstructions(options *VStartDatabaseOptions, vdb *VCoordinationDatabase) ([]ClusterOp, error) {
	var instructions []ClusterOp

	nmaHealthOp := makeNMAHealthOp(options.Hosts)
	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(vcc.Log, options.Hosts, true)
	// need username for https operations
	err := options.SetUsePassword()
	if err != nil {
		return instructions, err
	}

	checkDBRunningOp, err := makeHTTPCheckRunningDBOp(vcc.Log, options.Hosts,
		options.usePassword, *options.UserName, options.Password, StartDB)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
	)

	// When we cannot get db info from cluster_config.json, we will fetch it from NMA /nodes endpoint.
	if vdb == nil {
		vdb = new(VCoordinationDatabase)
		nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(options.Hosts, *options.DBName, *options.CatalogPrefix, vdb)
		instructions = append(instructions, &nmaGetNodesInfoOp)
	}

	// vdb here should contains only primary nodes
	nmaReadCatalogEditorOp, err := makeNMAReadCatalogEditorOp(vdb)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &nmaReadCatalogEditorOp)

	// sourceConfHost is set to nil value in upload and download step
	// we use information from catalog editor operation to update the sourceConfHost value
	// after we find host with the highest catalog and hosts that need to synchronize the catalog
	// we will remove the nil parameters in VER-88401 by adding them in execContext
	produceTransferConfigOps(&instructions,
		nil, /*source hosts for transferring configuration files*/
		options.Hosts,
		nil /*db configurations retrieved from a running db*/)

	nmaStartNewNodesOp := makeNMAStartNodeOp(options.Hosts)
	httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOpWithTimeoutAndCommand(options.Hosts,
		options.usePassword, *options.UserName, options.Password, options.StatePollingTimeout, StartDBCmd)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaStartNewNodesOp,
		&httpsPollNodeStateOp,
	)

	if options.IsEon.ToBool() {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(options.Hosts, true, *options.UserName, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}

	return instructions, nil
}
