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
	"sort"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VReviveDatabaseOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: revive db info
	LoadCatalogTimeout *uint
	ForceRemoval       *bool
	DisplayOnly        *bool
	IgnoreClusterLease *bool
}

func VReviveDBOptionsFactory() VReviveDatabaseOptions {
	opt := VReviveDatabaseOptions{}

	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (options *VReviveDatabaseOptions) setDefaultValues() {
	options.DatabaseOptions.SetDefaultValues()

	// set default values for revive db options
	options.LoadCatalogTimeout = new(uint)
	*options.LoadCatalogTimeout = util.DefaultLoadCatalogTimeoutSeconds
	options.ForceRemoval = new(bool)
	options.DisplayOnly = new(bool)
	options.IgnoreClusterLease = new(bool)
}

func (options *VReviveDatabaseOptions) validateRequiredOptions() error {
	// database name
	if *options.DBName == "" {
		return fmt.Errorf("must specify a database name")
	}
	err := util.ValidateName(*options.DBName, "database")
	if err != nil {
		return err
	}

	// new hosts
	// when --display-only is not specified, we require --hosts
	if len(options.RawHosts) == 0 && !*options.DisplayOnly {
		return fmt.Errorf("must specify a host or host list")
	}

	// communal storage
	return util.ValidateCommunalStorageLocation(*options.CommunalStorageLocation)
}

func (options *VReviveDatabaseOptions) validateParseOptions() error {
	return options.validateRequiredOptions()
}

// analyzeOptions will modify some options based on what is chosen
func (options *VReviveDatabaseOptions) analyzeOptions() (err error) {
	// when --display-only is specified but no hosts in user input, we will try to access communal storage from localhost
	if len(options.RawHosts) == 0 && *options.DisplayOnly {
		options.RawHosts = append(options.RawHosts, "localhost")
	}

	// resolve RawHosts to be IP addresses
	options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	return nil
}

func (options *VReviveDatabaseOptions) ValidateAnalyzeOptions() error {
	if err := options.validateParseOptions(); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// VReviveDatabase can revive a database which has been terminated but its communal storage data still exists
func (vcc *VClusterCommands) VReviveDatabase(options *VReviveDatabaseOptions) (dbInfo string, err error) {
	/*
	 *   - Validate options
	 *   - Run VClusterOpEngine to get terminated database info
	 *   - Run VClusterOpEngine again to revive the database
	 */

	// validate and analyze options
	err = options.ValidateAnalyzeOptions()
	if err != nil {
		return dbInfo, err
	}

	vdb := MakeVCoordinationDatabase()

	// part 1: produce instructions for getting terminated database info, and save the info to vdb
	preReviveDBInstructions, err := vcc.producePreReviveDBInstructions(options, &vdb)
	if err != nil {
		vlog.LogPrintError("fail to produce pre-revive database instructions %v", err)
		return dbInfo, err
	}

	// generate clusterOpEngine certs
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	// feed the pre-revive db instructions to the VClusterOpEngine
	clusterOpEngine := MakeClusterOpEngine(preReviveDBInstructions, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to collect the information of database in revive_db %v", err)
		return dbInfo, err
	}

	if *options.DisplayOnly {
		dbInfo = clusterOpEngine.execContext.dbInfo
		return dbInfo, nil
	}

	// part 2: produce instructions for reviving database using terminated database info
	reviveDBInstructions, err := produceReviveDBInstructions(options, &vdb)
	if err != nil {
		vlog.LogPrintError("fail to produce revive database instructions %v", err)
		return dbInfo, err
	}

	// feed revive db instructions to the VClusterOpEngine
	clusterOpEngine = MakeClusterOpEngine(reviveDBInstructions, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to revive database %v", err)
		return dbInfo, err
	}
	return dbInfo, nil
}

// revive db instructions are split into two parts:
// 1. get terminated database info
// 2. revive database using the info we got from step 1
// The reason of using two set of instructions is: the second set of instructions needs the database info
// to initialize, but that info can only be retrieved after we ran first set of instructions in clusterOpEngine
//
// producePreReviveDBInstructions will build the first half of revive_db instructions
// The generated instructions will later perform the following operations
//   - Check NMA connectivity
//   - Check NMA version
//   - Check any DB running on the hosts
//   - Download and read the description file from communal storage on the initiator
func (vcc *VClusterCommands) producePreReviveDBInstructions(options *VReviveDatabaseOptions,
	vdb *VCoordinationDatabase) ([]ClusterOp, error) {
	var instructions []ClusterOp

	nmaHealthOp := makeNMAHealthOp(options.Hosts)
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(vcc.Log, options.Hosts, true)

	checkDBRunningOp, err := makeHTTPCheckRunningDBOp(vcc.Log, options.Hosts, false, /*use password auth*/
		"" /*username for https call*/, nil /*password for https call*/, ReviveDB)
	if err != nil {
		return instructions, err
	}

	// use description file path as source file path
	sourceFilePath := options.getDescriptionFilePath()
	nmaDownLoadFileOp, err := makeNMADownloadFileOpForRevive(options.Hosts, sourceFilePath, destinationFilePath, catalogPath,
		options.ConfigurationParameters, vdb, *options.DisplayOnly, *options.IgnoreClusterLease)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaDownLoadFileOp,
	)

	return instructions, nil
}

// produceReviveDBInstructions will build the second half of revive_db instructions
// The generated instructions will later perform the following operations
//   - Prepare database directories for all the hosts
//   - Get network profiles for all the hosts
//   - Load remote catalog from communal storage on all the hosts
func produceReviveDBInstructions(options *VReviveDatabaseOptions, vdb *VCoordinationDatabase) ([]ClusterOp, error) {
	var instructions []ClusterOp

	newVDB, oldHosts, err := options.generateReviveVDB(vdb)
	if err != nil {
		return instructions, err
	}

	// create a new HostNodeMap to prepare directories
	hostNodeMap := makeVHostNodeMap()
	// remove user storage locations from storage locations in every node
	// user storage location will not be force deleted,
	// and fail to create user storage location will not cause a failure of NMA /directories/prepare call.
	// as a result, we separate user storage locations with other storage locations
	for host, vnode := range newVDB.HostNodeMap {
		userLocationSet := make(map[string]struct{})
		for _, userLocation := range vnode.UserStorageLocations {
			userLocationSet[userLocation] = struct{}{}
		}
		var newLocations []string
		for _, location := range vnode.StorageLocations {
			if _, exist := userLocationSet[location]; !exist {
				newLocations = append(newLocations, location)
			}
		}
		vnode.StorageLocations = newLocations
		hostNodeMap[host] = vnode
	}
	// prepare all directories
	nmaPrepareDirectoriesOp, err := makeNMAPrepareDirectoriesOp(hostNodeMap, *options.ForceRemoval, true /*for db revive*/)
	if err != nil {
		return instructions, err
	}

	nmaNetworkProfileOp := makeNMANetworkProfileOp(options.Hosts)

	nmaLoadRemoteCatalogOp := makeNMALoadRemoteCatalogOp(oldHosts, options.ConfigurationParameters, &newVDB, *options.LoadCatalogTimeout)

	instructions = append(instructions,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&nmaLoadRemoteCatalogOp,
	)

	return instructions, nil
}

// generateReviveVDB can create new vdb, and line up old hosts and vnodes with new hosts' order(user input order)
func (options *VReviveDatabaseOptions) generateReviveVDB(vdb *VCoordinationDatabase) (newVDB VCoordinationDatabase,
	oldHosts []string, err error) {
	newVDB = MakeVCoordinationDatabase()
	newVDB.Name = *options.DBName
	newVDB.CommunalStorageLocation = *options.CommunalStorageLocation
	// use new cluster hosts
	newVDB.HostList = options.Hosts

	/* for example, in old vdb, we could have the HostNodeMap
	{
	"192.168.1.101": {Name: v_test_db_node0001, Address: "192.168.1.101", ...},
	"192.168.1.102": {Name: v_test_db_node0002, Address: "192.168.1.102", ...},
	"192.168.1.103": {Name: v_test_db_node0003, Address: "192.168.1.103", ...}
	}
	in new vdb, we want to update the HostNodeMap with the values(can be unordered) in --hosts(user input), e.g. 10.1.10.2,10.1.10.1,10.1.10.3.
	we line up vnodes with new hosts' order(user input order). We will have the new HostNodeMap like:
	{
	"10.1.10.2": {Name: v_test_db_node0001, Address: "10.1.10.2", ...},
	"10.1.10.1": {Name: v_test_db_node0002, Address: "10.1.10.1", ...},
	"10.1.10.3": {Name: v_test_db_node0003, Address: "10.1.10.3", ...}
	}
	we also line up old nodes with new hosts' order so we will have oldHosts like:
	["192.168.1.102", "192.168.1.101", "192.168.1.103"]
	*/
	// sort nodes by their names, and then assign new hosts to them
	var vNodes []*VCoordinationNode
	for _, vnode := range vdb.HostNodeMap {
		vNodes = append(vNodes, vnode)
	}
	sort.Slice(vNodes, func(i, j int) bool {
		return vNodes[i].Name < vNodes[j].Name
	})

	newVDB.HostNodeMap = makeVHostNodeMap()
	if len(newVDB.HostList) != len(vNodes) {
		return newVDB, oldHosts, fmt.Errorf("the number of new hosts does not match the number of nodes in original database")
	}
	for index, newHost := range newVDB.HostList {
		// recreate the old host list with new hosts' order
		oldHosts = append(oldHosts, vNodes[index].Address)
		vNodes[index].Address = newHost
		newVDB.HostNodeMap[newHost] = vNodes[index]
	}

	return newVDB, oldHosts, nil
}
