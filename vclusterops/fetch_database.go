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
)

type VFetchCoordinationDatabaseOptions struct {
	DatabaseOptions
	Overwrite bool // overwrite existing config file at the same location

	// hidden option
	readOnly bool // this should be only used if we don't want to update the config file
}

func VRecoverConfigOptionsFactory() VFetchCoordinationDatabaseOptions {
	opt := VFetchCoordinationDatabaseOptions{}
	// set default values to the params
	opt.setDefaultValues()
	return opt
}

func (opt *VFetchCoordinationDatabaseOptions) validateParseOptions(logger vlog.Printer) error {
	return opt.validateBaseOptions(commandConfigRecover, logger)
}

func (opt *VFetchCoordinationDatabaseOptions) analyzeOptions() error {
	// resolve RawHosts to be IP addresses
	if len(opt.RawHosts) > 0 {
		hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, opt.IPv6)
		if err != nil {
			return err
		}
		opt.Hosts = hostAddresses
	}

	// process correct catalog path
	opt.CatalogPrefix = util.GetCleanPath(opt.CatalogPrefix)

	// check existing config file at the same location
	if !opt.readOnly && !opt.Overwrite {
		if util.CanWriteAccessPath(opt.ConfigPath) == util.FileExist {
			return fmt.Errorf("config file exists at %s. "+
				"You can use --overwrite to overwrite this existing config file", opt.ConfigPath)
		}
	}

	return nil
}

func (opt *VFetchCoordinationDatabaseOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := opt.validateParseOptions(logger); err != nil {
		return err
	}
	return opt.analyzeOptions()
}

func (vcc VClusterCommands) VFetchCoordinationDatabase(options *VFetchCoordinationDatabaseOptions) (VCoordinationDatabase, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	var vdb VCoordinationDatabase

	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return vdb, err
	}

	// pre-fill vdb from the user input
	vdb.Name = options.DBName
	vdb.HostList = options.Hosts
	vdb.CatalogPrefix = options.CatalogPrefix
	vdb.DepotPrefix = options.DepotPrefix
	vdb.Ipv6 = options.IPv6

	// produce list_allnodes instructions
	instructions, err := vcc.produceRecoverConfigInstructions(options, &vdb)
	if err != nil {
		return vdb, fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)

	// nmaVDB is an object obtained from the read catalog editor result
	// we use nmaVDB data to complete vdb
	nmaVDB := clusterOpEngine.execContext.nmaVDatabase

	if !options.readOnly && nmaVDB.CommunalStorageLocation != "" {
		vdb.IsEon = true
		vdb.CommunalStorageLocation = nmaVDB.CommunalStorageLocation
		// if depot path is not provided for an Eon DB,
		// we should error out
		if vdb.DepotPrefix == "" {
			return vdb,
				fmt.Errorf("the depot path must be provided for an Eon database")
		}
	}

	for h, n := range nmaVDB.HostNodeMap {
		vnode, ok := vdb.HostNodeMap[h]
		if !ok {
			return vdb, fmt.Errorf("host %s is not found in the vdb object", h)
		}
		vnode.Subcluster = n.Subcluster.Name
		vnode.StorageLocations = n.StorageLocations
		vnode.IsPrimary = n.IsPrimary
	}

	return vdb, runError
}

// produceRecoverConfigInstructions will build a list of instructions to execute for
// the recover config operation.

// The generated instructions will later perform the following operations necessary
// for a successful `manage_config --recover`
//   - Check NMA connectivity
//   - Get information of nodes
//   - Read catalog editor
func (vcc VClusterCommands) produceRecoverConfigInstructions(
	options *VFetchCoordinationDatabaseOptions,
	vdb *VCoordinationDatabase) ([]clusterOp, error) {
	var instructions []clusterOp

	nmaHealthOp := makeNMAHealthOp(options.Hosts)
	nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(options.Hosts, options.DBName, options.CatalogPrefix,
		true /* ignore internal errors */, vdb)
	nmaReadCatalogEditorOp, err := makeNMAReadCatalogEditorOp(vdb)
	if err != nil {
		return instructions, err
	}

	instructions = append(
		instructions,
		&nmaHealthOp,
		&nmaGetNodesInfoOp,
		&nmaReadCatalogEditorOp,
	)

	return instructions, nil
}
