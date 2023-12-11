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
	"os"

	"github.com/vertica/vcluster/vclusterops/util"
)

// VDropDatabaseOptions adds to VCreateDatabaseOptions the option to force delete directories.
type VDropDatabaseOptions struct {
	VCreateDatabaseOptions
	ForceDelete *bool // whether force delete directories
}

func VDropDatabaseOptionsFactory() VDropDatabaseOptions {
	opt := VDropDatabaseOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

// TODO: call this func when honor-user-input is implemented
func (options *VDropDatabaseOptions) AnalyzeOptions() error {
	hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	options.Hosts = hostAddresses
	return nil
}

func (options *VDropDatabaseOptions) validateAnalyzeOptions() error {
	if *options.DBName == "" {
		return fmt.Errorf("database name must be provided")
	}
	return nil
}

func (vcc *VClusterCommands) VDropDatabase(options *VDropDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.validateAnalyzeOptions()
	if err != nil {
		return err
	}

	// Analyze to produce vdb info for drop db use
	vdb := makeVCoordinationDatabase()

	// TODO: load from options if HonorUserInput is true

	// load vdb info from the YAML config file
	var configDir string

	if options.ConfigDirectory != nil {
		configDir = *options.ConfigDirectory
	} else {
		currentDir, e := os.Getwd()
		if e != nil {
			return fmt.Errorf("fail to get current directory")
		}
		configDir = currentDir
	}

	clusterConfig, err := ReadConfig(configDir, vcc.Log)
	if err != nil {
		return err
	}
	err = vdb.setFromClusterConfig(*options.DBName, &clusterConfig)
	if err != nil {
		return err
	}

	// produce drop_db instructions
	instructions, err := vcc.produceDropDBInstructions(&vdb, options)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to drop database: %w", runError)
	}

	// if the database is successfully dropped, the config file will be removed
	// if failed to remove it, we will ask users to manually do it
	err = removeConfigFile(configDir, vcc.Log)
	if err != nil {
		vcc.Log.PrintWarning("Fail to remove the config file(s), please manually clean up under directory %s", configDir)
	}

	return nil
}

// produceDropDBInstructions will build a list of instructions to execute for
// the drop db operation
//
// The generated instructions will later perform the following operations necessary
// for a successful drop_db:
//   - Check NMA connectivity
//   - Check to see if any dbs running
//   - Delete directories
func (vcc *VClusterCommands) produceDropDBInstructions(vdb *VCoordinationDatabase, options *VDropDatabaseOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	hosts := vdb.HostList
	usePassword := false
	if options.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	nmaHealthOp := makeNMAHealthOp(vcc.Log, hosts)

	// when checking the running database,
	// drop_db has the same checking items with create_db
	checkDBRunningOp, err := makeHTTPSCheckRunningDBOp(vcc.Log, hosts, usePassword,
		*options.UserName, options.Password, CreateDB)
	if err != nil {
		return instructions, err
	}

	nmaDeleteDirectoriesOp, err := makeNMADeleteDirectoriesOp(vcc.Log, vdb, *options.ForceDelete)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaHealthOp,
		&checkDBRunningOp,
		&nmaDeleteDirectoriesOp,
	)

	return instructions, nil
}
