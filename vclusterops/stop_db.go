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

type VStopDatabaseOptions struct {
	/* part 1: basic db info */
	DatabaseOptions

	/* part 2: eon db info */
	DrainSeconds *int // time in seconds to wait for database users' disconnection

	/* part 3: hidden info */

	CheckUserConn *bool // whether check user connection
	ForceKill     *bool // whether force kill connections
}

type VStopDatabaseInfo struct {
	DBName       string
	Hosts        []string
	UserName     string
	Password     *string
	DrainSeconds *int
	IsEon        bool
}

func VStopDatabaseOptionsFactory() VStopDatabaseOptions {
	opt := VStopDatabaseOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (options *VStopDatabaseOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()

	options.CheckUserConn = new(bool)
	options.ForceKill = new(bool)
}

func (options *VStopDatabaseOptions) validateRequiredOptions(log vlog.Printer) error {
	err := options.validateBaseOptions("stop_db", log)
	if err != nil {
		return err
	}

	return nil
}

func (options *VStopDatabaseOptions) validateEonOptions(config *ClusterConfig, log vlog.Printer) error {
	// if db is enterprise db and we see --drain-seconds, we will ignore it
	isEon, err := options.isEonMode(config)
	if err != nil {
		return err
	}

	if !isEon {
		if options.DrainSeconds != nil {
			log.PrintInfo("Notice: --drain-seconds option will be ignored because database is in enterprise mode." +
				" Connection draining is only available in eon mode.")
		}
		options.DrainSeconds = nil
	} else if options.DrainSeconds == nil {
		// if db is eon db and we do not see --drain-seconds, we will set it to 60 seconds (default value)
		options.DrainSeconds = new(int)
		*options.DrainSeconds = util.DefaultDrainSeconds
	}
	return nil
}

func (options *VStopDatabaseOptions) validateExtraOptions() error {
	return nil
}

func (options *VStopDatabaseOptions) validateParseOptions(config *ClusterConfig, log vlog.Printer) error {
	// batch 1: validate required parameters
	err := options.validateRequiredOptions(log)
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = options.validateEonOptions(config, log)
	if err != nil {
		return err
	}
	// batch 3: validate all other params
	err = options.validateExtraOptions()
	if err != nil {
		return err
	}
	return nil
}

// resolve hostnames to be IPs
func (options *VStopDatabaseOptions) analyzeOptions() (err error) {
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

func (options *VStopDatabaseOptions) validateAnalyzeOptions(config *ClusterConfig, log vlog.Printer) error {
	if err := options.validateParseOptions(config, log); err != nil {
		return err
	}
	return options.analyzeOptions()
}

func (vcc *VClusterCommands) VStopDatabase(options *VStopDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.validateAnalyzeOptions(options.Config, vcc.Log)
	if err != nil {
		return err
	}

	// build stopDBInfo from config file and options
	stopDBInfo := new(VStopDatabaseInfo)
	stopDBInfo.UserName = *options.UserName
	stopDBInfo.Password = options.Password
	stopDBInfo.DrainSeconds = options.DrainSeconds
	stopDBInfo.DBName, stopDBInfo.Hosts, err = options.getNameAndHosts(options.Config)
	if err != nil {
		return err
	}

	stopDBInfo.IsEon, err = options.isEonMode(options.Config)
	if err != nil {
		return err
	}

	instructions, err := vcc.produceStopDBInstructions(stopDBInfo, options)
	if err != nil {
		return fmt.Errorf("fail to production instructions: %w", err)
	}

	// Create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to stop database: %w", runError)
	}

	return nil
}

// produceStopDBInstructions will build a list of instructions to execute for
// the stop db operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful stop_db:
//   - Get up nodes through https call
//   - Sync catalog through the first up node
//   - Stop db through the first up node
//   - Check there is not any database running
func (vcc *VClusterCommands) produceStopDBInstructions(stopDBInfo *VStopDatabaseInfo,
	options *VStopDatabaseOptions,
) ([]ClusterOp, error) {
	var instructions []ClusterOp

	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if stopDBInfo.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	httpsGetUpNodesOp, err := makeHTTPSGetUpNodesOp(vcc.Log, stopDBInfo.DBName, stopDBInfo.Hosts,
		usePassword, *options.UserName, stopDBInfo.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &httpsGetUpNodesOp)

	if stopDBInfo.IsEon {
		httpsSyncCatalogOp, e := makeHTTPSSyncCatalogOpWithoutHosts(vcc.Log, usePassword, *options.UserName, stopDBInfo.Password)
		if e != nil {
			return instructions, e
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	} else {
		vcc.Log.PrintInfo("Skipping sync catalog for an enterprise database")
	}

	httpsStopDBOp, err := makeHTTPSStopDBOp(vcc.Log, usePassword, *options.UserName, stopDBInfo.Password, stopDBInfo.DrainSeconds)
	if err != nil {
		return instructions, err
	}

	httpsCheckDBRunningOp, err := makeHTTPCheckRunningDBOp(vcc.Log, stopDBInfo.Hosts,
		usePassword, *options.UserName, stopDBInfo.Password, StopDB)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsStopDBOp,
		&httpsCheckDBRunningOp,
	)

	return instructions, nil
}
