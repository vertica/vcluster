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
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VStopDatabaseOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: eon db info
	DrainSeconds *int
	// part 3: hidden info
	CheckUserConn *bool
	ForceKill     *bool
}

type VStopDatabaseInfo struct {
	DBName       string
	Hosts        []string
	UserName     string
	Password     *string
	DrainSeconds *int
}

func VStopDatabaseOptionsFactory() VStopDatabaseOptions {
	opt := VStopDatabaseOptions{}
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (options *VStopDatabaseOptions) SetDefaultValues() {
	options.DatabaseOptions.SetDefaultValues()

	options.CheckUserConn = new(bool)
	options.ForceKill = new(bool)
}

func (options *VStopDatabaseOptions) validateRequiredOptions() error {
	err := options.ValidateBaseOptions("stop_db")
	if err != nil {
		return err
	}

	return nil
}

func (options *VStopDatabaseOptions) validateEonOptions(config *ClusterConfig) error {
	// if db is enterprise db and we see --drain-seconds, we will ignore it
	if !options.IsEonMode(config) {
		if options.DrainSeconds != nil {
			vlog.LogPrintInfoln("Notice: --drain-seconds option will be ignored because database is in enterprise mode." +
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

func (options *VStopDatabaseOptions) validateParseOptions(config *ClusterConfig) error {
	// batch 1: validate required parameters
	err := options.validateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = options.validateEonOptions(config)
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

func (options *VStopDatabaseOptions) ValidateAnalyzeOptions(config *ClusterConfig) error {
	if err := options.validateParseOptions(config); err != nil {
		return err
	}
	if err := options.analyzeOptions(); err != nil {
		return err
	}
	return nil
}

func (vcc *VClusterCommands) VStopDatabase(options *VStopDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig()
	if err != nil {
		return err
	}

	err = options.ValidateAnalyzeOptions(config)
	if err != nil {
		return err
	}

	// build stopDBInfo from config file and options
	stopDBInfo := new(VStopDatabaseInfo)
	stopDBInfo.UserName = *options.UserName
	stopDBInfo.Password = options.Password
	stopDBInfo.DrainSeconds = options.DrainSeconds
	stopDBInfo.DBName, stopDBInfo.Hosts = options.GetNameAndHosts(config)

	instructions, err := produceStopDBInstructions(stopDBInfo, options)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %s", err)
		return err
	}

	// Create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		vlog.LogPrintError("fail to stop database, %s", runError)
		return runError
	}

	return nil
}

// produceStopDBInstructions will build a list of instructions to execute for
// the stop db operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful stop_db:
//   - Get up nodes through https call
//   - Stop db on the first up node
//   - Check there is not any database running
func produceStopDBInstructions(stopDBInfo *VStopDatabaseInfo,
	options *VStopDatabaseOptions,
) ([]ClusterOp, error) {
	var instructions []ClusterOp

	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if stopDBInfo.Password != nil {
		usePassword = true
		err := options.ValidateUserName()
		if err != nil {
			return instructions, err
		}
	}

	httpsGetUpNodesOp := MakeHTTPSGetUpNodesOp("HTTPSGetUpNodesOp", stopDBInfo.DBName, stopDBInfo.Hosts,
		usePassword, *options.UserName, stopDBInfo.Password)

	httpsStopDBOp := MakeHTTPSStopDBOp("HTTPSStopDBOp", usePassword, *options.UserName, stopDBInfo.Password, stopDBInfo.DrainSeconds)

	httpsCheckDBRunningOp := MakeHTTPCheckRunningDBOp("HTTPSCheckDBRunningOp", stopDBInfo.Hosts,
		usePassword, *options.UserName, stopDBInfo.Password, StopDB)

	instructions = append(instructions,
		&httpsGetUpNodesOp,
		&httpsStopDBOp,
		&httpsCheckDBRunningOp,
	)

	return instructions, nil
}
