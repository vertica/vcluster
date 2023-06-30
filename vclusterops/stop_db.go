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
	"path/filepath"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"github.com/vertica/vcluster/vclusterops/vstruct"
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

func (options *VStopDatabaseOptions) ValidateRequiredOptions() error {
	// TODO remove below validations since they will be done in ValidateBasicOptions of command_line_options.go
	// we validate dbName and hosts when HonorUserInput is set, otherwise we use db_name and hosts in yaml config
	if *options.HonorUserInput {
		if *options.Name == "" {
			return fmt.Errorf("must specify a database name")
		}
		err := util.ValidateDBName(*options.Name)
		if err != nil {
			return err
		}

		if len(options.RawHosts) == 0 {
			return fmt.Errorf("must specify a host or host list")
		}
	}

	if options.Password == nil {
		vlog.LogPrintInfoln("no password specified, using none")
	}

	if options.ConfigDirectory != nil {
		err := util.AbsPathCheck(*options.ConfigDirectory)
		if err != nil {
			return fmt.Errorf("must specify an absolute path for the config directory")
		}
	}

	return nil
}

func (options *VStopDatabaseOptions) ValidateEonOptions(config *ClusterConfig) error {
	// if db is enterprise db and we see --drain-seconds, we will ignore it
	if !IsEonMode(options, config) {
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

func (options *VStopDatabaseOptions) ValidateExtraOptions() error {
	return nil
}

func (options *VStopDatabaseOptions) ValidateParseOptions(config *ClusterConfig) error {
	// batch 1: validate required parameters
	err := options.ValidateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = options.ValidateEonOptions(config)
	if err != nil {
		return err
	}
	// batch 3: validate all other params
	err = options.ValidateExtraOptions()
	if err != nil {
		return err
	}
	return nil
}

// resolve hostnames to be IPs
func (options *VStopDatabaseOptions) AnalyzeOptions() error {
	// we analyze hostnames when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		for _, host := range options.RawHosts {
			if host == "" {
				return fmt.Errorf("invalid empty host found in the provided host list")
			}
			addr, err := util.ResolveToOneIP(host, options.Ipv6.ToBool())
			if err != nil {
				return err
			}
			// use a list to respect user input order
			options.Hosts = append(options.Hosts, addr)
		}
	}
	return nil
}

func IsEonMode(options *VStopDatabaseOptions, config *ClusterConfig) bool {
	// when config file is not available, we use user input
	// at this time HonorUserInput must be true
	if config == nil {
		return options.IsEon.ToBool()
	}

	isEon := config.IsEon
	// if HonorUserInput is set, we choose the user input
	if options.IsEon != vstruct.NotSet && *options.HonorUserInput {
		isEon = options.IsEon.ToBool()
	}
	return isEon
}

func GetNameAndHosts(options *VStopDatabaseOptions, config *ClusterConfig) (dbName string, hosts []string) {
	// when config file is not available, we use user input
	// at this time HonorUserInput must be true
	if config == nil {
		return *options.Name, options.Hosts
	}

	dbName = config.DBName
	hosts = config.Hosts
	// if HonorUserInput is set, we choose the user input
	if *options.Name != "" && *options.HonorUserInput {
		dbName = *options.Name
	}
	if len(options.Hosts) > 0 && *options.HonorUserInput {
		hosts = options.Hosts
	}
	return dbName, hosts
}

func (options *VStopDatabaseOptions) ValidateAnalyzeOptions(config *ClusterConfig) error {
	if err := options.ValidateParseOptions(config); err != nil {
		return err
	}
	if err := options.AnalyzeOptions(); err != nil {
		return err
	}
	return nil
}

func (vcc *VClusterCommands) VStopDatabase(options *VStopDatabaseOptions) (string, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */
	// get config from vertica_cluster.yaml
	var configDir string
	if options.ConfigDirectory != nil {
		configDir = *options.ConfigDirectory
	} else {
		currentDir, err := os.Getwd()
		if err != nil && !*options.HonorUserInput {
			return "", fmt.Errorf("fail to get current directory, details: %w", err)
		}
		configDir = currentDir
	}

	var config *ClusterConfig
	if configDir != "" {
		configContent, err := ReadConfig(configDir)
		config = &configContent
		if err != nil {
			// when we cannot read config file, config points to an empty ClusterConfig with default values
			// we want to reset config to nil so we will use user input later rather than those default values
			config = nil
			vlog.LogPrintInfoln("Failed to read " + filepath.Join(configDir, ConfigFileName))
			// when the customer wants to use user input, we can ignore config file error
			if !*options.HonorUserInput {
				return "", err
			}
		}
	}

	err := options.ValidateAnalyzeOptions(config)
	if err != nil {
		return "", err
	}

	// build stopDBInfo from config file and options
	stopDBInfo := new(VStopDatabaseInfo)
	stopDBInfo.UserName = *options.UserName
	stopDBInfo.Password = options.Password
	stopDBInfo.DrainSeconds = options.DrainSeconds
	stopDBInfo.DBName, stopDBInfo.Hosts = GetNameAndHosts(options, config)

	instructions, err := produceStopDBInstructions(stopDBInfo)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %w", err)
		return "", err
	}

	// Create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		vlog.LogPrintError("fail to stop database, %w", runError)
		return "", runError
	}

	return stopDBInfo.DBName, nil
}

/*
	We expect that we will ultimately produce the following instructions:
	1. Get up nodes through https call
	2. Stop db on the first up node
	3. Check there is not any database running
*/

func produceStopDBInstructions(stopDBInfo *VStopDatabaseInfo) ([]ClusterOp, error) {
	var instructions []ClusterOp

	// when password is specified, we will use username/password to call https endpoints
	useHTTPPassword := false
	username := stopDBInfo.UserName
	if stopDBInfo.Password != nil {
		var errGetUser error
		if username == "" {
			username, errGetUser = util.GetCurrentUsername()
			if errGetUser != nil {
				return instructions, errGetUser
			}
		}
		vlog.LogInfo("Current username is %s", username)
		useHTTPPassword = true
	}

	httpsGetUpNodesOp := MakeHTTPSGetUpNodesOp("HTTPSGetUpNodesOp", stopDBInfo.DBName, stopDBInfo.Hosts,
		useHTTPPassword, username, stopDBInfo.Password)

	httpsStopDBOp := MakeHTTPSStopDBOp("HTTPSStopDBOp", useHTTPPassword, username, stopDBInfo.Password, stopDBInfo.DrainSeconds)

	httpsCheckDBRunningOp := MakeHTTPCheckRunningDBOp("HTTPSCheckDBRunningOp", stopDBInfo.Hosts,
		useHTTPPassword, username, stopDBInfo.Password, StopDB)

	instructions = append(instructions,
		&httpsGetUpNodesOp,
		&httpsStopDBOp,
		&httpsCheckDBRunningOp,
	)

	return instructions, nil
}
