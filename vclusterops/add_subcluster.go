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

const (
	ControlSetSizeDefaultValue = -1
	ControlSetSizeLowerBound   = 1
	ControlSetSizeUpperBound   = 120
)

type VAddSubclusterOptions struct {
	// part 1: basic db info
	DatabaseOptions
	// part 2: subcluster info
	SCName         *string
	SCHosts        []string
	SCRawHosts     []string
	IsPrimary      *bool
	ControlSetSize *int
	CloneSC        *string
}

type VAddSubclusterInfo struct {
	DBName         string
	Hosts          []string
	UserName       string
	Password       *string
	SCName         string
	SCHosts        []string
	IsPrimary      bool
	ControlSetSize int
	CloneSC        string
}

func VAddSubclusterOptionsFactory() VAddSubclusterOptions {
	opt := VAddSubclusterOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (options *VAddSubclusterOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()

	options.SCName = new(string)
	options.IsPrimary = new(bool)
	options.ControlSetSize = new(int)
	*options.ControlSetSize = -1
	options.CloneSC = new(string)
}

func (options *VAddSubclusterOptions) validateRequiredOptions(log vlog.Printer) error {
	err := options.validateBaseOptions("db_add_subcluster", log)
	if err != nil {
		return err
	}

	if *options.SCName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}
	return nil
}

func (options *VAddSubclusterOptions) validateEonOptions(config *ClusterConfig) error {
	isEon, err := options.isEonMode(config)
	if err != nil {
		return err
	}

	if !isEon {
		return fmt.Errorf("add subcluster is only supported in Eon mode")
	}
	return nil
}

func (options *VAddSubclusterOptions) validateExtraOptions(log vlog.Printer) error {
	// control-set-size can only be -1 or [1 to 120]
	if !(*options.ControlSetSize == ControlSetSizeDefaultValue ||
		(*options.ControlSetSize >= ControlSetSizeLowerBound && *options.ControlSetSize <= ControlSetSizeUpperBound)) {
		return fmt.Errorf("control-set-size is out of bounds: valid values are %d or [%d to %d]",
			ControlSetSizeDefaultValue, ControlSetSizeLowerBound, ControlSetSizeUpperBound)
	}

	if *options.CloneSC != "" {
		// TODO remove this log after we supported subcluster clone
		log.PrintWarning("option CloneSC is not implemented yet so it will be ignored")
	}

	// verify the hosts of new subcluster does not exist in current database
	if len(options.SCHosts) > 0 {
		hostSet := make(map[string]struct{})
		for _, host := range options.SCHosts {
			hostSet[host] = struct{}{}
		}
		dupHosts := []string{}
		for _, host := range options.Hosts {
			if _, exist := hostSet[host]; exist {
				dupHosts = append(dupHosts, host)
			}
		}
		if len(dupHosts) > 0 {
			return fmt.Errorf("new subcluster has hosts %v which already exist in database %s", dupHosts, *options.DBName)
		}

		// TODO remove this log after we supported adding subcluster with nodes
		log.PrintWarning("options SCRawHosts and SCHosts are not implemented yet so they will be ignored")
	}

	return nil
}

func (options *VAddSubclusterOptions) validateParseOptions(config *ClusterConfig, vcc *VClusterCommands) error {
	// batch 1: validate required parameters
	err := options.validateRequiredOptions(vcc.Log)
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = options.validateEonOptions(config)
	if err != nil {
		return err
	}
	// batch 3: validate all other params
	err = options.validateExtraOptions(vcc.Log)
	if err != nil {
		return err
	}
	return nil
}

// resolve hostnames to be IPs
func (options *VAddSubclusterOptions) analyzeOptions() (err error) {
	// we analyze hostnames when HonorUserInput is set, otherwise we use hosts in yaml config
	if *options.HonorUserInput {
		// resolve RawHosts to be IP addresses
		options.Hosts, err = util.ResolveRawHostsToAddresses(options.RawHosts, options.Ipv6.ToBool())
		if err != nil {
			return err
		}
	}

	// resolve SCRawHosts to be IP addresses
	options.SCHosts, err = util.ResolveRawHostsToAddresses(options.SCRawHosts, options.Ipv6.ToBool())
	if err != nil {
		return err
	}

	return nil
}

func (options *VAddSubclusterOptions) validateAnalyzeOptions(config *ClusterConfig, vcc *VClusterCommands) error {
	if err := options.validateParseOptions(config, vcc); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// VAddSubcluster can add a new subcluster to a running database
func (vcc *VClusterCommands) VAddSubcluster(options *VAddSubclusterOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err := options.validateAnalyzeOptions(options.Config, vcc)
	if err != nil {
		return err
	}

	// build addSubclusterInfo from config file and options
	addSubclusterInfo := VAddSubclusterInfo{
		UserName:       *options.UserName,
		Password:       options.Password,
		SCName:         *options.SCName,
		SCHosts:        options.SCHosts,
		IsPrimary:      *options.IsPrimary,
		ControlSetSize: *options.ControlSetSize,
		CloneSC:        *options.CloneSC,
	}
	addSubclusterInfo.DBName, addSubclusterInfo.Hosts, err = options.getNameAndHosts(options.Config)
	if err != nil {
		return err
	}

	instructions, err := vcc.produceAddSubclusterInstructions(&addSubclusterInfo, options)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// Create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return fmt.Errorf("fail to add subcluster %s, %w", addSubclusterInfo.SCName, runError)
	}

	return nil
}

// produceAddSubclusterInstructions will build a list of instructions to execute for
// the add subcluster operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful add_subcluster:
//   - TODO: add nma connectivity check and nma version check
//   - Get UP nodes through HTTPS call, if any node is UP then the DB is UP and ready for adding a new subcluster
//   - Add the subcluster catalog object through HTTPS call, and check the response to error out
//     if the subcluster name already exists or the db is an enterprise db
//   - Check if the new subcluster is created in database through HTTPS call
//   - TODO: add new nodes to the subcluster
func (vcc *VClusterCommands) produceAddSubclusterInstructions(addSubclusterInfo *VAddSubclusterInfo,
	options *VAddSubclusterOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	// when password is specified, we will use username/password to call https endpoints
	usePassword := false
	if addSubclusterInfo.Password != nil {
		usePassword = true
		err := options.validateUserName(vcc.Log)
		if err != nil {
			return instructions, err
		}
	}

	username := *options.UserName
	httpsGetUpNodesOp, err := makeHTTPSGetUpNodesOp(vcc.Log, addSubclusterInfo.DBName, addSubclusterInfo.Hosts,
		usePassword, username, addSubclusterInfo.Password)
	if err != nil {
		return instructions, err
	}

	httpsAddSubclusterOp, err := makeHTTPSAddSubclusterOp(vcc.Log, usePassword, username, addSubclusterInfo.Password,
		addSubclusterInfo.SCName, addSubclusterInfo.IsPrimary, addSubclusterInfo.ControlSetSize)
	if err != nil {
		return instructions, err
	}

	httpsCheckSubclusterOp, err := makeHTTPSCheckSubclusterOp(vcc.Log, usePassword, username, addSubclusterInfo.Password,
		addSubclusterInfo.SCName, addSubclusterInfo.IsPrimary, addSubclusterInfo.ControlSetSize)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsGetUpNodesOp,
		&httpsAddSubclusterOp,
		&httpsCheckSubclusterOp,
	)

	return instructions, nil
}
