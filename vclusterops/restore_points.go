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

type VShowRestorePointsOptions struct {
	DatabaseOptions
}

func VShowRestorePointsFactory() VShowRestorePointsOptions {
	opt := VShowRestorePointsOptions{}
	// set default values to the params
	opt.setDefaultValues()

	return opt
}

func (opt *VShowRestorePointsOptions) validateParseOptions(logger vlog.Printer) error {
	err := opt.validateBaseOptions("show_restore_points", logger)
	if err != nil {
		return err
	}
	if *opt.HonorUserInput {
		err := util.ValidateCommunalStorageLocation(*opt.CommunalStorageLocation)
		if err != nil {
			return err
		}
	}

	return nil
}

// analyzeOptions will modify some options based on what is chosen
func (opt *VShowRestorePointsOptions) analyzeOptions() (err error) {
	// we analyze host names when HonorUserInput is set, otherwise we use hosts in yaml config
	if *opt.HonorUserInput {
		// resolve RawHosts to be IP addresses
		hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, opt.Ipv6.ToBool())
		if err != nil {
			return err
		}
		opt.Hosts = hostAddresses
	}
	return nil
}

func (opt *VShowRestorePointsOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := opt.validateParseOptions(logger); err != nil {
		return err
	}
	return opt.analyzeOptions()
}

// VShowRestorePoints can query the restore points from an archive
func (vcc *VClusterCommands) VShowRestorePoints(options *VShowRestorePointsOptions) (restorePoints []RestorePoint, err error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	err = options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return restorePoints, err
	}

	// get db name and hosts from config file and options
	dbName, hosts, err := options.getNameAndHosts(options.Config)
	if err != nil {
		return restorePoints, err
	}

	options.DBName = &dbName
	options.Hosts = hosts
	// get communal storage location from config file and options
	options.CommunalStorageLocation, err = options.getCommunalStorageLocation(options.Config)
	if err != nil {
		return restorePoints, err
	}

	// produce show restore points instructions
	instructions, err := vcc.produceShowRestorePointsInstructions(options)
	if err != nil {
		return restorePoints, fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		return restorePoints, fmt.Errorf("fail to show restore points: %w", runError)
	}
	restorePoints = clusterOpEngine.execContext.restorePoints
	return restorePoints, nil
}

// The generated instructions will later perform the following operations necessary
// for a successful show_restore_points:
//   - Check NMA connectivity
//   - Check Vertica versions
//   - Run show restore points on the target node
func (vcc *VClusterCommands) produceShowRestorePointsInstructions(options *VShowRestorePointsOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	hosts := options.Hosts
	initiator := getInitiator(hosts)
	bootstrapHost := []string{initiator}

	nmaHealthOp := makeNMAHealthOp(vcc.Log, hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(vcc.Log, hosts, true, true /*IsEon*/)

	nmaShowRestorePointOp := makeNMAShowRestorePointsOp(vcc.Log, bootstrapHost, *options.DBName,
		*options.CommunalStorageLocation, options.ConfigurationParameters)

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&nmaShowRestorePointOp)
	return instructions, nil
}
