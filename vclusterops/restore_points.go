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

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VShowRestorePointsOptions struct {
	DatabaseOptions
	// Optional arguments to list only restore points that
	// meet the specified condition(s)
	FilterOptions ShowRestorePointFilterOptions
}

func VShowRestorePointsFactory() VShowRestorePointsOptions {
	opt := VShowRestorePointsOptions{}
	// set default values to the params
	opt.setDefaultValues()

	opt.FilterOptions = ShowRestorePointFilterOptions{}

	return opt
}

func (p *ShowRestorePointFilterOptions) hasNonEmptyStartTimestamp() bool {
	return (p.StartTimestamp != "")
}

func (p *ShowRestorePointFilterOptions) hasNonEmptyEndTimestamp() bool {
	return (p.EndTimestamp != "")
}

// Check that all non-empty timestamps specified have valid date time or date only format,
// convert date only format to date time format when applicable, and make sure end timestamp
// is no earlier than start timestamp
func (p *ShowRestorePointFilterOptions) ValidateAndStandardizeTimestampsIfAny() (err error) {
	// shortcut of no validation needed
	if !p.hasNonEmptyStartTimestamp() && !p.hasNonEmptyEndTimestamp() {
		return nil
	}

	// check each individual timestamp in terms of format
	var dateTimeErr, dateOnlyErr error

	// try date time first
	parsedStartDatetime, dateTimeErr := util.IsEmptyOrValidTimeStr(util.DefaultDateTimeFormat, p.StartTimestamp)
	if dateTimeErr != nil {
		// fallback to date only
		parsedStartDatetime, dateOnlyErr = util.IsEmptyOrValidTimeStr(util.DefaultDateOnlyFormat, p.StartTimestamp)
		if dateOnlyErr != nil {
			// give up
			return fmt.Errorf("start timestamp %q is invalid; cannot parse as a datetime: %w; "+
				"cannot parse as a date as well: %w", p.StartTimestamp, dateTimeErr, dateOnlyErr)
		}
		// default value of time parsed from date only string is already indicating the start of a day
		// invoke this function here to only rewrite p.StartTimestamp in date time format
		util.FillInDefaultTimeForStartTimestamp(&p.StartTimestamp)
	}

	// try date time first
	parsedEndDatetime, dateTimeErr := util.IsEmptyOrValidTimeStr(util.DefaultDateTimeFormat, p.EndTimestamp)
	if dateTimeErr != nil {
		// fallback to date only
		_, dateOnlyErr = util.IsEmptyOrValidTimeStr(util.DefaultDateOnlyFormat, p.EndTimestamp)
		if dateOnlyErr != nil {
			// give up
			return fmt.Errorf("end timestamp %q is invalid; cannot parse as a datetime: %w; "+
				"cannot parse as a date as well: %w", p.EndTimestamp, dateTimeErr, dateOnlyErr)
		}
		// fill in default value for time and update the end timestamp
		parsedEndDatetime = util.FillInDefaultTimeForEndTimestamp(&p.EndTimestamp)
	}

	// check if endTime is after start time if both of them are non-empty
	if p.hasNonEmptyStartTimestamp() && p.hasNonEmptyEndTimestamp() {
		validRange := util.IsTimeEqualOrAfter(*parsedStartDatetime, *parsedEndDatetime)
		if !validRange {
			return errors.New("start timestamp must be before end timestamp")
		}
		return nil
	}

	return nil
}

func (opt *VShowRestorePointsOptions) validateParseOptions(logger vlog.Printer) error {
	err := opt.validateBaseOptions("show_restore_points", logger)
	if err != nil {
		return err
	}

	err = util.ValidateCommunalStorageLocation(opt.CommunalStorageLocation)
	if err != nil {
		return err
	}

	err = opt.FilterOptions.ValidateAndStandardizeTimestampsIfAny()
	if err != nil {
		return err
	}

	return nil
}

// analyzeOptions will modify some options based on what is chosen
func (opt *VShowRestorePointsOptions) analyzeOptions() (err error) {
	// we analyze host names when it is set in user input, otherwise we use hosts in yaml config
	if len(opt.RawHosts) > 0 {
		// resolve RawHosts to be IP addresses
		hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, opt.IPv6)
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
func (vcc VClusterCommands) VShowRestorePoints(options *VShowRestorePointsOptions) (restorePoints []RestorePoint, err error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// validate and analyze options
	err = options.validateAnalyzeOptions(vcc.Log)
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
func (vcc VClusterCommands) produceShowRestorePointsInstructions(options *VShowRestorePointsOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	hosts := options.Hosts
	initiator := getInitiator(hosts)
	bootstrapHost := []string{initiator}

	nmaHealthOp := makeNMAHealthOp(hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(hosts, true, true /*IsEon*/)

	nmaShowRestorePointOp := makeNMAShowRestorePointsOpWithFilterOptions(vcc.Log, bootstrapHost, options.DBName,
		options.CommunalStorageLocation, options.ConfigurationParameters, &options.FilterOptions)

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&nmaShowRestorePointOp)
	return instructions, nil
}
