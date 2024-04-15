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
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VReplicationDatabaseOptions struct {
	/* part 1: basic db info */
	DatabaseOptions

	/* part 2: replication info */
	TargetHosts     []string
	TargetDB        string
	TargetUserName  string
	TargetPassword  *string
	SourceTLSConfig string
}

func VReplicationDatabaseFactory() VReplicationDatabaseOptions {
	opt := VReplicationDatabaseOptions{}
	// set default values to the params
	opt.setDefaultValues()
	return opt
}

func (opt *VReplicationDatabaseOptions) validateEonOptions(_ vlog.Printer) error {
	if !opt.IsEon {
		return fmt.Errorf("replication is only supported in Eon mode")
	}
	return nil
}

func (opt *VReplicationDatabaseOptions) validateParseOptions(logger vlog.Printer) error {
	err := opt.validateEonOptions(logger)
	if err != nil {
		return err
	}
	if len(opt.TargetHosts) == 0 {
		return fmt.Errorf("must specify a target host or target host list")
	}

	// valiadate target database
	if opt.TargetDB == "" {
		return fmt.Errorf("must specify a target database name")
	}
	err = util.ValidateDBName(opt.TargetDB)
	if err != nil {
		return err
	}

	// need to provide a password or certs in source database
	if opt.Password == nil && (opt.Cert == "" || opt.Key == "") {
		return fmt.Errorf("must provide a password or certs")
	}

	// need to provide a password or TLSconfig if source and target username are different
	if opt.TargetUserName != opt.UserName {
		if opt.TargetPassword == nil && opt.SourceTLSConfig == "" {
			return fmt.Errorf("only trust authentication can support username without password or TLSConfig")
		}
	}

	return opt.validateBaseOptions(commandReplicationStart, logger)
}

// analyzeOptions will modify some options based on what is chosen
func (opt *VReplicationDatabaseOptions) analyzeOptions() (err error) {
	if len(opt.TargetHosts) > 0 {
		// resolve RawHosts to be IP addresses
		opt.TargetHosts, err = util.ResolveRawHostsToAddresses(opt.TargetHosts, opt.IPv6)
		if err != nil {
			return err
		}
	}
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

func (opt *VReplicationDatabaseOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := opt.validateParseOptions(logger); err != nil {
		return err
	}
	return opt.analyzeOptions()
}

// VReplicateDatabase can copy all table data and metadata from this cluster to another
func (vcc VClusterCommands) VReplicateDatabase(options *VReplicationDatabaseOptions) error {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// validate and analyze options
	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return err
	}

	// produce database replication instructions
	instructions, err := vcc.produceDBReplicationInstructions(options)
	if err != nil {
		return fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		if strings.Contains(runError.Error(), "EnableConnectCredentialForwarding is false") {
			runError = fmt.Errorf("target database authentication failed, need to do one of the following things: " +
				"1. provide tlsconfig or target username with password " +
				"2. set EnableConnectCredentialForwarding to True in source database using vsql " +
				"3. configure a Trust Authentication in target database using vsql")
		}
		return fmt.Errorf("fail to replicate database: %w", runError)
	}
	return nil
}

// The generated instructions will later perform the following operations necessary
// for a successful replication.
//   - Check nodes state
//   - Check NMA connectivity
//   - Check Vertica versions
//   - Replicate database
func (vcc VClusterCommands) produceDBReplicationInstructions(options *VReplicationDatabaseOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	// need username for https operations in source database
	err := options.setUsePassword(vcc.Log)
	if err != nil {
		return instructions, err
	}

	// verify the username for connecting to the target database
	targetUserPassword := false
	if options.TargetPassword != nil {
		targetUserPassword = true
		if options.TargetUserName == "" {
			username, e := util.GetCurrentUsername()
			if e != nil {
				return instructions, e
			}
			options.TargetUserName = username
		}
		vcc.Log.Info("Current target username", "username", options.TargetUserName)
	}

	httpsGetUpNodesOp, err := makeHTTPSCheckNodeStateOp(options.Hosts,
		options.usePassword, options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}

	nmaHealthOp := makeNMAHealthOp(options.Hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(options.Hosts, true, true /*IsEon*/)

	initiatorTargetHost := getInitiator(options.TargetHosts)
	httpsStartReplicationOp, err := makeHTTPSStartReplicationOp(options.DBName, options.Hosts, options.usePassword,
		options.UserName, options.Password, targetUserPassword, options.TargetDB, options.TargetUserName, initiatorTargetHost,
		options.TargetPassword, options.SourceTLSConfig)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&httpsGetUpNodesOp,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&httpsStartReplicationOp,
	)
	return instructions, nil
}
