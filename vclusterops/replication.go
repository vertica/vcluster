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

type TargetDatabaseOptions struct {
	TargetHosts    []string
	TargetDB       string
	TargetUserName string
	TargetPassword *string
}

type ReplicationOptions struct {
	TableOrSchemaName string
	IncludePattern    string
	ExcludePattern    string
	TargetNamespace   string
}

type VReplicationDatabaseOptions struct {
	/* part 1: basic db info */
	DatabaseOptions

	/* part 2: replication info */
	TargetDatabaseOptions
	SourceTLSConfig string
	SandboxName     string
	Async           bool
	ReplicationOptions
}

func VReplicationDatabaseFactory() VReplicationDatabaseOptions {
	options := VReplicationDatabaseOptions{}
	// set default values to the params
	options.setDefaultValues()
	return options
}

func (options *VReplicationDatabaseOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions(ReplicationStartCmd, logger)
	if err != nil {
		return err
	}
	return nil
}

func (options *VReplicationDatabaseOptions) validateEonOptions() error {
	if !options.IsEon {
		return fmt.Errorf("replication is only supported in Eon mode")
	}
	return nil
}

func (options *VReplicationDatabaseOptions) validateExtraOptions() error {
	if len(options.TargetHosts) == 0 {
		return fmt.Errorf("must specify a target host or target host list")
	}

	// valiadate target database
	if options.TargetDB == "" {
		return fmt.Errorf("must specify a target database name")
	}
	err := util.ValidateDBName(options.TargetDB)
	if err != nil {
		return err
	}

	// need to provide a password or TLSconfig if source and target username are different
	if options.TargetUserName != options.UserName {
		if options.TargetPassword == nil && options.SourceTLSConfig == "" {
			return fmt.Errorf("only trust authentication can support username without password or TLSConfig")
		}
	}

	if options.SandboxName != "" {
		err := util.ValidateSandboxName(options.SandboxName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (options *VReplicationDatabaseOptions) validateFineGrainedReplicationOptions() error {
	if options.TableOrSchemaName != "" {
		err := util.ValidateQualifiedObjectNamePattern(options.TableOrSchemaName, false)
		if err != nil {
			return err
		}
	}

	if options.IncludePattern != "" {
		err := util.ValidateQualifiedObjectNamePattern(options.IncludePattern, true)
		if err != nil {
			return err
		}
	}

	if options.ExcludePattern != "" {
		err := util.ValidateQualifiedObjectNamePattern(options.ExcludePattern, true)
		if err != nil {
			return err
		}
	}

	if options.TargetNamespace != "" {
		err := util.ValidateName(options.TargetNamespace, "target-namespace", true)
		if err != nil {
			return err
		}
	}
	return nil
}

func (options *VReplicationDatabaseOptions) validateParseOptions(logger vlog.Printer) error {
	// batch 1: validate required params
	err := options.validateRequiredOptions(logger)
	if err != nil {
		return err
	}

	// batch 2: validate eon params
	err = options.validateEonOptions()
	if err != nil {
		return err
	}

	// batch 3: validate auth params
	err = options.validateAuthOptions(ReplicationStartCmd.CmdString(), logger)
	if err != nil {
		return err
	}

	// batch 4: validate all other params
	err = options.validateExtraOptions()
	if err != nil {
		return err
	}

	// batch 5: validate fine-grained database replication options
	err = options.validateFineGrainedReplicationOptions()
	if err != nil {
		return err
	}
	return nil
}

// analyzeOptions will modify some options based on what is chosen
func (options *VReplicationDatabaseOptions) analyzeOptions() (err error) {
	if len(options.TargetHosts) > 0 {
		// resolve RawHosts to be IP addresses
		options.TargetHosts, err = util.ResolveRawHostsToAddresses(options.TargetHosts, options.IPv6)
		if err != nil {
			return err
		}
	}
	// we analyze host names when it is set in user input, otherwise we use hosts in yaml config
	if len(options.RawHosts) > 0 {
		// resolve RawHosts to be IP addresses
		hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, options.IPv6)
		if err != nil {
			return err
		}
		options.Hosts = hostAddresses
	}
	return nil
}

func (options *VReplicationDatabaseOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := options.validateParseOptions(logger); err != nil {
		return err
	}
	return options.analyzeOptions()
}

// VReplicateDatabase can copy all table data and metadata from this cluster to another
func (vcc VClusterCommands) VReplicateDatabase(options *VReplicationDatabaseOptions) (int64, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */

	// validate and analyze options
	err := options.validateAnalyzeOptions(vcc.Log)
	if err != nil {
		return 0, err
	}

	// retrieve information from the database to accurately determine the state of each node in both the main cluster and a given sandbox
	vdb := makeVCoordinationDatabase()
	err = vcc.getVDBFromRunningDBIncludeSandbox(&vdb, &options.DatabaseOptions, options.SandboxName)
	if err != nil {
		return 0, err
	}

	asyncReplicationTransactionID := new(int64)

	// produce database replication instructions
	instructions, err := vcc.produceDBReplicationInstructions(options, &vdb, asyncReplicationTransactionID)
	if err != nil {
		return 0, fmt.Errorf("fail to produce instructions, %w", err)
	}

	// create a VClusterOpEngine, and add certs to the engine
	clusterOpEngine := makeClusterOpEngine(instructions, options)

	// give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.run(vcc.Log)
	if runError != nil {
		if strings.Contains(runError.Error(), "EnableConnectCredentialForwarding is false") {
			runError = fmt.Errorf("target database authentication failed, need to do one of the following things: " +
				"1. provide tlsconfig or target username with password " +
				"2. set EnableConnectCredentialForwarding to True in source database using vsql " +
				"3. configure a Trust Authentication in target database using vsql")
		}
		return 0, fmt.Errorf("fail to replicate database: %w", runError)
	}
	return *asyncReplicationTransactionID, nil
}

// The generated instructions will later perform the following operations necessary
// for a successful replication.
//   - Check NMA connectivity
//   - Check Vertica versions
//   - Replicate database
func (vcc VClusterCommands) produceDBReplicationInstructions(options *VReplicationDatabaseOptions,
	vdb *VCoordinationDatabase, asyncReplicationTransactionID *int64) ([]clusterOp, error) {
	var instructions []clusterOp

	// need username for https operations in source database
	err := options.setUsePasswordAndValidateUsernameIfNeeded(vcc.Log)
	if err != nil {
		return instructions, err
	}

	// verify the username for connecting to the target database
	targetUsePassword := false
	if options.TargetPassword != nil {
		targetUsePassword = true
		if options.TargetUserName == "" {
			username, e := util.GetCurrentUsername()
			if e != nil {
				return instructions, e
			}
			options.TargetUserName = username
		}
		vcc.Log.Info("Current target username", "username", options.TargetUserName)
	}

	initiatorTargetHost := getInitiator(options.TargetHosts)
	if options.Async {
		nmaHealthOp := makeNMAHealthOp(append(options.Hosts, options.TargetHosts...))

		transactionIDs := &[]int64{}

		nmaReplicationStatusData := nmaReplicationStatusRequestData{}
		nmaReplicationStatusData.DBName = options.TargetDB
		nmaReplicationStatusData.ExcludedTransactionIDs = []int64{}
		nmaReplicationStatusData.GetTransactionIDsOnly = true
		nmaReplicationStatusData.TransactionID = 0
		nmaReplicationStatusData.UserName = options.TargetUserName
		nmaReplicationStatusData.Password = options.TargetPassword

		nmaReplicationStatusOp, err := makeNMAReplicationStatusOp(options.TargetHosts, targetUsePassword,
			&nmaReplicationStatusData, options.SandboxName, vdb, transactionIDs)
		if err != nil {
			return instructions, err
		}

		nmaReplicationData := nmaStartReplicationRequestData{}
		nmaReplicationData.DBName = options.DBName
		nmaReplicationData.ExcludePattern = options.ExcludePattern
		nmaReplicationData.IncludePattern = options.IncludePattern
		nmaReplicationData.TableOrSchemaName = options.TableOrSchemaName
		nmaReplicationData.Username = options.UserName
		nmaReplicationData.Password = options.Password
		nmaReplicationData.TargetDBName = options.TargetDB
		nmaReplicationData.TargetHost = initiatorTargetHost
		nmaReplicationData.TargetNamespace = options.TargetNamespace
		nmaReplicationData.TargetUserName = options.TargetUserName
		nmaReplicationData.TargetPassword = options.TargetPassword
		nmaReplicationData.TLSConfig = options.SourceTLSConfig

		nmaStartReplicationOp, err := makeNMAReplicationStartOp(options.Hosts, options.usePassword, targetUsePassword,
			&nmaReplicationData, vdb)
		if err != nil {
			return instructions, err
		}

		nmaPollReplicationStatusOp, err := makeNMAPollReplicationStatusOp(&options.TargetDatabaseOptions, targetUsePassword,
			options.SandboxName, vdb, transactionIDs, asyncReplicationTransactionID)
		if err != nil {
			return instructions, err
		}

		instructions = append(instructions,
			&nmaHealthOp,
			&nmaReplicationStatusOp,
			&nmaStartReplicationOp,
			&nmaPollReplicationStatusOp,
		)
	} else {
		httpsDisallowMultipleNamespacesOp, err := makeHTTPSDisallowMultipleNamespacesOp(options.Hosts,
			options.usePassword, options.UserName, options.Password, options.SandboxName, vdb)
		if err != nil {
			return instructions, err
		}

		httpsStartReplicationOp, err := makeHTTPSStartReplicationOp(options.DBName, options.Hosts, options.usePassword,
			options.UserName, options.Password, targetUsePassword, &options.TargetDatabaseOptions, initiatorTargetHost,
			options.SourceTLSConfig, options.SandboxName, vdb)
		if err != nil {
			return instructions, err
		}

		instructions = append(instructions,
			&httpsDisallowMultipleNamespacesOp,
			&httpsStartReplicationOp,
		)
	}

	return instructions, nil
}
