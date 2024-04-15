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
	"regexp"
	"strconv"
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

// VCreateDatabaseOptions represents the available options when you create a database with
// VCreateDatabase.
type VCreateDatabaseOptions struct {
	/* part 1: basic db info */

	DatabaseOptions
	Policy            string // database restart policy
	SQLFile           string // SQL file to run (as dbadmin) immediately on database creation
	LicensePathOnNode string // required to be a fully qualified path

	/* part 2: eon db info */

	ShardCount               int    // number of shards in the database"
	DepotSize                string // depot size with two supported formats: % and KMGT, e.g., 50% or 10G
	GetAwsCredentialsFromEnv bool   // whether get AWS credentials from environmental variables
	// part 3: optional info
	ForceCleanupOnFailure     bool // whether force remove existing directories on failure
	ForceRemovalAtCreation    bool // whether force remove existing directories before creating the database
	SkipPackageInstall        bool // whether skip package installation
	TimeoutNodeStartupSeconds int  // timeout in seconds for polling node start up state

	/* part 3: new params originally in installer generated admintools.conf, now in create db op */

	Broadcast          bool // configure Spread to use UDP broadcast traffic between nodes on the same subnet
	P2p                bool // configure Spread to use point-to-point communication between all Vertica nodes
	LargeCluster       int  // whether enables a large cluster layout
	ClientPort         int  // for internal QA test only, do not abuse
	SpreadLogging      bool // whether enable spread logging
	SpreadLoggingLevel int  // spread logging level

	/* part 4: other params */

	SkipStartupPolling bool // whether skip startup polling
	GenerateHTTPCerts  bool // whether generate http certificates
	// If the path is set, the NMA will store the Vertica start command at the path
	// instead of executing it. This is useful in containerized environments where
	// you may not want to have both the NMA and Vertica server in the same container.
	// This feature requires version 24.2.0+.
	StartUpConf string

	/* hidden options (which cache information only) */

	// the host used for bootstrapping
	bootstrapHost []string
}

func VCreateDatabaseOptionsFactory() VCreateDatabaseOptions {
	opt := VCreateDatabaseOptions{}
	// set default values to the params
	opt.setDefaultValues()
	return opt
}

func (opt *VCreateDatabaseOptions) setDefaultValues() {
	opt.DatabaseOptions.setDefaultValues()

	// basic db info
	defaultPolicy := util.DefaultRestartPolicy
	opt.Policy = defaultPolicy

	// optional info
	opt.TimeoutNodeStartupSeconds = util.DefaultTimeoutSeconds

	// new params originally in installer generated admintools.conf, now in create db op
	opt.P2p = util.DefaultP2p
	opt.LargeCluster = util.DefaultLargeCluster
	opt.ClientPort = util.DefaultClientPort
	opt.SpreadLoggingLevel = util.DefaultSpreadLoggingLevel
}

func (opt *VCreateDatabaseOptions) validateRequiredOptions(logger vlog.Printer) error {
	// validate required parameters with default values
	if opt.Password == nil {
		opt.Password = new(string)
		*opt.Password = ""
		logger.Info("no password specified, using none")
	}

	if !util.StringInArray(opt.Policy, util.RestartPolicyList) {
		return fmt.Errorf("policy must be one of %v", util.RestartPolicyList)
	}

	// MUST provide a fully qualified path,
	// because vcluster could be executed outside of Vertica cluster hosts
	// so no point to resolve relative paths to absolute paths by checking
	// localhost, where vcluster is run
	//
	// empty string ("") will be converted to the default license path (/opt/vertica/share/license.key)
	// in the /bootstrap-catalog endpoint
	if opt.LicensePathOnNode != "" && !util.IsAbsPath(opt.LicensePathOnNode) {
		return fmt.Errorf("must provide a fully qualified path for license file")
	}

	return nil
}

func validateDepotSizePercent(size string) (bool, error) {
	if !strings.Contains(size, "%") {
		return true, nil
	}
	cleanSize := strings.TrimSpace(size)
	// example percent depot size: '40%'
	r := regexp.MustCompile(`^([-+]?\d+)(%)$`)

	// example of matches: [[40%, 40, %]]
	matches := r.FindAllStringSubmatch(cleanSize, -1)

	if len(matches) != 1 {
		return false, fmt.Errorf("%s is not a well-formatted whole-number percentage of the format <int>%%", size)
	}

	valueStr := matches[0][1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false, fmt.Errorf("%s is not a well-formatted whole-number percent of the format <int>%%", size)
	}

	if value > util.MaxDepotSize {
		return false, fmt.Errorf("depot-size %s is invalid, because it is greater than 100%%", size)
	} else if value < util.MinDepotSize {
		return false, fmt.Errorf("depot-size %s is invalid, because it is less than 0%%", size)
	}

	return true, nil
}

func validateDepotSizeBytes(size string) (bool, error) {
	// no need to validate for bytes if string contains '%'
	if strings.Contains(size, "%") {
		return true, nil
	}
	cleanSize := strings.TrimSpace(size)

	// example depot size: 1024K, 1024M, 2048G, 400T
	r := regexp.MustCompile(`^([-+]?\d+)([KMGT])$`)
	matches := r.FindAllStringSubmatch(cleanSize, -1)
	if len(matches) != 1 {
		return false, fmt.Errorf("%s is not a well-formatted whole-number size in bytes of the format <int>[KMGT]", size)
	}

	valueStr := matches[0][1]
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false, fmt.Errorf("depot size %s is not a well-formatted whole-number size in bytes of the format <int>[KMGT]", size)
	}
	if value <= 0 {
		return false, fmt.Errorf("depot size %s is not a valid size because it is <= 0", size)
	}
	return true, nil
}

// may need to go back to consolt print for vcluster commands
// so return error information
func validateDepotSize(size string) (bool, error) {
	validDepotPercent, err := validateDepotSizePercent(size)
	if !validDepotPercent {
		return validDepotPercent, err
	}
	validDepotBytes, err := validateDepotSizeBytes(size)
	if !validDepotBytes {
		return validDepotBytes, err
	}
	return true, nil
}

func (opt *VCreateDatabaseOptions) validateEonOptions() error {
	if opt.CommunalStorageLocation != "" {
		err := util.ValidateCommunalStorageLocation(opt.CommunalStorageLocation)
		if err != nil {
			return err
		}
		if opt.DepotPrefix == "" {
			return fmt.Errorf("must specify a depot path with commual storage location")
		}
		if opt.ShardCount == 0 {
			return fmt.Errorf("must specify a shard count greater than 0 with communal storage location")
		}
	}
	if opt.DepotPrefix != "" && opt.CommunalStorageLocation == "" {
		return fmt.Errorf("when depot path is given, communal storage location cannot be empty")
	}
	if opt.GetAwsCredentialsFromEnv && opt.CommunalStorageLocation == "" {
		return fmt.Errorf("AWS credentials are only used in Eon mode")
	}
	if opt.DepotSize != "" {
		if opt.DepotPrefix == "" {
			return fmt.Errorf("when depot size is given, depot path cannot be empty")
		}
		validDepotSize, err := validateDepotSize(opt.DepotSize)
		if !validDepotSize {
			return err
		}
	}
	return nil
}

func (opt *VCreateDatabaseOptions) validateExtraOptions() error {
	if opt.Broadcast && opt.P2p {
		return fmt.Errorf("cannot use both Broadcast and Point-to-point networking mode")
	}
	// -1 is the default large cluster value, meaning 120 control nodes
	if opt.LargeCluster != util.DefaultLargeCluster && (opt.LargeCluster < 1 || opt.LargeCluster > util.MaxLargeCluster) {
		return fmt.Errorf("must specify a valid large cluster value in range [1, 120]")
	}
	return nil
}

func (opt *VCreateDatabaseOptions) validateParseOptions(logger vlog.Printer) error {
	// validate base options
	err := opt.validateBaseOptions("create_db", logger)
	if err != nil {
		return err
	}

	// batch 1: validate required parameters without default values
	err = opt.validateRequiredOptions(logger)
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = opt.validateEonOptions()
	if err != nil {
		return err
	}
	// batch 3: validate all other params
	err = opt.validateExtraOptions()
	if err != nil {
		return err
	}
	return nil
}

// Do advanced analysis on the options inputs, like resolve hostnames to be IPs
func (opt *VCreateDatabaseOptions) analyzeOptions() error {
	// resolve RawHosts to be IP addresses
	if len(opt.RawHosts) > 0 {
		hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, opt.IPv6)
		if err != nil {
			return err
		}
		opt.Hosts = hostAddresses
	}

	// process correct catalog path, data path and depot path prefixes
	opt.CatalogPrefix = util.GetCleanPath(opt.CatalogPrefix)
	opt.DataPrefix = util.GetCleanPath(opt.DataPrefix)
	opt.DepotPrefix = util.GetCleanPath(opt.DepotPrefix)

	return nil
}

func (opt *VCreateDatabaseOptions) validateAnalyzeOptions(logger vlog.Printer) error {
	if err := opt.validateParseOptions(logger); err != nil {
		return err
	}
	return opt.analyzeOptions()
}

func (vcc VClusterCommands) VCreateDatabase(options *VCreateDatabaseOptions) (VCoordinationDatabase, error) {
	vcc.Log.Info("starting VCreateDatabase")

	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */
	// Analyze to produce vdb info, for later create db use and for cache db info
	vdb := makeVCoordinationDatabase()
	err := vdb.setFromCreateDBOptions(options, vcc.Log)
	if err != nil {
		return vdb, err
	}
	// produce instructions
	instructions, err := vcc.produceCreateDBInstructions(&vdb, options)
	if err != nil {
		vcc.Log.Error(err, "fail to produce create db instructions")
		return vdb, err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		vcc.Log.Error(err, "fail to create database")
		return vdb, err
	}
	return vdb, nil
}

// produceCreateDBInstructions will build a list of instructions to execute for
// the create db operation.
//
// The generated instructions will later perform the following operations necessary
// for a successful create_db:
//   - Check NMA connectivity
//   - Check to see if any dbs running
//   - Check NMA versions
//   - Prepare directories
//   - Get network profiles
//   - Bootstrap the database
//   - Run the catalog editor
//   - Start bootstrap node
//   - Wait for the bootstrapped node to be UP
//   - Create other nodes
//   - Reload spread
//   - Transfer config files
//   - Start all nodes of the database
//   - Poll node startup
//   - Create depot (Eon mode only)
//   - Mark design ksafe
//   - Install packages
//   - Sync catalog
func (vcc VClusterCommands) produceCreateDBInstructions(
	vdb *VCoordinationDatabase,
	options *VCreateDatabaseOptions) ([]clusterOp, error) {
	instructions, err := vcc.produceCreateDBBootstrapInstructions(vdb, options)
	if err != nil {
		return instructions, err
	}

	workerNodesInstructions, err := vcc.produceCreateDBWorkerNodesInstructions(vdb, options)
	if err != nil {
		return instructions, err
	}

	additionalInstructions, err := vcc.produceAdditionalCreateDBInstructions(vdb, options)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions, workerNodesInstructions...)
	instructions = append(instructions, additionalInstructions...)

	return instructions, nil
}

// produceCreateDBBootstrapInstructions returns the bootstrap instructions for create_db.
func (vcc VClusterCommands) produceCreateDBBootstrapInstructions(
	vdb *VCoordinationDatabase,
	options *VCreateDatabaseOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	hosts := vdb.HostList
	initiator := getInitiator(hosts)

	nmaHealthOp := makeNMAHealthOp(hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := makeNMAVerticaVersionOp(hosts, true, vdb.IsEon)

	// need username for https operations
	err := options.validateUserName(vcc.Log)
	if err != nil {
		return instructions, err
	}

	checkDBRunningOp, err := makeHTTPSCheckRunningDBOp(hosts, true, /* use password auth */
		options.UserName, options.Password, CreateDB)
	if err != nil {
		return instructions, err
	}

	nmaPrepareDirectoriesOp, err := makeNMAPrepareDirectoriesOp(vdb.HostNodeMap,
		options.ForceRemovalAtCreation, false /*for db revive*/)
	if err != nil {
		return instructions, err
	}

	nmaNetworkProfileOp := makeNMANetworkProfileOp(hosts)

	// should be only one bootstrap host
	// making it an array to follow the convention of passing a list of hosts to each operation
	bootstrapHost := []string{initiator}
	options.bootstrapHost = bootstrapHost
	nmaBootstrapCatalogOp, err := makeNMABootstrapCatalogOp(vdb, options, bootstrapHost)
	if err != nil {
		return instructions, err
	}

	nmaReadCatalogEditorOp, err := makeNMAReadCatalogEditorOpWithInitiator(bootstrapHost, vdb)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&nmaBootstrapCatalogOp,
		&nmaReadCatalogEditorOp,
	)

	if enabled, keyType := options.isSpreadEncryptionEnabled(); enabled {
		instructions = append(instructions,
			vcc.addEnableSpreadEncryptionOp(keyType),
		)
	}

	nmaStartNodeOp := makeNMAStartNodeOp(bootstrapHost, options.StartUpConf)

	httpsPollBootstrapNodeStateOp, err := makeHTTPSPollNodeStateOpWithTimeoutAndCommand(bootstrapHost, true, /* useHTTPPassword */
		options.UserName, options.Password, options.TimeoutNodeStartupSeconds, CreateDBCmd)
	if err != nil {
		return instructions, err
	}

	instructions = append(instructions,
		&nmaStartNodeOp,
		&httpsPollBootstrapNodeStateOp,
	)

	return instructions, nil
}

// produceCreateDBWorkerNodesInstructions returns the workder nodes' instructions for create_db.
func (vcc VClusterCommands) produceCreateDBWorkerNodesInstructions(
	vdb *VCoordinationDatabase,
	options *VCreateDatabaseOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	hosts := vdb.HostList
	bootstrapHost := options.bootstrapHost

	newNodeHosts := util.SliceDiff(hosts, bootstrapHost)
	if len(hosts) > 1 {
		httpsCreateNodeOp, err := makeHTTPSCreateNodeOp(newNodeHosts, bootstrapHost,
			true /* use password auth */, options.UserName, options.Password, vdb, "")
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsCreateNodeOp)
	}

	httpsReloadSpreadOp, err := makeHTTPSReloadSpreadOpWithInitiator(bootstrapHost,
		true /* use password auth */, options.UserName, options.Password)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, &httpsReloadSpreadOp)

	hostNodeMap := make(map[string]string)
	for _, host := range hosts {
		hostNodeMap[host] = vdb.HostNodeMap[host].CatalogPath
	}

	if len(hosts) > 1 {
		httpsGetNodesInfoOp, err := makeHTTPSGetNodesInfoOp(options.DBName, bootstrapHost,
			true /* use password auth */, options.UserName, options.Password, vdb, false, util.MainClusterSandbox)
		if err != nil {
			return instructions, err
		}

		httpsStartUpCommandOp, err := makeHTTPSStartUpCommandOp(true, /*use https password*/
			options.UserName, options.Password, vdb)
		if err != nil {
			return instructions, err
		}

		instructions = append(instructions, &httpsGetNodesInfoOp, &httpsStartUpCommandOp)

		produceTransferConfigOps(
			&instructions,
			bootstrapHost,
			vdb.HostList,
			vdb /*db configurations retrieved from a running db*/)
		nmaStartNewNodesOp := makeNMAStartNodeOpWithVDB(newNodeHosts, options.StartUpConf, vdb)
		instructions = append(instructions, &nmaStartNewNodesOp)
	}

	return instructions, nil
}

// produceAdditionalCreateDBInstructions returns additional instruction necessary for create_db.
func (vcc VClusterCommands) produceAdditionalCreateDBInstructions(vdb *VCoordinationDatabase,
	options *VCreateDatabaseOptions) ([]clusterOp, error) {
	var instructions []clusterOp

	hosts := vdb.HostList
	bootstrapHost := options.bootstrapHost
	username := options.UserName

	if !options.SkipStartupPolling {
		httpsPollNodeStateOp, err := makeHTTPSPollNodeStateOpWithTimeoutAndCommand(hosts, true, username, options.Password,
			options.TimeoutNodeStartupSeconds, CreateDBCmd)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsPollNodeStateOp)
	}

	if vdb.UseDepot {
		httpsCreateDepotOp, err := makeHTTPSCreateClusterDepotOp(vdb, bootstrapHost, true, username, options.Password)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsCreateDepotOp)
	}

	if len(hosts) >= ksafetyThreshold {
		httpsMarkDesignKSafeOp, err := makeHTTPSMarkDesignKSafeOp(bootstrapHost, true, username,
			options.Password, ksafeValueOne)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsMarkDesignKSafeOp)
	}

	if !options.SkipPackageInstall {
		httpsInstallPackagesOp, err := makeHTTPSInstallPackagesOp(bootstrapHost, true, username, options.Password,
			false /* forceReinstall */, true /* verbose */)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsInstallPackagesOp)
	}

	if vdb.IsEon {
		httpsSyncCatalogOp, err := makeHTTPSSyncCatalogOp(bootstrapHost, true, username, options.Password, CreateDBSyncCat)
		if err != nil {
			return instructions, err
		}
		instructions = append(instructions, &httpsSyncCatalogOp)
	}
	return instructions, nil
}

func (vcc VClusterCommands) addEnableSpreadEncryptionOp(keyType string) clusterOp {
	vcc.Log.Info("adding instruction to set key for spread encryption")
	op := makeNMASpreadSecurityOp(vcc.Log, keyType)
	return &op
}
