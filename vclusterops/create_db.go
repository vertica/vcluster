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

const (
	ksafetyThreshold = 3
	ksafeValue       = 1
)

// A good rule of thumb is to use normal strings unless you need nil.
// Normal strings are easier and safer to use in Go.
type VCreateDatabaseOptions struct {
	// part 1: basic db info
	DatabaseOptions
	Policy            *string
	SQLFile           *string
	LicensePathOnNode *string // required to be a fully qualified path
	// part 2: eon db info
	ShardCount                *int
	CommunalStorageLocation   *string
	CommunalStorageParamsPath *string
	DepotSize                 *string // like 10G
	GetAwsCredentialsFromEnv  *bool
	// part 3: optional info
	ConfigurationParameters   map[string]string
	ForceCleanupOnFailure     *bool
	ForceRemovalAtCreation    *bool
	SkipPackageInstall        *bool
	TimeoutNodeStartupSeconds *int
	// part 4: new params originally in installer generated admintools.conf, now in create db op
	Broadcast          *bool
	P2p                *bool
	LargeCluster       *int
	ClientPort         *int // for internal QA test only, do not abuse
	SpreadLogging      *bool
	SpreadLoggingLevel *int
	// part 5: other params
	SkipStartupPolling *bool
	ConfigDirectory    *string

	// hidden options (which cache information only)
	bootstrapHost []string
}

func VCreateDatabaseOptionsFactory() VCreateDatabaseOptions {
	opt := VCreateDatabaseOptions{}
	// without initialization it will panic
	opt.ConfigurationParameters = make(map[string]string)
	// set default values to the params
	opt.SetDefaultValues()

	return opt
}

func (opt *VCreateDatabaseOptions) SetDefaultValues() {
	opt.DatabaseOptions.SetDefaultValues()

	// basic db info
	defaultPolicy := util.DefaultRestartPolicy
	opt.Policy = &defaultPolicy
	opt.SQLFile = new(string)
	opt.LicensePathOnNode = new(string)

	// eon db Info
	opt.ShardCount = new(int)
	opt.CommunalStorageLocation = new(string)
	opt.CommunalStorageParamsPath = new(string)
	opt.DepotSize = new(string)
	opt.GetAwsCredentialsFromEnv = new(bool)

	// optional info
	opt.ForceCleanupOnFailure = new(bool)
	opt.ForceRemovalAtCreation = new(bool)
	opt.SkipPackageInstall = new(bool)

	defaultTimeoutNodeStartupSeconds := util.DefaultTimeoutSeconds
	opt.TimeoutNodeStartupSeconds = &defaultTimeoutNodeStartupSeconds

	// new params originally in installer generated admintools.conf, now in create db op
	opt.Broadcast = new(bool)

	defaultP2p := true
	opt.P2p = &defaultP2p

	defaultLargeCluster := -1
	opt.LargeCluster = &defaultLargeCluster

	defaultClientPort := util.DefaultClientPort
	opt.ClientPort = &defaultClientPort
	opt.SpreadLogging = new(bool)

	defaultSpreadLoggingLevel := -1
	opt.SpreadLoggingLevel = &defaultSpreadLoggingLevel

	// other params
	opt.SkipStartupPolling = new(bool)
}

func (opt *VCreateDatabaseOptions) CheckNilPointerParams() error {
	if err := opt.DatabaseOptions.CheckNilPointerParams(); err != nil {
		return err
	}

	// basic params
	if opt.Policy == nil {
		return util.ParamNotSetErrorMsg("policy")
	}
	if opt.SQLFile == nil {
		return util.ParamNotSetErrorMsg("sql")
	}
	if opt.LicensePathOnNode == nil {
		return util.ParamNotSetErrorMsg("license")
	}

	// eon params
	if opt.ShardCount == nil {
		return util.ParamNotSetErrorMsg("shard-count")
	}
	if opt.CommunalStorageLocation == nil {
		return util.ParamNotSetErrorMsg("communal-storage-location")
	}
	if opt.CommunalStorageParamsPath == nil {
		return util.ParamNotSetErrorMsg("communal_storage-params")
	}
	if opt.DepotSize == nil {
		return util.ParamNotSetErrorMsg("depot-size")
	}
	if opt.GetAwsCredentialsFromEnv == nil {
		return util.ParamNotSetErrorMsg("get-aws-credentials-from-env-vars")
	}

	return opt.CheckExtraNilPointerParams()
}

func (opt *VCreateDatabaseOptions) CheckExtraNilPointerParams() error {
	// optional params
	if opt.ForceCleanupOnFailure == nil {
		return util.ParamNotSetErrorMsg("force-cleanup-on-failure")
	}
	if opt.ForceRemovalAtCreation == nil {
		return util.ParamNotSetErrorMsg("force-removal-at-creation")
	}
	if opt.SkipPackageInstall == nil {
		return util.ParamNotSetErrorMsg("skip-package-install")
	}
	if opt.TimeoutNodeStartupSeconds == nil {
		return util.ParamNotSetErrorMsg("startup-timeout")
	}

	// other params
	if opt.Broadcast == nil {
		return util.ParamNotSetErrorMsg("broadcast")
	}
	if opt.P2p == nil {
		return util.ParamNotSetErrorMsg("point-to-point")
	}
	if opt.LargeCluster == nil {
		return util.ParamNotSetErrorMsg("large-cluster")
	}
	if opt.ClientPort == nil {
		return util.ParamNotSetErrorMsg("client-port")
	}
	if opt.SpreadLogging == nil {
		return util.ParamNotSetErrorMsg("spread-logging")
	}
	if opt.SpreadLoggingLevel == nil {
		return util.ParamNotSetErrorMsg("spread-logging-level")
	}
	if opt.SkipStartupPolling == nil {
		return util.ParamNotSetErrorMsg("skip-startup-polling")
	}

	return nil
}

func (opt *VCreateDatabaseOptions) validateRequiredOptions() error {
	// validate required parameters with default values
	if opt.Password == nil {
		opt.Password = new(string)
		*opt.Password = ""
		vlog.LogPrintInfoln("no password specified, using none")
	}

	if !util.StringInArray(*opt.Policy, util.RestartPolicyList) {
		return fmt.Errorf("policy must be one of %v", util.RestartPolicyList)
	}

	// MUST provide a fully qualified path,
	// because vcluster could be executed outside of Vertica cluster hosts
	// so no point to resolve relative paths to absolute paths by checking
	// localhost, where vcluster is run
	//
	// empty string ("") will be converted to the default license path (/opt/vertica/share/license.key)
	// in the /bootstrap-catalog endpoint
	if *opt.LicensePathOnNode != "" && !util.IsAbsPath(*opt.LicensePathOnNode) {
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
	if *opt.CommunalStorageLocation != "" {
		if *opt.DepotPrefix == "" {
			return fmt.Errorf("must specify a depot path with commual storage location")
		}
		if *opt.ShardCount == 0 {
			return fmt.Errorf("must specify a shard count greater than 0 with communal storage location")
		}
	}
	if *opt.DepotPrefix != "" && *opt.CommunalStorageLocation == "" {
		return fmt.Errorf("when depot path is given, communal storage location cannot be empty")
	}
	if *opt.GetAwsCredentialsFromEnv && *opt.CommunalStorageLocation == "" {
		return fmt.Errorf("AWS credentials are only used in Eon mode")
	}
	if *opt.DepotSize != "" {
		if *opt.DepotPrefix == "" {
			return fmt.Errorf("when depot size is given, depot path cannot be empty")
		}
		validDepotSize, err := validateDepotSize(*opt.DepotSize)
		if !validDepotSize {
			return err
		}
	}
	return nil
}

func (opt *VCreateDatabaseOptions) validateExtraOptions() error {
	if *opt.Broadcast && *opt.P2p {
		return fmt.Errorf("cannot use both Broadcast and Point-to-point networking mode")
	}
	// -1 is the default large cluster value, meaning 120 control nodes
	if *opt.LargeCluster != util.DefaultLargeCluster && (*opt.LargeCluster < 1 || *opt.LargeCluster > util.MaxLargeCluster) {
		return fmt.Errorf("must specify a valid large cluster value in range [1, 120]")
	}
	return nil
}

func (opt *VCreateDatabaseOptions) validateParseOptions() error {
	// check nil pointers in the required options
	err := opt.CheckNilPointerParams()
	if err != nil {
		return err
	}

	// validate base options
	err = opt.ValidateBaseOptions("create_db")
	if err != nil {
		return err
	}

	// batch 1: validate required parameters without default values
	err = opt.validateRequiredOptions()
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
	hostAddresses, err := util.ResolveRawHostsToAddresses(opt.RawHosts, opt.Ipv6.ToBool())
	if err != nil {
		return err
	}
	opt.Hosts = hostAddresses

	// process correct catalog path, data path and depot path prefixes
	*opt.CatalogPrefix = util.GetCleanPath(*opt.CatalogPrefix)
	*opt.DataPrefix = util.GetCleanPath(*opt.DataPrefix)
	*opt.DepotPrefix = util.GetCleanPath(*opt.DepotPrefix)
	if opt.ClientPort == nil {
		*opt.ClientPort = util.DefaultClientPort
	}
	if opt.LargeCluster == nil {
		*opt.LargeCluster = util.DefaultLargeCluster
	}
	return nil
}

func (opt *VCreateDatabaseOptions) ValidateAnalyzeOptions() error {
	if err := opt.validateParseOptions(); err != nil {
		return err
	}
	if err := opt.analyzeOptions(); err != nil {
		return err
	}
	return nil
}

func (vcc *VClusterCommands) VCreateDatabase(options *VCreateDatabaseOptions) (VCoordinationDatabase, error) {
	/*
	 *   - Produce Instructions
	 *   - Create a VClusterOpEngine
	 *   - Give the instructions to the VClusterOpEngine to run
	 */
	// Analyze to produce vdb info, for later create db use and for cache db info
	vdb := MakeVCoordinationDatabase()
	err := vdb.SetFromCreateDBOptions(options)
	if err != nil {
		return vdb, err
	}
	// produce instructions
	instructions, err := produceCreateDBInstructions(&vdb, options)
	if err != nil {
		vlog.LogPrintError("fail to produce instructions, %w", err)
		return vdb, err
	}

	// create a VClusterOpEngine, and add certs to the engine
	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		vlog.LogPrintError("fail to create database, %w", runError)
		return vdb, runError
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
func produceCreateDBInstructions(vdb *VCoordinationDatabase, options *VCreateDatabaseOptions) ([]ClusterOp, error) {
	instructions, err := produceBasicCreateDBInstructions(vdb, options)
	if err != nil {
		return instructions, err
	}
	additionalInstructions, err := produceAdditionalCreateDBInstructions(vdb, options)
	if err != nil {
		return instructions, err
	}
	instructions = append(instructions, additionalInstructions...)
	return instructions, nil
}

// produceBasicCreateDBInstructions returns the first set of instructions for create_db.
func produceBasicCreateDBInstructions(vdb *VCoordinationDatabase, options *VCreateDatabaseOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	hosts := vdb.HostList
	initiator := getInitiator(hosts)

	nmaHealthOp := MakeNMAHealthOp("NMAHealthOp", hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp("NMAVerticaVersionOp", hosts, true)

	// need username for https operations
	err := options.ValidateUserName()
	if err != nil {
		return instructions, err
	}

	checkDBRunningOp := MakeHTTPCheckRunningDBOp("HTTPCheckDBRunningOp", hosts,
		true /* use password auth */, *options.UserName, options.Password, CreateDB)

	nmaPrepareDirectoriesOp, err := MakeNMAPrepareDirectoriesOp("NMAPrepareDirectoriesOp", vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}

	nmaNetworkProfileOp := MakeNMANetworkProfileOp("NMANetworkProfileOp", hosts)

	// should be only one bootstrap host
	// making it an array to follow the convention of passing a list of hosts to each operation
	bootstrapHost := []string{initiator}
	options.bootstrapHost = bootstrapHost
	nmaBootstrapCatalogOp, err := MakeNMABootstrapCatalogOp("NMABootstrapCatalogOp", vdb, options, bootstrapHost)
	if err != nil {
		return instructions, err
	}

	nmaFetchVdbFromCatEdOp, err := MakeNMAFetchVdbFromCatalogEditorOp("NMAFetchVdbFromCatalogEditorOp", vdb.HostNodeMap, bootstrapHost)
	if err != nil {
		return instructions, err
	}

	nmaStartNodeOp := MakeNMAStartNodeOp("NMAStartNodeOp", bootstrapHost)

	httpsPollBootstrapNodeStateOp := MakeHTTPSPollNodeStateOp("HTTPSPollNodeStateOp", bootstrapHost, true, *options.UserName, options.Password)

	instructions = append(instructions,
		&nmaHealthOp,
		&nmaVerticaVersionOp,
		&checkDBRunningOp,
		&nmaPrepareDirectoriesOp,
		&nmaNetworkProfileOp,
		&nmaBootstrapCatalogOp,
		&nmaFetchVdbFromCatEdOp,
		&nmaStartNodeOp,
		&httpsPollBootstrapNodeStateOp,
	)

	newNodeHosts := util.SliceDiff(hosts, bootstrapHost)
	if len(hosts) > 1 {
		httpCreateNodeOp := MakeHTTPCreateNodeOp("HTTPCreateNodeOp", newNodeHosts, bootstrapHost,
			true /* use password auth */, *options.UserName, options.Password, vdb)
		instructions = append(instructions, &httpCreateNodeOp)
	}

	httpsReloadSpreadOp := MakeHTTPSReloadSpreadOp("HTTPSReloadSpreadOp", bootstrapHost, true, *options.UserName, options.Password)
	instructions = append(instructions, &httpsReloadSpreadOp)

	hostNodeMap := make(map[string]string)
	for _, host := range hosts {
		hostNodeMap[host] = vdb.HostNodeMap[host].CatalogPath
	}

	if len(hosts) > 1 {
		instructions = append(instructions, &nmaFetchVdbFromCatEdOp)
		produceTransferConfigOps(&instructions, bootstrapHost, hosts, nil, hostNodeMap)
		newNodeHosts := util.SliceDiff(vdb.HostList, bootstrapHost)
		nmaStartNewNodesOp := MakeNMAStartNodeOp("NMAStartNodeOp", newNodeHosts)
		instructions = append(instructions, &nmaStartNewNodesOp)
	}

	return instructions, nil
}

// produceAdditionalCreateDBInstructions returns additional instruction necessary for create_db.
func produceAdditionalCreateDBInstructions(vdb *VCoordinationDatabase, options *VCreateDatabaseOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	hosts := vdb.HostList
	bootstrapHost := options.bootstrapHost
	username := *options.UserName

	if !*options.SkipStartupPolling {
		httpsPollNodeStateOp := MakeHTTPSPollNodeStateOp("HTTPSPollNodeStateOp", hosts, true, username, options.Password)
		instructions = append(instructions, &httpsPollNodeStateOp)
	}

	if vdb.UseDepot {
		httpsCreateDepotOp := MakeHTTPSCreateDepotOp("HTTPSCreateDepotOp", vdb, bootstrapHost, true, username, options.Password)
		instructions = append(instructions, &httpsCreateDepotOp)
	}

	if len(hosts) >= ksafetyThreshold {
		httpsMarkDesignKSafeOp := MakeHTTPSMarkDesignKSafeOp("HTTPSMarkDesignKsafeOp", bootstrapHost, true, username,
			options.Password, ksafeValue)
		instructions = append(instructions, &httpsMarkDesignKSafeOp)
	}

	if !*options.SkipPackageInstall {
		httpsInstallPackagesOp := MakeHTTPSInstallPackagesOp("HTTPSInstallPackagesOp", bootstrapHost, true, username, options.Password)
		instructions = append(instructions, &httpsInstallPackagesOp)
	}

	if vdb.IsEon {
		httpsSyncCatalogOp := MakeHTTPSSyncCatalogOp("HTTPSyncCatalogOp", bootstrapHost, true, username, options.Password)
		instructions = append(instructions, &httpsSyncCatalogOp)
	}
	return instructions, nil
}

func getInitiator(hosts []string) string {
	// simply use the first one in user input
	return hosts[0]
}
