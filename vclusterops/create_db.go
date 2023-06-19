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
	"regexp"
	"strconv"
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const ksafetyThreshold = 3
const ksafeValue = 1

// A good rule of thumb is to use normal strings unless you need nil.
// Normal strings are easier and safer to use in Go.
type VCreateDatabaseOptions struct {
	// part 1: basic db info
	VClusterDatabaseOptions
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
	LogPath            *string
	ConfigDirectory    *string
}

func VCreateDatabaseOptionsFactory() VCreateDatabaseOptions {
	newOptions := VCreateDatabaseOptions{}
	// without initialization it will panic
	newOptions.ConfigurationParameters = make(map[string]string)
	return newOptions
}

func (options *VCreateDatabaseOptions) ValidateRequiredOptions() error {
	// batch 1: validate required parameters that need user input
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

	if *options.CatalogPrefix == "" || !util.IsAbsPath(*options.CatalogPrefix) {
		return fmt.Errorf("must specify an absolute catalog path")
	}

	if *options.DataPrefix == "" || !util.IsAbsPath(*options.DataPrefix) {
		return fmt.Errorf("must specify an absolute data path")
	}

	// batch 2: validate required parameters with default values
	if options.Password == nil {
		options.Password = new(string)
		*options.Password = ""
		vlog.LogPrintInfoln("no password specified, using none")
	}

	if !util.StringInArray(*options.Policy, util.RestartPolicyList) {
		return fmt.Errorf("policy must be one of %v", util.RestartPolicyList)
	}

	// MUST provide a fully qualified path,
	// because vcluster could be executed outside of Vertica cluster hosts
	// so no point to resolve relative paths to absolute paths by checking
	// localhost, where vcluster is run
	//
	// empty string ("") will be converted to the default license path (/opt/vertica/share/license.key)
	// in the /bootstrap-catalog endpoint
	if *options.LicensePathOnNode != "" && !util.IsAbsPath(*options.LicensePathOnNode) {
		return fmt.Errorf("must provide a fully qualified path for license file")
	}

	// batch 3: validate other parameters
	err = util.ValidateAbsPath(options.LogPath, "must specify an absolute path for the log directory")
	if err != nil {
		return err
	}
	err = util.ValidateAbsPath(options.ConfigDirectory, "must specify an absolute path for the config directory")
	if err != nil {
		return err
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

func (options *VCreateDatabaseOptions) ValidateEonOptions() error {
	if *options.CommunalStorageLocation != "" {
		if *options.DepotPrefix == "" {
			return fmt.Errorf("must specify a depot path with commual storage location")
		}
		if *options.ShardCount == 0 {
			return fmt.Errorf("must specify a shard count greater than 0 with communal storage location")
		}
	}
	if *options.DepotPrefix != "" && *options.CommunalStorageLocation == "" {
		return fmt.Errorf("when depot path is given, communal storage location cannot be empty")
	}
	if *options.GetAwsCredentialsFromEnv && *options.CommunalStorageLocation == "" {
		return fmt.Errorf("AWS credentials are only used in Eon mode")
	}
	if *options.DepotSize != "" {
		if *options.DepotPrefix == "" {
			return fmt.Errorf("when depot size is given, depot path cannot be empty")
		}
		validDepotSize, err := validateDepotSize(*options.DepotSize)
		if !validDepotSize {
			return err
		}
	}
	return nil
}

func (options *VCreateDatabaseOptions) ValidateExtraOptions() error {
	if *options.Broadcast && *options.P2p {
		return fmt.Errorf("cannot use both Broadcast and Point-to-point networking mode")
	}
	// -1 is the default large cluster value, meaning 120 control nodes
	if *options.LargeCluster != util.DefaultLargeCluster && (*options.LargeCluster < 1 || *options.LargeCluster > util.MaxLargeCluster) {
		return fmt.Errorf("must specify a valid large cluster value in range [1, 120]")
	}
	return nil
}

func (options *VCreateDatabaseOptions) ValidateParseOptions() error {
	// batch 1: validate required parameters without default values
	err := options.ValidateRequiredOptions()
	if err != nil {
		return err
	}
	// batch 2: validate eon params
	err = options.ValidateEonOptions()
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

// Do advanced analysis on the options inputs, like resolve hostnames to be IPs
func (options *VCreateDatabaseOptions) AnalyzeOptions() error {
	// resolve RawHosts to be IP addresses
	hostAddresses, err := util.ResolveRawHostsToAddresses(options.RawHosts, *options.Ipv6)
	if err != nil {
		return err
	}
	options.Hosts = hostAddresses

	// process correct catalog path, data path and depot path prefixes
	*options.CatalogPrefix = util.GetCleanPath(*options.CatalogPrefix)
	*options.DataPrefix = util.GetCleanPath(*options.DataPrefix)
	*options.DepotPrefix = util.GetCleanPath(*options.DepotPrefix)
	if options.ClientPort == nil {
		*options.ClientPort = util.DefaultClientPort
	}
	if options.LargeCluster == nil {
		*options.LargeCluster = util.DefaultLargeCluster
	}
	return nil
}

func (options *VCreateDatabaseOptions) ValidateAnalyzeOptions() error {
	if err := options.ValidateParseOptions(); err != nil {
		return err
	}
	if err := options.AnalyzeOptions(); err != nil {
		return err
	}
	return nil
}

func VCreateDatabase(options *VCreateDatabaseOptions) (VCoordinationDatabase, error) {
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

	// create a VClusterOpEngine
	clusterOpEngine := MakeClusterOpEngine(instructions)

	// Give the instructions to the VClusterOpEngine to run
	runError := clusterOpEngine.Run()
	if runError != nil {
		vlog.LogPrintError("fail to create database, %w", runError)
		return vdb, runError
	}

	// write cluster information to the YAML config file
	err = writeClusterConfig(&vdb, options.ConfigDirectory)
	if err != nil {
		vlog.LogPrintWarning("fail to write config file, details: %w", err)
	}

	return vdb, nil
}

/*
We expect that we will ultimately produce the following instructions:
    1. Check NMA connectivity
	2. Check to see if any dbs running
	3. Check Vertica versions
	4. Prepare directories
	5. Get network profiles
	6. Bootstrap the database
	7. Run the catalog editor
	8. Start node
	9. Create node
	10. Reload spread
	11. Transfer config files
	12. Start all nodes of the database
	13. Poll node startup
	14. Create depot
	15. Mark design ksafe
	16. Install packages
	17. sync catalog
*/

//nolint:funlen // TODO this should be split into produceMandatoryInstructions and produceOptionalInstructions
func produceCreateDBInstructions(vdb *VCoordinationDatabase, options *VCreateDatabaseOptions) ([]ClusterOp, error) {
	var instructions []ClusterOp

	hosts := vdb.HostList
	initiator, err := getInitiator(hosts)

	if err != nil {
		return instructions, err
	}

	nmaHealthOp := MakeNMAHealthOp("NMAHealthOp", hosts)

	// require to have the same vertica version
	nmaVerticaVersionOp := MakeNMAVerticaVersionOp("NMAVerticaVersionOp", hosts, true)

	// need username for https operations
	username, errGetUser := util.GetCurrentUsername()
	if errGetUser != nil {
		return instructions, errGetUser
	}
	vlog.LogInfo("Current username is %s", username)

	checkDBRunningOp := MakeHTTPCheckRunningDBOp("HTTPCheckDBRunningOp", hosts,
		true /* use password auth */, username, options.Password, CreateDB)

	nmaPrepareDirectoriesOp, err := MakeNMAPrepareDirectoriesOp("NMAPrepareDirectoriesOp", vdb.HostNodeMap)
	if err != nil {
		return instructions, err
	}

	nmaNetworkProfileOp := MakeNMANetworkProfileOp("NMANetworkProfileOp", hosts)

	// should be only one bootstrap host
	// making it an array to follow the convention of passing a list of hosts to each operation
	bootstrapHost := []string{initiator}
	nmaBootstrapCatalogOp, err := MakeNMABootstrapCatalogOp("NMABootstrapCatalogOp", vdb, options, bootstrapHost)
	if err != nil {
		return instructions, err
	}

	nmaFetchVdbFromCatEdOp, err := MakeNMAFetchVdbFromCatalogEditorOp("NMAFetchVdbFromCatalogEditorOp", vdb.HostNodeMap, bootstrapHost)
	if err != nil {
		return instructions, err
	}

	nmaStartNodeOp := MakeNMAStartNodeOp("NMAStartNodeOp", bootstrapHost)

	httpsPollBootstrapNodeStateOp := MakeHTTPSPollNodeStateOp("HTTPSPollNodeStateOp", bootstrapHost, true, username, options.Password)

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

	if len(hosts) > 1 {
		httpCreateNodeOp := MakeHTTPCreateNodeOp("HTTPCreateNodeOp", bootstrapHost,
			true /* use password auth */, username, options.Password, vdb)
		instructions = append(instructions, &httpCreateNodeOp)
	}

	httpsReloadSpreadOp := MakeHTTPSReloadSpreadOp("HTTPSReloadSpreadOp", bootstrapHost, true, username, options.Password)
	instructions = append(instructions, &httpsReloadSpreadOp)

	if len(hosts) > 1 {
		produceTransferConfigOps(&instructions, bootstrapHost, vdb)
		newNodeHosts := util.SliceDiff(vdb.HostList, bootstrapHost)
		nmaStartNewNodesOp := MakeNMAStartNodeOp("NMAStartNodeOp", newNodeHosts)
		instructions = append(instructions,
			&nmaFetchVdbFromCatEdOp,
			&nmaStartNewNodesOp,
		)
	}

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

func getInitiator(hosts []string) (string, error) {
	errMsg := "fail to find initiator node from the host list"

	for _, host := range hosts {
		isLocalHost, err := util.IsLocalHost(host)
		if err != nil {
			return "", fmt.Errorf("%s, %w", errMsg, err)
		}

		if isLocalHost {
			return host, nil
		}
	}

	// If none of the hosts is localhost, we assign the first host as initiator.
	// Our assumptions is that vcluster and vclusterops are not always running on a host,
	// that is a part of vertica cluster.
	// Therefore, we can of course prioritize localhost,
	//   if localhost is a part of the --hosts;
	// but if none of the given hosts is localhost,
	//   we should just nominate hosts[0] as the initiator to bootstrap catalog.
	return hosts[0], errors.New(errMsg)
}

func produceTransferConfigOps(instructions *[]ClusterOp, bootstrapHost []string, vdb *VCoordinationDatabase) {
	var verticaConfContent string
	nmaDownloadVerticaConfigOp := MakeNMADownloadConfigOp(
		"NMADownloadVerticaConfigOp", vdb, bootstrapHost, "config/vertica", &verticaConfContent)
	nmaUploadVerticaConfigOp := MakeNMAUploadConfigOp(
		"NMAUploadVerticaConfigOp", vdb, bootstrapHost, "config/vertica", &verticaConfContent)
	var spreadConfContent string
	nmaDownloadSpreadConfigOp := MakeNMADownloadConfigOp(
		"NMADownloadSpreadConfigOp", vdb, bootstrapHost, "config/spread", &spreadConfContent)
	nmaUploadSpreadConfigOp := MakeNMAUploadConfigOp(
		"NMAUploadSpreadConfigOp", vdb, bootstrapHost, "config/spread", &spreadConfContent)
	*instructions = append(*instructions,
		&nmaDownloadVerticaConfigOp,
		&nmaUploadVerticaConfigOp,
		&nmaDownloadSpreadConfigOp,
		&nmaUploadSpreadConfigOp,
	)
}

func writeClusterConfig(vdb *VCoordinationDatabase, configDir *string) error {
	/* build config information
	 */
	clusterConfig := MakeClusterConfig()
	clusterConfig.DBName = vdb.Name
	clusterConfig.Hosts = vdb.HostList
	clusterConfig.CatalogPath = vdb.CatalogPrefix
	clusterConfig.DataPath = vdb.DataPrefix
	clusterConfig.DepotPath = vdb.DepotPrefix
	for _, host := range vdb.HostList {
		nodeConfig := NodeConfig{}
		node, ok := vdb.HostNodeMap[host]
		if !ok {
			errMsg := fmt.Sprintf("cannot find node info from host %s", host)
			panic(vlog.ErrorLog + errMsg)
		}
		nodeConfig.Address = host
		nodeConfig.Name = node.Name
		clusterConfig.Nodes = append(clusterConfig.Nodes, nodeConfig)
	}
	clusterConfig.IsEon = vdb.IsEon
	clusterConfig.Ipv6 = vdb.Ipv6

	/* write config to a YAML file
	 */
	configFilePath, err := GetConfigFilePath(vdb.Name, configDir)
	if err != nil {
		return err
	}

	// if the config file exists already
	// create its backup before overwriting it
	err = BackupConfigFile(configFilePath)
	if err != nil {
		return err
	}

	err = clusterConfig.WriteConfig(configFilePath)
	if err != nil {
		return err
	}

	return nil
}
