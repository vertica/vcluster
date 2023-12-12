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
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"github.com/vertica/vcluster/vclusterops/vstruct"
	"golang.org/x/exp/slices"
)

type DatabaseOptions struct {
	/* part 1: basic database info */

	// name of the database
	DBName *string
	// expected to be IP addresses or hostnames
	RawHosts []string
	// expected to be IP addresses resolved from RawHosts
	Hosts []string
	// whether using IPv6 for host addresses
	Ipv6 vstruct.NullableBool
	// path of catalog directory
	CatalogPrefix *string
	// path of data directory
	DataPrefix *string
	// directory of the YAML config file
	ConfigDirectory *string

	/* part 2: Eon database info */

	// path of depot directory
	DepotPrefix *string
	// whether the database is in Eon mode
	IsEon vstruct.NullableBool
	// path of the communal storage
	CommunalStorageLocation *string
	// database configuration parameters
	ConfigurationParameters map[string]string

	/* part 3: authentication info */

	// user name
	UserName *string
	// password
	Password *string
	// TLS Key
	Key string
	// TLS Certificate
	Cert string
	// TLS CA Certificate
	CaCert string

	/* part 4: other info */

	// path of the log file
	LogPath *string
	// whether honor user's input rather than reading values from the config file
	HonorUserInput *bool
	// whether use password
	usePassword bool
	// pointer to the cluster config object
	Config *ClusterConfig
}

const (
	descriptionFileName = "cluster_config.json"
	destinationFilePath = "/tmp/desc.json"
	// catalogPath is not used for now, will implement it in VER-88884
	catalogPath = ""
)

const (
	commandCreateDB   = "create_db"
	commandDropDB     = "drop_db"
	commandStopDB     = "stop_db"
	commandStartDB    = "start_db"
	commandAddCluster = "db_add_subcluster"
)

func (opt *DatabaseOptions) setDefaultValues() {
	opt.DBName = new(string)
	opt.CatalogPrefix = new(string)
	opt.DataPrefix = new(string)
	opt.DepotPrefix = new(string)
	opt.UserName = new(string)
	opt.HonorUserInput = new(bool)
	opt.Ipv6 = vstruct.NotSet
	opt.IsEon = vstruct.NotSet
	opt.CommunalStorageLocation = new(string)
	opt.ConfigurationParameters = make(map[string]string)
}

func (opt *DatabaseOptions) checkNilPointerParams() error {
	// basic params
	if opt.DBName == nil {
		return util.ParamNotSetErrorMsg("name")
	}
	if opt.CatalogPrefix == nil {
		return util.ParamNotSetErrorMsg("catalog-path")
	}
	if opt.DataPrefix == nil {
		return util.ParamNotSetErrorMsg("data-path")
	}
	if opt.DepotPrefix == nil {
		return util.ParamNotSetErrorMsg("depot-path")
	}

	return nil
}

func (opt *DatabaseOptions) validateBaseOptions(commandName string, log vlog.Printer) error {
	// get vcluster commands
	log.WithName(commandName)
	// database name
	if *opt.DBName == "" {
		return fmt.Errorf("must specify a database name")
	}
	err := util.ValidateDBName(*opt.DBName)
	if err != nil {
		return err
	}

	// raw hosts and password
	err = opt.validateHostsAndPwd(commandName, log)
	if err != nil {
		return err
	}

	// paths
	err = opt.validatePaths(commandName)
	if err != nil {
		return err
	}

	// config directory
	err = opt.validateConfigDir(commandName)
	if err != nil {
		return err
	}

	// log directory
	err = util.ValidateAbsPath(opt.LogPath, "log directory")
	if err != nil {
		return err
	}

	return nil
}

// validateHostsAndPwd will validate raw hosts and password
func (opt *DatabaseOptions) validateHostsAndPwd(commandName string, log vlog.Printer) error {
	// when we create db, we need hosts and set password to "" if user did not provide one
	if commandName == commandCreateDB {
		// raw hosts
		if len(opt.RawHosts) == 0 {
			return fmt.Errorf("must specify a host or host list")
		}
		// password
		if opt.Password == nil {
			opt.Password = new(string)
			*opt.Password = ""
			log.PrintInfo("no password specified, using none")
		}
	} else {
		// for other commands, we validate hosts when HonorUserInput is set, otherwise we use hosts in config file
		if *opt.HonorUserInput {
			if len(opt.RawHosts) == 0 {
				log.PrintInfo("no hosts specified, try to use the hosts in %s", ConfigFileName)
			}
		}
		// for other commands, we will not use "" as password
		if opt.Password == nil {
			log.PrintInfo("no password specified, using none")
		}
	}
	return nil
}

// validate catalog, data, and depot paths
func (opt *DatabaseOptions) validatePaths(commandName string) error {
	// validate for the following commands only
	// TODO: add other commands into the command list
	commands := []string{commandCreateDB, commandDropDB}
	if !slices.Contains(commands, commandName) {
		return nil
	}

	// catalog prefix path
	err := opt.validateCatalogPath()
	if err != nil {
		return err
	}

	// data prefix
	err = util.ValidateRequiredAbsPath(opt.DataPrefix, "data path")
	if err != nil {
		return err
	}

	// depot prefix
	if opt.IsEon == vstruct.True {
		err = util.ValidateRequiredAbsPath(opt.DepotPrefix, "depot path")
		if err != nil {
			return err
		}
	}
	return nil
}

func (opt *DatabaseOptions) validateCatalogPath() error {
	// catalog prefix path
	return util.ValidateRequiredAbsPath(opt.CatalogPrefix, "catalog path")
}

// validate config directory
func (opt *DatabaseOptions) validateConfigDir(commandName string) error {
	// validate for the following commands only
	// TODO: add other commands into the command list
	commands := []string{commandCreateDB, commandDropDB, commandStopDB, commandStartDB, commandAddCluster}
	if slices.Contains(commands, commandName) {
		return nil
	}

	err := util.ValidateAbsPath(opt.ConfigDirectory, "config directory")
	if err != nil {
		return err
	}

	return nil
}

// ParseHostList converts a comma-separated string of hosts into a slice of host names. During parsing,
// the hosts names are converted to lowercase.
// It returns any parsing error encountered.
func (opt *DatabaseOptions) ParseHostList(hosts string) error {
	inputHostList, err := util.SplitHosts(hosts)
	if err != nil {
		return err
	}

	opt.RawHosts = inputHostList

	return nil
}

func (opt *DatabaseOptions) validateUserName(log vlog.Printer) error {
	if *opt.UserName == "" {
		username, err := util.GetCurrentUsername()
		if err != nil {
			return err
		}
		*opt.UserName = username
	}
	log.Info("Current username", "username", *opt.UserName)

	return nil
}

func (opt *DatabaseOptions) setUsePassword(log vlog.Printer) error {
	// when password is specified,
	// we will use username/password to call https endpoints
	opt.usePassword = false
	if opt.Password != nil {
		opt.usePassword = true
		err := opt.validateUserName(log)
		if err != nil {
			return err
		}
	}

	return nil
}

// isEonMode can choose the right eon mode from user input and config file
func (opt *DatabaseOptions) isEonMode(config *ClusterConfig) (bool, error) {
	// when config file is not available, we use user input
	// HonorUserInput must be true at this time, otherwise vcluster has stopped when it cannot find the config file
	if config == nil {
		return opt.IsEon.ToBool(), nil
	}

	dbConfig, ok := (*config)[*opt.DBName]
	if !ok {
		return false, cannotFindDBFromConfigErr(*opt.DBName)
	}

	isEon := dbConfig.IsEon
	// if HonorUserInput is set, we choose the user input
	if opt.IsEon != vstruct.NotSet && *opt.HonorUserInput {
		isEon = opt.IsEon.ToBool()
	}
	return isEon, nil
}

// getNameAndHosts can choose the right dbName and hosts from user input and config file
func (opt *DatabaseOptions) getNameAndHosts(config *ClusterConfig) (dbName string, hosts []string, err error) {
	// DBName is now a required option with our without yaml config, so this function exists for legacy reasons
	dbName = *opt.DBName
	hosts, err = opt.getHosts(config)
	return dbName, hosts, err
}

// getHosts chooses the right hosts from user input and config file
func (opt *DatabaseOptions) getHosts(config *ClusterConfig) (hosts []string, err error) {
	// when config file is not available, we use user input
	// HonorUserInput must be true at this time, otherwise vcluster has stopped when it cannot find the config file
	if config == nil {
		return opt.Hosts, nil
	}

	dbConfig, ok := (*config)[*opt.DBName]
	if !ok {
		return hosts, cannotFindDBFromConfigErr(*opt.DBName)
	}

	hosts = dbConfig.getHosts()
	// if HonorUserInput is set, we choose the user input
	if len(opt.Hosts) > 0 && *opt.HonorUserInput {
		hosts = opt.Hosts
	}
	return hosts, nil
}

// getCatalogPrefix can choose the right catalog prefix from user input and config file
func (opt *DatabaseOptions) getCatalogPrefix(clusterConfig *ClusterConfig) (catalogPrefix *string, err error) {
	// when config file is not available, we use user input
	// HonorUserInput must be true at this time, otherwise vcluster has stopped when it cannot find the config file
	if clusterConfig == nil {
		return opt.CatalogPrefix, nil
	}

	catalogPrefix = new(string)
	*catalogPrefix, _, _, err = clusterConfig.getPathPrefix(*opt.DBName)
	if err != nil {
		return catalogPrefix, err
	}

	// if HonorUserInput is set, we choose the user input
	if *opt.CatalogPrefix != "" && *opt.HonorUserInput {
		catalogPrefix = opt.CatalogPrefix
	}
	return catalogPrefix, nil
}

// getDepotAndDataPrefix chooses the right depot/data prefix from user input and config file.
func (opt *DatabaseOptions) getDepotAndDataPrefix(
	clusterConfig *ClusterConfig) (depotPrefix, dataPrefix string, err error) {
	if clusterConfig == nil {
		return *opt.DepotPrefix, *opt.DataPrefix, nil
	}

	_, dataPrefix, depotPrefix, err = clusterConfig.getPathPrefix(*opt.DBName)
	if err != nil {
		return "", "", err
	}

	// if HonorUserInput is set, we choose the user input
	if !*opt.HonorUserInput {
		return depotPrefix, dataPrefix, nil
	}
	if *opt.DepotPrefix != "" {
		depotPrefix = *opt.DepotPrefix
	}
	if *opt.DataPrefix != "" {
		dataPrefix = *opt.DataPrefix
	}
	return depotPrefix, dataPrefix, nil
}

// GetDBConfig reads database configurations from vertica_cluster.yaml into a ClusterConfig struct.
// It returns the ClusterConfig and any error encountered.
func (opt *DatabaseOptions) GetDBConfig(vcc VClusterCommands) (config *ClusterConfig, e error) {
	var configDir string

	if opt.ConfigDirectory == nil && !*opt.HonorUserInput {
		return nil, fmt.Errorf("only supported two options: honor-user-input and config-directory")
	}

	if opt.ConfigDirectory != nil {
		configDir = *opt.ConfigDirectory
	} else {
		currentDir, err := os.Getwd()
		if err != nil && !*opt.HonorUserInput {
			return config, fmt.Errorf("fail to get current directory, details: %w", err)
		}
		configDir = currentDir
	}

	if configDir != "" {
		configContent, err := ReadConfig(configDir, vcc.Log)
		config = &configContent
		if err != nil {
			// when we cannot read config file, config points to an empty ClusterConfig with default values
			// we want to reset config to nil so we will use user input later rather than those default values
			config = nil
			vcc.Log.PrintWarning("Failed to read " + filepath.Join(configDir, ConfigFileName))
			// when the customer wants to use user input, we can ignore config file error
			if !*opt.HonorUserInput {
				return config, err
			}
		}
	}

	return config, nil
}

// normalizePaths replaces all '//' to be '/', and trim
// catalog, data and depot prefixes.
func (opt *DatabaseOptions) normalizePaths() {
	// process correct catalog path, data path and depot path prefixes
	*opt.CatalogPrefix = util.GetCleanPath(*opt.CatalogPrefix)
	*opt.DataPrefix = util.GetCleanPath(*opt.DataPrefix)
	*opt.DepotPrefix = util.GetCleanPath(*opt.DepotPrefix)
}

// getVDBWhenDBIsDown can retrieve db configurations from NMA /nodes endpoint and cluster_config.json when db is down
func (opt *DatabaseOptions) getVDBWhenDBIsDown(vcc *VClusterCommands) (vdb VCoordinationDatabase, err error) {
	/*
	 *   1. Get node names for input hosts from NMA /nodes.
	 *   2. Get other node information for input hosts from cluster_config.json.
	 *   3. Build vdb for input hosts using the information from step 1 and step 2.
	 *   From NMA /nodes, we can only get node names and catalog paths so we need the other nodes' info from cluster_config.json.
	 *   In cluster_config.json, we have all nodes' info, however, the node IPs in cluster_config.json could be old and cannot be
	 *   mapped to input hosts so we need node names (retrieved from NMA /nodes) as a bridge to connect cluster_config.json and
	 *   input hosts. For instance, if revive_db changed nodes' IPs, cluster_config.json cannot have the correct nodes' Ips. We
	 *   cannot map input hosts with nodes in cluster_config.json. We need to find node names from NMA /nodes for input hosts
	 *   and use the node names to retrieve other nodes' info from cluster_config.json.
	 */

	// step 1: get node names by calling NMA /nodes on input hosts
	// this step can map input hosts with node names
	vdb1 := VCoordinationDatabase{}
	var instructions1 []clusterOp
	nmaHealthOp := makeNMAHealthOp(vcc.Log, opt.Hosts)
	nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(vcc.Log, opt.Hosts, *opt.DBName, *opt.CatalogPrefix,
		false /* report all errors */, &vdb1)
	instructions1 = append(instructions1,
		&nmaHealthOp,
		&nmaGetNodesInfoOp,
	)

	certs := httpsCerts{key: opt.Key, cert: opt.Cert, caCert: opt.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions1, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		vcc.Log.PrintError("fail to retrieve node names from NMA /nodes: %v", err)
		return vdb, err
	}

	// step 2: get node details from cluster_config.json
	vdb2 := VCoordinationDatabase{}
	var instructions2 []clusterOp
	sourceFilePath := opt.getDescriptionFilePath()
	nmaDownLoadFileOp, err := makeNMADownloadFileOp(vcc.Log, opt.Hosts, sourceFilePath, destinationFilePath, catalogPath,
		opt.ConfigurationParameters, &vdb2)
	if err != nil {
		return vdb, err
	}
	instructions2 = append(instructions2, &nmaDownLoadFileOp)

	clusterOpEngine = makeClusterOpEngine(instructions2, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		vcc.Log.PrintError("fail to retrieve node details from %s: %v", descriptionFileName, err)
		return vdb, err
	}

	// step 3: build vdb for input hosts using node names from step 1 and node details from step 2
	// this step can map input hosts with node details
	vdb.HostList = vdb1.HostList
	vdb.HostNodeMap = makeVHostNodeMap()
	nodeNameVNodeMap := make(map[string]*VCoordinationNode)
	for _, vnode2 := range vdb2.HostNodeMap {
		nodeNameVNodeMap[vnode2.Name] = vnode2
	}
	for h1, vnode1 := range vdb1.HostNodeMap {
		nodeName := vnode1.Name
		vnode2, exists := nodeNameVNodeMap[nodeName]
		if exists {
			vnode := new(VCoordinationNode)
			*vnode = *vnode2
			// Update nodes' addresses using input hosts (the latest nodes' addresses) because
			// cluster_config.json stores the old addresses. revive_db and re_ip can modify
			// the nodes' addresses without syncing the change to cluster_config.json.
			vnode.Address = h1
			vdb.HostNodeMap[h1] = vnode
		} else {
			return vdb, fmt.Errorf("node name %s is not found in %s", nodeName, descriptionFileName)
		}
	}
	return vdb, nil
}

// getDescriptionFilePath can make the description file path using db name and communal storage location in the options
func (opt *DatabaseOptions) getDescriptionFilePath() string {
	const (
		descriptionFileMetadataFolder = "metadata"
	)
	// description file will be in the location: {communalStorageLocation}/metadata/{db_name}/cluster_config.json
	// an example: s3://tfminio/test_loc/metadata/test_db/cluster_config.json
	descriptionFilePath := filepath.Join(*opt.CommunalStorageLocation, descriptionFileMetadataFolder, *opt.DBName, descriptionFileName)
	// filepath.Join() will change "://" of the remote communal storage path to ":/"
	// as a result, we need to change the separator back to url format
	descriptionFilePath = strings.Replace(descriptionFilePath, ":/", "://", 1)

	return descriptionFilePath
}

func (opt *DatabaseOptions) isSpreadEncryptionEnabled() (enabled bool, encryptionType string) {
	const EncryptSpreadCommConfigName = "encryptspreadcomm"
	// We cannot use the map lookup because the key name is case insensitive.
	for key, val := range opt.ConfigurationParameters {
		if strings.EqualFold(key, EncryptSpreadCommConfigName) {
			return true, val
		}
	}
	return false, ""
}

func (opt *DatabaseOptions) runClusterOpEngine(log vlog.Printer, instructions []clusterOp) error {
	// Create a VClusterOpEngine, and add certs to the engine
	certs := httpsCerts{key: opt.Key, cert: opt.Cert, caCert: opt.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)

	// Give the instructions to the VClusterOpEngine to run
	return clusterOpEngine.run(log)
}
