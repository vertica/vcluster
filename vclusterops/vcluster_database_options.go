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
	// part 1: basic database info
	DBName          *string
	RawHosts        []string // expected to be IP addresses or hostnames
	Hosts           []string // expected to be IP addresses resolved from RawHosts
	Ipv6            vstruct.NullableBool
	CatalogPrefix   *string
	DataPrefix      *string
	ConfigDirectory *string

	// part 2: Eon database info
	DepotPrefix               *string
	IsEon                     vstruct.NullableBool
	CommunalStorageLocation   *string
	CommunalStorageParameters map[string]string

	// part 3: authentication info
	UserName *string
	Password *string
	Key      string
	Cert     string
	CaCert   string

	// part 4: other info
	LogPath        *string
	HonorUserInput *bool
	usePassword    bool
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

func (opt *DatabaseOptions) SetDefaultValues() {
	opt.DBName = new(string)
	opt.CatalogPrefix = new(string)
	opt.DataPrefix = new(string)
	opt.DepotPrefix = new(string)
	opt.UserName = new(string)
	opt.HonorUserInput = new(bool)
	opt.Ipv6 = vstruct.NotSet
	opt.IsEon = vstruct.NotSet
	opt.CommunalStorageLocation = new(string)
	opt.CommunalStorageParameters = make(map[string]string)
}

func (opt *DatabaseOptions) CheckNilPointerParams() error {
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

func (opt *DatabaseOptions) ValidateBaseOptions(commandName string) error {
	// database name
	if *opt.DBName == "" {
		return fmt.Errorf("must specify a database name")
	}
	err := util.ValidateName(*opt.DBName, "database")
	if err != nil {
		return err
	}

	// raw hosts and password
	err = opt.ValidateHostsAndPwd(commandName)
	if err != nil {
		return err
	}

	// paths
	err = opt.ValidatePaths(commandName)
	if err != nil {
		return err
	}

	// config directory
	err = opt.ValidateConfigDir(commandName)
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

// ValidateHostsAndPwd will validate raw hosts and password
func (opt *DatabaseOptions) ValidateHostsAndPwd(commandName string) error {
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
			vlog.LogPrintInfoln("no password specified, using none")
		}
	} else {
		// for other commands, we validate hosts when HonorUserInput is set, otherwise we use hosts in config file
		if *opt.HonorUserInput {
			if len(opt.RawHosts) == 0 {
				vlog.LogPrintInfo("no hosts specified, try to use the hosts in %s", ConfigFileName)
			}
		}
		// for other commands, we will not use "" as password
		if opt.Password == nil {
			vlog.LogPrintInfoln("no password specified, using none")
		}
	}
	return nil
}

// validate catalog, data, and depot paths
func (opt *DatabaseOptions) ValidatePaths(commandName string) error {
	// validate for the following commands only
	// TODO: add other commands into the command list
	commands := []string{commandCreateDB, commandDropDB}
	if !slices.Contains(commands, commandName) {
		return nil
	}

	// catalog prefix path
	err := opt.ValidateCatalogPath()
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

func (opt *DatabaseOptions) ValidateCatalogPath() error {
	// catalog prefix path
	return util.ValidateRequiredAbsPath(opt.CatalogPrefix, "catalog path")
}

// validate config directory
func (opt *DatabaseOptions) ValidateConfigDir(commandName string) error {
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

// ParseHostList converts a string into a list of hosts.
// The hosts should be separated by comma, and will be converted to lower case
func (opt *DatabaseOptions) ParseHostList(hosts string) error {
	inputHostList, err := util.SplitHosts(hosts)
	if err != nil {
		return err
	}

	opt.RawHosts = inputHostList

	return nil
}

func (opt *DatabaseOptions) ValidateUserName() error {
	if *opt.UserName == "" {
		username, err := util.GetCurrentUsername()
		if err != nil {
			return err
		}
		*opt.UserName = username
	}
	vlog.LogInfo("Current username is %s", *opt.UserName)

	return nil
}

func (opt *DatabaseOptions) SetUsePassword() error {
	// when password is specified,
	// we will use username/password to call https endpoints
	opt.usePassword = false
	if opt.Password != nil {
		opt.usePassword = true
		err := opt.ValidateUserName()
		if err != nil {
			return err
		}
	}

	return nil
}

// IsEonMode can choose the right eon mode from user input and config file
func (opt *DatabaseOptions) IsEonMode(config *ClusterConfig) bool {
	// when config file is not available, we use user input
	// HonorUserInput must be true at this time, otherwise vcluster has stopped when it cannot find the config file
	if config == nil {
		return opt.IsEon.ToBool()
	}

	isEon := config.IsEon
	// if HonorUserInput is set, we choose the user input
	if opt.IsEon != vstruct.NotSet && *opt.HonorUserInput {
		isEon = opt.IsEon.ToBool()
	}
	return isEon
}

// GetNameAndHosts can choose the right dbName and hosts from user input and config file
func (opt *DatabaseOptions) GetNameAndHosts(config *ClusterConfig) (dbName string, hosts []string) {
	// when config file is not available, we use user input
	// HonorUserInput must be true at this time, otherwise vcluster has stopped when it cannot find the config file
	if config == nil {
		return *opt.DBName, opt.Hosts
	}

	dbName = config.DBName
	hosts = config.Hosts
	// if HonorUserInput is set, we choose the user input
	if *opt.DBName != "" && *opt.HonorUserInput {
		dbName = *opt.DBName
	}
	if len(opt.Hosts) > 0 && *opt.HonorUserInput {
		hosts = opt.Hosts
	}
	return dbName, hosts
}

// GetHosts chooses the right hosts from user input and config file
func (opt *DatabaseOptions) GetHosts(config *ClusterConfig) (hosts []string) {
	// when config file is not available, we use user input
	// HonorUserInput must be true at this time, otherwise vcluster has stopped when it cannot find the config file
	if config == nil {
		return opt.Hosts
	}

	hosts = config.Hosts
	// if HonorUserInput is set, we choose the user input
	if len(opt.Hosts) > 0 && *opt.HonorUserInput {
		hosts = opt.Hosts
	}
	return hosts
}

// GetCatalogPrefix can choose the right catalog prefix from user input and config file
func (opt *DatabaseOptions) GetCatalogPrefix(config *ClusterConfig) (catalogPrefix *string) {
	// when config file is not available, we use user input
	// HonorUserInput must be true at this time, otherwise vcluster has stopped when it cannot find the config file
	if config == nil {
		return opt.CatalogPrefix
	}
	catalogPrefix = &config.CatalogPath
	// if HonorUserInput is set, we choose the user input
	if *opt.CatalogPrefix != "" && *opt.HonorUserInput {
		catalogPrefix = opt.CatalogPrefix
	}
	return catalogPrefix
}

// getDepotAndDataPrefix chooses the right depot/data prefix from user input and config file.
func (opt *DatabaseOptions) getDepotAndDataPrefix(config *ClusterConfig) (depotPrefix, dataPrefix string) {
	if config == nil {
		return *opt.DepotPrefix, *opt.DataPrefix
	}
	depotPrefix = config.DepotPath
	dataPrefix = config.DataPath
	// if HonorUserInput is set, we choose the user input
	if !*opt.HonorUserInput {
		return depotPrefix, dataPrefix
	}
	if *opt.DepotPrefix != "" {
		depotPrefix = *opt.DepotPrefix
	}
	if *opt.DataPrefix != "" {
		dataPrefix = *opt.DataPrefix
	}
	return depotPrefix, dataPrefix
}

// GetDBConfig can read database configurations from vertica_cluster.yaml to the struct ClusterConfig
func (opt *DatabaseOptions) GetDBConfig() (config *ClusterConfig, e error) {
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
		configContent, err := ReadConfig(configDir)
		config = &configContent
		if err != nil {
			// when we cannot read config file, config points to an empty ClusterConfig with default values
			// we want to reset config to nil so we will use user input later rather than those default values
			config = nil
			vlog.LogPrintWarningln("Failed to read " + filepath.Join(configDir, ConfigFileName))
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
func (opt *DatabaseOptions) getVDBWhenDBIsDown() (vdb VCoordinationDatabase, err error) {
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
	var instructions1 []ClusterOp
	nmaHealthOp := makeNMAHealthOp(opt.Hosts)
	nmaGetNodesInfoOp := makeNMAGetNodesInfoOp(opt.Hosts, *opt.DBName, *opt.CatalogPrefix, &vdb1)
	instructions1 = append(instructions1,
		&nmaHealthOp,
		&nmaGetNodesInfoOp,
	)

	certs := HTTPSCerts{key: opt.Key, cert: opt.Cert, caCert: opt.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions1, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to retrieve node names from NMA /nodes: %v", err)
		return vdb, err
	}

	// step 2: get node details from cluster_config.json
	vdb2 := VCoordinationDatabase{}
	var instructions2 []ClusterOp
	sourceFilePath := opt.getDescriptionFilePath()
	nmaDownLoadFileOp, err := makeNMADownloadFileOp(opt.Hosts, sourceFilePath, destinationFilePath, catalogPath,
		opt.CommunalStorageParameters, &vdb2)
	if err != nil {
		return vdb, err
	}
	instructions2 = append(instructions2, &nmaDownLoadFileOp)

	clusterOpEngine = MakeClusterOpEngine(instructions2, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to retrieve node details from %s: %v", descriptionFileName, err)
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
