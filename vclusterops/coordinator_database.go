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

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"golang.org/x/exp/maps"
)

// VCoordinationDatabase represents catalog and node information for a database. The
// VCreateDatabase command returns a VCoordinationDatabase struct. Operations on
// an existing database (e.g. VStartDatabase) consume a VCoordinationDatabase struct.
type VCoordinationDatabase struct {
	Name string
	// processed path prefixes
	CatalogPrefix string
	DataPrefix    string
	HostNodeMap   vHostNodeMap
	// for convenience
	HostList []string // expected to be resolved IP addresses

	// Eon params, the boolean values are for convenience
	IsEon                   bool
	CommunalStorageLocation string
	UseDepot                bool
	DepotPrefix             string
	DepotSize               string
	AwsIDKey                string
	AwsSecretKey            string
	NumShards               int

	// authentication
	LicensePathOnNode string

	// more to add when useful
	Ipv6 bool

	PrimaryUpNodes []string
}

type vHostNodeMap map[string]*VCoordinationNode

func makeVHostNodeMap() vHostNodeMap {
	return make(vHostNodeMap)
}

func makeVCoordinationDatabase() VCoordinationDatabase {
	return VCoordinationDatabase{}
}

func (vdb *VCoordinationDatabase) setFromCreateDBOptions(options *VCreateDatabaseOptions, logger vlog.Printer) error {
	// build after validating the options
	err := options.validateAnalyzeOptions(logger)
	if err != nil {
		return err
	}

	// build coordinate db object from the create db options
	// section 1: set db info
	vdb.Name = *options.DBName
	vdb.CatalogPrefix = *options.CatalogPrefix
	vdb.DataPrefix = *options.DataPrefix
	vdb.HostList = make([]string, len(options.Hosts))
	vdb.HostList = options.Hosts
	vdb.HostNodeMap = makeVHostNodeMap()
	vdb.LicensePathOnNode = *options.LicensePathOnNode
	vdb.Ipv6 = options.IPv6

	// section 2: eon info
	vdb.IsEon = false
	if *options.CommunalStorageLocation != "" {
		vdb.IsEon = true
		vdb.CommunalStorageLocation = *options.CommunalStorageLocation
		vdb.DepotPrefix = *options.DepotPrefix
		vdb.DepotSize = *options.DepotSize
	}
	vdb.UseDepot = false
	if *options.DepotPrefix != "" {
		vdb.UseDepot = true
	}
	if *options.GetAwsCredentialsFromEnv {
		err := vdb.getAwsCredentialsFromEnv()
		if err != nil {
			return err
		}
	}
	vdb.NumShards = *options.ShardCount

	// section 3: build VCoordinationNode info
	for _, host := range vdb.HostList {
		vNode := makeVCoordinationNode()
		err := vNode.setFromCreateDBOptions(options, host)
		if err != nil {
			return err
		}
		vdb.HostNodeMap[host] = &vNode
	}

	return nil
}

// addNode adds a given host to the VDB's HostList and HostNodeMap.
// Duplicate host will not be added.
func (vdb *VCoordinationDatabase) addNode(vnode *VCoordinationNode) error {
	if _, exist := vdb.HostNodeMap[vnode.Address]; exist {
		return fmt.Errorf("host %s has already been in the VDB's HostList", vnode.Address)
	}

	vdb.HostNodeMap[vnode.Address] = vnode
	vdb.HostList = append(vdb.HostList, vnode.Address)

	return nil
}

// addHosts adds a given list of hosts to the VDB's HostList
// and HostNodeMap.
func (vdb *VCoordinationDatabase) addHosts(hosts []string, scName string) error {
	totalHostCount := len(hosts) + len(vdb.HostList)
	nodeNameToHost := vdb.genNodeNameToHostMap()
	for _, host := range hosts {
		vNode := makeVCoordinationNode()
		name, ok := util.GenVNodeName(nodeNameToHost, vdb.Name, totalHostCount)
		if !ok {
			return fmt.Errorf("could not generate a vnode name for %s", host)
		}
		nodeNameToHost[name] = host
		nodeConfig := NodeConfig{
			Address:    host,
			Name:       name,
			Subcluster: scName,
		}
		vNode.setFromNodeConfig(&nodeConfig, vdb)
		err := vdb.addNode(&vNode)
		if err != nil {
			return err
		}
	}

	return nil
}

func (vdb *VCoordinationDatabase) setFromClusterConfig(dbName string,
	dbConfig *DatabaseConfig) error {
	// if db name from user input is different than the one in config file,
	// we throw an error
	if dbConfig.Name != dbName {
		return cannotFindDBFromConfigErr(dbName)
	}

	// we trust the information in the config file
	// so we do not perform validation here
	vdb.Name = dbName

	catalogPrefix, dataPrefix, depotPrefix, err := dbConfig.getPathPrefix(dbName)
	if err != nil {
		return err
	}
	vdb.CatalogPrefix = catalogPrefix
	vdb.DataPrefix = dataPrefix
	vdb.DepotPrefix = depotPrefix

	vdb.IsEon = dbConfig.IsEon
	vdb.CommunalStorageLocation = dbConfig.CommunalStorageLocation
	vdb.Ipv6 = dbConfig.Ipv6
	if vdb.DepotPrefix != "" {
		vdb.UseDepot = true
	}

	vdb.HostNodeMap = makeVHostNodeMap()
	for _, nodeConfig := range dbConfig.Nodes {
		vnode := VCoordinationNode{}
		vnode.setFromNodeConfig(nodeConfig, vdb)
		err = vdb.addNode(&vnode)
		if err != nil {
			return err
		}
	}

	return nil
}

// copy copies the receiver's fields into a new VCoordinationDatabase struct and
// returns that struct. You can choose to copy only a subset of the receiver's hosts
// by passing a slice of hosts to keep.
func (vdb *VCoordinationDatabase) copy(targetHosts []string) VCoordinationDatabase {
	v := VCoordinationDatabase{
		Name:                    vdb.Name,
		CatalogPrefix:           vdb.CatalogPrefix,
		DataPrefix:              vdb.DataPrefix,
		IsEon:                   vdb.IsEon,
		CommunalStorageLocation: vdb.CommunalStorageLocation,
		UseDepot:                vdb.UseDepot,
		DepotPrefix:             vdb.DepotPrefix,
		DepotSize:               vdb.DepotSize,
		AwsIDKey:                vdb.AwsIDKey,
		AwsSecretKey:            vdb.AwsSecretKey,
		NumShards:               vdb.NumShards,
		LicensePathOnNode:       vdb.LicensePathOnNode,
		Ipv6:                    vdb.Ipv6,
		PrimaryUpNodes:          util.CopySlice(vdb.PrimaryUpNodes),
	}

	if len(targetHosts) == 0 {
		v.HostNodeMap = util.CopyMap(vdb.HostNodeMap)
		v.HostList = util.CopySlice(vdb.HostList)
		return v
	}

	v.HostNodeMap = util.FilterMapByKey(vdb.HostNodeMap, targetHosts)
	v.HostList = targetHosts

	return v
}

// copyHostNodeMap copies the receiver's HostNodeMap. You can choose to copy
// only a subset of the receiver's hosts by passing a slice of hosts to keep.
func (vdb *VCoordinationDatabase) copyHostNodeMap(targetHosts []string) vHostNodeMap {
	if len(targetHosts) == 0 {
		return util.CopyMap(vdb.HostNodeMap)
	}

	return util.FilterMapByKey(vdb.HostNodeMap, targetHosts)
}

// genNodeNameToHostMap generates a map, with node name as key and
// host ip as value, from HostNodeMap.
func (vdb *VCoordinationDatabase) genNodeNameToHostMap() map[string]string {
	vnodes := make(map[string]string)
	for h, vnode := range vdb.HostNodeMap {
		vnodes[vnode.Name] = h
	}
	return vnodes
}

// getSCNames returns a slice of subcluster names which the nodes
// in the current VCoordinationDatabase instance belong to.
func (vdb *VCoordinationDatabase) getSCNames() []string {
	allKeys := make(map[string]bool)
	scNames := []string{}
	for _, vnode := range vdb.HostNodeMap {
		sc := vnode.Subcluster
		if _, value := allKeys[sc]; !value {
			allKeys[sc] = true
			scNames = append(scNames, sc)
		}
	}
	return scNames
}

// containNodes determines which nodes are in the vdb and which ones are not.
// The node is determined by looking up the host address.
func (vdb *VCoordinationDatabase) containNodes(nodes []string) (nodesInDB, nodesNotInDB []string) {
	hostSet := mapset.NewSet(nodes...)
	nodesInDB = []string{}
	for _, vnode := range vdb.HostNodeMap {
		address := vnode.Address
		if exist := hostSet.Contains(address); exist {
			nodesInDB = append(nodesInDB, address)
		}
	}

	if len(nodesInDB) == len(nodes) {
		return nodesInDB, nil
	}
	return nodesInDB, util.SliceDiff(nodes, nodesInDB)
}

// hasAtLeastOneDownNode returns true if the current VCoordinationDatabase instance
// has at least one down node.
func (vdb *VCoordinationDatabase) hasAtLeastOneDownNode() bool {
	for _, vnode := range vdb.HostNodeMap {
		if vnode.State == util.NodeDownState {
			return true
		}
	}

	return false
}

// genDataPath builds and returns the data path
func (vdb *VCoordinationDatabase) genDataPath(nodeName string) string {
	dataSuffix := fmt.Sprintf("%s_data", nodeName)
	return filepath.Join(vdb.DataPrefix, vdb.Name, dataSuffix)
}

// genDepotPath builds and returns the depot path
func (vdb *VCoordinationDatabase) genDepotPath(nodeName string) string {
	depotSuffix := fmt.Sprintf("%s_depot", nodeName)
	return filepath.Join(vdb.DepotPrefix, vdb.Name, depotSuffix)
}

// genCatalogPath builds and returns the catalog path
func (vdb *VCoordinationDatabase) genCatalogPath(nodeName string) string {
	catalogSuffix := fmt.Sprintf("%s_catalog", nodeName)
	return filepath.Join(vdb.CatalogPrefix, vdb.Name, catalogSuffix)
}

// set aws id key and aws secret key
func (vdb *VCoordinationDatabase) getAwsCredentialsFromEnv() error {
	awsIDKey := os.Getenv("AWS_ACCESS_KEY_ID")
	if awsIDKey == "" {
		return fmt.Errorf("unable to get AWS ID key from environment variable")
	}
	awsSecretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if awsSecretKey == "" {
		return fmt.Errorf("unable to get AWS Secret key from environment variable")
	}
	vdb.AwsIDKey = awsIDKey
	vdb.AwsSecretKey = awsSecretKey
	return nil
}

// filterPrimaryNodes will remove secondary nodes from vdb
func (vdb *VCoordinationDatabase) filterPrimaryNodes() {
	primaryHostNodeMap := makeVHostNodeMap()

	for h, vnode := range vdb.HostNodeMap {
		if vnode.IsPrimary {
			primaryHostNodeMap[h] = vnode
		}
	}
	vdb.HostNodeMap = primaryHostNodeMap

	vdb.HostList = maps.Keys(vdb.HostNodeMap)
}

// VCoordinationNode represents node information from the database catalog.
type VCoordinationNode struct {
	Name    string `json:"name"`
	Address string
	// complete paths, not just prefix
	CatalogPath          string `json:"catalog_path"`
	StorageLocations     []string
	UserStorageLocations []string
	DepotPath            string
	// DB client port, should be 5433 by default
	Port int
	// default should be ipv4
	ControlAddressFamily string
	IsPrimary            bool
	State                string
	// empty string if it is not an eon db
	Subcluster string
	// empty string if it is not in a sandbox
	Sandbox string
}

func makeVCoordinationNode() VCoordinationNode {
	return VCoordinationNode{}
}

func (vnode *VCoordinationNode) setFromCreateDBOptions(
	options *VCreateDatabaseOptions,
	host string,
) error {
	dbName := *options.DBName
	dbNameInNode := strings.ToLower(dbName)
	// compute node name and complete paths for each node
	for i, h := range options.Hosts {
		if h != host {
			continue
		}

		vnode.Address = host
		vnode.Port = *options.ClientPort
		nodeNameSuffix := i + 1
		vnode.Name = fmt.Sprintf("v_%s_node%04d", dbNameInNode, nodeNameSuffix)
		catalogSuffix := fmt.Sprintf("%s_catalog", vnode.Name)
		vnode.CatalogPath = filepath.Join(*options.CatalogPrefix, dbName, catalogSuffix)
		dataSuffix := fmt.Sprintf("%s_data", vnode.Name)
		dataPath := filepath.Join(*options.DataPrefix, dbName, dataSuffix)
		vnode.StorageLocations = append(vnode.StorageLocations, dataPath)
		if *options.DepotPrefix != "" {
			depotSuffix := fmt.Sprintf("%s_depot", vnode.Name)
			vnode.DepotPath = filepath.Join(*options.DepotPrefix, dbName, depotSuffix)
		}
		if options.IPv6 {
			vnode.ControlAddressFamily = util.IPv6ControlAddressFamily
		} else {
			vnode.ControlAddressFamily = util.DefaultControlAddressFamily
		}

		return nil
	}
	return fmt.Errorf("fail to set up vnode from options: host %s does not exist in options", host)
}

func (vnode *VCoordinationNode) setFromNodeConfig(nodeConfig *NodeConfig, vdb *VCoordinationDatabase) {
	// we trust the information in the config file
	// so we do not perform validation here
	vnode.Address = nodeConfig.Address
	vnode.Name = nodeConfig.Name
	vnode.Subcluster = nodeConfig.Subcluster
	vnode.CatalogPath = vdb.genCatalogPath(vnode.Name)
	dataPath := vdb.genDataPath(vnode.Name)
	vnode.StorageLocations = append(vnode.StorageLocations, dataPath)
	if vdb.DepotPrefix != "" {
		vnode.DepotPath = vdb.genDepotPath(vnode.Name)
	}
	if vdb.Ipv6 {
		vnode.ControlAddressFamily = util.IPv6ControlAddressFamily
	} else {
		vnode.ControlAddressFamily = util.DefaultControlAddressFamily
	}
}

// remove this function in VER-92369
// WriteClusterConfig updates cluster configuration with the YAML-formatted file in the configPath
// and writes to the log and stdout.
// It returns any error encountered.
func (vdb *VCoordinationDatabase) WriteClusterConfig(configPath string, logger vlog.Printer) error {
	// Early out if this config path not provided. This just means there is no
	// config file to write out. No error is provided as the config file is
	// meant to cache frequently specified items on the command line.
	if configPath == "" {
		return nil
	}

	logger.Info("vdb content", "vdb", fmt.Sprintf("%+v", *vdb))

	/* build config information
	 */
	dbConfig := MakeDatabaseConfig()
	// loop over HostList is needed as we want to preserve the order
	for _, host := range vdb.HostList {
		vnode, ok := vdb.HostNodeMap[host]
		if !ok {
			return fmt.Errorf("cannot find host %s from HostNodeMap", host)
		}
		logger.Info("vnode content", "vnode", fmt.Sprintf("%+v", *vnode))
		nodeConfig := NodeConfig{}
		nodeConfig.Name = vnode.Name
		nodeConfig.Address = vnode.Address
		nodeConfig.Subcluster = vnode.Subcluster

		// VER-91869 will replace the path prefixes with full paths
		if vdb.CatalogPrefix == "" {
			nodeConfig.CatalogPath = util.GetPathPrefix(vnode.CatalogPath)
		} else {
			nodeConfig.CatalogPath = vdb.CatalogPrefix
		}
		if vdb.DataPrefix == "" && len(vnode.StorageLocations) > 0 {
			nodeConfig.DataPath = util.GetPathPrefix(vnode.StorageLocations[0])
		} else {
			nodeConfig.DataPath = vdb.DataPrefix
		}
		if vdb.IsEon && vdb.DepotPrefix == "" {
			nodeConfig.DepotPath = util.GetPathPrefix(vnode.DepotPath)
		} else {
			nodeConfig.DepotPath = vdb.DepotPrefix
		}

		dbConfig.Nodes = append(dbConfig.Nodes, &nodeConfig)
	}
	dbConfig.IsEon = vdb.IsEon
	dbConfig.CommunalStorageLocation = vdb.CommunalStorageLocation
	dbConfig.Ipv6 = vdb.Ipv6
	dbConfig.Name = vdb.Name

	// if the config file exists already
	// create its backup before overwriting it
	err := backupConfigFile(configPath, logger)
	if err != nil {
		return err
	}

	// update db config with the given database info
	err = dbConfig.WriteConfig(configPath, logger)
	if err != nil {
		return err
	}

	return nil
}
