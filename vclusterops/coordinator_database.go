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
)

/* VCoordinationDatabase contains a copy of some of the CAT::Database
 * information from the catalog. It also contains a list of VCoordinationNodes.
 * It is similar to the admintools VDatabase object.
 *
 * The create database command produces a VCoordinationDatabase.
 * Start database, for example, consumes a VCoordinationDatabase.
 *
 */
type VCoordinationDatabase struct {
	Name string
	// processed path prefixes
	CatalogPrefix string
	DataPrefix    string
	HostNodeMap   map[string]VCoordinationNode
	// for convenience
	HostList []string

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
}

func MakeVCoordinationDatabase() VCoordinationDatabase {
	return VCoordinationDatabase{}
}

func (vdb *VCoordinationDatabase) SetFromCreateDBOptions(options *VCreateDatabaseOptions) error {
	// build after validating the options
	err := options.ValidateAnalyzeOptions()
	if err != nil {
		return err
	}

	// build coordinate db object from the create db options
	// section 1: set db info
	vdb.Name = *options.Name
	vdb.CatalogPrefix = *options.CatalogPrefix
	vdb.DataPrefix = *options.DataPrefix
	vdb.HostList = make([]string, len(options.Hosts))
	vdb.HostList = options.Hosts
	vdb.HostNodeMap = make(map[string]VCoordinationNode)
	vdb.LicensePathOnNode = *options.LicensePathOnNode
	vdb.Ipv6 = *options.Ipv6

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
		err := vdb.GetAwsCredentialsFromEnv()
		if err != nil {
			return err
		}
	}
	vdb.NumShards = *options.ShardCount

	// section 3: build VCoordinationNode info
	for _, host := range vdb.HostList {
		vNode := MakeVCoordinationNode()
		err := vNode.SetFromCreateDBOptions(options, host)
		if err != nil {
			return err
		}
		vdb.HostNodeMap[host] = vNode
	}

	return nil
}

func (vdb *VCoordinationDatabase) SetFromClusterConfig(clusterConfig *ClusterConfig) {
	// we trust the information in the config file
	// so we do not perform validation here
	vdb.Name = clusterConfig.DBName
	vdb.CatalogPrefix = clusterConfig.CatalogPath
	vdb.DataPrefix = clusterConfig.DataPath
	vdb.DepotPrefix = clusterConfig.DepotPath
	vdb.HostList = clusterConfig.Hosts
	vdb.IsEon = clusterConfig.IsEon
	if vdb.DepotPrefix != "" {
		vdb.UseDepot = true
	}

	vdb.HostNodeMap = make(map[string]VCoordinationNode)
	for _, nodeConfig := range clusterConfig.Nodes {
		vnode := VCoordinationNode{}
		vnode.SetFromNodeConfig(nodeConfig, vdb)
		vdb.HostNodeMap[vnode.Address] = vnode
	}
}

// set aws id key and aws secret key
func (vdb *VCoordinationDatabase) GetAwsCredentialsFromEnv() error {
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

/* VCoordinationNode contains a copy of the some of CAT::Node information
 * from the database catalog (visible in the vs_nodes table). It is similar
 * to the admintools VNode object.
 *
 */
type VCoordinationNode struct {
	Name    string
	Address string
	// complete paths, not just prefix
	CatalogPath      string
	StorageLocations []string
	DepotPath        string
	// DB client port, should be 5433 by default
	Port int
	// default should be ipv4
	ControlAddressFamily string
}

func MakeVCoordinationNode() VCoordinationNode {
	return VCoordinationNode{}
}

func (vnode *VCoordinationNode) SetFromCreateDBOptions(
	options *VCreateDatabaseOptions,
	host string,
) error {
	dbName := *options.Name
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
		if *options.Ipv6 {
			vnode.ControlAddressFamily = util.IPv6ControlAddressFamily
		} else {
			vnode.ControlAddressFamily = util.DefaultControlAddressFamily
		}

		return nil
	}
	return fmt.Errorf("fail to set up vnode from options: host %s does not exist in options", host)
}

func (vnode *VCoordinationNode) SetFromNodeConfig(nodeConfig NodeConfig, vdb *VCoordinationDatabase) {
	// we trust the information in the config file
	// so we do not perform validation here
	vnode.Address = nodeConfig.Address
	vnode.Name = nodeConfig.Name
	catalogSuffix := fmt.Sprintf("%s_catalog", vnode.Name)
	vnode.CatalogPath = filepath.Join(vdb.CatalogPrefix, vdb.Name, catalogSuffix)
	dataSuffix := fmt.Sprintf("%s_data", vnode.Name)
	dataPath := filepath.Join(vdb.DataPrefix, vdb.Name, dataSuffix)
	vnode.StorageLocations = append(vnode.StorageLocations, dataPath)
	if vdb.DepotPrefix != "" {
		depotSuffix := fmt.Sprintf("%s_depot", vnode.Name)
		vnode.DepotPath = filepath.Join(vdb.DepotPrefix, vdb.Name, depotSuffix)
	}
	if vdb.Ipv6 {
		vnode.ControlAddressFamily = util.IPv6ControlAddressFamily
	} else {
		vnode.ControlAddressFamily = util.DefaultControlAddressFamily
	}
}

func GetHostsFromHostNodeMap(hostNodeMap map[string]VCoordinationNode) []string {
	hosts := []string{}
	for h := range hostNodeMap {
		hosts = append(hosts, h)
	}
	return hosts
}
