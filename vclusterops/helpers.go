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
	"path"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

// produceTransferConfigOps generates instructions to transfert some config
// files from a bootstrap node to new nodes.
func produceTransferConfigOps(instructions *[]ClusterOp, bootstrapHost, hosts, newNodeHosts []string, hostNodeMap map[string]string) {
	var verticaConfContent string
	nmaDownloadVerticaConfigOp := MakeNMADownloadConfigOp(
		"NMADownloadVerticaConfigOp", hostNodeMap, bootstrapHost, "config/vertica", &verticaConfContent)
	nmaUploadVerticaConfigOp := MakeNMAUploadConfigOp(
		"NMAUploadVerticaConfigOp", hostNodeMap, bootstrapHost, hosts, newNodeHosts, "config/vertica", &verticaConfContent)
	var spreadConfContent string
	nmaDownloadSpreadConfigOp := MakeNMADownloadConfigOp(
		"NMADownloadSpreadConfigOp", hostNodeMap, bootstrapHost, "config/spread", &spreadConfContent)
	nmaUploadSpreadConfigOp := MakeNMAUploadConfigOp(
		"NMAUploadSpreadConfigOp", hostNodeMap, bootstrapHost, hosts, newNodeHosts, "config/spread", &spreadConfContent)
	*instructions = append(*instructions,
		&nmaDownloadVerticaConfigOp,
		&nmaUploadVerticaConfigOp,
		&nmaDownloadSpreadConfigOp,
		&nmaUploadSpreadConfigOp,
	)
}

// Get catalog path after we have db information from /catalog/database endpoint
func updateCatalogPathMapFromCatalogEditor(hosts []string, nmaVDB *NmaVDatabase, catalogPathMap map[string]string) error {
	if len(hosts) == 0 {
		return fmt.Errorf("[%s] fail to get host with highest catalog version", nmaVDB.Name)
	}
	for _, host := range hosts {
		vnode, ok := nmaVDB.HostNodeMap[host]
		if !ok {
			return fmt.Errorf("fail to get catalog path from host %s", host)
		}

		// catalog/database endpoint gets the catalog path as /data/{db_name}/v_{db_name}_node0001_catalog/Catalog
		// We need the parent dir of the full catalog path /data/{db_name}/v_{db_name}_node0001_catalog/
		catalogPathMap[host] = path.Dir(vnode.CatalogPath)
	}
	return nil
}

// WriteClusterConfig writes config information to a yaml file.
func WriteClusterConfig(vdb *VCoordinationDatabase, configDir *string) error {
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

func mapHostToCatalogPath(hostNodeMap map[string]VCoordinationNode) map[string]string {
	hostCatalogPathMap := make(map[string]string)
	for host, vnode := range hostNodeMap {
		hostCatalogPathMap[host] = vnode.CatalogPath
	}

	return hostCatalogPathMap
}
