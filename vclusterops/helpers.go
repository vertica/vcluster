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
	"path"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	ksafetyThreshold = 3
	ksafeValueZero   = 0
	ksafeValueOne    = 1
)

// produceTransferConfigOps generates instructions to transfert some config
// files from a sourceConfig node to new nodes.
func produceTransferConfigOps(instructions *[]ClusterOp, sourceConfigHost, hosts, newNodeHosts []string, vdb *VCoordinationDatabase) {
	var verticaConfContent string
	nmaDownloadVerticaConfigOp := makeNMADownloadConfigOp(
		"NMADownloadVerticaConfigOp", sourceConfigHost, "config/vertica", &verticaConfContent, vdb)
	nmaUploadVerticaConfigOp := makeNMAUploadConfigOp(
		"NMAUploadVerticaConfigOp", sourceConfigHost, hosts, newNodeHosts, "config/vertica", &verticaConfContent, vdb)
	var spreadConfContent string
	nmaDownloadSpreadConfigOp := makeNMADownloadConfigOp(
		"NMADownloadSpreadConfigOp", sourceConfigHost, "config/spread", &spreadConfContent, vdb)
	nmaUploadSpreadConfigOp := makeNMAUploadConfigOp(
		"NMAUploadSpreadConfigOp", sourceConfigHost, hosts, newNodeHosts, "config/spread", &spreadConfContent, vdb)
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
			return errors.New(vlog.ErrorLog + errMsg)
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

// The following structs will store hosts' necessary information for https_get_up_nodes_op,
// https_get_nodes_information_from_running_db, and incoming operations.
type NodeStateInfo struct {
	Address     string `json:"address"`
	State       string `json:"state"`
	Database    string `json:"database"`
	CatalogPath string `json:"catalog_path"`
	Subcluster  string `json:"subcluster_name"`
	IsPrimary   bool   `json:"is_primary"`
	Name        string `json:"name"`
}

type NodesStateInfo struct {
	NodeList []NodeStateInfo `json:"node_list"`
}

// getInitiatorHost returns as initiator the first primary up node that is not
// in the list of hosts to skip.
func getInitiatorHost(primaryUpNodes, hostsToSkip []string) string {
	initiatorHosts := util.SliceDiff(primaryUpNodes, hostsToSkip)
	if len(initiatorHosts) == 0 {
		return ""
	}
	return initiatorHosts[0]
}

// getVDBFromRunningDB will retrieve db configurations by calling https endpoints of a running db
func getVDBFromRunningDB(vdb *VCoordinationDatabase, options *DatabaseOptions) error {
	err := options.SetUsePassword()
	if err != nil {
		vlog.LogPrintError("fail to set userPassword while retrieving database configurations, %v", err)
		return err
	}

	httpsGetNodesInfoOp, err := makeHTTPSGetNodesInfoOp(*options.Name, options.Hosts,
		options.usePassword, *options.UserName, options.Password, vdb)
	if err != nil {
		vlog.LogPrintError("fail to produce httpsGetNodesInfo instructions while retrieving database configurations, %v", err)
		return err
	}

	httpsGetClusterInfoOp, err := makeHTTPSGetClusterInfoOp(*options.Name, options.Hosts,
		options.usePassword, *options.UserName, options.Password, vdb)
	if err != nil {
		vlog.LogPrintError("fail to produce httpsGetClusterInfo instructions while retrieving database configurations, %v", err)
		return err
	}

	var instructions []ClusterOp
	instructions = append(instructions, &httpsGetNodesInfoOp, &httpsGetClusterInfoOp)

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.Run()
	if err != nil {
		vlog.LogPrintError("fail to retrieve database configurations, %v", err)
		return err
	}

	return nil
}

// appendHTTPSFailureError is internally used by https operations for appending an error message to the existing error
func appendHTTPSFailureError(allErrs error) error {
	return errors.Join(allErrs, fmt.Errorf("could not find a host with a passing result"))
}
