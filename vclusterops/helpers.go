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
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	ksafetyThreshold        = 3
	ksafeValueZero          = 0
	ksafeValueOne           = 1
	numOfAWSAuthComponents  = 2
	nmaSuccessfulReturnCode = 0
)

// produceTransferConfigOps generates instructions to transfert some config
// files from a sourceConfig node to target nodes.
func produceTransferConfigOps(log vlog.Printer, instructions *[]ClusterOp, sourceConfigHost,
	targetHosts []string, vdb *VCoordinationDatabase) {
	var verticaConfContent string
	nmaDownloadVerticaConfigOp := makeNMADownloadConfigOp(
		log, "NMADownloadVerticaConfigOp", sourceConfigHost, "config/vertica", &verticaConfContent, vdb)
	nmaUploadVerticaConfigOp := makeNMAUploadConfigOp(
		log, "NMAUploadVerticaConfigOp", sourceConfigHost, targetHosts, "config/vertica", &verticaConfContent, vdb)
	var spreadConfContent string
	nmaDownloadSpreadConfigOp := makeNMADownloadConfigOp(
		log, "NMADownloadSpreadConfigOp", sourceConfigHost, "config/spread", &spreadConfContent, vdb)
	nmaUploadSpreadConfigOp := makeNMAUploadConfigOp(
		log, "NMAUploadSpreadConfigOp", sourceConfigHost, targetHosts, "config/spread", &spreadConfContent, vdb)
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
func getInitiatorHost(primaryUpNodes, hostsToSkip []string) (string, error) {
	initiatorHosts := util.SliceDiff(primaryUpNodes, hostsToSkip)
	if len(initiatorHosts) == 0 {
		return "", fmt.Errorf("could not find any primary up nodes")
	}

	return initiatorHosts[0], nil
}

// getVDBFromRunningDB will retrieve db configurations by calling https endpoints of a running db
func (vcc *VClusterCommands) getVDBFromRunningDB(vdb *VCoordinationDatabase, options *DatabaseOptions) error {
	err := options.SetUsePassword(vcc.Log)
	if err != nil {
		return fmt.Errorf("fail to set userPassword while retrieving database configurations, %w", err)
	}

	httpsGetNodesInfoOp, err := makeHTTPSGetNodesInfoOp(vcc.Log, *options.DBName, options.Hosts,
		options.usePassword, *options.UserName, options.Password, vdb)
	if err != nil {
		return fmt.Errorf("fail to produce httpsGetNodesInfo instructions while retrieving database configurations, %w", err)
	}

	httpsGetClusterInfoOp, err := makeHTTPSGetClusterInfoOp(vcc.Log, *options.DBName, options.Hosts,
		options.usePassword, *options.UserName, options.Password, vdb)
	if err != nil {
		return fmt.Errorf("fail to produce httpsGetClusterInfo instructions while retrieving database configurations, %w", err)
	}

	var instructions []ClusterOp
	instructions = append(instructions, &httpsGetNodesInfoOp, &httpsGetClusterInfoOp)

	certs := HTTPSCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := MakeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.Run(vcc.Log)
	if err != nil {
		return fmt.Errorf("fail to retrieve database configurations, %w", err)
	}

	return nil
}

// getCatalogPath returns the catalog path after
// removing `/Catalog` suffix (if present).
// It is useful when we get /data/{db_name}/v_{db_name}_node0001_catalog/Catalog
// and we want the parent dir of the full catalog path which is
// /data/{db_name}/v_{db_name}_node0001_catalog/
func getCatalogPath(fullPath string) string {
	if !strings.HasSuffix(fullPath, "/Catalog") {
		return fullPath
	}

	return path.Dir(fullPath)
}

// appendHTTPSFailureError is internally used by https operations for appending an error message to the existing error
func appendHTTPSFailureError(allErrs error) error {
	return errors.Join(allErrs, fmt.Errorf("could not find a host with a passing result"))
}

// getInitiator will pick an initiator from a host list to execute https calls
func getInitiator(hosts []string) string {
	// simply use the first one in user input
	return hosts[0]
}

func cannotFindDBFromConfigErr(dbName string) error {
	return fmt.Errorf("database %s cannot be found in the config file", dbName)
}
