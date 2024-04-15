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

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/vertica/vcluster/vclusterops/util"
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
func produceTransferConfigOps(instructions *[]clusterOp, sourceConfigHost,
	targetHosts []string, vdb *VCoordinationDatabase) {
	var verticaConfContent string
	nmaDownloadVerticaConfigOp := makeNMADownloadConfigOp(
		"NMADownloadVerticaConfigOp", sourceConfigHost, "config/vertica", &verticaConfContent, vdb)
	nmaUploadVerticaConfigOp := makeNMAUploadConfigOp(
		"NMAUploadVerticaConfigOp", sourceConfigHost, targetHosts, "config/vertica", &verticaConfContent, vdb)
	var spreadConfContent string
	nmaDownloadSpreadConfigOp := makeNMADownloadConfigOp(
		"NMADownloadSpreadConfigOp", sourceConfigHost, "config/spread", &spreadConfContent, vdb)
	nmaUploadSpreadConfigOp := makeNMAUploadConfigOp(
		"NMAUploadSpreadConfigOp", sourceConfigHost, targetHosts, "config/spread", &spreadConfContent, vdb)
	*instructions = append(*instructions,
		&nmaDownloadVerticaConfigOp,
		&nmaUploadVerticaConfigOp,
		&nmaDownloadSpreadConfigOp,
		&nmaUploadSpreadConfigOp,
	)
}

// Get catalog path after we have db information from /catalog/database endpoint
func updateCatalogPathMapFromCatalogEditor(hosts []string, nmaVDB *nmaVDatabase, catalogPathMap map[string]string) error {
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

// Get primary nodes with latest catalog from catalog editor if the primaryHostsWithLatestCatalog info doesn't exist in execContext
func getPrimaryHostsWithLatestCatalog(nmaVDB *nmaVDatabase, hostsWithLatestCatalog []string, execContext *opEngineExecContext) []string {
	if len(execContext.primaryHostsWithLatestCatalog) > 0 {
		return execContext.primaryHostsWithLatestCatalog
	}
	emptyPrimaryHostsString := []string{}
	primaryHostsSet := mapset.NewSet[string]()
	for host, vnode := range nmaVDB.HostNodeMap {
		if vnode.IsPrimary {
			primaryHostsSet.Add(host)
		}
	}
	hostsWithLatestCatalogSet := mapset.NewSet(hostsWithLatestCatalog...)
	primaryHostsWithLatestCatalog := hostsWithLatestCatalogSet.Intersect(primaryHostsSet)
	primaryHostsWithLatestCatalogList := primaryHostsWithLatestCatalog.ToSlice()
	if len(primaryHostsWithLatestCatalogList) == 0 {
		return emptyPrimaryHostsString
	}
	execContext.primaryHostsWithLatestCatalog = primaryHostsWithLatestCatalogList // save the primaryHostsWithLatestCatalog to execContext
	return primaryHostsWithLatestCatalogList
}

// The following structs will store hosts' necessary information for https_get_up_nodes_op,
// https_get_nodes_information_from_running_db, and incoming operations.
type nodeStateInfo struct {
	Address          string   `json:"address"`
	State            string   `json:"state"`
	Database         string   `json:"database"`
	CatalogPath      string   `json:"catalog_path"`
	DepotPath        string   `json:"depot_path"`
	StorageLocations []string `json:"data_path"`
	Subcluster       string   `json:"subcluster_name"`
	IsPrimary        bool     `json:"is_primary"`
	Name             string   `json:"name"`
	Sandbox          string   `json:"sandbox_name"`
	Version          string   `json:"build_info"`
}

func (node *nodeStateInfo) asNodeInfo() (n NodeInfo, err error) {
	n = node.asNodeInfoWoVer()
	// version can be, eg, v24.0.0-<revision> or v23.4.0-<hotfix|date>-<revision> including a hotfix or daily build date
	verWithHotfix := 3
	verWithoutHotfix := 2
	if parts := strings.Split(node.Version, "-"); len(parts) == verWithHotfix {
		n.Version = parts[0] + "-" + parts[1]
		n.Revision = parts[2]
	} else if len(parts) == verWithoutHotfix {
		n.Version = parts[0]
		n.Revision = parts[1]
	} else {
		err = fmt.Errorf("failed to parse version '%s'", node.Version)
	}
	return
}

// asNodeInfoWoVer will create a NodeInfo with empty Version and Revision
func (node *nodeStateInfo) asNodeInfoWoVer() (n NodeInfo) {
	n.Address = node.Address
	n.Name = node.Name
	n.State = node.State
	n.CatalogPath = node.CatalogPath
	n.Subcluster = node.Subcluster
	n.IsPrimary = node.IsPrimary
	return
}

type nodesStateInfo struct {
	NodeList []*nodeStateInfo `json:"node_list"`
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

// getVDBFromRunningDB will retrieve db configurations from a non-sandboxed host by calling https endpoints of a running db
func (vcc VClusterCommands) getVDBFromRunningDB(vdb *VCoordinationDatabase, options *DatabaseOptions) error {
	return vcc.getVDBFromRunningDBImpl(vdb, options, false, util.MainClusterSandbox)
}

// getVDBFromRunningDB will retrieve db configurations from any UP host by calling https endpoints of a running db
func (vcc VClusterCommands) getVDBFromRunningDBIncludeSandbox(vdb *VCoordinationDatabase, options *DatabaseOptions, sandbox string) error {
	return vcc.getVDBFromRunningDBImpl(vdb, options, true, sandbox)
}

// getVDBFromRunningDB will retrieve db configurations by calling https endpoints of a running db
func (vcc VClusterCommands) getVDBFromRunningDBImpl(vdb *VCoordinationDatabase, options *DatabaseOptions,
	allowUseSandboxRes bool, sandbox string) error {
	err := options.setUsePassword(vcc.Log)
	if err != nil {
		return fmt.Errorf("fail to set userPassword while retrieving database configurations, %w", err)
	}

	httpsGetNodesInfoOp, err := makeHTTPSGetNodesInfoOp(options.DBName, options.Hosts,
		options.usePassword, options.UserName, options.Password, vdb, allowUseSandboxRes, sandbox)
	if err != nil {
		return fmt.Errorf("fail to produce httpsGetNodesInfo instructions while retrieving database configurations, %w", err)
	}

	httpsGetClusterInfoOp, err := makeHTTPSGetClusterInfoOp(options.DBName, options.Hosts,
		options.usePassword, options.UserName, options.Password, vdb)
	if err != nil {
		return fmt.Errorf("fail to produce httpsGetClusterInfo instructions while retrieving database configurations, %w", err)
	}

	var instructions []clusterOp
	instructions = append(instructions, &httpsGetNodesInfoOp, &httpsGetClusterInfoOp)

	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		return fmt.Errorf("fail to retrieve database configurations, %w", err)
	}

	return nil
}

// getClusterInfoFromRunningDB will retrieve db configurations by calling https endpoints of a running db
func (vcc VClusterCommands) getClusterInfoFromRunningDB(vdb *VCoordinationDatabase, options *DatabaseOptions) error {
	err := options.setUsePassword(vcc.Log)
	if err != nil {
		return fmt.Errorf("fail to set userPassword while retrieving cluster configurations, %w", err)
	}

	httpsGetClusterInfoOp, err := makeHTTPSGetClusterInfoOp(options.DBName, options.Hosts,
		options.usePassword, options.UserName, options.Password, vdb)
	if err != nil {
		return fmt.Errorf("fail to produce httpsGetClusterInfo instructions while retrieving cluster configurations, %w", err)
	}

	var instructions []clusterOp
	instructions = append(instructions, &httpsGetClusterInfoOp)

	certs := httpsCerts{key: options.Key, cert: options.Cert, caCert: options.CaCert}
	clusterOpEngine := makeClusterOpEngine(instructions, &certs)
	err = clusterOpEngine.run(vcc.Log)
	if err != nil {
		return fmt.Errorf("fail to retrieve cluster configurations, %w", err)
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

// getInitiator will pick an initiator from the up host list to execute https calls
// such that the initiator is also among the user provided host list
func getInitiatorFromUpHosts(upHosts, userProvidedHosts []string) string {
	// Create a hash set for user-provided hosts
	userHostsSet := mapset.NewSet[string](userProvidedHosts...)

	// Iterate through upHosts and check if any host is in the userHostsSet
	for _, upHost := range upHosts {
		if userHostsSet.Contains(upHost) {
			return upHost
		}
	}

	// Return an empty string if no matching host is found
	return ""
}

// validates each host has an entry in each map
func validateHostMaps(hosts []string, maps ...map[string]string) error {
	var allErrors error
	for _, strMap := range maps {
		for _, host := range hosts {
			val, ok := strMap[host]
			if !ok || val == "" {
				allErrors = errors.Join(allErrors,
					fmt.Errorf("configuration map missing entry for host %s", host))
			}
		}
	}
	return allErrors
}
