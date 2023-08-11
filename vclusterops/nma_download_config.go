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

type NMADownloadConfigOp struct {
	OpBase
	catalogPathMap map[string]string
	endpoint       string
	fileContent    *string
	vdb            *VCoordinationDatabase
}

func makeNMADownloadConfigOp(
	opName string,
	sourceConfigHost []string,
	endpoint string,
	fileContent *string,
	vdb *VCoordinationDatabase,
) NMADownloadConfigOp {
	nmaDownloadConfigOp := NMADownloadConfigOp{}
	nmaDownloadConfigOp.name = opName
	nmaDownloadConfigOp.hosts = sourceConfigHost
	nmaDownloadConfigOp.endpoint = endpoint
	nmaDownloadConfigOp.fileContent = fileContent
	nmaDownloadConfigOp.vdb = vdb

	return nmaDownloadConfigOp
}

func (op *NMADownloadConfigOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint(op.endpoint)

		catalogPath, ok := op.catalogPathMap[host]
		if !ok {
			return fmt.Errorf("[%s] fail to get catalog path from host %s", op.name, host)
		}
		httpRequest.QueryParams = map[string]string{"catalog_path": catalogPath}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *NMADownloadConfigOp) prepare(execContext *OpEngineExecContext) error {
	op.catalogPathMap = make(map[string]string)
	// vdb is built by calling /cluster and /nodes endpoints of a running db.
	// If nodes' info is not available in vdb, we will get the host from execContext.nmaVDatabase which is build by reading the catalog editor
	if op.vdb == nil || len(op.vdb.HostNodeMap) == 0 {
		if op.hosts == nil {
			// If the host input is a nil value, we find the host with the latest catalog version to update the host input.
			// Otherwise, we use the host input.
			hostsWithLatestCatalog := execContext.hostsWithLatestCatalog
			if len(hostsWithLatestCatalog) == 0 {
				return fmt.Errorf("could not find at least one host with the latest catalog")
			}
			hostWithLatestCatalog := hostsWithLatestCatalog[:1]
			// update the host with the latest catalog
			op.hosts = hostWithLatestCatalog
		}
		// For createDb and AddNodes, sourceConfigHost input is the bootstrap host.
		// we update the catalogPathMap for next download operation's steps from information of catalog editor
		nmaVDB := execContext.nmaVDatabase
		err := updateCatalogPathMapFromCatalogEditor(op.hosts, &nmaVDB, op.catalogPathMap)
		if err != nil {
			return fmt.Errorf("failed to get catalog paths from catalog editor: %w", err)
		}
		// If vdb contains nodes' info, we will check if there are any primary up nodes.
		// If we found any primary up nodes, we set catalogPathMap based on their info in vdb.
	} else {
		// This case is used for restarting nodes operation.
		// Otherwise, we set catalogPathMap from the catalog editor (start_db, create_db).
		// For restartNodes, If the sourceConfigHost input is a nil value, we find any UP primary nodes as source host to update the host input.
		// we update the catalogPathMap for next download operation's steps from node information by using HTTPS /v1/nodes
		var primaryUpHosts []string
		for host := range op.vdb.HostNodeMap {
			if op.vdb.HostNodeMap[host].IsPrimary && op.vdb.HostNodeMap[host].State == util.NodeUpState {
				primaryUpHosts = append(primaryUpHosts, host)
				op.catalogPathMap[host] = path.Dir(op.vdb.HostNodeMap[host].CatalogPath)
				break
			}
		}
		if len(primaryUpHosts) == 0 {
			return fmt.Errorf("could not find any primary up nodes")
		}
		op.hosts = primaryUpHosts
	}

	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMADownloadConfigOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMADownloadConfigOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *NMADownloadConfigOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		// VER-88362 will re-enable the result details and hide sensitive info in it
		vlog.LogPrintInfo("[%s] result from host %s summary %s",
			op.name, host, result.status.getStatusString())
		if result.isPassing() {
			// The content of config file will be stored as content of the response
			*op.fileContent = result.content
			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}

	return appendHTTPSFailureError(allErrs)
}
