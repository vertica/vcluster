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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type NMAUploadConfigOp struct {
	OpBase
	catalogPathMap     map[string]string
	endpoint           string
	fileContent        *string
	hostRequestBodyMap map[string]string
	sourceConfigHost   []string
	newNodeHosts       []string
	skipExecute        bool // This can be set during prepare if we determine no work is needed
}

type uploadConfigRequestData struct {
	CatalogPath string `json:"catalog_path"`
	Content     string `json:"content"`
}

// MakeNMAUploadConfigOp sets up the input parameters from the user for the upload operation.
// To start the DB, insert a nil value for sourceConfigHost and newNodeHosts, and
// provide a list of database hosts for hosts.
// To create the DB, use the bootstrapHost value for sourceConfigHost, a nil value for newNodeHosts,
// and provide a list of database hosts for hosts.
// To add nodes to the DB, use the bootstrapHost value for sourceConfigHost, a list of newly added nodes
// for newNodeHosts and provide a nil value for hosts.
func MakeNMAUploadConfigOp(
	opName string,
	hostCatalogPath map[string]string, // map <host,catalogPath> e.g. <ip1:/data/{db_name}/v_{db_name}_node0001_catalog/>
	sourceConfigHost []string, // source host for transferring configuration files, specifically, it is
	// 1. the bootstrap host when creating the database
	// 2. the host with the highest catalog version for starting a database or starting nodes
	hosts []string, // list of hosts of database to participate in database
	newNodeHosts []string, // list of new hosts is added to the database
	endpoint string,
	fileContent *string,
) NMAUploadConfigOp {
	nmaUploadConfigOp := NMAUploadConfigOp{}
	nmaUploadConfigOp.name = opName
	nmaUploadConfigOp.endpoint = endpoint
	nmaUploadConfigOp.fileContent = fileContent
	nmaUploadConfigOp.catalogPathMap = make(map[string]string)
	nmaUploadConfigOp.hosts = hosts
	nmaUploadConfigOp.sourceConfigHost = sourceConfigHost
	nmaUploadConfigOp.newNodeHosts = newNodeHosts

	return nmaUploadConfigOp
}

func (op *NMAUploadConfigOp) setupRequestBody(hosts []string) error {
	op.hostRequestBodyMap = make(map[string]string)

	for _, host := range hosts {
		uploadConfigData := uploadConfigRequestData{}
		uploadConfigData.CatalogPath = op.catalogPathMap[host]
		uploadConfigData.Content = *op.fileContent

		dataBytes, err := json.Marshal(uploadConfigData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	return nil
}

func (op *NMAUploadConfigOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint(op.endpoint)
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMAUploadConfigOp) Prepare(execContext *OpEngineExecContext) error {
	if op.sourceConfigHost == nil {
		//  if the host with the highest catalog version for starting a database or starting nodes is nil value
		// 	we identify the hosts that need to be synchronized.
		hostsWithLatestCatalog := execContext.hostsWithLatestCatalog
		if len(hostsWithLatestCatalog) == 0 {
			return fmt.Errorf("could not find at least one host with the latest catalog")
		}
		hostsNeedCatalogSync := util.SliceDiff(op.hosts, hostsWithLatestCatalog)
		// Update the hosts that need to synchronize the catalog
		op.hosts = hostsNeedCatalogSync
		// If no hosts to upload, skip this operation. This can happen if all
		// hosts have the latest catalog.
		if len(op.hosts) == 0 {
			vlog.LogInfo("no hosts require an upload, skipping the operation")
			op.skipExecute = true
			return nil
		}
	} else {
		if op.newNodeHosts == nil {
			// If the list of newly added hosts is null, the sourceConfigHost host will be the bootstrapHost input
			// when creating the database
			// we identify the hosts that need to be synchronized from bootstrapHost and list of hosts input
			op.hosts = util.SliceDiff(op.hosts, op.sourceConfigHost)
		} else {
			// The hosts that need to be synchronized are the list of newly added hosts.
			op.hosts = op.newNodeHosts
		}
	}

	// Update the catalogPathMap for next upload operation's steps from information of catalog editor
	nmaVDB := execContext.nmaVDatabase
	op.catalogPathMap = make(map[string]string)
	err := updateCatalogPathMapFromCatalogEditor(op.hosts, &nmaVDB, op.catalogPathMap)
	if err != nil {
		return fmt.Errorf("failed to get catalog paths from catalog editor: %w", err)
	}

	err = op.setupRequestBody(op.hosts)
	if err != nil {
		return err
	}
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *NMAUploadConfigOp) Execute(execContext *OpEngineExecContext) error {
	// Behave as a no-op if Prepare() determined there was nothing to do
	if op.skipExecute {
		return nil
	}
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAUploadConfigOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

func (op *NMAUploadConfigOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// the response object will be a dictionary including the destination of the config file, e.g.,:
			// {"destination":"/data/vcluster_test_db/v_vcluster_test_db_node0003_catalog/vertica.conf"}
			responseObj, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
				allErrs = errors.Join(allErrs, err)
				continue
			}
			_, ok := responseObj["destination"]
			if !ok {
				err = fmt.Errorf(`[%s] response does not contain field "destination"`, op.name)
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
