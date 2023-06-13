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
}

type uploadConfigRequestData struct {
	CatalogPath string `json:"catalog_path"`
	Content     string `json:"content"`
}

func MakeNMAUploadConfigOp(
	name string,
	vdb *VCoordinationDatabase,
	bootstrapHosts []string,
	endpoint string,
	fileContent *string,
) NMAUploadConfigOp {
	nmaUploadConfigOp := NMAUploadConfigOp{}
	nmaUploadConfigOp.name = name
	nmaUploadConfigOp.endpoint = endpoint
	nmaUploadConfigOp.fileContent = fileContent
	nmaUploadConfigOp.catalogPathMap = make(map[string]string)
	newNodeHosts := util.SliceDiff(vdb.HostList, bootstrapHosts)
	nmaUploadConfigOp.hosts = newNodeHosts

	for _, host := range newNodeHosts {
		vnode, ok := vdb.HostNodeMap[host]
		if !ok {
			msg := fmt.Errorf("[%s] fail to get catalog path from host %s", name, host)
			panic(msg)
		}
		nmaUploadConfigOp.catalogPathMap[host] = vnode.CatalogPath
	}

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

func (op *NMAUploadConfigOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	err := op.setupRequestBody(op.hosts)
	if err != nil {
		return MakeClusterOpResultException()
	}
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *NMAUploadConfigOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *NMAUploadConfigOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

func (op *NMAUploadConfigOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// the response object will be a dictionary including the destination of the config file, e.g.,:
			// {"destination":"/data/vcluster_test_db/v_vcluster_test_db_node0003_catalog/vertica.conf"}
			responseObj, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
				success = false
				continue
			}
			_, ok := responseObj["destination"]
			if !ok {
				vlog.LogError(`[%s] response does not contain field "destination"`, op.name)
				success = false
			}
		} else {
			success = false
		}
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}
