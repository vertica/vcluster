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

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type HTTPCreateNodeOp struct {
	OpBase
	OpHTTPBase
	RequestParams map[string]string
}

func MakeHTTPCreateNodeOp(opName string, hosts []string, bootstrapHost []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	vdb *VCoordinationDatabase) HTTPCreateNodeOp {
	createNodeOp := HTTPCreateNodeOp{}
	createNodeOp.name = opName
	createNodeOp.hosts = bootstrapHost
	createNodeOp.RequestParams = make(map[string]string)
	// HTTPS create node endpoint requires passing everything before node name
	createNodeOp.RequestParams["catalog-prefix"] = vdb.CatalogPrefix + "/" + vdb.Name
	createNodeOp.RequestParams["data-prefix"] = vdb.DataPrefix + "/" + vdb.Name
	createNodeOp.RequestParams["hosts"] = util.ArrayToString(hosts, ",")
	createNodeOp.useHTTPPassword = useHTTPPassword

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)

	createNodeOp.userName = userName
	createNodeOp.httpsPassword = httpsPassword
	return createNodeOp
}

func (op *HTTPCreateNodeOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		// note that this will be updated in Prepare()
		// because the endpoint only accept parameters in query
		httpRequest.BuildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPCreateNodeOp) updateQueryParams(execContext *OpEngineExecContext) {
	for _, host := range op.hosts {
		profile, ok := execContext.networkProfiles[host]
		if !ok {
			msg := fmt.Sprintf("[%s] unable to find network profile for host %s", op.name, host)
			panic(msg)
		}
		op.RequestParams["broadcast"] = profile.Broadcast
	}
}

func (op *HTTPCreateNodeOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	op.updateQueryParams(execContext)
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPCreateNodeOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPCreateNodeOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

type HTTPCreateNodeResponse map[string][]map[string]string

func (op *HTTPCreateNodeOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// The response object will be a dictionary, an example:
			// {'created_nodes': [{'name': 'v_running_db_node0002', 'catalog_path': '/data/v_running_db_node0002_catalog'},
			//                    {'name': 'v_running_db_node0003', 'catalog_path': '/data/v_running_db_node0003_catalog'}]}
			var responseObj HTTPCreateNodeResponse
			err := op.parseAndCheckResponse(host, result.content, &responseObj)

			if err != nil {
				success = false
				continue
			}
			_, ok := responseObj["created_nodes"]
			if !ok {
				vlog.LogError(`[%s] response does not contain field "created_nodes"`, op.name)
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
