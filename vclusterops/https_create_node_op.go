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

	"github.com/vertica/vcluster/vclusterops/util"
)

type HTTPSCreateNodeOp struct {
	OpBase
	OpHTTPSBase
	RequestParams map[string]string
}

func makeHTTPSCreateNodeOp(hosts []string, bootstrapHost []string,
	useHTTPPassword bool, userName string, httpsPassword *string,
	vdb *VCoordinationDatabase, scName string) (HTTPSCreateNodeOp, error) {
	createNodeOp := HTTPSCreateNodeOp{}
	createNodeOp.name = "HTTPSCreateNodeOp"
	createNodeOp.hosts = bootstrapHost
	createNodeOp.RequestParams = make(map[string]string)
	// HTTPS create node endpoint requires passing everything before node name
	createNodeOp.RequestParams["catalog-prefix"] = vdb.CatalogPrefix + "/" + vdb.Name
	createNodeOp.RequestParams["data-prefix"] = vdb.DataPrefix + "/" + vdb.Name
	createNodeOp.RequestParams["hosts"] = util.ArrayToString(hosts, ",")
	if scName != "" {
		createNodeOp.RequestParams["subcluster"] = scName
	}
	createNodeOp.useHTTPPassword = useHTTPPassword

	err := util.ValidateUsernameAndPassword(createNodeOp.name, useHTTPPassword, userName)
	if err != nil {
		return createNodeOp, err
	}

	createNodeOp.userName = userName
	createNodeOp.httpsPassword = httpsPassword
	return createNodeOp, nil
}

func (op *HTTPSCreateNodeOp) setupClusterHTTPRequest(hosts []string) error {
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

	return nil
}

func (op *HTTPSCreateNodeOp) updateQueryParams(execContext *OpEngineExecContext) error {
	for _, host := range op.hosts {
		profile, ok := execContext.networkProfiles[host]
		if !ok {
			return fmt.Errorf("[%s] unable to find network profile for host %s", op.name, host)
		}
		op.RequestParams["broadcast"] = profile.Broadcast
	}
	return nil
}

func (op *HTTPSCreateNodeOp) prepare(execContext *OpEngineExecContext) error {
	err := op.updateQueryParams(execContext)
	if err != nil {
		return err
	}

	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSCreateNodeOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSCreateNodeOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

type HTTPCreateNodeResponse map[string][]map[string]string

func (op *HTTPSCreateNodeOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// The response object will be a dictionary, an example:
			// {'created_nodes': [{'name': 'v_running_db_node0002', 'catalog_path': '/data/v_running_db_node0002_catalog'},
			//                    {'name': 'v_running_db_node0003', 'catalog_path': '/data/v_running_db_node0003_catalog'}]}
			var responseObj HTTPCreateNodeResponse
			err := op.parseAndCheckResponse(host, result.content, &responseObj)

			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}
			_, ok := responseObj["created_nodes"]
			if !ok {
				err = fmt.Errorf(`[%s] response does not contain field "created_nodes"`, op.name)
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
