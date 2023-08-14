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

	"golang.org/x/exp/slices"

	"github.com/vertica/vcluster/vclusterops/util"
)

type HTTPSCreateDepotOp struct {
	OpBase
	OpHTTPSBase
	NodeDepotPaths map[string]string
	RequestParams  map[string]string
}

func makeHTTPSCreateClusterDepotOp(vdb *VCoordinationDatabase, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) (HTTPSCreateDepotOp, error) {
	httpsCreateDepotOp := HTTPSCreateDepotOp{}
	httpsCreateDepotOp.name = "HTTPSCreateDepotOp"
	httpsCreateDepotOp.hosts = hosts
	httpsCreateDepotOp.useHTTPPassword = useHTTPPassword

	// store nodeName-depotPath values for later http response verification
	httpsCreateDepotOp.NodeDepotPaths = make(map[string]string)
	for host := range vdb.HostNodeMap {
		httpsCreateDepotOp.NodeDepotPaths[vdb.HostNodeMap[host].Name] = vdb.HostNodeMap[host].DepotPath
	}

	// set the query params, "path" is required, "size" is optional
	httpsCreateDepotOp.RequestParams = make(map[string]string)
	httpsCreateDepotOp.RequestParams["path"] = vdb.DepotPrefix
	if vdb.DepotSize != "" {
		httpsCreateDepotOp.RequestParams["size"] = vdb.DepotSize
	}

	err := util.ValidateUsernameAndPassword(httpsCreateDepotOp.name, useHTTPPassword, userName)
	if err != nil {
		return httpsCreateDepotOp, err
	}
	httpsCreateDepotOp.userName = userName
	httpsCreateDepotOp.httpsPassword = httpsPassword
	return httpsCreateDepotOp, nil
}

func (op *HTTPSCreateDepotOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("cluster/depot")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *HTTPSCreateDepotOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSCreateDepotOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

// this struct is for parsing http response
type CreateDepotNodeRsp struct {
	NodeName  string `json:"node"`
	DepotPath string `json:"depot_location"`
}

type CreateDepotClusterRsp struct {
	ClusterRsp []CreateDepotNodeRsp `json:"depots"`
}

func (op *HTTPSCreateDepotOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		/* decode the json-format response
		The successful response object will be a dictionary list like below:
		{
		  "depots": [
		    {
		      "node": "node01",
		      "depot_location": "TMPDIR/create_depot/test_db/node01_depot"
		    },
		    {
		      "node": "node02",
		      "depot_location": "TMPDIR/create_depot/test_db/node01_depot"
		    },
		    {
		      "node": "node03",
		      "depot_location": "TMPDIR/create_depot/test_db/node01_depot"
		    }
		  ]
		} */
		createDepotClusterRsp := CreateDepotClusterRsp{}
		err := op.parseAndCheckResponse(host, result.content, &createDepotClusterRsp)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// verify if the node name and the depot location are correct
		for nodeName, depotPath := range op.NodeDepotPaths {
			idx := slices.IndexFunc(createDepotClusterRsp.ClusterRsp, func(rsp CreateDepotNodeRsp) bool {
				return rsp.NodeName == nodeName && rsp.DepotPath == depotPath
			})
			if idx == -1 {
				err = fmt.Errorf(`[%s] create depot %s failed for node %s on host %s`, op.name, depotPath, nodeName, host)
				allErrs = errors.Join(allErrs, err)
				// not break here because we want to log all the failed nodes
			}
		}
		// verify if https response contains some nodes/depots not in the required ones
		for _, nodeRsp := range createDepotClusterRsp.ClusterRsp {
			if depotPath, ok := op.NodeDepotPaths[nodeRsp.NodeName]; !ok || depotPath != nodeRsp.DepotPath {
				err = fmt.Errorf(`[%s] an unwanted depot %s gets created for node %s on host %s`,
					op.name, nodeRsp.DepotPath, nodeRsp.NodeName, host)
				allErrs = errors.Join(allErrs, err)
				// not break here because we want to log all the unwanted depots
			}
		}
	}

	return allErrs
}

func (op *HTTPSCreateDepotOp) finalize(_ *OpEngineExecContext) error {
	return nil
}
