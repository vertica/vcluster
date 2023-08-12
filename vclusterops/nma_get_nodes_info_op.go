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
)

// nmaGetNodesInfoOp get nodes info from the NMA /v1/nodes endpoint.
// The result will be saved into a VCoordinationDatabase object.
type nmaGetNodesInfoOp struct {
	OpBase
	dbName        string
	catalogPrefix string
	vdb           *VCoordinationDatabase
}

func makeNMAGetNodesInfoOp(hosts []string,
	dbName, catalogPrefix string,
	vdb *VCoordinationDatabase) nmaGetNodesInfoOp {
	op := nmaGetNodesInfoOp{}
	op.name = "NMAGetNodesInfoOp"
	op.hosts = hosts
	op.dbName = dbName
	op.catalogPrefix = catalogPrefix
	op.vdb = vdb
	op.vdb.HostNodeMap = make(map[string]VCoordinationNode)
	return op
}

func (op *nmaGetNodesInfoOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint("nodes")
		httpRequest.QueryParams = map[string]string{"db_name": op.dbName, "catalog_prefix": op.catalogPrefix}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *nmaGetNodesInfoOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *nmaGetNodesInfoOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaGetNodesInfoOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *nmaGetNodesInfoOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			var vnode VCoordinationNode
			err := op.parseAndCheckResponse(host, result.content, &vnode)
			if err != nil {
				return errors.Join(allErrs, err)
			}
			vnode.Address = host
			op.vdb.HostNodeMap[host] = vnode
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
