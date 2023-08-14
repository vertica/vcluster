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
	"strings"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type httpsGetNodesInfoOp struct {
	OpBase
	OpHTTPSBase
	dbName string
	vdb    *VCoordinationDatabase
}

func makeHTTPSGetNodesInfoOp(dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, vdb *VCoordinationDatabase,
) (httpsGetNodesInfoOp, error) {
	op := httpsGetNodesInfoOp{}
	op.name = "HTTPSGetNodeInfoOp"
	op.dbName = dbName
	op.hosts = hosts
	op.useHTTPPassword = useHTTPPassword
	op.vdb = vdb

	if useHTTPPassword {
		err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
		if err != nil {
			return op, err
		}
		op.userName = userName
		op.httpsPassword = httpsPassword
	}

	return op, nil
}

func (op *httpsGetNodesInfoOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsGetNodesInfoOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsGetNodesInfoOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsGetNodesInfoOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			// parse the /nodes endpoint response
			nodesStateInfo := NodesStateInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesStateInfo)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				break
			}
			// save nodes info to vdb
			op.vdb.HostNodeMap = make(map[string]VCoordinationNode)
			for _, node := range nodesStateInfo.NodeList {
				if node.Database != op.dbName {
					err = fmt.Errorf(`[%s] database %s is running on host %s, rather than database %s`, op.name, node.Database, host, op.dbName)
					allErrs = errors.Join(allErrs, err)
					return appendHTTPSFailureError(allErrs)
				}
				op.vdb.HostList = append(op.vdb.HostList, node.Address)
				vNode := MakeVCoordinationNode()
				vNode.Name = node.Name
				vNode.Address = node.Address
				vNode.CatalogPath = node.CatalogPath
				vNode.IsPrimary = node.IsPrimary
				vNode.State = node.State
				vNode.Subcluster = node.Subcluster
				if node.IsPrimary && node.State == util.NodeUpState {
					op.vdb.PrimaryUpNodes = append(op.vdb.PrimaryUpNodes, node.Address)
				}
				op.vdb.HostNodeMap[node.Address] = vNode
				// extract catalog prefix from node's catalog path
				// catalog prefix is preceding db name
				dbPath := "/" + node.Database
				index := strings.Index(node.CatalogPath, dbPath)
				if index == -1 {
					vlog.LogPrintWarning("[%s] failed to get catalog prefix because catalog path %s does not contain database name %s",
						op.name, node.CatalogPath, node.Database)
				}
				op.vdb.CatalogPrefix = node.CatalogPath[:index]
			}

			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}
	return appendHTTPSFailureError(allErrs)
}

func (op *httpsGetNodesInfoOp) finalize(_ *OpEngineExecContext) error {
	return nil
}
