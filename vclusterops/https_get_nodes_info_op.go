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
	opBase
	opHTTPSBase
	dbName string
	vdb    *VCoordinationDatabase
}

func makeHTTPSGetNodesInfoOp(logger vlog.Printer, dbName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string, vdb *VCoordinationDatabase,
) (httpsGetNodesInfoOp, error) {
	op := httpsGetNodesInfoOp{}
	op.name = "HTTPSGetNodeInfoOp"
	op.logger = logger.WithName(op.name)
	op.dbName = dbName
	op.hosts = hosts
	op.vdb = vdb

	err := op.validateAndSetUsernameAndPassword(op.name, useHTTPPassword, userName,
		httpsPassword)

	return op, err
}

func (op *httpsGetNodesInfoOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.buildHTTPSEndpoint("nodes")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsGetNodesInfoOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsGetNodesInfoOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsGetNodesInfoOp) processResult(_ *opEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isUnauthorizedRequest() {
			return fmt.Errorf("[%s] wrong password/certificate for https service on host %s",
				op.name, host)
		}

		if result.isPassing() {
			// parse the /nodes endpoint response
			nodesStates := nodesStateInfo{}
			err := op.parseAndCheckResponse(host, result.content, &nodesStates)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				break
			}
			// save nodes info to vdb
			op.vdb.HostNodeMap = makeVHostNodeMap()
			op.vdb.HostList = []string{}
			for _, node := range nodesStates.NodeList {
				if node.Database != op.dbName {
					err = fmt.Errorf(`[%s] database %s is running on host %s, rather than database %s`, op.name, node.Database, host, op.dbName)
					allErrs = errors.Join(allErrs, err)
					return appendHTTPSFailureError(allErrs)
				}
				vNode := makeVCoordinationNode()
				vNode.Name = node.Name
				vNode.Address = node.Address
				vNode.CatalogPath = node.CatalogPath
				vNode.IsPrimary = node.IsPrimary
				vNode.State = node.State
				vNode.Subcluster = node.Subcluster
				vNode.Sandbox = node.Sandbox
				if node.IsPrimary && node.State == util.NodeUpState {
					op.vdb.PrimaryUpNodes = append(op.vdb.PrimaryUpNodes, node.Address)
				}
				err := op.vdb.addNode(&vNode)
				if err != nil {
					allErrs = errors.Join(allErrs, err)
					return appendHTTPSFailureError(allErrs)
				}
				// extract catalog prefix from node's catalog path
				// catalog prefix is preceding db name
				dbPath := "/" + node.Database
				index := strings.Index(node.CatalogPath, dbPath)
				if index == -1 {
					op.logger.PrintWarning("[%s] failed to get catalog prefix because catalog path %s does not contain database name %s",
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

func (op *httpsGetNodesInfoOp) finalize(_ *opEngineExecContext) error {
	return nil
}
