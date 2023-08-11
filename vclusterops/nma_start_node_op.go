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
)

type nmaStartNodeOp struct {
	OpBase
	hostRequestBodyMap map[string]string
	vdb                *VCoordinationDatabase
}

func makeNMAStartNodeOp(hosts []string) nmaStartNodeOp {
	startNodeOp := nmaStartNodeOp{}
	startNodeOp.name = "NMAStartNodeOp"
	startNodeOp.hosts = hosts
	return startNodeOp
}

func makeNMAStartNodeOpWithVDB(hosts []string, vdb *VCoordinationDatabase) nmaStartNodeOp {
	startNodeOp := makeNMAStartNodeOp(hosts)
	startNodeOp.vdb = vdb
	return startNodeOp
}

func (op *nmaStartNodeOp) updateRequestBody(execContext *OpEngineExecContext) error {
	op.hostRequestBodyMap = make(map[string]string)
	// If the execContext.StartUpCommand  is nil, we will use startup command information from NMA Read Catalog Editor.
	// This case is used for certain operations (e.g., start_db, create_db) when the database is down,
	// and we need to use the NMA catalog/database endpoint.
	// Otherwise, we can use the startup command file from the HTTPS startup/commands endpoint when the database is up.
	if execContext.startupCommandMap != nil {
		// map {host: startCommand} e.g.,
		// {ip1:[/opt/vertica/bin/vertica -D /data/practice_db/v_practice_db_node0001_catalog -C
		// practice_db -n v_practice_db_node0001 -h 192.168.1.101 -p 5433 -P 4803 -Y ipv4]}
		hostStartCommandMap := make(map[string][]string)
		for host := range op.vdb.HostNodeMap {
			hoststartCommand, ok := execContext.startupCommandMap[op.vdb.HostNodeMap[host].Name]
			if ok {
				hostStartCommandMap[host] = hoststartCommand
			}
		}
		for _, host := range op.hosts {
			err := op.updateHostRequestBodyMapFromNodeStartCommand(host, hostStartCommandMap[host])
			if err != nil {
				return err
			}
		}
	} else {
		// use startup command information from NMA catalog/database endpoint when the database is down
		for _, host := range op.hosts {
			node, ok := execContext.nmaVDatabase.HostNodeMap[host]
			if !ok {
				return fmt.Errorf("[%s] the bootstrap node (%s) is not found from the catalog editor information: %+v",
					op.name, host, execContext.nmaVDatabase)
			}
			err := op.updateHostRequestBodyMapFromNodeStartCommand(host, node.StartCommand)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (op *nmaStartNodeOp) updateHostRequestBodyMapFromNodeStartCommand(host string, hostStartCommand []string) error {
	type NodeStartCommand struct {
		StartCommand []string `json:"start_command"`
	}
	nodeStartCommand := NodeStartCommand{StartCommand: hostStartCommand}
	marshaledCommand, err := json.Marshal(nodeStartCommand)
	if err != nil {
		return fmt.Errorf("[%s] fail to marshal start command to JSON string %w", op.name, err)
	}
	op.hostRequestBodyMap[host] = string(marshaledCommand)
	return nil
}

func (op *nmaStartNodeOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("nodes/start")
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *nmaStartNodeOp) prepare(execContext *OpEngineExecContext) error {
	err := op.updateRequestBody(execContext)
	if err != nil {
		return err
	}

	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *nmaStartNodeOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaStartNodeOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

type startNodeResponse struct {
	DBLogPath  string `json:"dbLogPath"`
	ReturnCode int    `json:"return_code"`
}

func (op *nmaStartNodeOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// the response object will be a dictionary including the dbLog path and a return code, e.g.,:
			// {'dbLogPath':  '/data/platform_test_db/dbLog',
			// 'return_code', 0 }

			responseObj := startNodeResponse{}
			err := op.parseAndCheckResponse(host, result.content, &responseObj)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}

			if responseObj.ReturnCode != 0 {
				err = fmt.Errorf(`[%s] return_code should be 0 but got %d`, op.name, responseObj.ReturnCode)
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
