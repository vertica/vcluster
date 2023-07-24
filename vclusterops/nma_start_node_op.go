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

type NMAStartNodeOp struct {
	OpBase
	hostRequestBodyMap map[string]string
}

func MakeNMAStartNodeOp(hosts []string) NMAStartNodeOp {
	nmaStartNodeOp := NMAStartNodeOp{}
	nmaStartNodeOp.name = "NMAStartNodeOp"
	nmaStartNodeOp.hosts = hosts

	return nmaStartNodeOp
}

func (op *NMAStartNodeOp) updateRequestBody(execContext *OpEngineExecContext) error {
	op.hostRequestBodyMap = make(map[string]string)

	for _, host := range op.hosts {
		node, ok := execContext.nmaVDatabase.HostNodeMap[host]
		if !ok {
			return fmt.Errorf("[%s] the bootstrap node (%s) is not found from the catalog editor information: %+v",
				op.name, host, execContext.nmaVDatabase)
		}

		marshaledCommand, err := json.Marshal(node.StartCommand)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal start command, %w", op.name, err)
		}
		op.hostRequestBodyMap[host] = fmt.Sprintf(`{"start_command": %s}`, string(marshaledCommand))
	}

	return nil
}

func (op *NMAStartNodeOp) setupClusterHTTPRequest(hosts []string) {
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
}

func (op *NMAStartNodeOp) Prepare(execContext *OpEngineExecContext) error {
	err := op.updateRequestBody(execContext)
	if err != nil {
		return err
	}

	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *NMAStartNodeOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAStartNodeOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

type startNodeResponse struct {
	DBLogPath  string `json:"dbLogPath"`
	ReturnCode int    `json:"return_code"`
}

func (op *NMAStartNodeOp) processResult(execContext *OpEngineExecContext) error {
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
