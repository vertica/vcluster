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

	"github.com/vertica/vcluster/vclusterops/vlog"
)

type nmaStageSystemTablesOp struct {
	scrutinizeOpBase
	username string
	password *string
}

type stageSystemTablesRequestData struct {
	Username string `json:"username"`
	Password string `json:"password,omitempty"`
}

func makeNMAStageSystemTablesOp(logger vlog.Printer,
	id, username string,
	password *string,
	hostNodeNameMap map[string]string,
	hosts []string) (nmaStageSystemTablesOp, error) {
	// base members
	op := nmaStageSystemTablesOp{}
	op.name = "NMAStageSystemTablesOp"
	op.logger = logger.WithName(op.name)
	op.hosts = hosts

	// scrutinize members
	op.id = id
	op.batch = scrutinizeBatchSystemTables
	op.hostNodeNameMap = hostNodeNameMap
	op.httpMethod = PostMethod
	op.urlSuffix = "/vs"

	// custom members
	op.username = username
	op.password = password

	// the caller is responsible for making sure hosts and maps match up exactly
	err := validateHostMaps(hosts, hostNodeNameMap)
	if err != nil {
		return op, err
	}

	return op, nil
}

func (op *nmaStageSystemTablesOp) setupRequestBody(host string) error {
	op.hostRequestBodyMap = make(map[string]string, 1)
	data := stageSystemTablesRequestData{}
	data.Username = op.username
	if op.password != nil {
		data.Password = *op.password
	}

	dataBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
	}

	op.hostRequestBodyMap[host] = string(dataBytes)

	return nil
}

func (op *nmaStageSystemTablesOp) prepare(execContext *opEngineExecContext) error {
	// only run this task on a single up node
	// if there are no up nodes, skip this task
	if len(execContext.upHosts) == 0 {
		op.logger.PrintWarning("no up hosts to collect system tables from, skipping the operation")
		op.skipExecute = true
		return nil
	}

	host := getInitiatorFromUpHosts(execContext.upHosts, op.hosts)
	if host == "" {
		op.logger.PrintWarning("no up hosts among user specified hosts to collect system tables from, skipping the operation")
		op.skipExecute = true
		return nil
	}

	err := op.setupRequestBody(host)
	if err != nil {
		return err
	}

	// construct host list for interface purposes
	op.hosts = []string{host}

	execContext.dispatcher.setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *nmaStageSystemTablesOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaStageSystemTablesOp) finalize(_ *opEngineExecContext) error {
	return nil
}

func (op *nmaStageSystemTablesOp) processResult(_ *opEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// the body should be empty in non-error conditions
			op.logger.Info("System table staging initiated", "Host", host)
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
