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

	"github.com/vertica/vcluster/vclusterops/vlog"
)

type nmaCheckSystemTablesOp struct {
	scrutinizeOpBase
}

type checkSystemTablesResponseData struct {
	Name string `json:"name"`
}

func makeNMACheckSystemTablesOp(logger vlog.Printer,
	id string,
	hostNodeNameMap map[string]string,
	hosts []string) (nmaCheckSystemTablesOp, error) {
	// base members
	op := nmaCheckSystemTablesOp{}
	op.name = "NMACheckSystemTablesOp"
	op.logger = logger.WithName(op.name)
	op.hosts = hosts

	// scrutinize members
	op.id = id
	op.batch = scrutinizeBatchSystemTables
	op.hostNodeNameMap = hostNodeNameMap
	op.httpMethod = GetMethod
	op.urlSuffix = "/vs-status"

	// the caller is responsible for making sure hosts and maps match up exactly
	err := validateHostMaps(hosts, hostNodeNameMap)
	if err != nil {
		return op, err
	}

	return op, nil
}

func (op *nmaCheckSystemTablesOp) getPollingTimeout() int {
	return ScrutinizePollingTimeout
}

func (op *nmaCheckSystemTablesOp) prepare(execContext *opEngineExecContext) error {
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

	// construct host list for interface purposes
	op.hosts = []string{host}

	// prepare GET request with no params or body
	execContext.dispatcher.setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *nmaCheckSystemTablesOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaCheckSystemTablesOp) finalize(_ *opEngineExecContext) error {
	return nil
}

func (op *nmaCheckSystemTablesOp) processResult(execContext *opEngineExecContext) error {
	// wait until initiator has finished gathering system tables
	err := pollState(op, execContext)
	if err != nil {
		return fmt.Errorf("failed to stage system tables, %w", err)
	}

	// parse and log responses
	op.logger.Info("system tables finished staging")
	tableList := make([]checkSystemTablesResponseData, 0)
	return processStagedItemsResult(&op.scrutinizeOpBase, tableList)
}

func (op *nmaCheckSystemTablesOp) shouldStopPolling() (bool, error) {
	var allErrs error
	isProcessing := false

	// in practice, only one host
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// passing but not success -> 202 accepted
			if !result.isSuccess() {
				isProcessing = true
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	// any errors -> finished
	// 200 -> finished (isProcessing == false)
	// 202 -> continue (isProcessing == true)
	stop := (allErrs != nil) || !isProcessing
	return stop, allErrs
}
