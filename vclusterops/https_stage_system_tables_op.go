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

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"golang.org/x/exp/maps"
)

type httpsStageSystemTablesOp struct {
	opBase
	opHTTPSBase
	id              string
	hostNodeNameMap map[string]string
	stagingDir      *string
}

type prepareStagingSystemTableRequestData struct {
	StagingDirectory string            `json:"staging_directory"`
	SystemTableList  []systemTableInfo `json:"system_table_list"`
}

func makeHTTPSStageSystemTablesOp(logger vlog.Printer,
	useHTTPPassword bool, userName string, httpsPassword *string,
	id string, hostNodeNameMap map[string]string,
	stagingDir *string,
) (httpsStageSystemTablesOp, error) {
	op := httpsStageSystemTablesOp{}
	op.name = "HTTPSStageSystemTablesOp"
	op.logger = logger.WithName(op.name)
	op.hosts = maps.Keys(hostNodeNameMap)
	op.useHTTPPassword = useHTTPPassword
	op.id = id
	op.hostNodeNameMap = hostNodeNameMap
	op.stagingDir = stagingDir

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

func (op *httpsStageSystemTablesOp) setupClusterHTTPRequest(hosts []string, schema, tableName string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.buildHTTPSEndpoint("system-tables/stage")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}

		systemTable := systemTableInfo{}
		systemTable.Schema = schema
		systemTable.TableName = tableName
		requestData := prepareStagingSystemTableRequestData{}
		requestData.StagingDirectory = *op.stagingDir
		requestData.SystemTableList = []systemTableInfo{systemTable}

		dataBytes, err := json.Marshal(requestData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}
		httpRequest.RequestData = string(dataBytes)

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsStageSystemTablesOp) prepare(execContext *opEngineExecContext) error {
	host := getInitiatorFromUpHosts(execContext.upHosts, op.hosts)
	if host == "" {
		op.logger.PrintWarning("no up hosts among user specified hosts to collect system tables from, skipping the operation")
		op.skipExecute = true
		return nil
	}

	// construct host list for interface purposes
	op.hosts = []string{host}

	execContext.dispatcher.setup(op.hosts)
	return nil
}

func (op *httpsStageSystemTablesOp) execute(execContext *opEngineExecContext) error {
	for _, systemTableInfo := range execContext.systemTableList.SystemTableList {
		if err := op.setupClusterHTTPRequest(op.hosts, systemTableInfo.Schema, systemTableInfo.TableName); err != nil {
			return err
		}
		op.logger.Info("Staging System Table:", "Schema", systemTableInfo.Schema, "Table", systemTableInfo.TableName)
		if err := op.runExecute(execContext); err != nil {
			return err
		}
		if err := op.processResult(execContext); err != nil {
			return err
		}
	}
	return nil
}

func (op *httpsStageSystemTablesOp) processResult(_ *opEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			op.logger.Info("Staging System Table Success")
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}

func (op *httpsStageSystemTablesOp) finalize(_ *opEngineExecContext) error {
	return nil
}
