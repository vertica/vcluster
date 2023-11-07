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

type NMAStageVerticaLogsOp struct {
	OpBase
	hostRequestBodyMap map[string]string
	id                 string
	hostNodeNameMap    map[string]string // must correspond to host list exactly!
	hostCatPathMap     map[string]string // must correspond to host list exactly!
	batch              string
	logSizeLimitBytes  int64
	logAgeHours        int // The maximum age of archieved logs in hours to retrieve
}

type stageVerticaLogsRequestData struct {
	CatalogPath       string `json:"catalog_path"`
	LogSizeLimitBytes int64  `json:"log_size_limit_bytes"`
	LogAgeHours       int    `json:"log_age_hours"`
}

type stageVerticaLogsResponseData struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"size_bytes"`
	ModTime   string `json:"mod_time"`
}

func makeNMAStageVerticaLogsOp(log vlog.Printer,
	id string,
	hosts []string,
	hostNodeNameMap map[string]string,
	hostCatPathMap map[string]string,
	logSizeLimitBytes int64,
	logAgeHours int) (NMAStageVerticaLogsOp, error) {
	nmaStageVerticaLogsOp := NMAStageVerticaLogsOp{}
	nmaStageVerticaLogsOp.id = id
	nmaStageVerticaLogsOp.hostNodeNameMap = hostNodeNameMap
	nmaStageVerticaLogsOp.hostCatPathMap = hostCatPathMap
	nmaStageVerticaLogsOp.batch = scrutinizeBatchNormal
	nmaStageVerticaLogsOp.log = log
	nmaStageVerticaLogsOp.hosts = hosts
	nmaStageVerticaLogsOp.logSizeLimitBytes = logSizeLimitBytes
	nmaStageVerticaLogsOp.logAgeHours = logAgeHours

	// the caller is responsible for making sure hosts and maps match up exactly
	err := validateHostMaps(hosts, hostNodeNameMap, hostCatPathMap)
	return nmaStageVerticaLogsOp, err
}

func (op *NMAStageVerticaLogsOp) setupRequestBody(hosts []string) error {
	op.hostRequestBodyMap = make(map[string]string)

	for _, host := range hosts {
		stageVerticaLogsData := stageVerticaLogsRequestData{}
		stageVerticaLogsData.CatalogPath = op.hostCatPathMap[host]
		stageVerticaLogsData.LogSizeLimitBytes = op.logSizeLimitBytes
		stageVerticaLogsData.LogAgeHours = op.logAgeHours

		dataBytes, err := json.Marshal(stageVerticaLogsData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	return nil
}

func (op *NMAStageVerticaLogsOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		nodeName := op.hostNodeNameMap[host]

		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.buildNMAEndpoint(scrutinizeURLPrefix + op.id + "/" + nodeName + "/" + op.batch + "/vertica.log")
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *NMAStageVerticaLogsOp) prepare(execContext *OpEngineExecContext) error {
	err := op.setupRequestBody(op.hosts)
	if err != nil {
		return err
	}
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMAStageVerticaLogsOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAStageVerticaLogsOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *NMAStageVerticaLogsOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			if result.content == "" {
				// note that an empty response (nothing staged) is not an error
				op.log.Info("no logs staged on host", "Host", host)
				continue
			}
			// the response is an array of file info structs
			fileList := make([]stageVerticaLogsResponseData, 0)
			err := op.parseAndCheckResponse(host, result.content, fileList)
			if err != nil {
				err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w", op.name, host, err)
				allErrs = errors.Join(allErrs, err)
				continue
			}
			for _, fileEntry := range fileList {
				op.log.Info("file staged on host", "Host", host, "FileInfo", fileEntry)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
