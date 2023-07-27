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

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const NoVersion = "NO_VERSION"

type NMAVerticaVersionOp struct {
	OpBase
	RequireSameVersion bool
	HostVersionMap     map[string]string
}

func MakeNMAVerticaVersionOp(hosts []string, sameVersion bool) NMAVerticaVersionOp {
	nmaVerticaVersionOp := NMAVerticaVersionOp{}
	nmaVerticaVersionOp.name = "NMAVerticaVersionOp"
	nmaVerticaVersionOp.hosts = hosts
	nmaVerticaVersionOp.RequireSameVersion = sameVersion
	nmaVerticaVersionOp.HostVersionMap = map[string]string{}
	return nmaVerticaVersionOp
}

func (op *NMAVerticaVersionOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint("vertica/version")
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMAVerticaVersionOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *NMAVerticaVersionOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAVerticaVersionOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

type NMAVerticaVersionOpResponse map[string]string

func (op *NMAVerticaVersionOp) parseAndCheckResponse(host, resultContent string) error {
	// each result is a pair {"vertica_version": <vertica version string>}
	// example result:
	// {"vertica_version": "Vertica Analytic Database v12.0.3"}
	var responseObj NMAVerticaVersionOpResponse
	err := util.GetJSONLogErrors(resultContent, &responseObj, op.name)
	if err != nil {
		return err
	}

	version, ok := responseObj["vertica_version"]
	// missing key "vertica_version"
	if !ok {
		return errors.New("Unable to get vertica version from host " + host)
	}

	vlog.LogInfo("[%s] JSON response from %s is %v\n", op.name, host, responseObj)
	op.HostVersionMap[host] = version
	return nil
}

func (op *NMAVerticaVersionOp) logResponseCollectVersions() error {
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		if !result.isPassing() {
			errStr := fmt.Sprintf("[%s] result from host %s summary %s, details: %+v\n",
				op.name, host, FailureResult, result)
			return errors.New(errStr)
		}

		err := op.parseAndCheckResponse(host, result.content)
		if err != nil {
			vlog.LogInfo("[%s] result from host %s summary %s, details: %+v, parsing failure details: %s\n",
				op.name, host, FailureResult, result, err.Error())
			return err
		}

		vlog.LogPrintInfo("[%s] result from host %s summary %s, details: %+v",
			op.name, host, SuccessResult, result)
	}
	return nil
}

func (op *NMAVerticaVersionOp) logCheckVersionMatch() error {
	versionStr := NoVersion
	for host, version := range op.HostVersionMap {
		vlog.LogInfo("[%s] Host {%s}: version {%s}", op.name, host, version)
		if version == "" {
			return fmt.Errorf("[%s] No version collected for host: [%s]", op.name, host)
		} else if versionStr == NoVersion {
			// first time seeing a valid version, set it as the versionStr
			versionStr = version
		} else if version != versionStr && op.RequireSameVersion {
			return fmt.Errorf("[%s] Found mismatched versions: [%s] and [%s]", op.name, versionStr, version)
		}
	}
	// no version collected at all
	if versionStr == NoVersion {
		return fmt.Errorf("[%s] No version collected for all hosts", op.name)
	}
	return nil
}

func (op *NMAVerticaVersionOp) processResult(execContext *OpEngineExecContext) error {
	err := op.logResponseCollectVersions()
	if err != nil {
		return err
	}
	return op.logCheckVersionMatch()
}
