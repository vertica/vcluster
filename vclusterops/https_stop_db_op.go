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
	"regexp"
	"strconv"

	"github.com/vertica/vcluster/vclusterops/util"
)

type HTTPSStopDBOp struct {
	OpBase
	OpHTTPBase
	RequestParams map[string]string
}

func MakeHTTPSStopDBOp(opName string, useHTTPPassword bool, userName string,
	httpsPassword *string, timeout *int) HTTPSStopDBOp {
	httpsStopDBOp := HTTPSStopDBOp{}
	httpsStopDBOp.name = opName
	httpsStopDBOp.useHTTPPassword = useHTTPPassword

	// set the query params, "timeout" is optional
	httpsStopDBOp.RequestParams = make(map[string]string)
	if timeout != nil {
		httpsStopDBOp.RequestParams["timeout"] = strconv.Itoa(*timeout)
	}

	if useHTTPPassword {
		util.ValidateUsernameAndPassword(useHTTPPassword, userName)
		httpsStopDBOp.userName = userName
		httpsStopDBOp.httpsPassword = httpsPassword
	}
	return httpsStopDBOp
}

func (op *HTTPSStopDBOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("cluster/shutdown")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSStopDBOp) Prepare(execContext *OpEngineExecContext) error {
	if len(execContext.upHosts) == 0 {
		return fmt.Errorf(`[%s] Cannot find any up hosts in OpEngineExecContext`, op.name)
	}
	// use first up host to execute https post request
	hosts := []string{execContext.upHosts[0]}
	execContext.dispatcher.Setup(hosts)
	op.setupClusterHTTPRequest(hosts)

	return nil
}

func (op *HTTPSStopDBOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSStopDBOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	re := regexp.MustCompile(`Set subcluster \(.*\) to draining state.*`)

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary:
		// 1. shutdown without drain
		// {"detail": "Shutdown: moveout complete"}
		// 2. shutdown with drain
		// {"detail": "Set subcluster (default_subcluster) to draining state\n
		//  Waited for 1 nodes to drain\n
		//	Sync catalog complete\n
		//  Shutdown message sent to subcluster (default_subcluster)\n\n"}
		response, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		if _, ok := op.RequestParams["timeout"]; ok {
			if re.MatchString(response["details"]) {
				err = fmt.Errorf(`[%s] response detail should like 'Set subcluster to draining state ...' but got '%s'`,
					op.name, response["detail"])
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			if response["detail"] != "Shutdown: moveout complete" {
				err = fmt.Errorf(`[%s] response detail should be 'Shutdown: moveout complete' but got '%s'`, op.name, response["detail"])
				allErrs = errors.Join(allErrs, err)
			}
		}
	}

	return allErrs
}

func (op *HTTPSStopDBOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}
