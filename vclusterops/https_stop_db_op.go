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

type httpsStopDBOp struct {
	opBase
	opHTTPSBase
	sandbox       string
	mainCluster   bool
	RequestParams map[string]string
}

func makeHTTPSStopDBOp(useHTTPPassword bool, userName string,
	httpsPassword *string, timeout *int, sandbox string, mainCluster bool) (httpsStopDBOp, error) {
	op := httpsStopDBOp{}
	op.name = "HTTPSStopDBOp"
	op.description = "Stop database"
	op.useHTTPPassword = useHTTPPassword
	op.sandbox = sandbox
	op.mainCluster = mainCluster

	// set the query params, "timeout" is optional
	op.RequestParams = make(map[string]string)
	if timeout != nil {
		op.RequestParams["timeout"] = strconv.Itoa(*timeout)
	}

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

func (op *httpsStopDBOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.buildHTTPSEndpoint("cluster/shutdown")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsStopDBOp) prepare(execContext *opEngineExecContext) error {
	// Stop db cases:
	// case 1: stop db on a sandbox -- send stop db request to one UP host of the sandbox.
	// case 2: stop db on the main cluster -- send stop db request to on UP host of the main cluster.
	// case 3: stop db on every host -- send stop db request to one UP host of the given sandbox and to one UP host of the main cluster.
	if len(execContext.upHostsToSandboxes) == 0 {
		return fmt.Errorf(`[%s] Cannot find any up hosts in OpEngineExecContext`, op.name)
	}
	sandboxOnly := false
	var mainHost string
	var hosts []string
	for h, sb := range execContext.upHostsToSandboxes {
		if sb == op.sandbox && sb != "" {
			// stop db only on sandbox
			hosts = []string{h}
			sandboxOnly = true
			break
		}
		if sb == "" {
			mainHost = h
		} else {
			hosts = append(hosts, h)
		}
	}
	// Main cluster should run the command after sandboxes
	if !sandboxOnly && op.sandbox == "" {
		hosts = append(hosts, mainHost)
	}
	// Stop db on Main cluster only
	if op.mainCluster {
		hosts = []string{mainHost}
	}
	execContext.dispatcher.setup(hosts)

	return op.setupClusterHTTPRequest(hosts)
}

func (op *httpsStopDBOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsStopDBOp) processResult(_ *opEngineExecContext) error {
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

func (op *httpsStopDBOp) finalize(_ *opEngineExecContext) error {
	return nil
}
