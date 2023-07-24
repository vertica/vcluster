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

type HTTPSFindSubclusterOrDefaultOp struct {
	OpBase
	OpHTTPBase
	scName string
}

func MakeHTTPSFindSubclusterOrDefaultOp(hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string, scName string) HTTPSFindSubclusterOrDefaultOp {
	httpsFindSubclusterOrDefaultOp := HTTPSFindSubclusterOrDefaultOp{}
	httpsFindSubclusterOrDefaultOp.name = "HTTPSFindSubclusterOrDefaultOp"
	httpsFindSubclusterOrDefaultOp.hosts = hosts
	httpsFindSubclusterOrDefaultOp.scName = scName

	httpsFindSubclusterOrDefaultOp.useHTTPPassword = useHTTPPassword
	if useHTTPPassword {
		util.ValidateUsernameAndPassword(useHTTPPassword, userName)
		httpsFindSubclusterOrDefaultOp.userName = userName
		httpsFindSubclusterOrDefaultOp.httpsPassword = httpsPassword
	}
	return httpsFindSubclusterOrDefaultOp
}

func (op *HTTPSFindSubclusterOrDefaultOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildHTTPSEndpoint("subclusters")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSFindSubclusterOrDefaultOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *HTTPSFindSubclusterOrDefaultOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

// the following struct will store a subcluster's information for this op
type SubclusterInfo struct {
	SCName    string `json:"subcluster_name"`
	IsDefault bool   `json:"is_default"`
}

type SCResp struct {
	SCInfoList []SubclusterInfo `json:"subcluster_list"`
}

func (op *HTTPSFindSubclusterOrDefaultOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			// skip checking response from other nodes because we will get the same error there
			return result.err
		}
		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			// try processing other hosts' responses when the current host has some server errors
			continue
		}

		// decode the json-format response
		// A successful response object will be like below:
		/*
			{
				"subcluster_list": [
					{
						"subcluster_name": "default_subcluster",
						"control_set_size": -1,
						"is_secondary": false,
						"is_default": true,
						"sandbox": ""
					},
					{
						"subcluster_name": "sc1",
						"control_set_size": 2,
						"is_secondary": true,
						"is_default": false,
						"sandbox": ""
					}
				]
			}
		*/
		scResp := SCResp{}
		err := op.parseAndCheckResponse(host, result.content, &scResp)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			return allErrs
		}

		// find if subcluster exists if scName is provided
		// otherwise, get the default subcluster name
		if op.scName != "" {
			exists := false
			for _, scInfo := range scResp.SCInfoList {
				if scInfo.SCName == op.scName {
					exists = true
					vlog.LogInfo(`[%s] subcluster '%s' exists in the database`, op.name, scInfo.SCName)
					break
				}
			}
			if !exists {
				return fmt.Errorf(`[%s] subcluster '%s' does not exist in the database`, op.name, op.scName)
			}
		} else {
			foundDefault := false
			for _, scInfo := range scResp.SCInfoList {
				if scInfo.IsDefault {
					// store the default sc name for later rebalance-shards use
					execContext.defaultSCName = scInfo.SCName
					foundDefault = true
					vlog.LogInfo(`[%s] found default subcluster '%s' in the database`, op.name, scInfo.SCName)
					break
				}
			}
			if !foundDefault {
				return fmt.Errorf(`[%s] cannot find a default subcluster in the database`, op.name)
			}
		}

		return nil
	}
	return allErrs
}

func (op *HTTPSFindSubclusterOrDefaultOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}
