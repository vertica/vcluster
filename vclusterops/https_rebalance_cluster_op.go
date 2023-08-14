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
)

const RebalanceClusterSuccMsg = "REBALANCED"
const RebalanceShardsSuccMsg = "REBALANCED SHARDS"

type HTTPSRebalanceClusterOp struct {
	OpBase
	OpHTTPSBase
}

// makeHTTPSRebalanceClusterOp will make an op that call vertica-http service to rebalance the cluster
func makeHTTPSRebalanceClusterOp(initiatorHost []string, useHTTPPassword bool, userName string,
	httpsPassword *string) (HTTPSRebalanceClusterOp, error) {
	httpsRBCOp := HTTPSRebalanceClusterOp{}
	httpsRBCOp.name = "HTTPSRebalanceClusterOp"
	httpsRBCOp.hosts = initiatorHost

	httpsRBCOp.useHTTPPassword = useHTTPPassword
	err := util.ValidateUsernameAndPassword(httpsRBCOp.name, useHTTPPassword, userName)
	if err != nil {
		return httpsRBCOp, err
	}
	httpsRBCOp.userName = userName
	httpsRBCOp.httpsPassword = httpsPassword
	return httpsRBCOp, nil
}

func (op *HTTPSRebalanceClusterOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("cluster/rebalance")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
	return nil
}

func (op *HTTPSRebalanceClusterOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSRebalanceClusterOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSRebalanceClusterOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			// skip checking response from other nodes because we will get the same error there
			return result.err
		}
		if !result.IsSuccess() {
			allErrs = errors.Join(allErrs, result.err)
			// try processing other hosts' responses when the current host has some server errors
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary:
		/*
			{
			  "detail": "REBALANCED"
			}
			or
			{
			  "detail": "REBALANCED SHARDS"
			}
			if eon
		*/
		resp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			return allErrs
		}
		// verify if the response's content is correct
		if resp["detail"] != RebalanceClusterSuccMsg &&
			resp["detail"] != RebalanceShardsSuccMsg {
			err = fmt.Errorf(`[%s] response detail should be '%s' but got '%s'`, op.name, RebalanceClusterSuccMsg, resp["detail"])
			allErrs = errors.Join(allErrs, err)
			return allErrs
		}

		break
	}

	return allErrs
}

func (op *HTTPSRebalanceClusterOp) finalize(_ *OpEngineExecContext) error {
	return nil
}
