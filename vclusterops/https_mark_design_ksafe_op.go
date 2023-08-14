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
	"strconv"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const zeroSafeRspStr = "Marked design 0-safe"
const oneSafeRspStr = "Marked design 1-safe"

type HTTPSMarkDesignKSafeOp struct {
	OpBase
	OpHTTPSBase
	RequestParams map[string]string
	ksafeValue    int
}

func makeHTTPSMarkDesignKSafeOp(
	hosts []string,
	useHTTPPassword bool,
	userName string,
	httpsPassword *string,
	ksafeValue int,
) (HTTPSMarkDesignKSafeOp, error) {
	httpsMarkDesignKSafeOp := HTTPSMarkDesignKSafeOp{}
	httpsMarkDesignKSafeOp.name = "HTTPSMarkDesignKsafeOp"
	httpsMarkDesignKSafeOp.hosts = hosts
	httpsMarkDesignKSafeOp.useHTTPPassword = useHTTPPassword

	// set ksafeValue.  Should be 1 or 0.
	// store directly for later response verification
	httpsMarkDesignKSafeOp.ksafeValue = ksafeValue
	httpsMarkDesignKSafeOp.RequestParams = make(map[string]string)
	httpsMarkDesignKSafeOp.RequestParams["k"] = strconv.Itoa(ksafeValue)

	err := util.ValidateUsernameAndPassword(httpsMarkDesignKSafeOp.name, useHTTPPassword, userName)
	if err != nil {
		return httpsMarkDesignKSafeOp, err
	}
	httpsMarkDesignKSafeOp.userName = userName
	httpsMarkDesignKSafeOp.httpsPassword = httpsPassword
	return httpsMarkDesignKSafeOp, nil
}

func (op *HTTPSMarkDesignKSafeOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	// in practice, initiator only
	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PutMethod
		httpRequest.BuildHTTPSEndpoint("cluster/k-safety")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		httpRequest.QueryParams = op.RequestParams
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *HTTPSMarkDesignKSafeOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *HTTPSMarkDesignKSafeOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

// MarkDesignKSafeRsp will be either
// {"detail": "Marked design 0-safe"} OR
// {"detail": "Marked design 1-safe"}
type MarkDesignKSafeRsp struct {
	Detail string `json:"detail"`
}

func (op *HTTPSMarkDesignKSafeOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	// in practice, just the initiator node
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		// The response object will be a dictionary, an example:
		// {"detail": "Marked design 0-safe"}
		markDesignKSafeRsp := MarkDesignKSafeRsp{}
		err := op.parseAndCheckResponse(host, result.content, &markDesignKSafeRsp)
		if err != nil {
			err = fmt.Errorf(`[%s] fail to parse result on host %s, details: %w`, op.name, host, err)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// retrieve and verify the mark ksafety response
		var ksafeValue int
		if markDesignKSafeRsp.Detail == zeroSafeRspStr {
			ksafeValue = 0
		} else if markDesignKSafeRsp.Detail == oneSafeRspStr {
			ksafeValue = 1
		} else {
			err = fmt.Errorf(`[%s] fail to parse the ksafety value information, detail: %s`,
				op.name, markDesignKSafeRsp.Detail)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		// compare the ksafety_value from output JSON with k_value input value
		// verify if the return ksafety_value is the one we insert into the endpoint
		if ksafeValue != op.ksafeValue {
			err = fmt.Errorf(`[%s] mismatch between request and response k-safety values, request: %d, response: %d`,
				op.name, op.ksafeValue, ksafeValue)
			allErrs = errors.Join(allErrs, err)
			continue
		}

		vlog.LogPrintInfo(`[%s] The K-safety value of the database is set as %d`, op.name, ksafeValue)
	}

	return allErrs
}

func (op *HTTPSMarkDesignKSafeOp) finalize(_ *OpEngineExecContext) error {
	return nil
}
