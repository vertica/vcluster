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
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const HTTPSSuccMsg = "REBALANCED SHARDS"

type HTTPSRebalanceSubclusterShardsOp struct {
	OpBase
	OpHTTPBase
	scName string
}

// MakeHTTPSRebalanceSubclusterShardsOp will make an op that call vertica-http service to rebalance shards of a subcluster
func MakeHTTPSRebalanceSubclusterShardsOp(bootstrapHost []string, useHTTPPassword bool, userName string,
	httpsPassword *string, scName string) HTTPSRebalanceSubclusterShardsOp {
	httpsRBSCShardsOp := HTTPSRebalanceSubclusterShardsOp{}
	httpsRBSCShardsOp.name = "HTTPSRebalanceSubclusterShardsOp"
	httpsRBSCShardsOp.hosts = bootstrapHost
	httpsRBSCShardsOp.scName = scName

	httpsRBSCShardsOp.useHTTPPassword = useHTTPPassword
	if useHTTPPassword {
		util.ValidateUsernameAndPassword(useHTTPPassword, userName)
		httpsRBSCShardsOp.userName = userName
		httpsRBSCShardsOp.httpsPassword = httpsPassword
	}
	return httpsRBSCShardsOp
}

func (op *HTTPSRebalanceSubclusterShardsOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("subclusters/" + op.scName + "/rebalance")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSRebalanceSubclusterShardsOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	// rebalance shards on the default subcluster if scName isn't provided
	if op.scName == "" {
		if execContext.defaultSCName == "" {
			vlog.LogPrintError("default subcluster is not set")
			return MakeClusterOpResultFail()
		}
		op.scName = execContext.defaultSCName
	}

	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *HTTPSRebalanceSubclusterShardsOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *HTTPSRebalanceSubclusterShardsOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := false

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.IsUnauthorizedRequest() {
			// skip checking response from other nodes because we will get the same error there
			break
		}
		if !result.isPassing() {
			// try processing other hosts' responses when the current host has some server errors
			continue
		}

		// decode the json-format response
		// The successful response object will be a dictionary:
		/*
			{
			  "detail": "REBALANCED SHARDS"
			}
		*/
		resp, err := op.parseAndCheckMapResponse(host, result.content)
		if err != nil {
			vlog.LogPrintError(`[%s] fail to parse result on host %s, details: %s`, op.name, host, err)
			break
		}
		// verify if the response's content is correct
		if resp["detail"] != HTTPSSuccMsg {
			vlog.LogError(`[%s] response detail should be '%s' but got '%s'`, op.name, HTTPSSuccMsg, resp["detail"])
			break
		}

		success = true
		break
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *HTTPSRebalanceSubclusterShardsOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}
