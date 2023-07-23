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

type NMANetworkProfileOp struct {
	OpBase
}

func MakeNMANetworkProfileOp(hosts []string) NMANetworkProfileOp {
	nmaNetworkProfileOp := NMANetworkProfileOp{}
	nmaNetworkProfileOp.name = "NMANetworkProfileOp"
	nmaNetworkProfileOp.hosts = hosts
	return nmaNetworkProfileOp
}

func (op *NMANetworkProfileOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint("network-profiles")
		httpRequest.QueryParams = map[string]string{"broadcast-hint": host}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMANetworkProfileOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *NMANetworkProfileOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *NMANetworkProfileOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

type NetworkProfile struct {
	Name      string
	Address   string
	Subnet    string
	Netmask   string
	Broadcast string
}

func (op *NMANetworkProfileOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	allNetProfiles := make(map[string]NetworkProfile)

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// unmarshal the result content
			profile, err := op.parseResponse(host, result.content)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse network profile on host %s, details: %s",
					op.name, host, err)
				return MakeClusterOpResultException()
			}
			allNetProfiles[host] = profile
		} else {
			success = false
		}
	}

	// save network profiles to execContext
	execContext.networkProfiles = allNetProfiles

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}

func (op *NMANetworkProfileOp) parseResponse(host, resultContent string) (NetworkProfile, error) {
	var responseObj NetworkProfile

	// the response_obj will be a dictionary like the following:
	// {
	//   "name" : "eth0",
	//   "address" : "192.168.100.1",
	//   "subnet" : "192.168.0.0/16"
	//   "netmask" : "255.255.0.0"
	//   "broadcast": "192.168.255.255"
	// }
	err := op.parseAndCheckResponse(host, resultContent, &responseObj)
	if err != nil {
		return responseObj, err
	}

	// check whether any field is empty
	err = util.CheckMissingFields(responseObj)

	return responseObj, err
}
