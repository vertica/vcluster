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
	"fmt"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

type NMAReIPOp struct {
	OpBase
	catalogPathMap     map[string]string
	reIPList           []reIPInfo
	quorumCount        int // quorumCount = (1/2 * number of primary nodes) + 1
	primaryNodeCount   int
	hostRequestBodyMap map[string]string
}

func makeNMAReIPOp(name string,
	catalogPathMap map[string]string,
	reIPList []reIPInfo) NMAReIPOp {
	op := NMAReIPOp{}
	op.name = name
	op.catalogPathMap = catalogPathMap
	op.reIPList = reIPList

	return op
}

type reIPInfo struct {
	NodeName               string `json:"node_name"`
	NodeAddress            string `json:"-"`
	TargetAddress          string `json:"address"`
	TargetControlAddress   string `json:"control_address"`
	TargetControlBroadcast string `json:"control_broadcast"`
}

type reIPParams struct {
	CatalogPath  string     `json:"catalog_path"`
	ReIPInfoList []reIPInfo `json:"re_ip_list"`
}

func (op *NMAReIPOp) updateRequestBody(hosts []string, execContext *OpEngineExecContext) error {
	op.hostRequestBodyMap = make(map[string]string)

	for _, host := range hosts {
		var p reIPParams
		p.CatalogPath = op.catalogPathMap[host]
		p.ReIPInfoList = op.reIPList
		dataBytes, err := json.Marshal(p)
		if err != nil {
			vlog.LogError(`[%s] fail to marshal request data to JSON string, detail %s, detail: %v`, op.name, err)
			return err
		}
		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	vlog.LogInfo("[%s] request data: %+v\n", op.name, op.hostRequestBodyMap)
	return nil
}

func (op *NMAReIPOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PutMethod
		httpRequest.BuildNMAEndpoint("catalog/re-ip")
		httpRequest.RequestData = op.hostRequestBodyMap[host]

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

// updateReIPList is used for the vcluster CLI to update node names
func (op *NMAReIPOp) updateReIPList(execContext *OpEngineExecContext) error {
	hostNodeMap := execContext.nmaVDatabase.HostNodeMap

	for i := 0; i < len(op.reIPList); i++ {
		info := op.reIPList[i]
		if info.NodeName == "" {
			vnode, ok := hostNodeMap[info.NodeAddress]
			if !ok {
				return fmt.Errorf("the provided IP %s cannot be found from the database catalog",
					info.NodeAddress)
			}
			info.NodeName = vnode.Name
			op.reIPList[i] = info
		}
	}

	return nil
}

func (op *NMAReIPOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	// calculate quorum and update the hosts
	hostNodeMap := execContext.nmaVDatabase.HostNodeMap
	for _, host := range execContext.hostsWithLatestCatalog {
		vnode, ok := hostNodeMap[host]
		if !ok {
			vlog.LogPrintError("[%s] cannot find %s from the catalog", op.name, host)
			return MakeClusterOpResultException()
		}
		if vnode.IsPrimary {
			op.hosts = append(op.hosts, host)
		}
	}

	// count the quorum
	op.primaryNodeCount = 0
	for h := range hostNodeMap {
		vnode := hostNodeMap[h]
		if vnode.IsPrimary {
			op.primaryNodeCount++
		}
	}
	op.quorumCount = op.primaryNodeCount/2 + 1

	// quorum check
	if !op.hasQuorum(len(op.hosts)) {
		return MakeClusterOpResultFail()
	}

	// update re-ip list
	err := op.updateReIPList(execContext)
	if err != nil {
		vlog.LogPrintError("[%s] %v", op.name, err)
		return MakeClusterOpResultException()
	}

	// build request body for hosts
	err = op.updateRequestBody(op.hosts, execContext)
	if err != nil {
		return MakeClusterOpResultException()
	}

	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *NMAReIPOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *NMAReIPOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

func (op *NMAReIPOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true
	var successCount int
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			var reIPResult []reIPInfo
			err := op.parseAndCheckResponse(host, result.content, &reIPResult)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %w",
					op.name, host, err)
				success = false
				continue
			}

			successCount++
		} else {
			success = false
			// VER-88054 rollback the commits
		}
	}

	// quorum check
	if !op.hasQuorum(successCount) {
		success = false
		// VER-88054 rollback the commits
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultPass()
}

func (op *NMAReIPOp) hasQuorum(hostCount int) bool {
	if hostCount < op.quorumCount {
		vlog.LogPrintError("[%s] Quorum check failed: "+
			"number of hosts with latest catalog (%d) is not "+
			"greater than or equal to 1/2 of number of the primary nodes (%d)\n",
			op.name, len(op.hosts), op.primaryNodeCount)
		return false
	}

	return true
}
