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

type NMAReIPOp struct {
	OpBase
	reIPList             []ReIPInfo
	vdb                  *VCoordinationDatabase
	quorumCount          int // quorumCount = (1/2 * number of primary nodes) + 1
	primaryNodeCount     int
	hostRequestBodyMap   map[string]string
	mapHostToNodeName    map[string]string
	mapHostToCatalogPath map[string]string
}

func makeNMAReIPOp(reIPList []ReIPInfo, vdb *VCoordinationDatabase) NMAReIPOp {
	op := NMAReIPOp{}
	op.name = "NMAReIPOp"
	op.reIPList = reIPList
	op.vdb = vdb
	return op
}

type ReIPInfo struct {
	NodeName               string `json:"node_name"`
	NodeAddress            string `json:"-"`
	TargetAddress          string `json:"address"`
	TargetControlAddress   string `json:"control_address"`
	TargetControlBroadcast string `json:"control_broadcast"`
}

type reIPParams struct {
	CatalogPath  string     `json:"catalog_path"`
	ReIPInfoList []ReIPInfo `json:"re_ip_list"`
}

func (op *NMAReIPOp) updateRequestBody(_ *OpEngineExecContext) error {
	op.hostRequestBodyMap = make(map[string]string)

	for _, host := range op.hosts {
		var p reIPParams
		p.CatalogPath = op.mapHostToCatalogPath[host]
		p.ReIPInfoList = op.reIPList
		dataBytes, err := json.Marshal(p)
		if err != nil {
			vlog.LogError(`[%s] fail to marshal request data to JSON string, detail %s`, op.name, err)
			return err
		}
		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	vlog.LogInfo("[%s] request data: %+v\n", op.name, op.hostRequestBodyMap)
	return nil
}

func (op *NMAReIPOp) setupClusterHTTPRequest(hosts []string) error {
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

	return nil
}

// updateReIPList is used for the vcluster CLI to update node names
func (op *NMAReIPOp) updateReIPList(execContext *OpEngineExecContext) error {
	hostNodeMap := execContext.nmaVDatabase.HostNodeMap

	for i := 0; i < len(op.reIPList); i++ {
		info := op.reIPList[i]
		// update node name if not given
		if info.NodeName == "" {
			vnode, ok := hostNodeMap[info.NodeAddress]
			if !ok {
				return fmt.Errorf("the provided IP %s cannot be found from the database catalog",
					info.NodeAddress)
			}
			info.NodeName = vnode.Name
		}
		// update control address if not given
		if info.TargetControlAddress == "" {
			info.TargetControlAddress = info.TargetAddress
		}
		// update control broadcast if not given
		if info.TargetControlBroadcast == "" {
			profile, ok := execContext.networkProfiles[info.TargetAddress]
			if !ok {
				return fmt.Errorf("[%s] unable to find network profile for address %s", op.name, info.TargetAddress)
			}
			info.TargetControlBroadcast = profile.Broadcast
		}

		op.reIPList[i] = info
	}

	return nil
}

func (op *NMAReIPOp) prepare(execContext *OpEngineExecContext) error {
	// build mapHostToNodeName and catalogPathMap from vdb
	op.mapHostToNodeName = make(map[string]string)
	op.mapHostToCatalogPath = make(map[string]string)
	// VER-88453 will put vnode back in the loop
	for h := range op.vdb.HostNodeMap {
		op.mapHostToNodeName[h] = op.vdb.HostNodeMap[h].Name
		op.mapHostToCatalogPath[h] = op.vdb.HostNodeMap[h].CatalogPath
	}

	// get the primary node names
	// this step is needed as the new host addresses
	// are not in the catalog
	primaryNodes := make(map[string]struct{})
	nodeList := execContext.nmaVDatabase.Nodes
	for i := 0; i < len(nodeList); i++ {
		vnode := nodeList[i]
		if vnode.IsPrimary {
			primaryNodes[vnode.Name] = struct{}{}
		}
	}

	// calculate quorum and update the hosts
	for _, host := range execContext.hostsWithLatestCatalog {
		nodeName := op.mapHostToNodeName[host]
		if _, ok := primaryNodes[nodeName]; ok {
			op.hosts = append(op.hosts, host)
		}
	}

	// get the quorum count
	op.quorumCount = execContext.nmaVDatabase.QuorumCount

	// quorum check
	if !op.hasQuorum(len(op.hosts)) {
		return fmt.Errorf("failed quorum check, not enough primaries exist with: %d", len(op.hosts))
	}

	// update re-ip list
	err := op.updateReIPList(execContext)
	if err != nil {
		return fmt.Errorf("[%s] error updating reIP list: %w", op.name, err)
	}

	// build request body for hosts
	err = op.updateRequestBody(execContext)
	if err != nil {
		return err
	}

	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMAReIPOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAReIPOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *NMAReIPOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	var successCount int
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			var reIPResult []ReIPInfo
			err := op.parseAndCheckResponse(host, result.content, &reIPResult)
			if err != nil {
				err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w",
					op.name, host, err)
				allErrs = errors.Join(allErrs, err)
				continue
			}

			successCount++
		} else {
			allErrs = errors.Join(allErrs, result.err)
			// VER-88054 rollback the commits
		}
	}

	// quorum check
	if !op.hasQuorum(successCount) {
		// VER-88054 rollback the commits
		err := fmt.Errorf("failed quroum check for re-ip update. Success count: %d", successCount)
		allErrs = errors.Join(allErrs, err)
	}

	return allErrs
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
