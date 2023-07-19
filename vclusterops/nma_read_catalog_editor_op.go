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
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type NMAReadCatalogEditorOp struct {
	OpBase                           // base fields of cluster ops
	catalogPathMap map[string]string // A map of catalogPath built from new hosts
}

func MakeNMAReadCatalogEditorOp(
	name string,
	hostCatalogPath map[string]string,
) (NMAReadCatalogEditorOp, error) {
	op := NMAReadCatalogEditorOp{}
	op.name = name

	op.catalogPathMap = make(map[string]string)
	for host, catalogPath := range hostCatalogPath {
		op.hosts = append(op.hosts, host)
		op.catalogPathMap[host] = catalogPath
	}

	return op, nil
}

func (op *NMAReadCatalogEditorOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint("catalog/database")

		catalogPath, ok := op.catalogPathMap[host]
		if !ok {
			vlog.LogError("[%s] cannot find catalog path of host %s", op.name, host)
		}
		httpRequest.QueryParams = map[string]string{"catalog_path": catalogPath}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMAReadCatalogEditorOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *NMAReadCatalogEditorOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *NMAReadCatalogEditorOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

func (op *NMAReadCatalogEditorOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

	var hostsWithLatestCatalog []string
	var maxSpreadVersion int64
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			nmaVDB := NmaVDatabase{}
			err := op.parseAndCheckResponse(host, result.content, &nmaVDB)
			if err != nil {
				vlog.LogPrintError("[%s] fail to parse result on host %s, details: %w",
					op.name, host, err)
				success = false
				continue
			}

			// build host to node map for NMAStartNodeOp
			hostNodeMap := make(map[string]NmaVNode)
			for i := 0; i < len(nmaVDB.Nodes); i++ {
				n := nmaVDB.Nodes[i]
				hostNodeMap[n.Address] = n
			}
			nmaVDB.HostNodeMap = hostNodeMap

			// find hosts with latest catalog version
			spreadVersion, err := nmaVDB.Versions.Spread.Int64()
			if err != nil {
				vlog.LogPrintError("[%s] fail to convert spread Version to integer %s, details: %w",
					op.name, host, err)
				success = false
				continue
			}
			if spreadVersion > maxSpreadVersion {
				hostsWithLatestCatalog = []string{host}
				maxSpreadVersion = spreadVersion
				// save the latest NMAVDatabase to execContext
				execContext.nmaVDatabase = nmaVDB
			} else if spreadVersion == maxSpreadVersion {
				hostsWithLatestCatalog = append(hostsWithLatestCatalog, host)
			}
		} else {
			success = false
		}
	}

	// save hostsWithLatestCatalog to execContext
	execContext.hostsWithLatestCatalog = hostsWithLatestCatalog

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}
