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

type NMAFetchVdbFromCatalogEditorOp struct {
	OpBase
	catalogPathMap map[string]string
}

func MakeNMAFetchVdbFromCatalogEditorOp(
	opName string,
	hostNodeMap map[string]VCoordinationNode,
	bootstrapHosts []string) (NMAFetchVdbFromCatalogEditorOp, error) {
	nmaFetchVdbFromCatalogEditorOp := NMAFetchVdbFromCatalogEditorOp{}
	nmaFetchVdbFromCatalogEditorOp.name = opName
	nmaFetchVdbFromCatalogEditorOp.hosts = bootstrapHosts

	nmaFetchVdbFromCatalogEditorOp.catalogPathMap = make(map[string]string)
	for _, host := range bootstrapHosts {
		vnode, ok := hostNodeMap[host]
		if !ok {
			return nmaFetchVdbFromCatalogEditorOp,
				fmt.Errorf("[%s] fail to get catalog path from host %s", opName, host)
		}
		nmaFetchVdbFromCatalogEditorOp.catalogPathMap[host] = vnode.CatalogPath
	}

	return nmaFetchVdbFromCatalogEditorOp, nil
}

func (op *NMAFetchVdbFromCatalogEditorOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint("catalog/database")

		catalogPath, ok := op.catalogPathMap[host]
		if !ok {
			vlog.LogError("[%s] cannot find catalog path of host %s", op.name, op)
		}
		httpRequest.QueryParams = map[string]string{"catalog_path": catalogPath}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMAFetchVdbFromCatalogEditorOp) Prepare(execContext *OpEngineExecContext) ClusterOpResult {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return MakeClusterOpResultPass()
}

func (op *NMAFetchVdbFromCatalogEditorOp) Execute(execContext *OpEngineExecContext) ClusterOpResult {
	if err := op.execute(execContext); err != nil {
		return MakeClusterOpResultException()
	}

	return op.processResult(execContext)
}

func (op *NMAFetchVdbFromCatalogEditorOp) Finalize(execContext *OpEngineExecContext) ClusterOpResult {
	return MakeClusterOpResultPass()
}

type NmaVersions struct {
	Global      json.Number `json:"global"`
	Local       json.Number `json:"local"`
	Session     json.Number `json:"session"`
	Spread      json.Number `json:"spread"`
	Transaction json.Number `json:"transaction"`
	TwoPhaseID  json.Number `json:"two_phase_id"`
}

type NmaVNode struct {
	Address              string      `json:"address"`
	AddressFamily        string      `json:"address_family"`
	CatalogPath          string      `json:"catalog_path"`
	ClientPort           json.Number `json:"client_port"`
	ControlAddress       string      `json:"control_address"`
	ControlAddressFamily string      `json:"control_address_family"`
	ControlBroadcast     string      `json:"control_broadcast"`
	ControlNode          json.Number `json:"control_node"`
	ControlPort          json.Number `json:"control_port"`
	EiAddress            json.Number `json:"ei_address"`
	HasCatalog           bool        `json:"has_catalog"`
	IsEphemeral          bool        `json:"is_ephemeral"`
	IsPrimary            bool        `json:"is_primary"`
	IsRecoveryClerk      bool        `json:"is_recovery_clerk"`
	Name                 string      `json:"name"`
	NodeParamMap         []any       `json:"node_param_map"`
	NodeType             json.Number `json:"node_type"`
	Oid                  json.Number `json:"oid"`
	ParentFaultGroupID   json.Number `json:"parent_fault_group_id"`
	ReplacedNode         json.Number `json:"replaced_node"`
	Schema               json.Number `json:"schema"`
	SiteUniqueID         json.Number `json:"site_unique_id"`
	StartCommand         []string    `json:"start_command"`
	StorageLocations     []string    `json:"storage_locations"`
	Tag                  json.Number `json:"tag"`
}

type NmaVDatabase struct {
	Name     string      `json:"name"`
	Versions NmaVersions `json:"versions"`
	Nodes    []NmaVNode  `json:"nodes"`
	// this map will not be unmarshaled but will be used in NMAStartNodeOp
	HostNodeMap             map[string]NmaVNode `json:",omitempty"`
	ControlMode             string              `json:"control_mode"`
	WillUpgrade             bool                `json:"will_upgrade"`
	SpreadEncryption        string              `json:"spread_encryption"`
	CommunalStorageLocation string              `json:"communal_storage_location"`
}

func (op *NMAFetchVdbFromCatalogEditorOp) processResult(execContext *OpEngineExecContext) ClusterOpResult {
	success := true

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

			// save NMAVDatabase to execContext
			execContext.nmaVDatabase = nmaVDB
		} else {
			success = false
		}
	}

	if success {
		return MakeClusterOpResultPass()
	}
	return MakeClusterOpResultFail()
}
