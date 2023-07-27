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
	"golang.org/x/exp/maps"
)

type NMAReadCatalogEditorOp struct {
	OpBase
	catalogPathMap map[string]string
}

func MakeNMAReadCatalogEditorOp(
	mapHostToCatalogPath map[string]string,
	initiator []string,
) (NMAReadCatalogEditorOp, error) {
	op := NMAReadCatalogEditorOp{}
	op.name = "NMAReadCatalogEditorOp"

	op.catalogPathMap = make(map[string]string)

	if len(initiator) == 0 {
		op.hosts = maps.Keys(mapHostToCatalogPath)
		op.catalogPathMap = mapHostToCatalogPath
	} else {
		for _, host := range initiator {
			op.hosts = append(op.hosts, host)
			op.catalogPathMap[host] = mapHostToCatalogPath[host]
		}
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

func (op *NMAReadCatalogEditorOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *NMAReadCatalogEditorOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAReadCatalogEditorOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
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
	// the quorum count will not be unmarshaled but will be used in NMAReIPOp
	// quorumCount = (1/2 * number of primary nodes) + 1
	QuorumCount int `json:",omitempty"`
}

func (op *NMAReadCatalogEditorOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	var hostsWithLatestCatalog []string
	var maxSpreadVersion int64
	var latestNmaVDB NmaVDatabase
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			nmaVDB := NmaVDatabase{}
			err := op.parseAndCheckResponse(host, result.content, &nmaVDB)
			if err != nil {
				err = fmt.Errorf("[%s] fail to parse result on host %s, details: %w",
					op.name, host, err)
				allErrs = errors.Join(allErrs, err)
				continue
			}

			var primaryNodeCount int
			// build host to node map for NMAStartNodeOp
			hostNodeMap := make(map[string]NmaVNode)
			for i := 0; i < len(nmaVDB.Nodes); i++ {
				n := nmaVDB.Nodes[i]
				hostNodeMap[n.Address] = n
				if n.IsPrimary {
					primaryNodeCount++
				}
			}
			nmaVDB.HostNodeMap = hostNodeMap
			nmaVDB.QuorumCount = primaryNodeCount/2 + 1

			// find hosts with latest catalog version
			spreadVersion, err := nmaVDB.Versions.Spread.Int64()
			if err != nil {
				err = fmt.Errorf("[%s] fail to convert spread Version to integer %s, details: %w",
					op.name, host, err)
				allErrs = errors.Join(allErrs, err)
				continue
			}
			if spreadVersion > maxSpreadVersion {
				hostsWithLatestCatalog = []string{host}
				maxSpreadVersion = spreadVersion
				// save the latest NMAVDatabase to execContext
				latestNmaVDB = nmaVDB
			} else if spreadVersion == maxSpreadVersion {
				hostsWithLatestCatalog = append(hostsWithLatestCatalog, host)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	// save hostsWithLatestCatalog to execContext
	if len(hostsWithLatestCatalog) == 0 {
		err := fmt.Errorf("[%s] cannot find any host with the latest catalog", op.name)
		allErrs = errors.Join(allErrs, err)
		return allErrs
	}

	execContext.hostsWithLatestCatalog = hostsWithLatestCatalog
	// save the latest nmaVDB to execContext
	execContext.nmaVDatabase = latestNmaVDB

	return allErrs
}
