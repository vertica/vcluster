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

type NMAPrepareDirectoriesOp struct {
	OpBase
	hostRequestBodyMap map[string]string
}

type prepareDirectoriesRequestData struct {
	CatalogPath      string   `json:"catalog_path"`
	DepotPath        string   `json:"depot_path,omitempty"`
	StorageLocations []string `json:"storage_locations"`
	ForceCleanup     bool     `json:"force_cleanup"`
	ForRevive        bool     `json:"for_revive"`
	IgnoreParent     bool     `json:"ignore_parent"`
}

func makeNMAPrepareDirectoriesOp(
	hostNodeMap map[string]VCoordinationNode) (NMAPrepareDirectoriesOp, error) {
	nmaPrepareDirectoriesOp := NMAPrepareDirectoriesOp{}
	nmaPrepareDirectoriesOp.name = "NMAPrepareDirectoriesOp"

	err := nmaPrepareDirectoriesOp.setupRequestBody(hostNodeMap)
	if err != nil {
		return nmaPrepareDirectoriesOp, err
	}

	nmaPrepareDirectoriesOp.hosts = maps.Keys(hostNodeMap)

	return nmaPrepareDirectoriesOp, nil
}

func (op *NMAPrepareDirectoriesOp) setupRequestBody(hostNodeMap map[string]VCoordinationNode) error {
	op.hostRequestBodyMap = make(map[string]string)

	for host := range hostNodeMap {
		prepareDirData := prepareDirectoriesRequestData{}
		prepareDirData.CatalogPath = hostNodeMap[host].CatalogPath
		prepareDirData.DepotPath = hostNodeMap[host].DepotPath
		prepareDirData.StorageLocations = hostNodeMap[host].StorageLocations
		prepareDirData.ForceCleanup = false
		prepareDirData.ForRevive = false
		prepareDirData.IgnoreParent = false

		dataBytes, err := json.Marshal(prepareDirData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}
	vlog.LogInfo("[%s] request data: %+v", op.name, op.hostRequestBodyMap)

	return nil
}

func (op *NMAPrepareDirectoriesOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("directories/prepare")
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *NMAPrepareDirectoriesOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMAPrepareDirectoriesOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMAPrepareDirectoriesOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *NMAPrepareDirectoriesOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			// the response_obj will be a dictionary like the following:
			// {'/data/good/procedures': 'created',
			//  '/data/good/v_good_node0002_catalog': 'created',
			//  '/data/good/v_good_node0003_data': 'created',
			//  '/data/good/v_good_node0003_depot': 'created',
			//  '/opt/vertica/config/logrotate': 'created'}
			_, err := op.parseAndCheckMapResponse(host, result.content)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
			}
		} else {
			allErrs = errors.Join(allErrs, result.err)
		}
	}

	return allErrs
}
