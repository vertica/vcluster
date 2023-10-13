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

type nmaLoadRemoteCatalogOp struct {
	OpBase
	hostRequestBodyMap      map[string]string
	configurationParameters map[string]string
	oldHosts                []string
	vdb                     *VCoordinationDatabase
	timeout                 uint
	primaryNodeCount        uint
}

type loadRemoteCatalogRequestData struct {
	DBName             string              `json:"db_name"`
	StorageLocations   []string            `json:"storage_locations"`
	CommunalLocation   string              `json:"communal_location"`
	CatalogPath        string              `json:"catalog_path"`
	Host               string              `json:"host"`
	NodeName           string              `json:"node_name"`
	AWSAccessKeyID     string              `json:"aws_access_key_id,omitempty"`
	AWSSecretAccessKey string              `json:"aws_secret_access_key,omitempty"`
	NodeAddresses      map[string][]string `json:"node_addresses"`
	Parameters         map[string]string   `json:"parameters,omitempty"`
}

func makeNMALoadRemoteCatalogOp(oldHosts []string, configurationParameters map[string]string,
	vdb *VCoordinationDatabase, timeout uint) nmaLoadRemoteCatalogOp {
	op := nmaLoadRemoteCatalogOp{}
	op.name = "NMALoadRemoteCatalogOp"
	op.hosts = vdb.HostList
	op.oldHosts = oldHosts
	op.configurationParameters = configurationParameters
	op.vdb = vdb
	op.timeout = timeout

	op.primaryNodeCount = 0
	for _, vnode := range vdb.HostNodeMap {
		if vnode.IsPrimary {
			op.primaryNodeCount++
		}
	}

	return op
}

// make https json data
func (op *nmaLoadRemoteCatalogOp) setupRequestBody(execContext *OpEngineExecContext) error {
	if len(execContext.networkProfiles) != len(op.hosts) {
		return fmt.Errorf("[%s] the number of hosts in networkProfiles does not match"+
			" the number of hosts that will load remote catalogs", op.name)
	}

	// NodeAddresses format {node_name : [new_ip, new_ip_control_ip, new_ip_broadcast_ip]}
	nodeAddresses := make(map[string][]string)
	for host, profile := range execContext.networkProfiles {
		var addresses []string
		addresses = append(addresses, host, profile.Address, profile.Broadcast)
		if node, found := op.vdb.HostNodeMap[host]; found {
			nodeAddresses[node.Name] = addresses
		} else {
			return fmt.Errorf("[%s] fail to find host %s in host node map", op.name, host)
		}
	}

	op.hostRequestBodyMap = make(map[string]string)
	for index, host := range op.hosts {
		requestData := loadRemoteCatalogRequestData{}
		requestData.DBName = op.vdb.Name
		requestData.CommunalLocation = op.vdb.CommunalStorageLocation
		requestData.Host = op.oldHosts[index]
		vNode := op.vdb.HostNodeMap[host]
		requestData.NodeName = vNode.Name
		requestData.CatalogPath = vNode.CatalogPath
		requestData.StorageLocations = vNode.StorageLocations
		requestData.NodeAddresses = nodeAddresses
		requestData.Parameters = op.configurationParameters

		dataBytes, err := json.Marshal(requestData)
		if err != nil {
			return fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	return nil
}

func (op *nmaLoadRemoteCatalogOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("catalog/revive")
		httpRequest.RequestData = op.hostRequestBodyMap[host]
		httpRequest.Timeout = int(op.timeout)

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *nmaLoadRemoteCatalogOp) prepare(execContext *OpEngineExecContext) error {
	err := op.setupRequestBody(execContext)
	if err != nil {
		return err
	}

	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *nmaLoadRemoteCatalogOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaLoadRemoteCatalogOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

func (op *nmaLoadRemoteCatalogOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error
	var successPrimaryNodeCount uint

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			response := httpsResponseStatus{}
			err := op.parseAndCheckResponse(host, result.content, &response)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}

			err = op.checkResponseStatusCode(response, host)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}

			if op.vdb.HostNodeMap[host].IsPrimary {
				successPrimaryNodeCount++
			}
			continue
		}

		httpsErr := errors.Join(fmt.Errorf("[%s] HTTPS call failed on host %s", op.name, host), result.err)
		allErrs = errors.Join(allErrs, httpsErr)
	}

	// quorum check
	if !op.hasQuorum(successPrimaryNodeCount, op.primaryNodeCount) {
		err := fmt.Errorf("[%s] fail to load catalog on enough primary nodes. Success count: %d", op.name, successPrimaryNodeCount)
		vlog.LogError(err.Error())
		allErrs = errors.Join(allErrs, err)
		return allErrs
	}

	return nil
}
