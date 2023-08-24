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
	"path/filepath"
	"strings"

	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	respSuccResult   = "Download successful"
	userStorageType  = 4
	depotStorageType = 5
	catalogSuffix    = "Catalog"
)

type NMADownloadFileOp struct {
	OpBase
	hostRequestBodyMap map[string]string
	// vdb will be used to save downloaded file info for revive_db
	vdb *VCoordinationDatabase
	// newNodes is used to verify node number in http response for revive_db
	newNodes []string
}

type downloadFileRequestData struct {
	SourceFilePath      string            `json:"source_file_path"`
	DestinationFilePath string            `json:"destination_file_path"`
	CatalogPath         string            `json:"catalog_path,omitempty"`
	AWSAccessKeyID      string            `json:"aws_access_key_id,omitempty"`
	AWSSecretAccessKey  string            `json:"aws_secret_access_key,omitempty"`
	Parameters          map[string]string `json:"parameters,omitempty"`
}

func makeNMADownloadFileOp(hosts, newNodes []string, sourceFilePath, destinationFilePath, catalogPath string,
	communalStorageParameters map[string]string, vdb *VCoordinationDatabase) (NMADownloadFileOp, error) {
	op := NMADownloadFileOp{}
	op.name = "NMADownloadFileOp"
	op.hosts = hosts
	op.vdb = vdb
	op.newNodes = newNodes

	// make https json data
	op.hostRequestBodyMap = make(map[string]string)
	for _, host := range hosts {
		requestData := downloadFileRequestData{}
		requestData.SourceFilePath = sourceFilePath
		requestData.DestinationFilePath = destinationFilePath
		requestData.CatalogPath = catalogPath
		requestData.Parameters = communalStorageParameters

		dataBytes, err := json.Marshal(requestData)
		if err != nil {
			return op, fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}

		op.hostRequestBodyMap[host] = string(dataBytes)
	}

	return op, nil
}

func (op *NMADownloadFileOp) setupClusterHTTPRequest(hosts []string) error {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildNMAEndpoint("vertica/download-file")
		httpRequest.RequestData = op.hostRequestBodyMap[host]

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *NMADownloadFileOp) prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *NMADownloadFileOp) execute(execContext *OpEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMADownloadFileOp) finalize(_ *OpEngineExecContext) error {
	return nil
}

type downloadResponse struct {
	Result      string `json:"std_out"`
	FileContent string `json:"file_content"`
}

type fileContent struct {
	NodeList []struct {
		Name        string `json:"name"`
		Address     string `json:"address"`
		CatalogPath string `json:"catalogPath"`
		IsPrimary   bool   `json:"isPrimary"`
	} `json:"Node"`
	StorageLocations []struct {
		Name  string `json:"name"`
		Path  string `json:"path"`
		Usage int    `json:"usage"`
	} `json:"StorageLocation"`
}

func (op *NMADownloadFileOp) processResult(_ *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			response := downloadResponse{}
			err := op.parseAndCheckResponse(host, result.content, &response)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				break
			}

			result := strings.TrimSpace(response.Result)
			if result != respSuccResult {
				err = fmt.Errorf(`[%s] fail to download file on host %s, error result in the response is %s`, op.name, host, result)
				vlog.LogError(err.Error())
				allErrs = errors.Join(allErrs, err)
				break
			}

			// file content in the response is a string, we need to unmarshal it again
			descFileContent := fileContent{}
			err = op.parseAndCheckResponse(host, response.FileContent, &descFileContent)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				break
			}

			if len(descFileContent.NodeList) != len(op.newNodes) {
				err := fmt.Errorf(`[%s] nodes mismatch found on host %s: the number of the new nodes in --hosts is %d,`+
					` but the number of the old nodes in description file is %d`,
					op.name, host, len(op.newNodes), len(descFileContent.NodeList))
				vlog.LogError(err.Error())
				allErrs = errors.Join(allErrs, err)
				break
			}

			// save descFileContent in vdb
			op.vdb.HostNodeMap = make(map[string]VCoordinationNode)
			for _, node := range descFileContent.NodeList {
				op.vdb.HostList = append(op.vdb.HostList, node.Address)
				vNode := MakeVCoordinationNode()
				vNode.Name = node.Name
				vNode.Address = node.Address
				vNode.IsPrimary = node.IsPrimary

				// remove suffix "/Catalog" from node catalog path
				// e.g. /data/test_db/v_test_db_node0002_catalog/Catalog -> /data/test_db/v_test_db_node0002_catalog
				if filepath.Base(node.CatalogPath) == catalogSuffix {
					vNode.CatalogPath = filepath.Dir(node.CatalogPath)
				} else {
					vNode.CatalogPath = node.CatalogPath
				}

				for _, storage := range descFileContent.StorageLocations {
					// when storage name contains the node name, we know this storage is for that node
					// an example of storage name: "__location_1_v_test_db_node0001"
					// this will filter out communal storage location
					if strings.Contains(storage.Name, node.Name) {
						// we separate depot path and other storage locations
						if storage.Usage == depotStorageType {
							vNode.DepotPath = storage.Path
						} else {
							vNode.StorageLocations = append(vNode.StorageLocations, storage.Path)
							// we store the user storage location for later prepare directory use
							if storage.Usage == userStorageType {
								vNode.UserStorageLocations = append(vNode.UserStorageLocations, storage.Path)
							}
						}
					}
				}

				op.vdb.HostNodeMap[node.Address] = vNode
			}
			return nil
		}

		httpsErr := errors.Join(fmt.Errorf("[%s] HTTPS call failed on host %s", op.name, host), result.err)
		allErrs = errors.Join(allErrs, httpsErr)
	}

	return appendHTTPSFailureError(allErrs)
}
