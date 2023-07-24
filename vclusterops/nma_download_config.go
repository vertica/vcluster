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
	"errors"
	"fmt"
)

type NMADownloadConfigOp struct {
	OpBase
	catalogPathMap map[string]string
	endpoint       string
	fileContent    *string
}

func MakeNMADownloadConfigOp(
	opName string,
	hostCatalogPath map[string]string,
	bootstrapHosts []string,
	endpoint string,
	fileContent *string,
) NMADownloadConfigOp {
	nmaDownloadConfigOp := NMADownloadConfigOp{}
	nmaDownloadConfigOp.name = opName
	nmaDownloadConfigOp.hosts = bootstrapHosts
	nmaDownloadConfigOp.endpoint = endpoint
	nmaDownloadConfigOp.fileContent = fileContent

	return nmaDownloadConfigOp
}

func (op *NMADownloadConfigOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.BuildNMAEndpoint(op.endpoint)

		catalogPath, ok := op.catalogPathMap[host]
		if !ok {
			msg := fmt.Errorf("[%s] fail to get catalog path from host %s", op.name, host)
			panic(msg)
		}
		httpRequest.QueryParams = map[string]string{"catalog_path": catalogPath}

		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *NMADownloadConfigOp) Prepare(execContext *OpEngineExecContext) error {
	// If the host input is a nil value, we find the host with the highest catalog version to update the host input.
	// Otherwise, we use the host input.
	if op.hosts == nil {
		hostsWithLatestCatalog := execContext.hostsWithLatestCatalog
		if len(hostsWithLatestCatalog) == 0 {
			return fmt.Errorf("could not find at least one host with the latest catalog")
		}
		hostWithHighestCatalog := hostsWithLatestCatalog[:1]
		// update the host with the highest catalog
		op.hosts = hostWithHighestCatalog
	}
	// Update the catalogPathMap for next download operation's steps from information of catalog editor
	nmaVDB := execContext.nmaVDatabase
	op.catalogPathMap = make(map[string]string)
	err := updateCatalogPathMapFromCatalogEditor(op.hosts, &nmaVDB, op.catalogPathMap)
	if err != nil {
		return fmt.Errorf("failed to get catalog paths from catalog editor: %w", err)
	}

	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *NMADownloadConfigOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *NMADownloadConfigOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

func (op *NMADownloadConfigOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error
	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)
		if result.isPassing() {
			// The content of config file will be stored as content of the response
			*op.fileContent = result.content
			return nil
		}
		allErrs = errors.Join(allErrs, result.err)
	}

	return errors.Join(allErrs, fmt.Errorf("could not find a host with a passing result"))
}
