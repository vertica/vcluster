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

type nmaShowRestorePointsOp struct {
	opBase
	dbName                  string
	communalLocation        string
	configurationParameters map[string]string
}

type showRestorePointsRequestData struct {
	DBName           string            `json:"db_name"`
	CommunalLocation string            `json:"communal_location"`
	Parameters       map[string]string `json:"parameters,omitempty"`
}

// This op is used to show restore points in a database
func makeNMAShowRestorePointsOp(logger vlog.Printer,
	hosts []string, dbName, communalLocation string, configurationParameters map[string]string) nmaShowRestorePointsOp {
	return nmaShowRestorePointsOp{
		opBase: opBase{
			name:   "NMAShowRestorePointsOp",
			logger: logger.WithName("NMAShowRestorePointsOp"),
			hosts:  hosts,
		},
		dbName:                  dbName,
		configurationParameters: configurationParameters,
		communalLocation:        communalLocation,
	}
}

// make https json data
func (op *nmaShowRestorePointsOp) setupRequestBody() (map[string]string, error) {
	hostRequestBodyMap := make(map[string]string, len(op.hosts))
	for _, host := range op.hosts {
		requestData := showRestorePointsRequestData{}
		requestData.DBName = op.dbName
		requestData.CommunalLocation = op.communalLocation
		requestData.Parameters = op.configurationParameters

		dataBytes, err := json.Marshal(requestData)
		if err != nil {
			return nil, fmt.Errorf("[%s] fail to marshal request data to JSON string, detail %w", op.name, err)
		}
		hostRequestBodyMap[host] = string(dataBytes)
	}
	return hostRequestBodyMap, nil
}

func (op *nmaShowRestorePointsOp) setupClusterHTTPRequest(hostRequestBodyMap map[string]string) error {
	for host, requestBody := range hostRequestBodyMap {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = GetMethod
		httpRequest.buildNMAEndpoint("restore-points")
		httpRequest.RequestData = requestBody
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *nmaShowRestorePointsOp) prepare(execContext *opEngineExecContext) error {
	hostRequestBodyMap, err := op.setupRequestBody()
	if err != nil {
		return err
	}

	execContext.dispatcher.setup(op.hosts)
	return op.setupClusterHTTPRequest(hostRequestBodyMap)
}

func (op *nmaShowRestorePointsOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *nmaShowRestorePointsOp) finalize(_ *opEngineExecContext) error {
	return nil
}

// RestorePoint contains information about a single restore point.
type RestorePoint struct {
	// Name of the archive that this restore point was created in.
	Archive string
	// The ID of the restore point. This is a form of a UID that is static for the restore point.
	ID string
	// The current index of this restore point. Lower value means it was taken more recently.
	// This changes when new restore points are created.
	Index int
	// The timestamp when the restore point was created.
	Timestamp string
	// The version of Vertica running when the restore point was created.
	VerticaVersion string
}

func (op *nmaShowRestorePointsOp) processResult(execContext *opEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if result.isPassing() {
			/*  [
					{
						"archive": "db",
						"id": "4ee4119b-802c-4bb4-94b0-061c8748b602",
						"index": 1,
						"timestamp": "2023-05-02 14:10:31.038289"
					},
					{
						"archive": "db",
						"id": "bdaa4764-d8aa-4979-89e5-e642cc58d972",
						"index": 2,
						"timestamp": "2023-05-02 14:10:28.717667"
					}
			    ]
			*/
			var responseObj []RestorePoint
			err := op.parseAndCheckResponse(host, result.content, &responseObj)
			if err != nil {
				allErrs = errors.Join(allErrs, err)
				continue
			}

			op.logger.PrintInfo("[%s] response: %v", op.name, result.content)
			execContext.restorePoints = responseObj
			return nil
		}

		allErrs = errors.Join(allErrs, result.err)
	}
	return allErrs
}
