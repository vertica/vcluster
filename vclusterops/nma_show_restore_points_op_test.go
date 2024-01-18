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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

func TestShowRestorePointsRequestBody(t *testing.T) {
	const hostName = "host1"
	const dbName = "testDB"
	const communalLocation = "/communal"
	op := makeNMAShowRestorePointsOp(vlog.Printer{}, []string{hostName}, dbName, communalLocation, nil)

	requestBody, err := op.setupRequestBody()
	assert.NoError(t, err)
	assert.Len(t, requestBody, 1)
	assert.Contains(t, requestBody, hostName)
	hostReq := requestBody[hostName]
	assert.Contains(t, hostReq, `"communal_location":"`+communalLocation+`"`)
	assert.Contains(t, hostReq, `"db_name":"`+dbName+`"`)
}
