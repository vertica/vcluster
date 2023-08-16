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

package util

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tonglil/buflogr"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type NMAHealthOpResponse map[string]string

func redirectLog() *bytes.Buffer {
	// redirect log to a local bytes.Buffer
	var logBuffer bytes.Buffer
	log := buflogr.NewWithBuffer(&logBuffer)
	vlogger := vlog.GetGlobalLogger()
	vlogger.Log = log

	return &logBuffer
}

func TestGetJSONLogErrors(t *testing.T) {
	/* positive case
	 */
	resultContent := `{"healthy": "true"}`
	var responseObj NMAHealthOpResponse
	expectedResponseObj := NMAHealthOpResponse{"healthy": "true"}

	err := GetJSONLogErrors(resultContent, &responseObj, "")

	assert.Nil(t, err)
	assert.Equal(t, responseObj, expectedResponseObj)

	/* netative case
	 */
	logBuffer := redirectLog()

	resultContent = `{"healthy": 123}`
	err = GetJSONLogErrors(resultContent, &responseObj, "")

	assert.NotNil(t, err)
	assert.Contains(t, logBuffer.String(), "ERROR <nil> fail to unmarshal the response content")

	err = GetJSONLogErrors(resultContent, &responseObj, "NMAHealthOp")
	assert.NotNil(t, err)
	assert.Contains(t, logBuffer.String(), "ERROR <nil> [NMAHealthOp] fail to unmarshal the response content")
}

func TestStringInArray(t *testing.T) {
	list := []string{"str1", "str2", "str3"}

	// positive case
	str := "str1"
	found := StringInArray(str, list)
	assert.Equal(t, found, true)

	// negative case
	strNeg := "randomStr"
	found = StringInArray(strNeg, list)
	assert.Equal(t, found, false)
}

func TestResolveToAbsPath(t *testing.T) {
	// positive case
	// not testing ~ because the output depends on devjail users
	path := "/data"
	res, err := ResolveToAbsPath(path)
	assert.Nil(t, err)
	assert.Equal(t, path, res)

	// negative case
	path = "/data/~/test"
	res, err = ResolveToAbsPath(path)
	assert.NotNil(t, err)
	assert.Equal(t, "", res)
}

func TestResolveToOneIP(t *testing.T) {
	// positive case
	hostname := "192.168.1.1"
	res, err := ResolveToOneIP(hostname, false)
	assert.Nil(t, err)
	assert.Equal(t, res, hostname)

	// negative case
	hostname = "randomIP"
	res, err = ResolveToOneIP(hostname, false)
	assert.NotNil(t, err)
	assert.Equal(t, res, "")
}

func TestGetCleanPath(t *testing.T) {
	// positive cases
	path := ""
	res := GetCleanPath(path)
	assert.Equal(t, res, "")

	path = "//data"
	res = GetCleanPath(path)
	assert.Equal(t, res, "/data")

	path = "//data "
	res = GetCleanPath(path)
	assert.Equal(t, res, "/data")
}

func TestSplitHosts(t *testing.T) {
	// positive case
	hosts := "vnode1, vnode2"
	res, err := SplitHosts(hosts)
	expected := []string{"vnode1", "vnode2"}
	assert.Nil(t, err)
	assert.Equal(t, res, expected)

	// negative case
	hosts = " "
	res, err = SplitHosts(hosts)
	assert.NotNil(t, err)
	assert.Equal(t, res, []string{})
}

type testStruct struct {
	Field1 string
	Field2 int
	Field3 []int
}

func TestCheckMissingFields(t *testing.T) {
	/* negative cases
	 */
	testObj := testStruct{}
	err := CheckMissingFields(testObj)
	assert.ErrorContains(t, err, "unexpected or missing fields in response object: [Field1 Field2 Field3]")

	testObj.Field1 = "Value 1"
	err = CheckMissingFields(testObj)
	assert.ErrorContains(t, err, "unexpected or missing fields in response object: [Field2 Field3]")

	/* positive case
	 */
	testObj.Field2 = 2
	testObj.Field3 = []int{3, 4, 5}
	err = CheckMissingFields(testObj)
	assert.Nil(t, err)
}

func TestSliceDiff(t *testing.T) {
	a := []string{"1", "2"}
	b := []string{"1", "3", "4"}
	expected := []string{"2"}
	actual := SliceDiff(a, b)
	assert.Equal(t, expected, actual)
}

func TestMapKeyDiff(t *testing.T) {
	a := map[string]bool{"1": true, "2": true}
	b := map[string]bool{"1": true, "3": true, "4": false}

	expected := []string{"2"}
	actual := MapKeyDiff(a, b)
	assert.Equal(t, expected, actual)
}

func TestFilterMapByKey(t *testing.T) {
	a := map[string]int{"1": 1, "2": 2}
	b := map[string]int{"1": 1, "3": 3, "4": 4, "2": 2}
	keys := []string{"1", "2"}
	c := FilterMapByKey(b, keys)
	assert.EqualValues(t, a, c)
}

func TestGetEnv(t *testing.T) {
	key := "NO_SUCH_ENV"
	fallback := "test"
	actual := GetEnv(key, fallback)
	assert.Equal(t, fallback, actual)
}

func TestValidateUsernamePassword(t *testing.T) {
	// when user name is "" but use password, the check should fail
	err := ValidateUsernameAndPassword("mock_op", true, "")
	assert.Error(t, err)

	// when user name is not empty and use password, the check should succeed
	err = ValidateUsernameAndPassword("mock_op", true, "dkr_dbadmin")
	assert.NoError(t, err)
}

func TestNewErrorFormatVerb(t *testing.T) {
	err := errors.New("test error")
	// replace %s with %w case 1
	oldErr1 := fmt.Errorf("fail to read config file, details: %s", err)
	newErr1 := fmt.Errorf("fail to read config file, details: %w", err)
	assert.EqualError(t, oldErr1, newErr1.Error())

	// replace %s with %w case 2
	oldErr2 := fmt.Errorf("fail to marshal config data, details: %s", err.Error())
	newErr2 := fmt.Errorf("fail to marshal config data, details: %w", err)
	assert.EqualError(t, oldErr2, newErr2.Error())

	// replace %v with %w
	oldErr3 := fmt.Errorf("fail to marshal start command, %v", err)
	newErr3 := fmt.Errorf("fail to marshal start command, %w", err)
	assert.EqualError(t, oldErr3, newErr3.Error())
}

func TestValidateDBName(t *testing.T) {
	// positive cases
	obj := "database"
	err := ValidateName("test_db", obj)
	assert.Nil(t, err)

	err = ValidateName("db1", obj)
	assert.Nil(t, err)

	// negative cases
	err = ValidateName("test$db", obj)
	assert.ErrorContains(t, err, "invalid character in "+obj+" name: $")

	err = ValidateName("[db1]", obj)
	assert.ErrorContains(t, err, "invalid character in "+obj+" name: [")

	err = ValidateName("!!??!!db1", obj)
	assert.ErrorContains(t, err, "invalid character in "+obj+" name: !")
}

func TestSetOptFlagHelpMsg(t *testing.T) {
	msg := "The name of the database to be created"
	finalMsg := "The name of the database to be created [Optional]"
	assert.Equal(t, GetOptionalFlagMsg(msg), finalMsg)
}

func TestSetEonFlagHelpMsg(t *testing.T) {
	msg := "Path to depot directory"
	finalMsg := "[Eon only] Path to depot directory"
	assert.Equal(t, GetEonFlagMsg(msg), finalMsg)
}

func TestParseConfigParams(t *testing.T) {
	configParamsListStr := ""
	configParams, err := ParseConfigParams(configParamsListStr)
	assert.Nil(t, err)
	assert.Nil(t, configParams)

	configParamsListStr = "key1=val1,key2"
	configParams, err = ParseConfigParams(configParamsListStr)
	assert.NotNil(t, err)
	assert.Nil(t, configParams)

	configParamsListStr = "key1=val1,=val2"
	configParams, err = ParseConfigParams(configParamsListStr)
	assert.NotNil(t, err)
	assert.Nil(t, configParams)

	configParamsListStr = "key1=val1,key2=val2"
	configParams, err = ParseConfigParams(configParamsListStr)
	assert.Nil(t, err)
	assert.ObjectsAreEqualValues(configParams, map[string]string{"key1": "val1", "key2": "val2"})
}

func TestGenVNodeName(t *testing.T) {
	dbName := "test_db"
	// returns vnode
	vnodes := make(map[string]string)
	vnodes["v_test_db_node0002"] = "vnode1"
	totalCount := 2
	vnode, ok := GenVNodeName(vnodes, dbName, totalCount)
	assert.Equal(t, true, ok)
	assert.Equal(t, "v_test_db_node0001", vnode)

	// returns empty string
	vnodes[vnode] = "vnode2"
	vnode, ok = GenVNodeName(vnodes, dbName, totalCount)
	assert.Equal(t, false, ok)
	assert.Equal(t, "", vnode)
}

func TestCopySlice(t *testing.T) {
	s1 := []string{"one", "two"}
	s2 := CopySlice(s1)
	assert.Equal(t, len(s2), len(s1))
	assert.Equal(t, s1[0], s2[0])
	assert.Equal(t, s1[1], s2[1])
	s2 = append(s2, "three")
	assert.NotEqual(t, len(s2), len(s1))
}

func TestCopyMap(t *testing.T) {
	s1 := map[string]string{
		"1": "one",
		"2": "two",
	}
	s2 := CopyMap(s1)
	assert.Equal(t, len(s2), len(s1))
	assert.Equal(t, s1["1"], s2["1"])
	assert.Equal(t, s1["2"], s2["2"])
	s2["3"] = "three"
	assert.NotEqual(t, len(s2), len(s1))
}
