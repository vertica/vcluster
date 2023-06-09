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
	"log"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type NMAHealthOpResponse map[string]string

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
	// redirect log to a local bytes.Buffer
	var logStr bytes.Buffer
	log.SetOutput(&logStr)

	resultContent = `{"healthy": 123}`
	err = GetJSONLogErrors(resultContent, &responseObj, "")

	assert.NotNil(t, err)
	assert.Contains(t, logStr.String(), "[ERROR] fail to unmarshal the response content")

	err = GetJSONLogErrors(resultContent, &responseObj, "NMAHealthOp")
	assert.NotNil(t, err)
	assert.Contains(t, logStr.String(), "[ERROR] [NMAHealthOp] fail to unmarshal the response content")
}

func TestIsLocalHost(t *testing.T) {
	// test providing host name
	localHostName, _ := os.Hostname()
	isLocalHost, err := IsLocalHost(localHostName)
	assert.Nil(t, err)
	assert.True(t, isLocalHost)

	// test providing IP address
	localHostAddresses, _ := net.LookupIP(localHostName)
	localAddr := localHostAddresses[0].String()
	isLocalHost, err = IsLocalHost(localAddr)
	assert.Nil(t, err)
	assert.True(t, isLocalHost)
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

func TestGetEnv(t *testing.T) {
	key := "NO_SUCH_ENV"
	fallback := "test"
	actual := GetEnv(key, fallback)
	assert.Equal(t, fallback, actual)
}

func TestValidateUsernamePassword(t *testing.T) {
	// when user name is "" but use password, the check should fail
	checkFunc := func() {
		ValidateUsernameAndPassword(true, "")
	}
	require.Panics(t, checkFunc)

	// when user name is not empty and use password, the check should succeed
	checkFunc = func() {
		ValidateUsernameAndPassword(true, "dkr_dbadmin")
	}
	require.NotPanics(t, checkFunc)
}
