/*
 (c) Copyright [2023-2024] Open Text.
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

func TestVManageConnectionsOptions_validateParseOptions(t *testing.T) {
	logger := vlog.Printer{}

	opt := VManageConnectionDrainingOptionsFactory()
	testPassword := "test-password"
	testSCName := "test-sc"
	testDBName := "testdbname"
	testUserName := "test-username"
	testRedirectHostname := "test-redirect-hostname"

	opt.SCName = testSCName
	opt.IsEon = true
	opt.RawHosts = append(opt.RawHosts, "test-raw-host")
	opt.DBName = testDBName
	opt.UserName = testUserName
	opt.Password = &testPassword
	opt.Action = ActionRedirect
	opt.RedirectHostname = testRedirectHostname

	err := opt.validateParseOptions(logger)
	assert.NoError(t, err)

	// positive: no username (in which case default OS username will be used)
	opt.UserName = ""
	err = opt.validateParseOptions(logger)
	assert.NoError(t, err)

	// negative: no database name
	opt.UserName = testUserName
	opt.DBName = ""
	err = opt.validateParseOptions(logger)
	assert.Error(t, err)

	// negative: no redirect host name when action is redirect
	opt.DBName = testDBName
	opt.RedirectHostname = ""
	err = opt.validateParseOptions(logger)
	assert.Error(t, err)

	// negative: wrong action
	opt.RedirectHostname = testRedirectHostname
	opt.Action = "wrong-action"
	err = opt.validateParseOptions(logger)
	assert.Error(t, err)
}
