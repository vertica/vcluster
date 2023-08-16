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

package commands

import (
	"bytes"
	"os"
	"testing"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"

	"github.com/stretchr/testify/assert"
	"github.com/tonglil/buflogr"
)

func TestInitCmd(t *testing.T) {
	// no hosts provided, the case should fail
	c := makeCmdInit()
	err := c.Parse([]string{})
	assert.ErrorContains(t, err, "must provide the host list with --hosts")

	// hosts provided, the case should pass
	err = c.Parse([]string{"--hosts", "vnode1,vnode2,vnode3"})
	assert.Nil(t, err)

	// no directory provided, current directory will be used
	currentDir, _ := os.Getwd()
	assert.Equal(t, currentDir, *c.directory)

	// directory provided, the given directory will be used
	const configDir = "/opt/vertica/config"
	c = makeCmdInit()
	err = c.Parse([]string{
		"--hosts", "vnode1,vnode2,vnode3",
		"--directory", configDir})
	assert.Nil(t, err)
	assert.Equal(t, "/opt/vertica/config", *c.directory)
}

func TestConfigCmd(t *testing.T) {
	// redirect log to a local bytes.Buffer
	var logStr bytes.Buffer
	log := buflogr.NewWithBuffer(&logStr)
	vlogger := vlog.GetGlobalLogger()
	vlogger.Log = log

	// create a stub YAML file
	const yamlPath = vclusterops.ConfigFileName
	const yamlStr = "hosts\n  - vnode1\n  - vnode2\n  - vnode3"
	_ = os.WriteFile(yamlPath, []byte(yamlStr), vclusterops.ConfigFilePerm)
	defer os.Remove(yamlPath)

	// if `--show` is not specified, the config content should not show
	c := makeCmdConfig()
	err := c.Parse([]string{})
	assert.Nil(t, err)

	err = c.Run(log)
	assert.Nil(t, err)
	assert.NotContains(t, logStr.String(), yamlStr)

	// if `--show` is specified, the config content should show
	c = makeCmdConfig()
	err = c.Parse([]string{"--show"})
	assert.Nil(t, err)

	err = c.Run(log)
	assert.Nil(t, err)
	assert.Contains(t, logStr.String(), yamlStr)

	// now run `init`, the command should fail
	// because the config file under the current directory already exists
	cmdInit := makeCmdInit()
	err = cmdInit.Parse([]string{"--hosts", "vnode1,vnode2,vnode3"})
	assert.Nil(t, err)

	err = cmdInit.Run(log)
	assert.ErrorContains(t, err, vclusterops.ConfigFileName+" already exists")
}
