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

package commands

import (
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var tempConfigFilePath = os.TempDir() + "/test_vertica_cluster.yaml"

func simulateVClusterCli(vclusterCmd string) error {
	// if no log file is given, the log will go to stdout
	dbOptions.LogPath = ""

	// convert the input string into a slice
	// extra spaces will be trimmed
	os.Args = strings.Fields(vclusterCmd)
	fmt.Println("")

	// simulate a VCluster CLI call
	log.Printf("Simulating VCluster CLI call %+v\n", os.Args)
	err := rootCmd.Execute()

	// reset os.Args
	os.Args = nil
	return err
}

func TestConfigRecover(t *testing.T) {
	err := simulateVClusterCli("vcluster manage_config recover")
	assert.ErrorContains(t, err, `required flag(s) "catalog-path", "db-name", "hosts" not set`)

	err = simulateVClusterCli("vcluster manage_config recover --db-name test_db")
	assert.ErrorContains(t, err, `required flag(s) "catalog-path", "hosts" not set`)

	err = simulateVClusterCli("vcluster manage_config recover --db-name test_db " +
		"--hosts 192.168.1.101")
	assert.ErrorContains(t, err, `required flag(s) "catalog-path" not set`)

	tempConfig, _ := os.Create(tempConfigFilePath)
	tempConfig.Close()
	defer os.Remove(tempConfigFilePath)

	err = simulateVClusterCli("vcluster manage_config recover --db-name test_db " +
		"--hosts 192.168.1.101 --catalog-path /data " +
		"--config " + tempConfigFilePath)
	assert.ErrorContains(t, err, "config file exists at "+tempConfigFilePath)
}

func TestManageConfig(t *testing.T) {
	// if none of recover or show provided, `vcluster manage_config` should succeed and show help message
	err := simulateVClusterCli("vcluster manage_config")
	assert.NoError(t, err)

	err = simulateVClusterCli("vcluster manage_config show recover")
	assert.ErrorContains(t, err, `unknown command "recover" for "vcluster manage_config show"`)
}

func TestManageReplication(t *testing.T) {
	// vcluster replication should succeed and show help message
	err := simulateVClusterCli("vcluster replication")
	assert.NoError(t, err)

	err = simulateVClusterCli("vcluster replication start test")
	assert.ErrorContains(t, err, `unknown command "test" for "vcluster replication start"`)
}

func TestStartReplication(t *testing.T) {
	// vcluster replication start should succeed
	// since there is no op for this subcommand
	err := simulateVClusterCli("vcluster replication start")
	assert.ErrorContains(t, err, `required flag(s) "target-db-name", "target-hosts" not set`)
}
