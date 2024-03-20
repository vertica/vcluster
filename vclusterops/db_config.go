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

// remove this file in VER-92369
package vclusterops

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"gopkg.in/yaml.v3"
)

const (
	ConfigDirPerm            = 0755
	ConfigFilePerm           = 0600
	CurrentConfigFileVersion = "1.0"
	ConfigFileName           = "vertica_cluster.yaml"
	ConfigBackupName         = "vertica_cluster.yaml.backup"
)

type Config struct {
	Version  string         `yaml:"configFileVersion"`
	Database DatabaseConfig `yaml:",inline"`
}

type DatabaseConfig struct {
	Name                    string        `yaml:"dbName"`
	Nodes                   []*NodeConfig `yaml:"nodes"`
	IsEon                   bool          `yaml:"eonMode"`
	CommunalStorageLocation string        `yaml:"communalStorageLocation"`
	Ipv6                    bool          `yaml:"ipv6"`
}

type NodeConfig struct {
	Name        string `yaml:"name"`
	Address     string `yaml:"address"`
	Subcluster  string `yaml:"subcluster"`
	CatalogPath string `yaml:"catalogPath"`
	DataPath    string `yaml:"dataPath"`
	DepotPath   string `yaml:"depotPath"`
}

func MakeDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{}
}

// ReadConfig reads database configuration information from a YAML-formatted file in configDirectory.
// It returns a DatabaseConfig and any error encountered when reading and parsing the file.
func ReadConfig(configFilePath string, logger vlog.Printer) (*DatabaseConfig, error) {
	if configFilePath == "" {
		return nil, fmt.Errorf("no config file provided")
	}
	configBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return nil, fmt.Errorf("fail to read config file, details: %w", err)
	}

	var config Config
	err = yaml.Unmarshal(configBytes, &config)
	if err != nil {
		return nil, fmt.Errorf("fail to unmarshal config file, details: %w", err)
	}

	// the config file content will look like
	/*
		configFileVersion: "1.0"
		dbName: test_db
		nodes:
			- name: v_test_db_node0001
			  address: 192.168.1.101
			  subcluster: default_subcluster
			  catalogPath: /data
			  dataPath: /data
			  depotPath: /data
			...
		eonMode: false
		communalStorageLocation: ""
		ipv6: false
	*/

	dbConfig := config.Database
	logger.PrintInfo("The content of database config: %+v", dbConfig)
	nodesDetails := "The database config nodes details: "
	for _, node := range dbConfig.Nodes {
		nodesDetails += fmt.Sprintf("%+v ", *node)
	}
	logger.PrintInfo(nodesDetails)
	return &dbConfig, nil
}

// WriteConfig writes configuration information to configFilePath.
// It returns any write error encountered.
func (c *DatabaseConfig) WriteConfig(configFilePath string, logger vlog.Printer) error {
	var config Config
	config.Version = CurrentConfigFileVersion
	config.Database = *c

	configBytes, err := yaml.Marshal(&config)
	if err != nil {
		return fmt.Errorf("fail to marshal config data, details: %w", err)
	}
	err = os.WriteFile(configFilePath, configBytes, ConfigFilePerm)
	if err != nil {
		return fmt.Errorf("fail to write config file, details: %w", err)
	}
	logger.Info("Config file successfully written", "config file path", configFilePath)

	return nil
}

// getPathPrefix returns catalog, data, and depot prefixes
func (c *DatabaseConfig) getPathPrefix(dbName string) (catalogPrefix string,
	dataPrefix string, depotPrefix string, err error) {
	if dbName != c.Name {
		return "", "", "", cannotFindDBFromConfigErr(dbName)
	}

	if len(c.Nodes) == 0 {
		return "", "", "",
			fmt.Errorf("no node was found from the config file of %s", dbName)
	}

	return c.Nodes[0].CatalogPath, c.Nodes[0].DataPath,
		c.Nodes[0].DepotPath, nil
}

func (c *DatabaseConfig) getCommunalStorageLocation(dbName string) (communalStorageLocation string,
	err error) {
	if dbName != c.Name {
		return "", cannotFindDBFromConfigErr(dbName)
	}
	return c.CommunalStorageLocation, nil
}

func (c *DatabaseConfig) removeConfigFile(configFilePath string, logger vlog.Printer) error {
	// back up the old config file
	err := backupConfigFile(configFilePath, logger)
	if err != nil {
		return err
	}

	// remove the old db config
	return os.Remove(configFilePath)
}

func (c *DatabaseConfig) getHosts() []string {
	var hostList []string

	for _, vnode := range c.Nodes {
		hostList = append(hostList, vnode.Address)
	}

	return hostList
}

func backupConfigFile(configFilePath string, logger vlog.Printer) error {
	if util.CanReadAccessDir(configFilePath) == nil {
		// copy file to vertica_cluster.yaml.backup
		configDirPath := filepath.Dir(configFilePath)
		configFileBackup := filepath.Join(configDirPath, ConfigBackupName)
		logger.Info("Config file exists and, creating a backup", "config file", configFilePath,
			"backup file", configFileBackup)
		err := util.CopyFile(configFilePath, configFileBackup, ConfigFilePerm)
		if err != nil {
			return err
		}
	}

	return nil
}
