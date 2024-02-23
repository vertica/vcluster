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
	CurrentConfigFileVersion = 1
)

const ConfigFileName = "vertica_cluster.yaml"
const ConfigBackupName = "vertica_cluster.yaml.backup"

type Config struct {
	Version   int           `yaml:"config_file_version"`
	Databases ClusterConfig `yaml:"databases"`
}

// ClusterConfig is a map that stores configuration information for each database in the cluster.
type ClusterConfig map[string]DatabaseConfig

type DatabaseConfig struct {
	Nodes                   []*NodeConfig `yaml:"nodes"`
	IsEon                   bool          `yaml:"eon_mode"`
	CommunalStorageLocation string        `yaml:"communal_storage_location"`
	Ipv6                    bool          `yaml:"ipv6"`
}

type NodeConfig struct {
	Name        string `yaml:"name"`
	Address     string `yaml:"address"`
	Subcluster  string `yaml:"subcluster"`
	CatalogPath string `yaml:"catalog_path"`
	DataPath    string `yaml:"data_path"`
	DepotPath   string `yaml:"depot_path"`
}

func MakeClusterConfig() ClusterConfig {
	return make(ClusterConfig)
}

func MakeDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{}
}

// ReadConfig reads cluster configuration information from a YAML-formatted file in configDirectory.
// It returns a ClusterConfig and any error encountered when reading and parsing the file.
func ReadConfig(configFilePath string, logger vlog.Printer) (ClusterConfig, error) {
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
		config_file_version: 1
		databases:
			test_db:
				nodes:
					- name: v_test_db_node0001
					  address: 192.168.1.101
					  subcluster: default_subcluster
					  catalog_path: /data
					  data_path: /data
					  depot_path: /data
					...
				eon_mode: false
				communal_storage_location: ""
				ipv6: false
	*/

	clusterConfig := config.Databases
	logger.PrintInfo("The content of cluster config: %+v", clusterConfig)
	return clusterConfig, nil
}

// WriteConfig writes configuration information to configFilePath.
// It returns any write error encountered.
func (c *ClusterConfig) WriteConfig(configFilePath string) error {
	var config Config
	config.Version = CurrentConfigFileVersion
	config.Databases = *c

	configBytes, err := yaml.Marshal(&config)
	if err != nil {
		return fmt.Errorf("fail to marshal config data, details: %w", err)
	}
	err = os.WriteFile(configFilePath, configBytes, ConfigFilePerm)
	if err != nil {
		return fmt.Errorf("fail to write config file, details: %w", err)
	}

	return nil
}

// getPathPrefix returns catalog, data, and depot prefixes
func (c *ClusterConfig) getPathPrefix(dbName string) (catalogPrefix string,
	dataPrefix string, depotPrefix string, err error) {
	dbConfig, ok := (*c)[dbName]
	if !ok {
		return "", "", "", cannotFindDBFromConfigErr(dbName)
	}

	if len(dbConfig.Nodes) == 0 {
		return "", "", "",
			fmt.Errorf("no node was found from the config file of %s", dbName)
	}

	return dbConfig.Nodes[0].CatalogPath, dbConfig.Nodes[0].DataPath,
		dbConfig.Nodes[0].DepotPath, nil
}

func (c *ClusterConfig) getCommunalStorageLocation(dbName string) (communalStorageLocation string,
	err error) {
	dbConfig, ok := (*c)[dbName]
	if !ok {
		return "", cannotFindDBFromConfigErr(dbName)
	}
	return dbConfig.CommunalStorageLocation, nil
}

func (c *ClusterConfig) removeDatabaseFromConfigFile(dbName, configFilePath string, logger vlog.Printer) error {
	// back up the old config file
	err := backupConfigFile(configFilePath, logger)
	if err != nil {
		return err
	}

	// remove the target database from the cluster config
	// and overwrite the config file
	delete(*c, dbName)

	return c.WriteConfig(configFilePath)
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
