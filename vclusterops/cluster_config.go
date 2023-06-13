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

// TODO move this file to commands package
package vclusterops

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	ConfigDirPerm  = 0755
	ConfigFilePerm = 0600
)

const ConfigFileName = "vertica_cluster.yaml"
const ConfigBackupName = "vertica_cluster.yaml.backup"

type ClusterConfig struct {
	DBName string       `yaml:"db_name"`
	Hosts  []string     `yaml:"hosts"`
	Nodes  []NodeConfig `yaml:"nodes"`
	IsEon  bool         `yaml:"eon_mode"`
}

type NodeConfig struct {
	Name    string `yaml:"name"`
	Address string `yaml:"address"`
}

func MakeClusterConfig() ClusterConfig {
	return ClusterConfig{}
}

// read config information from the YAML file
func ReadConfig(configDirectory string) (ClusterConfig, error) {
	clusterConfig := ClusterConfig{}

	configFilePath := filepath.Join(configDirectory, ConfigFileName)
	configBytes, err := os.ReadFile(configFilePath)
	if err != nil {
		return clusterConfig, fmt.Errorf("fail to read config file, details: %w", err)
	}

	err = yaml.Unmarshal(configBytes, &clusterConfig)
	if err != nil {
		return clusterConfig, fmt.Errorf("fail to unmarshal config file, details: %w", err)
	}

	return clusterConfig, nil
}

// write config information to the YAML file
func (clusterConfig *ClusterConfig) WriteConfig(configFilePath string) error {
	configBytes, err := yaml.Marshal(&clusterConfig)
	if err != nil {
		return fmt.Errorf("fail to marshal config data, details: %w", err)
	}
	err = os.WriteFile(configFilePath, configBytes, ConfigFilePerm)
	if err != nil {
		return fmt.Errorf("fail to write config file, details: %w", err)
	}

	return nil
}

func GetConfigFilePath(dbName string, inputConfigDir *string) (string, error) {
	var configParentPath string

	// if the input config directory is given and has write permission,
	// write the YAML config file under this directory
	if inputConfigDir != nil {
		if err := os.MkdirAll(*inputConfigDir, ConfigDirPerm); err != nil {
			return "", fmt.Errorf("fail to create config path at %s, detail: %w", *inputConfigDir, err)
		}

		return filepath.Join(*inputConfigDir, ConfigFileName), nil
	}

	// otherwise write it under the current directory
	// as <current_dir>/vertica_cluster.yaml
	currentDir, err := os.Getwd()
	if err != nil {
		vlog.LogWarning("Fail to get current directory\n")
		configParentPath = currentDir
	}

	// create a directory with the database name
	// then write the config content inside this directory
	configDirPath := filepath.Join(configParentPath, dbName)
	if err := os.MkdirAll(configDirPath, ConfigDirPerm); err != nil {
		return "", fmt.Errorf("fail to create config path at %s, detail: %w", configDirPath, err)
	}

	configFilePath := filepath.Join(configDirPath, ConfigFileName)
	return configFilePath, nil
}

func BackupConfigFile(configFilePath string) error {
	if util.CanReadAccessDir(configFilePath) == nil {
		// copy file to vertica_cluster.yaml.backup
		configDirPath := filepath.Dir(configFilePath)
		configFileBackup := filepath.Join(configDirPath, ConfigBackupName)
		vlog.LogInfo("Config file exists at %s, creating a backup at %s",
			configFilePath, configFileBackup)
		err := util.CopyFile(configFilePath, configFileBackup, ConfigFilePerm)
		if err != nil {
			return err
		}
	}

	return nil
}
