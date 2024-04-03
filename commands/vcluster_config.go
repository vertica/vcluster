/*
 (c) Copyright [2024] Open Text.
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
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"gopkg.in/yaml.v3"
)

const (
	vclusterConfigEnv = "VCLUSTER_CONFIG"
	// If no config file was provided, we will pick a default one. This is the
	// default file name that we'll use.
	defConfigFileName        = "vertica_cluster.yaml"
	currentConfigFileVersion = "1.0"
	configBackupName         = "vertica_cluster.yaml.backup"
	configFilePerm           = 0600
)

// Config is the struct of vertica_cluster.yaml
type Config struct {
	Version  string         `yaml:"configFileVersion"`
	Database DatabaseConfig `yaml:",inline"`
}

// DatabaseConfig contains basic information for operating a database
type DatabaseConfig struct {
	Name                    string        `yaml:"dbName" mapstructure:"dbName"`
	Nodes                   []*NodeConfig `yaml:"nodes" mapstructure:"nodes"`
	IsEon                   bool          `yaml:"eonMode" mapstructure:"eonMode"`
	CommunalStorageLocation string        `yaml:"communalStorageLocation" mapstructure:"communalStorageLocation"`
	Ipv6                    bool          `yaml:"ipv6" mapstructure:"ipv6"`
}

// NodeConfig contains node information in the database
type NodeConfig struct {
	Name        string `yaml:"name" mapstructure:"name"`
	Address     string `yaml:"address" mapstructure:"address"`
	Subcluster  string `yaml:"subcluster" mapstructure:"subcluster"`
	CatalogPath string `yaml:"catalogPath" mapstructure:"catalogPath"`
	DataPath    string `yaml:"dataPath" mapstructure:"dataPath"`
	DepotPath   string `yaml:"depotPath" mapstructure:"depotPath"`
}

// MakeDatabaseConfig() can create an instance of DatabaseConfig
func MakeDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{}
}

// initConfig will initialize the dbOptions.ConfigPath field for the vcluster exe.
func initConfig() {
	vclusterExePath, err := os.Executable()
	cobra.CheckErr(err)
	// If running vcluster from /opt/vertica/bin, we will ensure
	// /opt/vertica/config exists before using it.
	const ensureOptVerticaConfigExists = true
	// If using the user config director ($HOME/.config), we will ensure the necessary dir exists.
	const ensureUserConfigDirExists = true
	initConfigImpl(vclusterExePath, ensureOptVerticaConfigExists, ensureUserConfigDirExists)
}

// initConfigImpl will initialize the dbOptions.ConfigPath field. It will make an
// attempt to figure out the best value. In certain circumstances, it may fail
// to have a config path at all. In that case dbOptions.ConfigPath will be left
// as an empty string.
func initConfigImpl(vclusterExePath string, ensureOptVerticaConfigExists, ensureUserConfigDirExists bool) {
	// We need to find the path to the config. The order of precedence is as follows:
	// 1. Option
	// 2. Environment variable
	// 3. Default locations
	//   a. /opt/vertica/config/vertica_config.yaml if running vcluster in /opt/vertica/bin
	//   b. $HOME/.config/vcluster/vertica_config.yaml otherwise
	//
	// If none of these things are true, then we run the cli without a config file.

	// If option is set, nothing else to do in here
	if dbOptions.ConfigPath != "" {
		return
	}

	// Check environment variable
	if dbOptions.ConfigPath == "" {
		val, ok := os.LookupEnv(vclusterConfigEnv)
		if ok && val != "" {
			dbOptions.ConfigPath = val
			return
		}
	}

	// Pick a default config file.

	// If we are running vcluster from /opt/vertica/bin, we'll assume we
	// have installed the vertica package on this machine and so can assume
	// /opt/vertica/config exists too.
	if vclusterExePath == defaultExecutablePath {
		const rpmConfDir = "/opt/vertica/config"
		_, err := os.Stat(rpmConfDir)
		if ensureOptVerticaConfigExists && err != nil {
			if os.IsNotExist(err) {
				err = nil
			}
			cobra.CheckErr(err)
		} else {
			dbOptions.ConfigPath = fmt.Sprintf("%s/%s", rpmConfDir, defConfigFileName)
			return
		}
	}

	// Finally default to the .config directory in the users home. This is used
	// by many CLI applications.
	cfgDir, err := os.UserConfigDir()
	cobra.CheckErr(err)

	// Ensure the config directory exists.
	path := filepath.Join(cfgDir, "vcluster")
	if ensureUserConfigDirExists {
		const configDirPerm = 0755
		err = os.MkdirAll(path, configDirPerm)
		if err != nil {
			// Just abort if we don't have write access to the config path
			return
		}
	}
	dbOptions.ConfigPath = fmt.Sprintf("%s/%s", path, defConfigFileName)
}

// loadConfigToViper can fill viper keys using vertica_cluster.yaml
func loadConfigToViper() error {
	// read config file
	viper.SetConfigFile(dbOptions.ConfigPath)
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Printf("Warning: fail to read configuration file %q for viper: %v\n", dbOptions.ConfigPath, err)
		return nil
	}

	// retrieve db info in viper
	dbConfig := MakeDatabaseConfig()
	err = viper.Unmarshal(&dbConfig)
	if err != nil {
		fmt.Printf("Warning: fail to unmarshal config file into DatabaseConfig: %v\n", err)
		return nil
	}

	// if we can read config file, check if dbName in user input matches the one in config file
	if viper.IsSet(dbNameKey) && dbConfig.Name != viper.GetString(dbNameKey) {
		return fmt.Errorf("database %q does not match name found in the config file %q", dbConfig.Name, viper.GetString(dbNameKey))
	}

	// hosts, catalogPrefix, dataPrefix, depotPrefix are special in config file,
	// they are the values in each node so they need extra process.
	if !viper.IsSet(hostsKey) {
		viper.Set(hostsKey, dbConfig.getHosts())
	}
	catalogPrefix, dataPrefix, depotPrefix := dbConfig.getPathPrefixes()
	if !viper.IsSet(catalogPathKey) {
		viper.Set(catalogPathKey, catalogPrefix)
	}
	if !viper.IsSet(dataPathKey) {
		viper.Set(dataPathKey, dataPrefix)
	}
	if !viper.IsSet(depotPathKey) {
		viper.Set(depotPathKey, depotPrefix)
	}
	return nil
}

// writeConfig can write database information to vertica_cluster.yaml.
// It will be called in the end of some subcommands that will change the db state.
func writeConfig(vdb *vclusterops.VCoordinationDatabase, logger vlog.Printer) error {
	if dbOptions.ConfigPath == "" {
		return fmt.Errorf("config path is empty")
	}

	dbConfig, err := readVDBToDBConfig(vdb)
	if err != nil {
		return err
	}

	// if the config file exists already,
	// create its backup before overwriting it
	err = backupConfigFile(dbOptions.ConfigPath, logger)
	if err != nil {
		return err
	}

	// update db config with the given database info
	err = dbConfig.write(dbOptions.ConfigPath)
	if err != nil {
		return err
	}

	return nil
}

// readVDBToDBConfig converts vdb to DatabaseConfig
func readVDBToDBConfig(vdb *vclusterops.VCoordinationDatabase) (DatabaseConfig, error) {
	dbConfig := MakeDatabaseConfig()
	// loop over HostList is needed as we want to preserve the order
	for _, host := range vdb.HostList {
		vnode, ok := vdb.HostNodeMap[host]
		if !ok {
			return dbConfig, fmt.Errorf("cannot find host %s from HostNodeMap", host)
		}
		nodeConfig := NodeConfig{}
		nodeConfig.Name = vnode.Name
		nodeConfig.Address = vnode.Address
		nodeConfig.Subcluster = vnode.Subcluster

		// VER-91869 will replace the path prefixes with full paths
		if vdb.CatalogPrefix == "" {
			nodeConfig.CatalogPath = util.GetPathPrefix(vnode.CatalogPath)
		} else {
			nodeConfig.CatalogPath = vdb.CatalogPrefix
		}
		if vdb.DataPrefix == "" && len(vnode.StorageLocations) > 0 {
			nodeConfig.DataPath = util.GetPathPrefix(vnode.StorageLocations[0])
		} else {
			nodeConfig.DataPath = vdb.DataPrefix
		}
		if vdb.IsEon && vdb.DepotPrefix == "" {
			nodeConfig.DepotPath = util.GetPathPrefix(vnode.DepotPath)
		} else {
			nodeConfig.DepotPath = vdb.DepotPrefix
		}

		dbConfig.Nodes = append(dbConfig.Nodes, &nodeConfig)
	}
	dbConfig.IsEon = vdb.IsEon
	dbConfig.CommunalStorageLocation = vdb.CommunalStorageLocation
	dbConfig.Ipv6 = vdb.Ipv6
	dbConfig.Name = vdb.Name

	return dbConfig, nil
}

// backupConfigFile backs up config file before we update it.
// This function will add ".backup" suffix to previous config file.
func backupConfigFile(configFilePath string, logger vlog.Printer) error {
	if util.CanReadAccessDir(configFilePath) == nil {
		// copy file to vertica_cluster.yaml.backup
		configDirPath := filepath.Dir(configFilePath)
		configFileBackup := filepath.Join(configDirPath, configBackupName)
		logger.Info("Config file exists and, creating a backup", "config file", configFilePath,
			"backup file", configFileBackup)
		err := util.CopyFile(configFilePath, configFileBackup, configFilePerm)
		if err != nil {
			return err
		}
	}

	return nil
}

// read reads information from configFilePath to a DatabaseConfig object.
// It returns any read error encountered.
func readConfig() (dbConfig *DatabaseConfig, err error) {
	configFilePath := dbOptions.ConfigPath

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

	return &config.Database, nil
}

// write writes configuration information to configFilePath. It returns
// any write error encountered. The viper in-built write function cannot
// work well(the order of keys cannot be customized) so we used yaml.Marshal()
// and os.WriteFile() to write the config file.
func (c *DatabaseConfig) write(configFilePath string) error {
	var config Config
	config.Version = currentConfigFileVersion
	config.Database = *c

	configBytes, err := yaml.Marshal(&config)
	if err != nil {
		return fmt.Errorf("fail to marshal config data, details: %w", err)
	}
	err = os.WriteFile(configFilePath, configBytes, configFilePerm)
	if err != nil {
		return fmt.Errorf("fail to write config file, details: %w", err)
	}

	return nil
}

// getHosts returns host addresses of all nodes in database
func (c *DatabaseConfig) getHosts() []string {
	var hostList []string

	for _, vnode := range c.Nodes {
		hostList = append(hostList, vnode.Address)
	}

	return hostList
}

// getPathPrefix returns catalog, data, and depot prefixes
func (c *DatabaseConfig) getPathPrefixes() (catalogPrefix string,
	dataPrefix string, depotPrefix string) {
	if len(c.Nodes) == 0 {
		return "", "", ""
	}

	return c.Nodes[0].CatalogPath, c.Nodes[0].DataPath, c.Nodes[0].DepotPath
}
