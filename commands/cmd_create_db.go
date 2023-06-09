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
	"flag"
	"fmt"
	"os"
	"strings"

	"vertica.com/vcluster/vclusterops"
	"vertica.com/vcluster/vclusterops/util"
	"vertica.com/vcluster/vclusterops/vlog"
)

/* CmdCreateDB
 *
 * Parses arguments to createDB and calls
 * the high-level function for createDB.
 *
 * Implements ClusterCommand interface
 */

const (
	keyValueArrayLen = 2
)

type CmdCreateDB struct {
	argv            []string
	parser          *flag.FlagSet
	createDBOptions *vclusterops.VCreateDatabaseOptions

	hostListStr        *string // raw string from user input, need further processing
	configParamListStr *string // raw input from user, need further processing
}

func MakeCmdCreateDB() CmdCreateDB {
	// CmdCreateDB
	newCmd := CmdCreateDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("create_db", flag.ExitOnError)
	createDBOptions := vclusterops.VCreateDatabaseOptionsFactory()

	// required flags
	createDBOptions.Name = newCmd.parser.String("name", "", "The name of the database to be created")
	createDBOptions.Password = newCmd.parser.String("password", "", "Database password in single quotes [Optional]")
	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated list of hosts to participate in database")
	createDBOptions.LicensePathOnNode = newCmd.parser.String("license", "", "Database license [Optional]")
	createDBOptions.CatalogPrefix = newCmd.parser.String("catalog-path", "", "Path of catalog directory")
	createDBOptions.DataPrefix = newCmd.parser.String("data-path", "", "Path of data directory")

	// optional
	createDBOptions.Policy = newCmd.parser.String("policy", util.DefaultRestartPolicy, "Restart policy of the database")
	createDBOptions.SQLFile = newCmd.parser.String("sql", "", "SQL file to run (as dbadmin) immediately on database creation")
	createDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "", "Directory where the config file will be generated")

	// Eon flags
	createDBOptions.CommunalStorageLocation = newCmd.parser.String("communal-storage-location", "",
		EonOnlyOption+"Location of communal storage")
	createDBOptions.ShardCount = newCmd.parser.Int("shard-count", 0, EonOnlyOption+"Number of shards in the database")
	createDBOptions.CommunalStorageParamsPath = newCmd.parser.String("communal_storage-params", "",
		EonOnlyOption+"Location of communal storage parameter file")
	createDBOptions.DepotPrefix = newCmd.parser.String("depot-path", "", EonOnlyOption+"Path to depot directory")
	createDBOptions.DepotSize = newCmd.parser.String("depot-size", "", EonOnlyOption+"Size of depot")
	createDBOptions.GetAwsCredentialsFromEnv = newCmd.parser.Bool("get-aws-credentials-from-env-vars", false,
		EonOnlyOption+"Read AWS credentials from environment variables")

	newCmd.configParamListStr = newCmd.parser.String("config-param", "",
		"Comma-separated list of NAME=VALUE pairs for setting database configuration parametesr immediately on database creation")

	// new flags comparing to adminTools create_db
	createDBOptions.Ipv6 = newCmd.parser.Bool("ipv6", false, "Create database with IPv6 hosts")
	// by default use pt2pt mode
	createDBOptions.P2p = newCmd.parser.Bool("point-to-point", true,
		"Configure Spread to use point-to-point communication between all Vertica nodes")
	createDBOptions.Broadcast = newCmd.parser.Bool("broadcast", false,
		"Configure Spread to use UDP broadcast traffic between nodes on the same subnet")
	createDBOptions.LargeCluster = newCmd.parser.Int("large-cluster", -1, "Enables a large cluster layout")
	createDBOptions.SpreadLogging = newCmd.parser.Bool("spread-logging", false, "Whether enable spread logging")
	createDBOptions.SpreadLoggingLevel = newCmd.parser.Int("spread-logging-level", -1, "Spread logging level")

	createDBOptions.ForceCleanupOnFailure = newCmd.parser.Bool("force-cleanup-on-failure", false,
		"Force removal of existing directories on failure of command")
	createDBOptions.ForceRemovalAtCreation = newCmd.parser.Bool("force-removal-at-creation", false,
		"Force removal of existing directories before creating the database")

	createDBOptions.SkipPackageInstall = newCmd.parser.Bool("skip-package-install", false,
		"Skip the installation of packages from /opt/vertica/packages.")

	createDBOptions.TimeoutNodeStartupSeconds = newCmd.parser.Int("startup-timeout", util.DefaultTimeoutSeconds,
		"Timeout for polling node start up state")

	// hidden options
	createDBOptions.ClientPort = newCmd.parser.Int("client-port", util.DefaultClientPort, "Create database with specified client port")
	createDBOptions.SkipStartupPolling = newCmd.parser.Bool("skip-startup-polling", false, "Skip polling node startup state")

	newCmd.createDBOptions = &createDBOptions

	return newCmd
}

func (c *CmdCreateDB) CommandType() string {
	return "create_db"
}

func (c *CmdCreateDB) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv

	if len(c.argv) == 0 {
		c.PrintUsage()
		return fmt.Errorf("zero args found, at least one argument expected")
	}
	parserError := c.parser.Parse(c.argv)
	if parserError != nil {
		return parserError
	}

	// handle options that are not passed in
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.createDBOptions.ConfigDirectory = nil
	}

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdCreateDB) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// parse raw host str input into a []string of createDBOptions
	err := c.parseHostList()
	if err != nil {
		return err
	}

	// check the format of config param string, and parse it into configParams
	err = c.parseConfigParams()
	if err != nil {
		return err
	}
	return c.createDBOptions.ValidateParseOptions()
}

// the hosts should be separated by comma, and will be converted to lower case
func (c *CmdCreateDB) parseHostList() error {
	inputHostList, err := util.SplitHosts(*c.hostListStr)
	if err != nil {
		return err
	}
	c.createDBOptions.RawHosts = inputHostList
	return nil
}

func (c *CmdCreateDB) parseConfigParams() error {
	if *c.configParamListStr == "" {
		return nil
	}
	configParamList := strings.Split(strings.TrimSpace(*c.configParamListStr), ",")
	// passed an empty string in --config-param
	if len(configParamList) == 0 {
		return nil
	}

	for _, param := range configParamList {
		// expected to see key value pairs of the format key=value
		keyValue := strings.Split(param, "=")
		if len(keyValue) != keyValueArrayLen {
			return fmt.Errorf("--config-param option must take NAME=VALUE as argument: %s is invalid", param)
		} else if len(keyValue) > 0 && strings.TrimSpace(keyValue[0]) == "" {
			return fmt.Errorf("--config-param option must take NAME=VALUE as argument with NAME being non-empty: %s is invalid", param)
		}
		key := strings.TrimSpace(keyValue[0])
		// it's possible we need empty string value for config parameter
		value := strings.TrimSpace(keyValue[1])
		c.createDBOptions.ConfigurationParameters[key] = value
	}
	return nil
}

func (c *CmdCreateDB) Analyze() error {
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdCreateDB) Run() error {
	vlog.LogInfoln("Called method Run()")
	vdb, createError := vclusterops.VCreateDatabase(c.createDBOptions)
	if createError != nil {
		return createError
	}
	vlog.LogPrintInfo("Created a database with name [%s]", vdb.Name)
	return nil
}

func (c *CmdCreateDB) PrintUsage() {
	thisCommand := c.CommandType()
	fmt.Fprintf(os.Stderr,
		"vcluster %s --name <db_name>\nExample: vcluster %s --name db1\n",
		thisCommand,
		thisCommand)
	c.parser.PrintDefaults()
}
