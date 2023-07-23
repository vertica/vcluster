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

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"github.com/vertica/vcluster/vclusterops/vstruct"
)

/* CmdCreateDB
 *
 * Parses arguments to createDB and calls
 * the high-level function for createDB.
 *
 * Implements ClusterCommand interface
 */

type CmdCreateDB struct {
	createDBOptions    *vclusterops.VCreateDatabaseOptions
	configParamListStr *string // raw input from user, need further processing

	CmdBase
}

func MakeCmdCreateDB() *CmdCreateDB {
	// CmdCreateDB
	newCmd := &CmdCreateDB{}

	// parser, used to parse command-line flags
	newCmd.parser = flag.NewFlagSet("create_db", flag.ExitOnError)
	createDBOptions := vclusterops.VCreateDatabaseOptionsFactory()

	// required flags
	createDBOptions.Name = newCmd.parser.String("name", "", "The name of the database to be created")
	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated list of hosts to participate in database")
	createDBOptions.CatalogPrefix = newCmd.parser.String("catalog-path", "", "Path of catalog directory")
	createDBOptions.DataPrefix = newCmd.parser.String("data-path", "", "Path of data directory")

	// optional
	createDBOptions.Password = newCmd.parser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	createDBOptions.LicensePathOnNode = newCmd.parser.String("license", "", util.GetOptionalFlagMsg("Database license"))
	createDBOptions.Policy = newCmd.parser.String("policy", util.DefaultRestartPolicy,
		util.GetOptionalFlagMsg("Restart policy of the database"))
	createDBOptions.SQLFile = newCmd.parser.String("sql", "",
		util.GetOptionalFlagMsg("SQL file to run (as dbadmin) immediately on database creation"))
	createDBOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))
	createDBOptions.LogPath = newCmd.parser.String("log-path", "", "Directory where the log file will be generated")

	// Eon flags
	createDBOptions.CommunalStorageLocation = newCmd.parser.String("communal-storage-location", "",
		util.GetEonFlagMsg("Location of communal storage"))
	createDBOptions.ShardCount = newCmd.parser.Int("shard-count", 0, util.GetEonFlagMsg("Number of shards in the database"))
	createDBOptions.CommunalStorageParamsPath = newCmd.parser.String("communal_storage-params", "",
		util.GetEonFlagMsg("Location of communal storage parameter file"))
	createDBOptions.DepotPrefix = newCmd.parser.String("depot-path", "", util.GetEonFlagMsg("Path to depot directory"))
	createDBOptions.DepotSize = newCmd.parser.String("depot-size", "", util.GetEonFlagMsg("Size of depot"))
	createDBOptions.GetAwsCredentialsFromEnv = newCmd.parser.Bool("get-aws-credentials-from-env-vars", false,
		util.GetEonFlagMsg("Read AWS credentials from environment variables"))

	newCmd.configParamListStr = newCmd.parser.String("config-param", "", util.GetOptionalFlagMsg(
		"Comma-separated list of NAME=VALUE pairs for setting database configuration parametesr immediately on database creation"))

	// new flags comparing to adminTools create_db
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Create database with IPv6 hosts"))
	// by default use pt2pt mode
	createDBOptions.P2p = newCmd.parser.Bool("point-to-point", true,
		util.GetOptionalFlagMsg("Configure Spread to use point-to-point communication between all Vertica nodes"))
	createDBOptions.Broadcast = newCmd.parser.Bool("broadcast", false,
		util.GetOptionalFlagMsg("Configure Spread to use UDP broadcast traffic between nodes on the same subnet"))
	createDBOptions.LargeCluster = newCmd.parser.Int("large-cluster", -1, util.GetOptionalFlagMsg("Enables a large cluster layout"))
	createDBOptions.SpreadLogging = newCmd.parser.Bool("spread-logging", false, util.GetOptionalFlagMsg("Whether enable spread logging"))
	createDBOptions.SpreadLoggingLevel = newCmd.parser.Int("spread-logging-level", -1, util.GetOptionalFlagMsg("Spread logging level"))

	createDBOptions.ForceCleanupOnFailure = newCmd.parser.Bool("force-cleanup-on-failure", false,
		util.GetOptionalFlagMsg("Force removal of existing directories on failure of command"))
	createDBOptions.ForceRemovalAtCreation = newCmd.parser.Bool("force-removal-at-creation", false,
		util.GetOptionalFlagMsg("Force removal of existing directories before creating the database"))

	createDBOptions.SkipPackageInstall = newCmd.parser.Bool("skip-package-install", false,
		util.GetOptionalFlagMsg("Skip the installation of packages from /opt/vertica/packages."))

	createDBOptions.TimeoutNodeStartupSeconds = newCmd.parser.Int("startup-timeout", util.DefaultTimeoutSeconds,
		util.GetOptionalFlagMsg("Timeout for polling node start up state"))

	// hidden options
	createDBOptions.ClientPort = newCmd.parser.Int("client-port", util.DefaultClientPort, util.SuppressHelp)
	createDBOptions.SkipStartupPolling = newCmd.parser.Bool("skip-startup-polling", false, util.SuppressHelp)

	newCmd.createDBOptions = &createDBOptions

	newCmd.parser.Usage = func() {
		util.SetParserUsage(newCmd.parser, "create_db")
	}

	return newCmd
}

func (c *CmdCreateDB) CommandType() string {
	return "create_db"
}

func (c *CmdCreateDB) Parse(inputArgv []string) error {
	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	// handle options that are not passed in
	if !util.IsOptionSet(c.parser, "log-path") {
		c.createDBOptions.LogPath = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.createDBOptions.ConfigDirectory = nil
	}
	if !util.IsOptionSet(c.parser, "password") {
		c.createDBOptions.Password = nil
	}

	if util.IsOptionSet(c.parser, "depot-path") {
		c.createDBOptions.IsEon = vstruct.True
	} else {
		c.createDBOptions.IsEon = vstruct.False
	}

	return c.validateParse()
}

// all validations of the arguments should go in here
func (c *CmdCreateDB) validateParse() error {
	vlog.LogInfoln("Called validateParse()")

	// parse raw host str input into a []string of createDBOptions
	err := c.createDBOptions.ParseHostList(*c.hostListStr)
	if err != nil {
		return err
	}

	// parse Ipv6
	c.createDBOptions.Ipv6.FromBoolPointer(c.CmdBase.ipv6)

	// check the format of config param string, and parse it into configParams
	configParams, err := util.ParseConfigParams(*c.configParamListStr)
	if err != nil {
		return err
	}
	if configParams != nil {
		c.createDBOptions.ConfigurationParameters = configParams
	}
	return nil
}

func (c *CmdCreateDB) Analyze() error {
	vlog.LogInfoln("Called method Analyze()")
	return nil
}

func (c *CmdCreateDB) Run() error {
	vlog.LogInfo("[%s] Called method Run()", c.CommandType())
	vcc := vclusterops.VClusterCommands{}
	vdb, createError := vcc.VCreateDatabase(c.createDBOptions)
	if createError != nil {
		return createError
	}
	// write cluster information to the YAML config file
	err := vclusterops.WriteClusterConfig(&vdb, c.createDBOptions.ConfigDirectory)
	if err != nil {
		vlog.LogPrintWarning("fail to write config file, details: %s", err)
	}
	vlog.LogPrintInfo("Created a database with name [%s]", vdb.Name)
	return nil
}
