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
	"strconv"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStartDB
 *
 * Implements ClusterCommand interface
 */
type CmdStartDB struct {
	CmdBase
	startDBOptions *vclusterops.VStartDatabaseOptions

	Force               *bool   // force cleanup to start the database
	AllowFallbackKeygen *bool   // Generate spread encryption key from Vertica. Use under support guidance only
	IgnoreClusterLease  *bool   // ignore the cluster lease in communal storage
	Unsafe              *bool   // Start database unsafely, skipping recovery.
	Fast                *bool   // Attempt fast startup database
	configurationParams *string // raw input from user, need further processing
}

func makeCmdStartDB() *CmdStartDB {
	// CmdStartDB
	newCmd := &CmdStartDB{}

	// parser, used to parse command-line flags
	newCmd.oldParser = flag.NewFlagSet("start_db", flag.ExitOnError)
	startDBOptions := vclusterops.VStartDatabaseOptionsFactory()

	// require flags
	startDBOptions.DBName = newCmd.oldParser.String("db-name", "", util.GetOptionalFlagMsg("The name of the database to be started."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))

	// optional flags
	startDBOptions.Password = newCmd.oldParser.String("password", "", util.GetOptionalFlagMsg("Database password in single quotes"))
	startDBOptions.CatalogPrefix = newCmd.oldParser.String("catalog-path", "", "The catalog path of the database")
	newCmd.hostListStr = newCmd.oldParser.String("hosts", "", util.GetOptionalFlagMsg(
		"Comma-separated list of hosts to participate in database."+" Use it when you do not trust "+vclusterops.ConfigFileName))
	newCmd.ipv6 = newCmd.oldParser.Bool("ipv6", false, "start database with with IPv6 hosts")

	startDBOptions.HonorUserInput = newCmd.oldParser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))
	newCmd.oldParser.StringVar(&startDBOptions.ConfigPath, "config", "", util.GetOptionalFlagMsg("Path to the config file"))
	startDBOptions.StatePollingTimeout = newCmd.oldParser.Int("timeout", util.DefaultTimeoutSeconds,
		util.GetOptionalFlagMsg("Set a timeout (in seconds) for polling node state operation, default timeout is "+
			strconv.Itoa(util.DefaultTimeoutSeconds)+"seconds"))
	// eon flags
	newCmd.isEon = newCmd.oldParser.Bool("eon-mode", false, util.GetEonFlagMsg("Indicate if the database is an Eon database."+
		" Use it when you do not trust "+vclusterops.ConfigFileName))
	startDBOptions.CommunalStorageLocation = newCmd.oldParser.String("communal-storage-location", "",
		util.GetEonFlagMsg("Location of communal storage"))
	newCmd.configurationParams = newCmd.oldParser.String("config-param", "", util.GetOptionalFlagMsg(
		"Comma-separated list of NAME=VALUE pairs for configuration parameters"))

	// hidden options
	// TODO: the following options will be processed later
	newCmd.Unsafe = newCmd.oldParser.Bool("unsafe", false, util.SuppressHelp)
	newCmd.Force = newCmd.oldParser.Bool("force", false, util.SuppressHelp)
	newCmd.AllowFallbackKeygen = newCmd.oldParser.Bool("allow_fallback_keygen", false, util.SuppressHelp)
	newCmd.IgnoreClusterLease = newCmd.oldParser.Bool("ignore_cluster_lease", false, util.SuppressHelp)
	newCmd.Fast = newCmd.oldParser.Bool("fast", false, util.SuppressHelp)
	startDBOptions.TrimHostList = newCmd.oldParser.Bool("trim-hosts", false, util.SuppressHelp)

	newCmd.startDBOptions = &startDBOptions
	newCmd.oldParser.Usage = func() {
		util.SetParserUsage(newCmd.oldParser, "start_db")
	}
	return newCmd
}

func (c *CmdStartDB) CommandType() string {
	return "start_db"
}

func (c *CmdStartDB) Parse(inputArgv []string, logger vlog.Printer) error {
	if c.oldParser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType(), logger)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.oldParser, "eon-mode") {
		c.CmdBase.isEon = nil
	}

	if !util.IsOptionSet(c.oldParser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}

	return c.validateParse(logger)
}

func (c *CmdStartDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()", "command", c.CommandType())

	// check the format of configuration params string, and parse it into configParams
	configurationParams, err := util.ParseConfigParams(*c.configurationParams)
	if err != nil {
		return err
	}
	if configurationParams != nil {
		c.startDBOptions.ConfigurationParameters = configurationParams
	}

	return c.OldValidateParseBaseOptions(&c.startDBOptions.DatabaseOptions)
}

func (c *CmdStartDB) Analyze(logger vlog.Printer) error {
	// Analyze() is needed to fulfill an interface
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdStartDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.V(1).Info("Called method Run()")

	options := c.startDBOptions

	// load vdb info from the YAML config file
	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	err = vcc.VStartDatabase(options)
	if err != nil {
		vcc.Log.Error(err, "failed to start the database")
		return err
	}

	vcc.Log.PrintInfo("Successfully start the database %s\n", *options.DBName)
	return nil
}
