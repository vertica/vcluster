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
	"fmt"
	"strconv"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStopDB
 *
 * Parses arguments to stopDB and calls
 * the high-level function for stopDB.
 *
 * Implements ClusterCommand interface
 */

type CmdStopDB struct {
	CmdBase
	stopDBOptions *vclusterops.VStopDatabaseOptions
}

func makeCmdStopDB() *cobra.Command {
	newCmd := &CmdStopDB{}
	newCmd.isEon = new(bool)
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VStopDatabaseOptionsFactory()
	newCmd.stopDBOptions = &opt
	newCmd.stopDBOptions.DrainSeconds = new(int)

	cmd := makeBasicCobraCmd(
		newCmd,
		"stop_db",
		"Stop a database",
		`This stops a database or sandbox on a given set of hosts.

The name of the database must be provided.

Example:
  vcluster stop_db --db-name <db_name> --hosts <any_hosts_of_the_db> --password <password> --config <config_file>
`,
	)

	// common db flags
	setCommonFlags(cmd, []string{"db-name", "password", "hosts", "honor-user-input", "config", "log-path"})

	// local flags
	newCmd.setLocalFlags(cmd)

	// check if hidden flags can be implemented/removed in VER-92259
	// hidden flags
	newCmd.setHiddenFlags(cmd)

	// require db-name, and one of (--honor-user-input, --config)
	cmd.PreRun = func(c *cobra.Command, args []string) {
		markFlagsRequired(c, []string{"db-name"})
		requireHonorUserInputOrConfDir(c)
	}

	// require the flags to be file name
	markFlagsFileName(cmd, map[string][]string{"config": {"yaml"}, "log-path": {"log"}})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdStopDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		c.isEon,
		"eon-mode",
		false,
		util.GetEonFlagMsg("indicate if the database is an Eon db."+
			" Use it when you do not trust "+vclusterops.ConfigFileName),
	)
	cmd.Flags().IntVar(
		c.stopDBOptions.DrainSeconds,
		"drain-seconds",
		util.DefaultDrainSeconds,
		util.GetEonFlagMsg("seconds to wait for user connections to close."+
			" Default value is "+strconv.Itoa(util.DefaultDrainSeconds)+" seconds."+
			" When the time expires, connections will be forcibly closed and the db will shut down"),
	)
	cmd.Flags().StringVar(
		c.stopDBOptions.Sandbox,
		"sandbox",
		"",
		util.GetOptionalFlagMsg("Name of the sandbox to stop"),
	)
	cmd.Flags().BoolVar(
		c.stopDBOptions.MainCluster,
		"main-cluster-only",
		false,
		util.GetOptionalFlagMsg("Stop the database, but don't stop any of the sandboxes"),
	)
}

// setLocalFlags will set the hidden flags the command has.
// These hidden flags will not be shown in help and usage of the command, and they will be used internally.
func (c *CmdStopDB) setHiddenFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		c.stopDBOptions.CheckUserConn,
		"if-no-users",
		false,
		"",
	)
	cmd.Flags().BoolVar(
		c.stopDBOptions.ForceKill,
		"force-kill",
		false,
		"",
	)
	hideLocalFlags(cmd, []string{"if-no-users", "force-kill"})
}

func (c *CmdStopDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !c.parser.Changed("password") {
		c.stopDBOptions.Password = nil
	}
	if !c.parser.Changed("eon-mode") {
		c.CmdBase.isEon = nil
	}
	if !c.parser.Changed("ipv6") {
		c.CmdBase.ipv6 = nil
	}
	if !c.parser.Changed("drain-seconds") {
		c.stopDBOptions.DrainSeconds = nil
	}
	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdStopDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	return c.ValidateParseBaseOptions(&c.stopDBOptions.DatabaseOptions)
}

func (c *CmdStopDB) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.Info("Called method Run()")

	options := c.stopDBOptions

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	err = vcc.VStopDatabase(options)
	if err != nil {
		vcc.Log.Error(err, "failed to stop the database")
		return err
	}
	msg := fmt.Sprintf("Stopped a database with name %s", *options.DBName)
	if *options.Sandbox != "" {
		sandboxMsg := fmt.Sprintf(" on sandbox %s", *options.Sandbox)
		vcc.Log.PrintInfo(msg + sandboxMsg)
		return nil
	}
	if *options.MainCluster {
		stopMsg := " on main cluster"
		vcc.Log.PrintInfo(msg + stopMsg)
		return nil
	}
	vcc.Log.PrintInfo(msg)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdStopDB
func (c *CmdStopDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.stopDBOptions.DatabaseOptions = *opt
}
