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
	opt := vclusterops.VStopDatabaseOptionsFactory()
	newCmd.stopDBOptions = &opt
	newCmd.stopDBOptions.DrainSeconds = new(int)

	cmd := makeBasicCobraCmd(
		newCmd,
		stopDBSubCmd,
		"Stop a database",
		`This subcommand stops a database or sandbox.

Examples:
  vcluster stop_db --password <password> --config <config_file>
`,
		[]string{dbNameFlag, hostsFlag, ipv6Flag, eonModeFlag, configFlag, passwordFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// check if hidden flags can be implemented/removed in VER-92259
	// hidden flags
	newCmd.setHiddenFlags(cmd)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdStopDB) setLocalFlags(cmd *cobra.Command) {
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
		"Name of the sandbox to stop",
	)
	cmd.Flags().BoolVar(
		c.stopDBOptions.MainCluster,
		"main-cluster-only",
		false,
		"Stop the database, but don't stop any of the sandboxes",
	)
}

// setHiddenFlags will set the hidden flags the command has.
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
	c.OldResetUserInputOptions()

	if !c.parser.Changed("drain-seconds") {
		c.stopDBOptions.DrainSeconds = nil
	}
	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdStopDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	err := c.ValidateParseBaseOptions(&c.stopDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.stopDBOptions.DatabaseOptions)
}

func (c *CmdStopDB) Run(vcc vclusterops.ClusterCommands) error {
	vcc.LogInfo("Called method Run()")

	options := c.stopDBOptions

	err := vcc.VStopDatabase(options)
	if err != nil {
		vcc.LogError(err, "failed to stop the database")
		return err
	}
	msg := fmt.Sprintf("Stopped a database with name %s", *options.DBName)
	if *options.Sandbox != "" {
		sandboxMsg := fmt.Sprintf(" on sandbox %s", *options.Sandbox)
		vcc.PrintInfo(msg + sandboxMsg)
		return nil
	}
	if *options.MainCluster {
		stopMsg := " on main cluster"
		vcc.PrintInfo(msg + stopMsg)
		return nil
	}
	vcc.PrintInfo(msg)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdStopDB
func (c *CmdStopDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.stopDBOptions.DatabaseOptions = *opt
}
