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
	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdAddSubcluster
 *
 * Parses arguments to addSubcluster and calls
 * the high-level function for addSubcluster.
 *
 * Implements ClusterCommand interface
 */

type CmdAddSubcluster struct {
	CmdBase
	addSubclusterOptions *vclusterops.VAddSubclusterOptions
	scHostListStr        string
}

func makeCmdAddSubcluster() *cobra.Command {
	// CmdAddSubcluster
	newCmd := &CmdAddSubcluster{}
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VAddSubclusterOptionsFactory()
	newCmd.addSubclusterOptions = &opt

	cmd := OldMakeBasicCobraCmd(
		newCmd,
		"db_add_subcluster",
		"Add a subcluster",
		`This subcommand adds a new subcluster to an existing Eon Mode database.

You must provide a subcluster name with the --subcluster option.

By default, the new subcluster is secondary. To add a primary subcluster, use
the --is-primary flag.

Examples:
  # Add a subcluster with config file
  vcluster db_add_subcluster --subcluster sc1 --config \
  /opt/vertica/config/vertica_cluster.yaml --is-primary --control-set-size 1

  # Add a subcluster with user input
  vcluster db_add_subcluster --subcluster sc1 --db-name test_db \
  --hosts 10.20.30.40,10.20.30.41,10.20.30.42 --is-primary --control-set-size -1
`,
	)

	// common db flags
	newCmd.setCommonFlags(cmd, []string{dbNameFlag, configFlag, hostsFlag, passwordFlag})

	// local flags
	newCmd.setLocalFlags(cmd)

	// check if hidden flags can be implemented/removed in VER-92259
	// hidden flags
	newCmd.setHiddenFlags(cmd)

	// require name of subcluster to add
	markFlagsRequired(cmd, []string{"subcluster"})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdAddSubcluster) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		c.addSubclusterOptions.SCName,
		"subcluster",
		"",
		"The name of the new subcluster",
	)
	cmd.Flags().BoolVar(
		c.addSubclusterOptions.IsPrimary,
		"is-primary",
		false,
		"The new subcluster will be a primary subcluster",
	)
	cmd.Flags().IntVar(
		c.addSubclusterOptions.ControlSetSize,
		"control-set-size",
		vclusterops.ControlSetSizeDefaultValue,
		"The number of nodes that will run spread within the subcluster",
	)
}

// setHiddenFlags will set the hidden flags the command has.
// These hidden flags will not be shown in help and usage of the command, and they will be used internally.
func (c *CmdAddSubcluster) setHiddenFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.scHostListStr,
		"sc-hosts",
		"",
		"",
	)
	cmd.Flags().StringVar(
		c.addSubclusterOptions.CloneSC,
		"like",
		"",
		"",
	)
	hideLocalFlags(cmd, []string{"sc-hosts", "like"})
}

func (c *CmdAddSubcluster) CommandType() string {
	return "db_add_subcluster"
}

func (c *CmdAddSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)
	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdAddSubcluster) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	err := c.ValidateParseBaseOptions(&c.addSubclusterOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.addSubclusterOptions.DatabaseOptions)
}

func (c *CmdAddSubcluster) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")
	return nil
}

func (c *CmdAddSubcluster) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.addSubclusterOptions

	// get config from vertica_cluster.yaml
	config, err := options.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	options.Config = config

	err = vcc.VAddSubcluster(options)
	if err != nil {
		vcc.LogError(err, "failed to add subcluster")
		return err
	}

	vcc.PrintInfo("Added subcluster %s to database %s", *options.SCName, *options.DBName)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdAddSubcluster
func (c *CmdAddSubcluster) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.addSubclusterOptions.DatabaseOptions = *opt
}
