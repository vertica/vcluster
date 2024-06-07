/*
 (c) Copyright [2023-2024] Open Text.
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
	"github.com/spf13/viper"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdStartSubcluster
 *
 * Implements ClusterCommand interface
 */
type CmdStartSubcluster struct {
	startScOptions *vclusterops.VStartScOptions

	CmdBase
}

func makeCmdStartSubcluster() *cobra.Command {
	// CmdStartSubcluster
	newCmd := &CmdStartSubcluster{}
	opt := vclusterops.VStartScOptionsFactory()
	newCmd.startScOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		startSCSubCmd,
		"Start a subcluster",
		`This command starts a stopped subcluster in a running Eon database.

You must provide the subcluster name with the --subcluster option.

Examples:
  # Start a subcluster with config file
  vcluster start_subcluster --subcluster sc1 \
    --config /opt/vertica/config/vertica_cluster.yaml

  # Start a subcluster with user input
  vcluster start_subcluster --db-name test_db \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 --subcluster sc1
`,
		[]string{dbNameFlag, configFlag, hostsFlag, ipv6Flag, eonModeFlag, passwordFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// require name of subcluster to start
	markFlagsRequired(cmd, subclusterFlag)

	// hide eon mode flag since we expect it to come from config file, not from user input
	hideLocalFlags(cmd, []string{eonModeFlag})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdStartSubcluster) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.startScOptions.SCName,
		subclusterFlag,
		"",
		"Name of subcluster to start",
	)
	cmd.Flags().IntVar(
		&c.startScOptions.StatePollingTimeout,
		"timeout",
		util.DefaultTimeoutSeconds,
		"The timeout (in seconds) to wait for polling node state operation",
	)
}

func (c *CmdStartSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	// reset some options that are not included in user input
	c.ResetUserInputOptions(&c.startScOptions.DatabaseOptions)

	// start_subcluster only works for an Eon db so we assume the user always runs this subcommand
	// on an Eon db. When Eon mode cannot be found in config file, we set its value to true.
	if !viper.IsSet(eonModeKey) {
		c.startScOptions.IsEon = true
	}
	return c.validateParse(logger)
}

func (c *CmdStartSubcluster) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	if !c.usePassword() {
		err := c.getCertFilesFromCertPaths(&c.startScOptions.DatabaseOptions)
		if err != nil {
			return err
		}
	}

	err := c.ValidateParseBaseOptions(&c.startScOptions.DatabaseOptions)
	if err != nil {
		return nil
	}
	return c.setDBPassword(&c.startScOptions.DatabaseOptions)
}

func (c *CmdStartSubcluster) Analyze(_ vlog.Printer) error {
	return nil
}

func (c *CmdStartSubcluster) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	options := c.startScOptions

	err := vcc.VStartSubcluster(options)
	if err != nil {
		vcc.LogError(err, "fail to start subcluster")
		return err
	}

	vcc.DisplayInfo("Successfully started subcluster %s for database %s",
		options.SCName, options.DBName)

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdStartSubcluster
func (c *CmdStartSubcluster) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.startScOptions.DatabaseOptions = *opt
}
