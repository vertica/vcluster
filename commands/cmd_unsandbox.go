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

/* CmdUnsandbox
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for Unsandboxing operation.
 * Prepares the inputs for the library.
 *
 */
type CmdUnsandboxSubcluster struct {
	CmdBase
	usOptions vclusterops.VUnsandboxOptions
}

func (c *CmdUnsandboxSubcluster) TypeName() string {
	return "CmdUnsandboxSubcluster"
}

func makeCmdUnsandboxSubcluster() *cobra.Command {
	// CmdUnsandboxSubcluster
	newCmd := &CmdUnsandboxSubcluster{}
	opt := vclusterops.VUnsandboxOptionsFactory()
	newCmd.usOptions = opt

	cmd := makeBasicCobraCmd(
		newCmd,
		unsandboxSubCmd,
		"Unsandbox a subcluster",
		`This subcommand unsandboxes a subcluster in an existing Eon Mode database.

When you unsandbox a subcluster, its hosts shut down and restart as part of the
main cluster.

When all subclusters are removed from a sandbox, the sandbox catalog and
metadata are deleted. To reuse the sandbox name, you must manually clean the 
/metadata/<sandbox-name> directory in your communal storage location.
To reuse the sandbox name, you must manually clean the /metadata/<sandbox-name>
directory in your communal storage location.

The comma-separated list of hosts passed to the --hosts option must include at
least one up host in the main cluster.

You must provide the subcluster name with the --subcluster option.

Examples:
  # Unsandbox a subcluster with config file
  vcluster unsandbox_subcluster --subcluster sc1 \
    --config /opt/vertica/config/vertica_cluster.yaml

  # Unsandbox a subcluster with user input
  vcluster unsandbox_subcluster --subcluster sc1 \
    --hosts 10.20.30.40,10.20.30.41,10.20.30.42 --db-name test_db
`,
		[]string{dbNameFlag, configFlag, passwordFlag, hostsFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// require name of subcluster to unsandbox
	markFlagsRequired(cmd, []string{subclusterFlag})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdUnsandboxSubcluster) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.usOptions.SCName,
		subclusterFlag,
		"",
		"The name of the subcluster to be unsandboxed",
	)
}

func (c *CmdUnsandboxSubcluster) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	return c.parseInternal(logger)
}

// ParseInternal parses internal commands for unsandboxed subclusters.
func (c *CmdUnsandboxSubcluster) parseInternal(logger vlog.Printer) error {
	logger.Info("Called parseInternal()")

	err := c.getCertFilesFromCertPaths(&c.usOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.ValidateParseBaseOptions(&c.usOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.setDBPassword(&c.usOptions.DatabaseOptions)
}

func (c *CmdUnsandboxSubcluster) Analyze(logger vlog.Printer) error {
	logger.Info("Called method Analyze()")

	return nil
}

func (c *CmdUnsandboxSubcluster) Run(vcc vclusterops.ClusterCommands) error {
	vcc.PrintInfo("Running unsandbox subcluster")
	vcc.LogInfo("Calling method Run() for command " + unsandboxSubCmd)

	options := c.usOptions

	err := vcc.VUnsandbox(&options)
	vcc.PrintInfo("Completed method Run() for command " + unsandboxSubCmd)
	return err
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdUnsandboxSubcluster
func (c *CmdUnsandboxSubcluster) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.usOptions.DatabaseOptions = *opt
}
