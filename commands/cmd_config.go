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
	"os"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdConfig
 *
 * A command managing the YAML config file "vertica_cluster.yaml"
 * under the current or a specified directory.
 *
 * Implements ClusterCommand interface
 */
type CmdConfig struct {
	show     bool
	sOptions vclusterops.DatabaseOptions
	CmdBase
}

func makeCmdConfig() *cobra.Command {
	newCmd := &CmdConfig{}
	cmd := makeBasicCobraCmd(
		newCmd,
		"config",
		"Show the content of the config file",
		`This subcommand is used to print the content of the config file.

Examples:
  # show the vertica_cluster.yaml file in the default location
  vcluster config --show

  # show the contents of the config file at /tmp/vertica_cluster.yaml
  vcluster config --show --config /tmp/vertica_cluster.yaml
`,
		[]string{"config"},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// require show
	markFlagsRequired(cmd, []string{"show"})
	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdConfig) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		&c.show,
		"show",
		false,
		"show the content of the config file",
	)
}

func (c *CmdConfig) CommandType() string {
	return "config"
}

func (c *CmdConfig) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	return nil
}

func (c *CmdConfig) Run(_ vclusterops.ClusterCommands) error {
	fileBytes, err := os.ReadFile(dbOptions.ConfigPath)
	if err != nil {
		return fmt.Errorf("fail to read config file, details: %w", err)
	}
	fmt.Printf("%s", string(fileBytes))
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdConfig
func (c *CmdConfig) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.sOptions = *opt
}
