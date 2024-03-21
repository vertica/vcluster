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
		configSubCmd,
		"Show the content of the config file",
		`This subcommand prints or recovers the content of the config file.

When recovering a config file, you must provide the all hosts that participate 
in this database.

If there is an existing file at the provided config file location, the recover
function will not create a new config file, unless you specify
--overwrite explicitly.

Examples:
  # Show the cluster config file in the default location
  vcluster config --show

  # Show the contents of the config file at /tmp/vertica_cluster.yaml
  vcluster config --show --config /tmp/vertica_cluster.yaml

  # Recover the config file to the default location
  vcluster config --recover --db-name test_db \
    --hosts 10.20.30.41,10.20.30.42,10.20.30.43 \
    --catalog-path /data --data-path /data --depot-path /data

  # Recover the config file to /tmp/vertica_cluster.yaml
  vcluster config --recover --db-name test_db \
    --hosts 10.20.30.41,10.20.30.42,10.20.30.43 \
    --catalog-path /data --data-path /data --depot-path /data \
    --config /tmp/vertica_cluster.yaml
`,
		[]string{configFlag},
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
