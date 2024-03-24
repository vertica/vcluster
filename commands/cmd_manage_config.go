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
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdManageConfig
 *
 * A subcommand managing the YAML config file
 * in the default or a specified directory.
 *
 * Implements ClusterCommand interface
 */
type CmdManageConfig struct {
	sOptions vclusterops.DatabaseOptions
	cobraCmd *cobra.Command
	CmdBase
}

func makeCmdManageConfig() *cobra.Command {
	newCmd := &CmdManageConfig{}

	cmd := makeBasicCobraCmd(
		newCmd,
		manageConfigSubCmd,
		"Show or recover the content of the config file",
		`This subcommand is used to print or recover the content of the config file.`,
		[]string{configFlag},
	)

	// VER-92676: move some of the descriptions to the local flags
	// e.g., --hosts

	cmd.AddCommand(makeCmdConfigShow())
	cmd.AddCommand(makeCmdConfigRecover())

	// this allows `vcluster manage_config` output the help info
	newCmd.cobraCmd = cmd

	return cmd
}

func (c *CmdManageConfig) Parse(_ []string, _ vlog.Printer) error {
	return nil
}

func (c *CmdManageConfig) Run(_ vclusterops.ClusterCommands) error {
	if c.cobraCmd != nil {
		return c.cobraCmd.Help()
	}

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdManageConfig
func (c *CmdManageConfig) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.sOptions = *opt
}
