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
	"fmt"

	"github.com/spf13/cobra"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdConfigRecover
 *
 * A subcommand recovering the YAML config file
 * in the default or a specified location.
 *
 * Implements ClusterCommand interface
 */
type CmdConfigRecover struct {
	recoverConfigOptions *vclusterops.VFetchCoordinationDatabaseOptions
	CmdBase
}

func makeCmdConfigRecover() *cobra.Command {
	newCmd := &CmdConfigRecover{}
	opt := vclusterops.VRecoverConfigOptionsFactory()
	newCmd.recoverConfigOptions = &opt

	cmd := makeBasicCobraCmd(
		newCmd,
		configRecoverSubCmd,
		"recover the content of the config file",
		`This subcommand is used to recover the content of the config file.

You must provide all the hosts that participate in the database.
db-name and password are required fields in case of running db. In case the password 
is wrong or not provided, config info is recovered from the catalog editor.
For accurate sandbox information recovery, the database needs to be running.

If there is an existing file at the provided config file location, the recover function
will not create a new config file unless you explicitly specify --overwrite.

Examples:
  # Recover the config file to the default location
  vcluster manage_config recover --db-name test_db \
	--hosts 10.20.30.41,10.20.30.42,10.20.30.43 \
	--catalog-path /data --depot-path /data --password ""

  # Recover the config file to /tmp/vertica_cluster.yaml
  vcluster manage_config recover --db-name test_db \
	--hosts 10.20.30.41,10.20.30.42,10.20.30.43 \
	--catalog-path /data --depot-path /data \
	--config /tmp/vertica_cluster.yaml --password ""
`,
		[]string{dbNameFlag, hostsFlag, catalogPathFlag, depotPathFlag, ipv6Flag, configFlag, passwordFlag},
	)

	// require db-name, hosts, catalog-path, and data-path
	markFlagsRequired(cmd, dbNameFlag, hostsFlag, catalogPathFlag)

	// local flags
	newCmd.setLocalFlags(cmd)

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdConfigRecover) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		&c.recoverConfigOptions.Overwrite,
		"overwrite",
		false,
		"overwrite the existing config file",
	)
	cmd.Flags().BoolVar(
		&c.recoverConfigOptions.AfterRevive,
		"after-revive",
		false,
		"whether recover config file right after reviving a database",
	)
}

func (c *CmdConfigRecover) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	return c.validateParse(logger)
}

// all validations of the arguments should go in here
func (c *CmdConfigRecover) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	err := c.ValidateParseBaseOptions(&c.recoverConfigOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	return c.setDBPassword(&c.recoverConfigOptions.DatabaseOptions)
}

func (c *CmdConfigRecover) Run(vcc vclusterops.ClusterCommands) error {
	vdb, err := vcc.VFetchCoordinationDatabase(c.recoverConfigOptions)
	if err != nil {
		vcc.LogError(err, "fail to recover the config file")
		return err
	}
	// write db info to vcluster config file
	vdb.FirstStartAfterRevive = c.recoverConfigOptions.AfterRevive
	err = writeConfig(&vdb, true /*forceOverwrite*/)
	if err != nil {
		return fmt.Errorf("fail to write config file, details: %s", err)
	}
	vcc.DisplayInfo("Successfully recovered config file for database %s at %s", vdb.Name,
		c.recoverConfigOptions.ConfigPath)

	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance
func (c *CmdConfigRecover) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.recoverConfigOptions.DatabaseOptions = *opt
}
