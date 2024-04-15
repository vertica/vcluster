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

/* CmdDropDB
 *
 * Implements ClusterCommand interface
 */
type CmdDropDB struct {
	dropDBOptions *vclusterops.VDropDatabaseOptions

	CmdBase
}

func makeCmdDropDB() *cobra.Command {
	newCmd := &CmdDropDB{}
	opt := vclusterops.VDropDatabaseOptionsFactory()
	newCmd.dropDBOptions = &opt

	// VER-92345 update the long description about the hosts option
	cmd := makeBasicCobraCmd(
		newCmd,
		dropDBSubCmd,
		"Drop a database",
		`This subcommand drops a stopped database.

For an Eon database, communal storage is not deleted. You can recover 
the dropped database with revive_db.

The config file must be specified to retrieve host information. If the config
file path is not specified via --config, the default path will be used (refer
to create_db subcommand for information about how the default config file path 
is determined). When the command completes, the config file is removed.

To remove the local directories like catalog, depot, and data, you can use the 
--force-delete option. The data deleted with this option is unrecoverable.

Examples:
  # Drop a database with config file
  vcluster drop_db --db-name test_db \
    --config /opt/vertica/config/vertica_cluster.yaml
`,
		[]string{dbNameFlag, configFlag, hostsFlag, catalogPathFlag, dataPathFlag, depotPathFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	// hide flags since we expect it to come from config file, not from user input
	hideLocalFlags(cmd, []string{hostsFlag, catalogPathFlag, dataPathFlag, depotPathFlag})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdDropDB) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		&c.dropDBOptions.ForceDelete,
		"force-delete",
		false,
		"Delete local directories like catalog, depot, and data.",
	)
}

func (c *CmdDropDB) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	return c.validateParse(logger)
}

func (c *CmdDropDB) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")
	err := c.getCertFilesFromCertPaths(&c.dropDBOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.ValidateParseBaseOptions(&c.dropDBOptions.DatabaseOptions)
}

func (c *CmdDropDB) Run(vcc vclusterops.ClusterCommands) error {
	vcc.V(1).Info("Called method Run()")

	err := vcc.VDropDatabase(c.dropDBOptions)
	if err != nil {
		vcc.LogError(err, "failed do drop the database")
		return err
	}

	vcc.PrintInfo("Successfully dropped database %s", c.dropDBOptions.DBName)
	// if the database is successfully dropped, the config file will be removed
	// if failed to remove it, we will ask users to manually do it
	err = removeConfig(vcc.GetLog())
	if err != nil {
		vcc.PrintWarning("Fail to remove config file %q, "+
			"please manually do it. Details: %v", c.dropDBOptions.ConfigPath, err)
	}
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdDropDB
func (c *CmdDropDB) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.dropDBOptions.DatabaseOptions = *opt
}
