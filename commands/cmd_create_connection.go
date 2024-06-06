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

/* CmdCreateConnection
 *
 * Implements ClusterCommand interface
 */
type CmdCreateConnection struct {
	connectionOptions *vclusterops.VReplicationDatabaseOptions
	CmdBase
}

func makeCmdCreateConnection() *cobra.Command {
	newCmd := &CmdCreateConnection{}
	opt := vclusterops.VReplicationDatabaseFactory()
	newCmd.connectionOptions = &opt
	opt.TargetPassword = new(string)

	cmd := makeBasicCobraCmd(
		newCmd,
		createConnectionSubCmd,
		"create the content of the connection file",
		`This command is used to create the content of the connection file. 

You must specify the database name and host list. If the database has a 
password, you need to provide password. If the database uses 
trust authentication, the password can be ignored.

Examples:
  # create the connection file to /tmp/vertica_connection.yaml
  vcluster create_connection --db-name platform_test_db --hosts 10.20.30.43 --db-user \ 
    dkr_dbadmin --password-file /tmp/password.txt --conn /tmp/vertica_connection.yaml
`,
		[]string{connFlag},
	)

	// local flags
	newCmd.setLocalFlags(cmd)

	markFlagsRequired(cmd, dbNameFlag, hostsFlag, connFlag)
	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdCreateConnection) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.connectionOptions.TargetDB,
		dbNameFlag,
		"",
		"The name of the database",
	)
	cmd.Flags().StringSliceVar(
		&c.connectionOptions.TargetHosts,
		hostsFlag,
		[]string{},
		"Comma-separated list of hosts in database")
	cmd.Flags().StringVar(
		&c.connectionOptions.TargetUserName,
		dbUserFlag,
		"",
		"The username for connecting to the database",
	)
	//  password flags
	cmd.Flags().StringVar(
		c.connectionOptions.TargetPassword,
		passwordFileFlag,
		"",
		"Path to the file to read the password from. ",
	)
	cmd.Flags().StringVar(
		&globals.connFile,
		connFlag,
		"",
		"Path to the connection file")
	markFlagsFileName(cmd, map[string][]string{connFlag: {"yaml"}})
}

func (c *CmdCreateConnection) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogMaskedArgParse(c.argv)

	return nil
}

func (c *CmdCreateConnection) Run(vcc vclusterops.ClusterCommands) error {
	vcc.LogInfo("Called method Run()")

	// write target db info to vcluster connection file
	err := writeConn(c.connectionOptions)
	if err != nil {
		return fmt.Errorf("fail to write connection file, details: %s", err)
	}
	vcc.DisplayInfo("Successfully wrote the connection file in %s", globals.connFile)
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance
func (c *CmdCreateConnection) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.connectionOptions.DatabaseOptions = *opt
}
