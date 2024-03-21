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

/* CmdListAllNodes
 *
 * Implements ClusterCommand interface
 */
type CmdReIP struct {
	reIPOptions  *vclusterops.VReIPOptions
	reIPFilePath string

	CmdBase
}

func makeCmdReIP() *cobra.Command {
	newCmd := &CmdReIP{}
	newCmd.ipv6 = new(bool)
	opt := vclusterops.VReIPFactory()
	newCmd.reIPOptions = &opt

	// VER-91801: the examle below needs to be updated
	// as we need a better example with config file
	cmd := OldMakeBasicCobraCmd(
		newCmd,
		reIPSubCmd,
		"Re-ip database nodes",
		`This subcommand alters the IP addresses of database nodes in the catalog.

The database must be offline when running this command. If an IP change 
is required and the database is up, you can use restart_node to handle it.

The file specified by the argument must be a JSON file in the following format:
[  
	{"from_address": "10.20.30.40", "to_address": "10.20.30.41"},  
	{"from_address": "10.20.30.42", "to_address": "10.20.30.43"}  
] 
		
Only the nodes whose IP addresses you want to change need to be included in the
file.
		
Examples:
  # Alter the IP address of database nodes with user input
  vcluster re_ip --db-name test_db --hosts 10.20.30.40,10.20.30.41,10.20.30.42 \
  	--catalog-path /data --re-ip-file /data/re_ip_map.json
`,
	)

	// common db flags
	newCmd.setCommonFlags(cmd, []string{dbNameFlag, hostsFlag, catalogPathFlag})

	// local flags
	newCmd.setLocalFlags(cmd)

	// require re-ip-file
	markFlagsRequired(cmd, []string{"re-ip-file"})
	markFlagsFileName(cmd, map[string][]string{"re-ip-file": {"json"}})

	return cmd
}

// setLocalFlags will set the local flags the command has
func (c *CmdReIP) setLocalFlags(cmd *cobra.Command) {
	cmd.Flags().StringVar(
		&c.reIPFilePath,
		"re-ip-file",
		"",
		"Path of the re-ip file",
	)
}

func (c *CmdReIP) Parse(inputArgv []string, logger vlog.Printer) error {
	c.argv = inputArgv
	logger.LogArgParse(&c.argv)

	return c.validateParse(logger)
}

func (c *CmdReIP) validateParse(logger vlog.Printer) error {
	logger.Info("Called validateParse()")

	err := c.ValidateParseBaseOptions(&c.reIPOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	err = c.getCertFilesFromCertPaths(&c.reIPOptions.DatabaseOptions)
	if err != nil {
		return err
	}
	return c.reIPOptions.ReadReIPFile(c.reIPFilePath)
}

func (c *CmdReIP) Run(vcc vclusterops.ClusterCommands) error {
	vcc.LogInfo("Called method Run()")
	err := vcc.VReIP(c.reIPOptions)
	if err != nil {
		vcc.LogError(err, "fail to re-ip")
		return err
	}

	vcc.PrintInfo("Re-ip is successfully completed")
	return nil
}

// SetDatabaseOptions will assign a vclusterops.DatabaseOptions instance to the one in CmdReIP
func (c *CmdReIP) SetDatabaseOptions(opt *vclusterops.DatabaseOptions) {
	c.reIPOptions.DatabaseOptions = *opt
}
