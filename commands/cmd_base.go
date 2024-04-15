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
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	outputFilePerm = 0600
)

/* CmdBase
 *
 * Basic/common fields of vcluster commands
 */
type CmdBase struct {
	argv   []string
	parser *pflag.FlagSet

	// for some commands like list_allnodes, we want to allow the output to be written
	// to a file instead of being displayed in stdout. This is the file the output will
	// be written to
	output                 string
	passwordFile           string
	readPasswordFromPrompt bool
}

// ValidateParseBaseOptions will validate and parse the required base options in each command
func (c *CmdBase) ValidateParseBaseOptions(opt *vclusterops.DatabaseOptions) error {
	// parse raw hosts
	if len(opt.RawHosts) > 0 {
		err := util.ParseHostList(&opt.RawHosts)
		if err != nil {
			return err
		}
	}

	return nil
}

// SetParser can assign a pflag parser to CmdBase
func (c *CmdBase) SetParser(parser *pflag.FlagSet) {
	c.parser = parser
}

// setCommonFlags is a helper function to let subcommands set some shared flags among them
func (c *CmdBase) setCommonFlags(cmd *cobra.Command, flags []string) {
	if len(flags) == 0 {
		return
	}
	setConfigFlags(cmd, flags)
	if util.StringInArray(passwordFlag, flags) {
		c.setPasswordFlags(cmd)
	}
	// log-path is a flag that all the subcommands need
	cmd.Flags().StringVarP(
		&dbOptions.LogPath,
		logPathFlag,
		"l",
		logPath,
		"Path location used for the debug logs",
	)
	markFlagsFileName(cmd, map[string][]string{logPathFlag: {"log"}})

	// verbose is a flag that all the subcommands need
	cmd.Flags().BoolVar(
		&globals.verbose,
		verboseFlag,
		false,
		"Show the details of VCluster run in the console",
	)
	// keyFile and certFile are flags that all subcommands require,
	// except for manage_config and `manage_config show`
	if cmd.Name() != configShowSubCmd {
		cmd.Flags().StringVar(
			&globals.keyFile,
			keyFileFlag,
			"",
			"Path to the key file",
		)
		markFlagsFileName(cmd, map[string][]string{keyFileFlag: {"key"}})

		cmd.Flags().StringVar(
			&globals.certFile,
			certFileFlag,
			"",
			"Path to the cert file",
		)
		markFlagsFileName(cmd, map[string][]string{certFileFlag: {"pem", "crt"}})
		cmd.MarkFlagsRequiredTogether(keyFileFlag, certFileFlag)
	}
	if util.StringInArray(outputFileFlag, flags) {
		cmd.Flags().StringVarP(
			&c.output,
			outputFileFlag,
			"o",
			"",
			"Write output to this file instead of stdout",
		)
	}
	if util.StringInArray(dbUserFlag, flags) {
		cmd.Flags().StringVar(
			&dbOptions.UserName,
			dbUserFlag,
			"",
			"The username for connecting to the database",
		)
	}
}

// setConfigFlags sets the config flag as well as all the common flags that
// can also be set with values from the config file
func setConfigFlags(cmd *cobra.Command, flags []string) {
	if util.StringInArray(dbNameFlag, flags) {
		cmd.Flags().StringVarP(
			&dbOptions.DBName,
			dbNameFlag,
			"d",
			"",
			"The name of the database")
	}
	if util.StringInArray(configFlag, flags) {
		cmd.Flags().StringVarP(
			&dbOptions.ConfigPath,
			configFlag,
			"c",
			"",
			"Path to the config file")
		markFlagsFileName(cmd, map[string][]string{configFlag: {"yaml"}})
	}
	if util.StringInArray(hostsFlag, flags) {
		cmd.Flags().StringSliceVar(
			&dbOptions.RawHosts,
			hostsFlag,
			[]string{},
			"Comma-separated list of hosts in database.")
	}
	if util.StringInArray(catalogPathFlag, flags) {
		cmd.Flags().StringVar(
			&dbOptions.CatalogPrefix,
			catalogPathFlag,
			"",
			"Path of catalog directory")
		markFlagsDirName(cmd, []string{catalogPathFlag})
	}
	if util.StringInArray(dataPathFlag, flags) {
		cmd.Flags().StringVar(
			&dbOptions.DataPrefix,
			dataPathFlag,
			"",
			"Path of data directory")
		markFlagsDirName(cmd, []string{dataPathFlag})
	}
	if util.StringInArray(communalStorageLocationFlag, flags) {
		cmd.Flags().StringVar(
			&dbOptions.CommunalStorageLocation,
			communalStorageLocationFlag,
			"",
			util.GetEonFlagMsg("Location of communal storage"))
	}
	if util.StringInArray(depotPathFlag, flags) {
		cmd.Flags().StringVar(
			&dbOptions.DepotPrefix,
			depotPathFlag,
			"",
			util.GetEonFlagMsg("Path to depot directory"))
		markFlagsDirName(cmd, []string{depotPathFlag})
	}
	if util.StringInArray(ipv6Flag, flags) {
		cmd.Flags().BoolVar(
			&dbOptions.IPv6,
			ipv6Flag,
			false,
			"Whether the hosts are using IPv6 addresses")
	}
	if util.StringInArray(eonModeFlag, flags) {
		cmd.Flags().BoolVar(
			&dbOptions.IsEon,
			eonModeFlag,
			false,
			util.GetEonFlagMsg("indicate if the database is an Eon db."+
				" Use it when you do not trust the configuration file"))
	}
	if util.StringInArray(configParamFlag, flags) {
		cmd.Flags().StringToStringVar(
			&dbOptions.ConfigurationParameters,
			configParamFlag,
			map[string]string{},
			"Comma-separated list of NAME=VALUE pairs of existing configuration parameters")
	}
}

// setPasswordFlags sets all the password flags
func (c *CmdBase) setPasswordFlags(cmd *cobra.Command) {
	cmd.Flags().StringVarP(
		dbOptions.Password,
		passwordFlag,
		"p",
		"",
		"Database password",
	)
	cmd.Flags().StringVar(
		&c.passwordFile,
		passwordFileFlag,
		"",
		"Path to the file to read the password from. "+
			"If - is passed, the password is read from stdin",
	)
	cmd.Flags().BoolVar(
		&c.readPasswordFromPrompt,
		readPasswordFromPromptFlag,
		false,
		"Prompt the user to enter the password",
	)
	cmd.MarkFlagsMutuallyExclusive([]string{passwordFlag, passwordFileFlag,
		readPasswordFromPromptFlag}...)
}

// ResetUserInputOptions reset password option to nil in each command
// if it is not provided in cli
func (c *CmdBase) ResetUserInputOptions(opt *vclusterops.DatabaseOptions) {
	if !c.parser.Changed(passwordFlag) {
		opt.Password = nil
	}
}

// setDBPassword sets the password option if one of the password flags
// is provided in the cli
func (c *CmdBase) setDBPassword(opt *vclusterops.DatabaseOptions) error {
	if !c.usePassword() {
		// reset password option to nil if password is not provided in cli
		opt.Password = nil
		return nil
	}

	if c.parser.Changed(passwordFlag) {
		// no-op, password has been set elsewhere,
		// through --password flag
		return nil
	}
	if opt.Password == nil {
		opt.Password = new(string)
	}
	if c.readPasswordFromPrompt {
		password, err := readDBPasswordFromPrompt()
		if err != nil {
			return err
		}
		*opt.Password = password
		return nil
	}

	password, err := c.passwordFileHelper(c.passwordFile)
	if err != nil {
		return err
	}
	*opt.Password = password
	return nil
}

func (c *CmdBase) passwordFileHelper(passwordFile string) (string, error) {
	if passwordFile == "" {
		return "", fmt.Errorf("password file path is empty")
	}
	// hyphen(`-`) is used to indicate that input should come
	// from stdin rather than from a file
	if passwordFile == "-" {
		password, err := readFromStdin()
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(password, "\n"), nil
	}

	// Read password from file
	passwordBytes, err := os.ReadFile(passwordFile)
	if err != nil {
		return "", fmt.Errorf("error reading password from file %q: %w", passwordFile, err)
	}
	// Convert bytes to string, removing any newline characters
	return strings.TrimSuffix(string(passwordBytes), "\n"), nil
}

// usePassword returns true if at least one of the password
// flags is passed in the cli
func (c *CmdBase) usePassword() bool {
	return c.parser.Changed(passwordFlag) ||
		c.parser.Changed(passwordFileFlag) ||
		c.parser.Changed(readPasswordFromPromptFlag)
}

// writeCmdOutputToFile if output-file is set, writes the output of the command
// to a file, otherwise to stdout
func (c *CmdBase) writeCmdOutputToFile(f *os.File, output []byte, logger vlog.Printer) {
	_, err := f.Write(output)
	if err != nil {
		if f == os.Stdout {
			logger.PrintWarning("%s", err)
		} else {
			logger.PrintWarning("Could not write command output to file %s, details: %s", c.output, err)
		}
	}
}

// initCmdOutputFile returns the open file descriptor, that will
// be used to write the command output, or stdout
func (c *CmdBase) initCmdOutputFile() (*os.File, error) {
	if !c.parser.Changed(outputFileFlag) {
		return os.Stdout, nil
	}
	if c.output == "" {
		return nil, fmt.Errorf("output-file cannot be empty")
	}
	return os.OpenFile(c.output, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, outputFilePerm)
}

// getCertFilesFromPaths will update cert and key file from cert path options
func (c *CmdBase) getCertFilesFromCertPaths(opt *vclusterops.DatabaseOptions) error {
	if globals.certFile != "" {
		certData, err := os.ReadFile(globals.certFile)
		if err != nil {
			return fmt.Errorf("failed to read certificate file, details %w", err)
		}
		opt.Cert = string(certData)
	}
	if globals.keyFile != "" {
		keyData, err := os.ReadFile(globals.keyFile)
		if err != nil {
			return fmt.Errorf("failed to read private key file, details %w", err)
		}
		opt.Key = string(keyData)
	}
	return nil
}
