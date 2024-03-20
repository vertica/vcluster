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
	"flag"
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
	argv      []string
	parser    *pflag.FlagSet
	oldParser *flag.FlagSet // remove this variable in VER-92222

	hostListStr *string // raw string from user input, need further processing
	// remove below two fields in VER-92369
	isEon *bool // need further processing to see if the user inputted this flag or not
	ipv6  *bool // need further processing to see if the user inputted this flag or not
	// for some commands like list_allnodes, we want to allow the output to be written
	// to a file instead of being displayed in stdout. This is the file the output will
	// be written to
	output                 string
	passwordFile           string
	readPasswordFromPrompt bool
}

// print usage of a command
func (c *CmdBase) PrintUsage(commandType string) {
	fmt.Fprintf(os.Stderr,
		"Please refer the usage of \"vcluster %s\" using \"vcluster %s --help\"\n",
		commandType, commandType)
}

// parse argv
func (c *CmdBase) ParseArgv() error {
	parserError := c.oldParser.Parse(c.argv)
	if parserError != nil {
		return parserError
	}

	return nil
}

// validate and parse argv
func (c *CmdBase) ValidateParseArgv(commandType string, logger vlog.Printer) error {
	logger.LogArgParse(&c.argv)
	return c.ValidateParseArgvHelper(commandType)
}

// validate and parse masked argv
// Some database actions, such as createDB and reviveDB, need to mask sensitive parameters in the log
func (c *CmdBase) ValidateParseMaskedArgv(commandType string, logger vlog.Printer) error {
	logger.LogMaskedArgParse(c.argv)
	return c.ValidateParseArgvHelper(commandType)
}

func (c *CmdBase) ValidateParseArgvHelper(commandType string) error {
	if c.oldParser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}
	if len(c.argv) == 0 {
		c.PrintUsage(commandType)
		return fmt.Errorf("zero args found, at least one argument expected")
	}
	return c.ParseArgv()
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

	// remove IsEon and Ipv6 process below in VER-92369
	// parse IsEon
	opt.OldIsEon.FromBoolPointer(c.isEon)
	// parse Ipv6
	opt.OldIpv6.FromBoolPointer(c.ipv6)

	return nil
}

// SetParser can assign a pflag parser to CmdBase
func (c *CmdBase) SetParser(parser *pflag.FlagSet) {
	c.parser = parser
}

// remove this function in VER-92369
// SetIPv6 can create the flag --ipv6 for a cobra command
func (c *CmdBase) SetIPv6(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		c.ipv6,
		"ipv6",
		false,
		"Whether the hosts are using IPv6 addresses",
	)
}

// setCommonFlags is a helper function to let subcommands set some shared flags among them
func (c *CmdBase) setCommonFlags(cmd *cobra.Command, flags []string) {
	setConfigFlags(cmd, flags)
	if util.StringInArray(passwordFlag, flags) {
		c.setPasswordFlags(cmd)
	}
	// log-path is a flag that all the subcommands need
	cmd.Flags().StringVarP(
		dbOptions.LogPath,
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
	if util.StringInArray(outputFileFlag, flags) {
		cmd.Flags().StringVarP(
			&c.output,
			outputFileFlag,
			"o",
			"",
			"Write output to this file instead of stdout",
		)
	}
}

// setConfigFlags sets the config flag as well as all the common flags that
// can also be set with values from the config file
func setConfigFlags(cmd *cobra.Command, flags []string) {
	if util.StringInArray(dbNameFlag, flags) {
		cmd.Flags().StringVarP(
			dbOptions.DBName,
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
			dbOptions.CatalogPrefix,
			catalogPathFlag,
			"",
			"Path of catalog directory")
		markFlagsDirName(cmd, []string{catalogPathFlag})
	}
	if util.StringInArray(dataPathFlag, flags) {
		cmd.Flags().StringVar(
			dbOptions.DataPrefix,
			dataPathFlag,
			"",
			"Path of data directory")
		markFlagsDirName(cmd, []string{dataPathFlag})
	}
	if util.StringInArray(communalStorageLocationFlag, flags) {
		cmd.Flags().StringVar(
			dbOptions.CommunalStorageLocation,
			communalStorageLocationFlag,
			"",
			util.GetEonFlagMsg("Location of communal storage"))
	}
	if util.StringInArray(depotPathFlag, flags) {
		cmd.Flags().StringVar(
			dbOptions.DepotPrefix,
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
				" Use it when you do not trust "+vclusterops.ConfigFileName))
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
	if !c.parser.Changed("password") {
		opt.Password = nil
	}
}

// remove this function in VER-92369
// OldResetUserInputOptions reset password and ipv6 options to nil in each command
// if they are not provided in cli
func (c *CmdBase) OldResetUserInputOptions() {
	if !c.parser.Changed("ipv6") {
		c.ipv6 = nil
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

	if c.passwordFile == "" {
		return fmt.Errorf("password file path is empty")
	}
	// hyphen(`-`) is used to indicate that input should come
	// from stdin rather than from a file
	if c.passwordFile == "-" {
		password, err := readFromStdin()
		if err != nil {
			return err
		}
		*opt.Password = strings.TrimSuffix(password, "\n")
	} else {
		// Read password from file
		passwordBytes, err := os.ReadFile(c.passwordFile)
		if err != nil {
			return fmt.Errorf("error reading password from file %q: %w", c.passwordFile, err)
		}
		// Convert bytes to string, removing any newline characters
		*opt.Password = strings.TrimSuffix(string(passwordBytes), "\n")
	}
	return nil
}

// remove this function in VER-92222
// ValidateParseBaseOptions will validate and parse the required base options in each command
func (c *CmdBase) OldValidateParseBaseOptions(opt *vclusterops.DatabaseOptions) error {
	// parse raw host str input into a []string
	err := c.parseHostList(opt)
	if err != nil {
		return err
	}
	// parse IsEon
	opt.OldIsEon.FromBoolPointer(c.isEon)
	// parse Ipv6
	opt.OldIpv6.FromBoolPointer(c.ipv6)

	return nil
}

// remove this function in VER-92222
// convert a host string into a list of hosts,
// save the list into options.RawHosts;
// the hosts should be separated by comma, and will be converted to lower case
func (c *CmdBase) parseHostList(options *vclusterops.DatabaseOptions) error {
	if *c.hostListStr != "" {
		inputHostList, err := util.SplitHosts(*c.hostListStr)
		if err != nil {
			return err
		}
		options.RawHosts = inputHostList
	}

	return nil
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
