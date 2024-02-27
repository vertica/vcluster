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

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
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
	isEon       *bool   // need further processing to see if the user inputted this flag or not
	ipv6        *bool   // need further processing to see if the user inputted this flag or not
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
	if *opt.HonorUserInput {
		// parse raw hosts
		err := util.ParseHostList(&opt.RawHosts)
		if err != nil {
			return err
		}
		// parse IsEon
		opt.IsEon.FromBoolPointer(c.isEon)
		// parse Ipv6
		opt.Ipv6.FromBoolPointer(c.ipv6)
	}

	return nil
}

// SetParser can assign a pflag parser to CmdBase
func (c *CmdBase) SetParser(parser *pflag.FlagSet) {
	c.parser = parser
}

// SetIPv6 can create the flag --ipv6 for a cobra command
func (c *CmdBase) SetIPv6(cmd *cobra.Command) {
	cmd.Flags().BoolVar(
		c.ipv6,
		"ipv6",
		false,
		util.GetOptionalFlagMsg("Whether the hosts are using IPv6 addresses"),
	)
}

// remove this function in VER-92222
// ValidateParseBaseOptions will validate and parse the required base options in each command
func (c *CmdBase) OldValidateParseBaseOptions(opt *vclusterops.DatabaseOptions) error {
	if *opt.HonorUserInput {
		// parse raw host str input into a []string
		err := c.parseHostList(opt)
		if err != nil {
			return err
		}
		// parse IsEon
		opt.IsEon.FromBoolPointer(c.isEon)
		// parse Ipv6
		opt.Ipv6.FromBoolPointer(c.ipv6)
	}

	return nil
}

// remove this function in VER-92222
// convert a host string into a list of hosts,
// save the list into options.RawHosts;
// the hosts should be separated by comma, and will be converted to lower case
func (c *CmdBase) parseHostList(options *vclusterops.DatabaseOptions) error {
	inputHostList, err := util.SplitHosts(*c.hostListStr)
	if err != nil {
		return err
	}

	options.RawHosts = inputHostList

	return nil
}
