package commands

import (
	"flag"
	"fmt"
	"os"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdBase
 *
 * Basic/common fields of vcluster commands
 */
type CmdBase struct {
	argv   []string
	parser *flag.FlagSet

	hostListStr *string // raw string from user input, need further processing
	isEon       *bool   // need further processing to see if the user inputted this flag or not
	ipv6        *bool   // need further processing to see if the user inputted this flag or not
}

// convert a host string into a list of hosts,
// save the list into options.RawHosts;
// the hosts should be separated by comma, and will be converted to lower case
func (c *CmdBase) ParseHostList(options *vclusterops.DatabaseOptions) error {
	inputHostList, err := util.SplitHosts(*c.hostListStr)
	if err != nil {
		return err
	}

	options.RawHosts = inputHostList

	return nil
}

// print usage of a command
func (c *CmdBase) PrintUsage(commandType string) {
	fmt.Fprintf(os.Stderr,
		"Please refer the usage of \"vcluster %s\" using \"vcluster %s --help\"\n",
		commandType, commandType)
}

// parse argv
func (c *CmdBase) ParseArgv() error {
	parserError := c.parser.Parse(c.argv)
	if parserError != nil {
		return parserError
	}

	return nil
}

// validate and parse argv
func (c *CmdBase) ValidateParseArgv(commandType string) error {
	vlog.LogArgParse(&c.argv)
	if c.parser == nil {
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
		// parse raw host str input into a []string
		err := c.ParseHostList(opt)
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
