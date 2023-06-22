package commands

import (
	"flag"
	"fmt"
	"os"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
)

/* CmdBase
 *
 * Basic/common fields of vcluster commands
 */
type CmdBase struct {
	argv   []string
	parser *flag.FlagSet

	hostListStr *string // raw string from user input, need further processing
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
	if len(c.argv) == 0 {
		c.PrintUsage(commandType)
		return fmt.Errorf("zero args found, at least one argument expected")
	}

	return c.ParseArgv()
}
