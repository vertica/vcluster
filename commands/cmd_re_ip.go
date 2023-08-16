package commands

import (
	"errors"
	"flag"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

/* CmdListAllNodes
 *
 * Implements ClusterCommand interface
 */
type CmdReIP struct {
	reIPOptions  *vclusterops.VReIPOptions
	reIPFilePath *string

	CmdBase
}

func makeCmdReIP() *CmdReIP {
	newCmd := &CmdReIP{}
	newCmd.parser = flag.NewFlagSet("re_ip", flag.ExitOnError)

	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated list of hosts in the database (provide at least one)")
	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, "Whether the database hosts use IPv6 addresses")
	newCmd.reIPFilePath = newCmd.parser.String("re-ip-file", "", "Absolute path of the re-ip file")

	reIPOpt := vclusterops.VReIPFactory()
	reIPOpt.Name = newCmd.parser.String("name", "", "The name of the database")
	reIPOpt.CatalogPrefix = newCmd.parser.String("catalog-path", "", "The catalog path of the database")
	newCmd.reIPOptions = &reIPOpt

	return newCmd
}

func (c *CmdReIP) CommandType() string {
	return "re_ip"
}

func (c *CmdReIP) Parse(inputArgv []string) error {
	vlog.LogArgParse(&inputArgv)

	if c.parser == nil {
		return fmt.Errorf("unexpected nil - the parser was nil")
	}

	c.argv = inputArgv
	err := c.ValidateParseArgv(c.CommandType())
	if err != nil {
		return err
	}

	return c.validateParse()
}

func (c *CmdReIP) validateParse() error {
	vlog.LogInfo("[%s] Called validateParse()\n", c.CommandType())

	// parse raw host str input into a []string
	err := c.ParseHostList(&c.reIPOptions.DatabaseOptions)
	if err != nil {
		return err
	}

	// parse Ipv6
	c.reIPOptions.Ipv6.FromBoolPointer(c.CmdBase.ipv6)

	return nil
}

func (c *CmdReIP) Analyze() error {
	if *c.reIPFilePath == "" {
		return errors.New("must specify the re-ip-file path")
	}

	return c.reIPOptions.ReadReIPFile(*c.reIPFilePath)
}

func (c *CmdReIP) Run(log logr.Logger) error {
	vcc := vclusterops.VClusterCommands{
		Log: log.WithName(c.CommandType()),
	}
	vcc.Log.V(1).Info("Called method Run()")
	err := vcc.VReIP(c.reIPOptions)
	if err != nil {
		vcc.Log.Error(err, "fail to re-ip")
		return err
	}

	vlog.LogPrintInfo("Re-ip is successfully completed")
	return nil
}
