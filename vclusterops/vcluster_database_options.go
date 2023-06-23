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

package vclusterops

import (
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
	"golang.org/x/exp/slices"
)

type DatabaseOptions struct {
	// part 1: basic database info
	Name            *string
	RawHosts        []string // expected to be IP addresses or hostnames
	Hosts           []string // expected to be IP addresses resolved from RawHosts
	Ipv6            *bool
	CatalogPrefix   *string
	DataPrefix      *string
	ConfigDirectory *string

	// part 2: Eon database info
	DepotPrefix *string
	IsEon       *bool

	// part 3: authentication info
	UserName *string
	Password *string
	Key      string
	Cert     string
	CaCert   string

	// part 4: other info
	LogPath        *string
	HonorUserInput *bool
}

func (opt *DatabaseOptions) SetDefaultValues() {
	opt.Name = new(string)
	opt.Ipv6 = new(bool)
	opt.CatalogPrefix = new(string)
	opt.DataPrefix = new(string)
	opt.DepotPrefix = new(string)
	opt.UserName = new(string)
}

func (opt *DatabaseOptions) CheckNilPointerParams() error {
	// basic params
	if opt.Name == nil {
		return util.ParamNotSetErrorMsg("name")
	}
	if opt.Ipv6 == nil {
		return util.ParamNotSetErrorMsg("ipv6")
	}
	if opt.CatalogPrefix == nil {
		return util.ParamNotSetErrorMsg("catalog-path")
	}
	if opt.DataPrefix == nil {
		return util.ParamNotSetErrorMsg("data-path")
	}
	if opt.DepotPrefix == nil {
		return util.ParamNotSetErrorMsg("depot-path")
	}

	return nil
}

func (opt *DatabaseOptions) ValidateBaseOptions(commandName string) error {
	// database name
	if *opt.Name == "" {
		return fmt.Errorf("must specify a database name")
	}
	err := util.ValidateDBName(*opt.Name)
	if err != nil {
		return err
	}

	// raw hosts
	if len(opt.RawHosts) == 0 {
		return fmt.Errorf("must specify a host or host list")
	}

	// password
	if opt.Password == nil {
		opt.Password = new(string)
		*opt.Password = ""
		vlog.LogPrintInfoln("no password specified, using none")
	}

	// paths
	err = opt.ValidatePaths(commandName)
	if err != nil {
		return err
	}

	// config directory
	err = opt.ValidateConfigDir(commandName)
	if err != nil {
		return err
	}

	// log directory
	err = util.ValidateAbsPath(opt.LogPath, "log directory")
	if err != nil {
		return err
	}

	return nil
}

// validate catalog, data, and depot paths
func (opt *DatabaseOptions) ValidatePaths(commandName string) error {
	// validate for the following commands only
	// TODO: add other commands into the command list
	commands := []string{"create_db", "drop_db"}
	if !slices.Contains(commands, commandName) {
		return nil
	}

	// catalog prefix path
	err := util.ValidateRequiredAbsPath(opt.CatalogPrefix, "catalog path")
	if err != nil {
		return err
	}

	// data prefix
	err = util.ValidateRequiredAbsPath(opt.DataPrefix, "data path")
	if err != nil {
		return err
	}

	// depot prefix
	if opt.IsEon != nil && *opt.IsEon {
		err = util.ValidateRequiredAbsPath(opt.DepotPrefix, "depot path")
		if err != nil {
			return err
		}
	}

	return nil
}

// validate config directory
func (opt *DatabaseOptions) ValidateConfigDir(commandName string) error {
	// validate for the following commands only
	// TODO: add other commands into the command list
	commands := []string{"create_db", "drop_db"}
	if slices.Contains(commands, commandName) {
		return nil
	}

	err := util.ValidateAbsPath(opt.ConfigDirectory, "config directory")
	if err != nil {
		return err
	}

	return nil
}

// ParseHostList converts a string into a list of hosts.
// The hosts should be separated by comma, and will be converted to lower case
func (opt *DatabaseOptions) ParseHostList(hosts string) error {
	inputHostList, err := util.SplitHosts(hosts)
	if err != nil {
		return err
	}

	opt.RawHosts = inputHostList

	return nil
}
