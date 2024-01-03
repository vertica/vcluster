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

	"github.com/vertica/vcluster/vclusterops/vlog"
)

type VUnsandboxOptions struct {
	DatabaseOptions
	SCName *string
}

func VUnsandboxOptionsFactory() VUnsandboxOptions {
	opt := VUnsandboxOptions{}
	opt.setDefaultValues()
	return opt
}

func (options *VUnsandboxOptions) setDefaultValues() {
	options.DatabaseOptions.setDefaultValues()
	options.SCName = new(string)
}

func (options *VUnsandboxOptions) validateRequiredOptions(logger vlog.Printer) error {
	err := options.validateBaseOptions("unsandbox_subcluster", logger)
	if err != nil {
		return err
	}

	if *options.SCName == "" {
		return fmt.Errorf("must specify a subcluster name")
	}
	return nil
}

func (options *VUnsandboxOptions) ValidateAnalyzeOptions(vcc *VClusterCommands) error {
	if err := options.validateRequiredOptions(vcc.Log); err != nil {
		return err
	}

	// TODO: More validations here
	// check eon db, up
	// validate sc info, if sandboxed or not
	return nil
}

func (vcc *VClusterCommands) VUnsandbox(options *VUnsandboxOptions) error {
	vcc.Log.V(0).Info("VUnsandbox method called with options " + fmt.Sprintf("%#v", options))
	// check required options
	err := options.ValidateAnalyzeOptions(vcc)
	if err != nil {
		vcc.Log.Error(err, "validation of unsandboxing arguments failed")
		return err
	}
	return nil
}
