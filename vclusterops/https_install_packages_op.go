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
	"errors"
	"fmt"

	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

type httpsInstallPackagesOp struct {
	opBase
	opHTTPSBase
}

func makeHTTPSInstallPackagesOp(logger vlog.Printer, hosts []string, useHTTPPassword bool,
	userName string, httpsPassword *string,
) (httpsInstallPackagesOp, error) {
	op := httpsInstallPackagesOp{}
	op.name = "HTTPSInstallPackagesOp"
	op.logger = logger.WithName(op.name)
	op.hosts = hosts

	err := util.ValidateUsernameAndPassword(op.name, useHTTPPassword, userName)
	if err != nil {
		return op, err
	}
	op.useHTTPPassword = useHTTPPassword
	op.userName = userName
	op.httpsPassword = httpsPassword
	return op, nil
}

func (op *httpsInstallPackagesOp) setupClusterHTTPRequest(hosts []string) error {
	for _, host := range hosts {
		httpRequest := hostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.buildHTTPSEndpoint("packages")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}

	return nil
}

func (op *httpsInstallPackagesOp) prepare(execContext *opEngineExecContext) error {
	execContext.dispatcher.setup(op.hosts)

	return op.setupClusterHTTPRequest(op.hosts)
}

func (op *httpsInstallPackagesOp) execute(execContext *opEngineExecContext) error {
	if err := op.runExecute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *httpsInstallPackagesOp) finalize(_ *opEngineExecContext) error {
	return nil
}

/*
	httpsInstallPackagesResponse example:

{'packages': [

	             {
	               'package_name': 'ComplexTypes',
	               'install_status': 'skipped'
	             },
	             {
	               'package_name': 'DelimitedExport',
	               'install_status': 'skipped'
	             },
	           ...
	           ]
	}
*/
type httpsInstallPackagesResponse map[string][]map[string]string

func (op *httpsInstallPackagesOp) processResult(_ *opEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		var responseObj httpsInstallPackagesResponse
		err := op.parseAndCheckResponse(host, result.content, &responseObj)

		if err != nil {
			allErrs = errors.Join(allErrs, err)
			continue
		}

		installedPackages, ok := responseObj["packages"]
		if !ok {
			err = fmt.Errorf(`[%s] response does not contain field "packages"`, op.name)
			allErrs = errors.Join(allErrs, err)
		}

		op.logger.PrintInfo("[%s] installed packages: %v", op.name, installedPackages)
	}
	return allErrs
}
