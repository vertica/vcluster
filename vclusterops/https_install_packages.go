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

type HTTPSInstallPackagesOp struct {
	OpBase
	OpHTTPBase
}

func MakeHTTPSInstallPackagesOp(opName string, hosts []string,
	useHTTPPassword bool, userName string, httpsPassword *string) HTTPSInstallPackagesOp {
	installPackagesOp := HTTPSInstallPackagesOp{}
	installPackagesOp.name = opName
	installPackagesOp.hosts = hosts

	util.ValidateUsernameAndPassword(useHTTPPassword, userName)
	installPackagesOp.useHTTPPassword = useHTTPPassword
	installPackagesOp.userName = userName
	installPackagesOp.httpsPassword = httpsPassword
	return installPackagesOp
}

func (op *HTTPSInstallPackagesOp) setupClusterHTTPRequest(hosts []string) {
	op.clusterHTTPRequest = ClusterHTTPRequest{}
	op.clusterHTTPRequest.RequestCollection = make(map[string]HostHTTPRequest)
	op.setVersionToSemVar()

	for _, host := range hosts {
		httpRequest := HostHTTPRequest{}
		httpRequest.Method = PostMethod
		httpRequest.BuildHTTPSEndpoint("packages")
		if op.useHTTPPassword {
			httpRequest.Password = op.httpsPassword
			httpRequest.Username = op.userName
		}
		op.clusterHTTPRequest.RequestCollection[host] = httpRequest
	}
}

func (op *HTTPSInstallPackagesOp) Prepare(execContext *OpEngineExecContext) error {
	execContext.dispatcher.Setup(op.hosts)
	op.setupClusterHTTPRequest(op.hosts)

	return nil
}

func (op *HTTPSInstallPackagesOp) Execute(execContext *OpEngineExecContext) error {
	if err := op.execute(execContext); err != nil {
		return err
	}

	return op.processResult(execContext)
}

func (op *HTTPSInstallPackagesOp) Finalize(execContext *OpEngineExecContext) error {
	return nil
}

/*
	HTTPSInstallPackagesResponse example:

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
type HTTPSInstallPackagesResponse map[string][]map[string]string

func (op *HTTPSInstallPackagesOp) processResult(execContext *OpEngineExecContext) error {
	var allErrs error

	for host, result := range op.clusterHTTPRequest.ResultCollection {
		op.logResponse(host, result)

		if !result.isPassing() {
			allErrs = errors.Join(allErrs, result.err)
			continue
		}

		var responseObj HTTPSInstallPackagesResponse
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

		vlog.LogPrintInfo("[%s] installed packages: %v", op.name, installedPackages)
	}
	return allErrs
}
