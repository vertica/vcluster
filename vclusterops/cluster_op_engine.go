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

	"vertica.com/vcluster/vclusterops/vlog"
)

type VClusterOpEngine struct {
	instructions []ClusterOp
}

func MakeClusterOpEngine(instructions []ClusterOp) VClusterOpEngine {
	newClusterOpEngine := VClusterOpEngine{}
	newClusterOpEngine.instructions = instructions
	return newClusterOpEngine
}

func (opEngine *VClusterOpEngine) Run() error {
	var statusCode = SUCCESS

	execContext := MakeOpEngineExecContext()

	for _, op := range opEngine.instructions {
		op.logPrepare()
		prepareResult := op.Prepare(&execContext)

		// execute an instruction if prepare succeed
		if prepareResult.isPassing() {
			op.logExecute()
			executeResult := op.Execute(&execContext)
			statusCode = executeResult.status
			if executeResult.isFailing() {
				vlog.LogPrintInfo("Execute %s failed, details: %+v\n", op.getName(), executeResult)
			}
			if executeResult.isException() {
				vlog.LogPrintInfo("An exception happened during executing %s, details: %+v\n", op.getName(), executeResult)
			}
		} else if prepareResult.isFailing() {
			vlog.LogPrintInfo("Prepare %s failed, details: %+v\n", op.getName(), prepareResult)
		} else if prepareResult.isException() {
			vlog.LogPrintInfo("Prepare %s got exception, details: %+v\n", op.getName(), prepareResult)
		}

		op.logFinalize()
		op.Finalize(&execContext)
		if statusCode != SUCCESS {
			return fmt.Errorf("status code %d (%s)", statusCode, statusCode.getStatusString())
		}
	}

	return nil
}
