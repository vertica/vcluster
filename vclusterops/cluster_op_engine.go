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
)

type VClusterOpEngine struct {
	instructions []ClusterOp
	certs        *HTTPSCerts
	execContext  *OpEngineExecContext
}

func MakeClusterOpEngine(instructions []ClusterOp, certs *HTTPSCerts) VClusterOpEngine {
	newClusterOpEngine := VClusterOpEngine{}
	newClusterOpEngine.instructions = instructions
	newClusterOpEngine.certs = certs
	return newClusterOpEngine
}

func (opEngine *VClusterOpEngine) shouldGetCertsFromOptions() bool {
	return (opEngine.certs.key != "" && opEngine.certs.cert != "" && opEngine.certs.caCert != "")
}

func (opEngine *VClusterOpEngine) Run() error {
	execContext := MakeOpEngineExecContext()
	opEngine.execContext = &execContext

	findCertsInOptions := opEngine.shouldGetCertsFromOptions()

	for _, op := range opEngine.instructions {
		op.logPrepare()
		err := op.prepare(&execContext)
		if err != nil {
			return fmt.Errorf("prepare %s failed, details: %w", op.getName(), err)
		}

		if !op.isSkipExecute() {
			err = op.loadCertsIfNeeded(opEngine.certs, findCertsInOptions)
			if err != nil {
				return fmt.Errorf("loadCertsIfNeeded for %s failed, details: %w", op.getName(), err)
			}

			// execute an instruction
			op.logExecute()
			err = op.execute(&execContext)
			if err != nil {
				return fmt.Errorf("execute %s failed, details: %w", op.getName(), err)
			}
		}

		op.logFinalize()
		err = op.finalize(&execContext)
		if err != nil {
			return fmt.Errorf("finalize failed %w", err)
		}
	}

	return nil
}
