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
	"errors"
	"os"
	"testing"

	"github.com/vertica/vcluster/vclusterops/vlog"

	"github.com/stretchr/testify/assert"
)

const (
	kubePort    = "5433"
	dbName      = "test_db"
	catalogPath = "/catalog/path"
)

// TestK8sSecretRetriever is a test implementation of k8sSecretRetrieverStruct.
type TestK8sSecretRetriever struct {
	success       bool
	ca, cert, key string
}

// RetrieveSecret retrieves a secret and returns mock values.
func (t TestK8sSecretRetriever) RetrieveSecret(_, _ string) (caBytes []byte, certBytes []byte,
	keyBytes []byte, err error) {
	if !t.success { // Allow for dependency injection
		return nil, nil, nil, errors.New("failed to retrieve secrets")
	}
	caBytes, certBytes, keyBytes = []byte(t.ca), []byte(t.cert), []byte(t.key)
	return caBytes, certBytes, keyBytes, nil
}

func TestScrutinCmd(t *testing.T) {
	// Positive case
	os.Setenv(kubernetesPort, kubePort)
	os.Setenv(databaseName, dbName)
	os.Setenv(catalogPathPref, catalogPath)
	c := makeCmdScrutinize()
	*c.sOptions.HonorUserInput = true
	err := c.Analyze(vlog.Printer{})
	assert.Nil(t, err)
	assert.Equal(t, dbName, *c.sOptions.DBName)
	assert.Equal(t, catalogPath, *c.sOptions.CatalogPrefix)

	// Catalog Path not provided
	os.Setenv(catalogPathPref, "")
	c = makeCmdScrutinize()
	*c.sOptions.HonorUserInput = true
	err = c.Analyze(vlog.Printer{})
	assert.ErrorContains(t, err, "unable to get catalog path from environment variable")

	// Database Name not provided
	os.Setenv(databaseName, "")
	os.Setenv(catalogPathPref, catalogPath)
	c = makeCmdScrutinize()
	*c.sOptions.HonorUserInput = true
	err = c.Analyze(vlog.Printer{})
	assert.ErrorContains(t, err, "unable to get database name from environment variable")
}

func TestUpdateCertTextsFromK8s(t *testing.T) {
	const randomBytes = "123"
	c := makeCmdScrutinize()
	c.k8secretRetreiver = TestK8sSecretRetriever{
		success: true,
		ca:      "test cert 1",
		cert:    "test cert 2",
		key:     "test cert 3",
	}
	os.Setenv("KUBERNETES_SERVICE_HOST", randomBytes)
	os.Setenv("KUBERNETES_SERVICE_PORT", randomBytes)
	os.Setenv("KUBERNETES_PORT", randomBytes)
	os.Setenv(secretNameSpaceEnvVar, randomBytes)
	os.Setenv(secretNameEnvVar, randomBytes)

	// Case 2: when the certs are configured correctly

	err := c.updateCertTextsFromk8s(vlog.Printer{})
	assert.NoError(t, err)
	assert.Equal(t, "test cert 1", c.sOptions.CaCert)
	assert.Equal(t, "test cert 2", c.sOptions.Cert)
	assert.Equal(t, "test cert 3", c.sOptions.Key)

	// If some of the keys are missing
	c = makeCmdScrutinize()
	c.k8secretRetreiver = TestK8sSecretRetriever{
		success: true,
		ca:      "test cert 1",
		cert:    "test cert 2",
		key:     "", // Missing
	}
	err = c.updateCertTextsFromk8s(vlog.Printer{})
	assert.Error(t, err)

	// Failure to retrieve the secret should fail the request
	c = makeCmdScrutinize()
	c.k8secretRetreiver = TestK8sSecretRetriever{success: false}
	err = c.updateCertTextsFromk8s(vlog.Printer{})
	assert.Error(t, err)

	// If the nma env vars aren't set, then we go onto the next retrieval method
	os.Clearenv()
	os.Setenv("KUBERNETES_PORT", randomBytes)
	c = makeCmdScrutinize()
	err = c.updateCertTextsFromk8s(vlog.Printer{})
	assert.NoError(t, err)
}
