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

func TestNMACertLookupFromK8sSecret(t *testing.T) {
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

	ok, err := c.nmaCertLookupFromK8sSecret(vlog.Printer{})
	assert.NoError(t, err)
	assert.True(t, ok)
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
	ok, err = c.nmaCertLookupFromK8sSecret(vlog.Printer{})
	assert.Error(t, err)
	assert.False(t, ok)

	// Failure to retrieve the secret should fail the request
	c = makeCmdScrutinize()
	c.k8secretRetreiver = TestK8sSecretRetriever{success: false}
	ok, err = c.nmaCertLookupFromK8sSecret(vlog.Printer{})
	assert.Error(t, err)
	assert.False(t, ok)

	// If the nma env vars aren't set, then we go onto the next retrieval method
	os.Clearenv()
	os.Setenv("KUBERNETES_PORT", randomBytes)
	c = makeCmdScrutinize()
	ok, err = c.nmaCertLookupFromK8sSecret(vlog.Printer{})
	assert.NoError(t, err)
	assert.False(t, ok)
}

func TestNMACertLookupFromEnv(t *testing.T) {
	sampleRootCA := "== sample root CA =="
	sampleCert := "== sample cert =="
	sampleKey := "== sample key =="

	frootCA, err := os.CreateTemp("", "root-ca-")
	assert.NoError(t, err)
	defer frootCA.Close()
	defer os.Remove(frootCA.Name())
	_, err = frootCA.WriteString(sampleRootCA)
	assert.NoError(t, err)
	frootCA.Close()

	var fcert *os.File
	fcert, err = os.CreateTemp("", "cert-")
	assert.NoError(t, err)
	defer fcert.Close()
	defer os.Remove(fcert.Name())
	_, err = fcert.WriteString(sampleCert)
	assert.NoError(t, err)
	fcert.Close()

	var fkeyEmpty *os.File
	fkeyEmpty, err = os.CreateTemp("", "key-")
	assert.NoError(t, err)
	// Omit writing any data to test code path
	fkeyEmpty.Close()
	defer os.Remove(fkeyEmpty.Name())

	os.Setenv(nmaRootCAPathEnvVar, frootCA.Name())
	os.Setenv(nmaCertPathEnvVar, fcert.Name())
	// intentionally omit key path env var to test error path

	// Should fail because only 2 of 3 env vars are set
	c := makeCmdScrutinize()
	ok, err := c.nmaCertLookupFromEnv(vlog.Printer{})
	assert.Error(t, err)
	assert.False(t, ok)

	// Set 3rd env var
	os.Setenv(nmaKeyPathEnvVar, fkeyEmpty.Name())

	// Should fail because one of the files is empty
	c = makeCmdScrutinize()
	ok, err = c.nmaCertLookupFromEnv(vlog.Printer{})
	assert.Error(t, err)
	assert.False(t, ok)

	// Populate empty file with contents
	var fkey *os.File
	fkey, err = os.CreateTemp("", "key-")
	assert.NoError(t, err)
	defer fkey.Close()
	defer os.Remove(fkey.Name())
	_, err = fkey.WriteString(sampleKey)
	assert.NoError(t, err)
	fkey.Close()

	// Point to key that is non-empty
	os.Setenv(nmaKeyPathEnvVar, fkey.Name())

	// Should succeed now as everything is setup properly
	c = makeCmdScrutinize()
	ok, err = c.nmaCertLookupFromEnv(vlog.Printer{})
	assert.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, sampleRootCA, c.sOptions.CaCert)
	assert.Equal(t, sampleCert, c.sOptions.Cert)
	assert.Equal(t, sampleKey, c.sOptions.Key)
}
