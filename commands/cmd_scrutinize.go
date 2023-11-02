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
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/vertica/vcluster/vclusterops"
	"github.com/vertica/vcluster/vclusterops/util"
	"github.com/vertica/vcluster/vclusterops/vlog"
)

const (
	// Environment variable names storing paths to PEM text.
	secretNameSpaceEnvVar = "NMA_SECRET_NAMESPACE"
	secretNameEnvVar      = "NMA_SECRET_NAME"
)

const (
	kubernetesPort  = "KUBERNETES_PORT"
	databaseName    = "DATABASE_NAME"
	catalogPathPref = "CATALOG_PATH"
)

// secretRetriever is an interface for retrieving secrets.
type secretRetriever interface {
	RetrieveSecret(namespace, secretName string) ([]byte, []byte, []byte, error)
}

// k8sSecretRetrieverStruct is an implementation of secretRetriever.
type k8sSecretRetrieverStruct struct {
}

/* CmdScrutinize
 *
 * Implements ClusterCommand interface
 *
 * Parses CLI arguments for scrutinize operation.
 * Prepares the inputs for the library.
 *
 */
type CmdScrutinize struct {
	CmdBase
	k8secretRetreiver secretRetriever
	sOptions          vclusterops.VScrutinizeOptions
}

func makeCmdScrutinize() *CmdScrutinize {
	newCmd := &CmdScrutinize{}
	newCmd.parser = flag.NewFlagSet("scrutinize", flag.ExitOnError)
	newCmd.sOptions = vclusterops.VScrutinizOptionsFactory()
	newCmd.k8secretRetreiver = k8sSecretRetrieverStruct{}
	// required flags
	newCmd.sOptions.DBName = newCmd.parser.String("db-name", "", "The name of the database to run scrutinize.  May be omitted on k8s.")

	newCmd.hostListStr = newCmd.parser.String("hosts", "", "Comma-separated host list")

	// optional flags
	newCmd.sOptions.Password = newCmd.parser.String("password", "",
		util.GetOptionalFlagMsg("Database password. Consider using in single quotes to avoid shell substitution."))

	newCmd.sOptions.UserName = newCmd.parser.String("db-user", "",
		util.GetOptionalFlagMsg("Database username. Consider using single quotes to avoid shell substitution."))

	newCmd.sOptions.HonorUserInput = newCmd.parser.Bool("honor-user-input", false,
		util.GetOptionalFlagMsg("Forcefully use the user's input instead of reading the options from "+vclusterops.ConfigFileName))

	newCmd.sOptions.ConfigDirectory = newCmd.parser.String("config-directory", "",
		util.GetOptionalFlagMsg("Directory where "+vclusterops.ConfigFileName+" is located"))

	newCmd.ipv6 = newCmd.parser.Bool("ipv6", false, util.GetOptionalFlagMsg("Scrutinize database with IPv6 hosts"))

	return newCmd
}

func (c *CmdScrutinize) CommandType() string {
	return vclusterops.VScrutinizeTypeName
}

func (c *CmdScrutinize) Parse(inputArgv []string, log vlog.Printer) error {
	log.PrintInfo("Parsing scrutinize command input")
	c.argv = inputArgv
	err := c.ValidateParseMaskedArgv(c.CommandType(), log)
	if err != nil {
		return err
	}

	// for some options, we do not want to use their default values,
	// if they are not provided in cli,
	// reset the value of those options to nil
	if !util.IsOptionSet(c.parser, "password") {
		c.sOptions.Password = nil
	}
	if !util.IsOptionSet(c.parser, "config-directory") {
		c.sOptions.ConfigDirectory = nil
	}
	if !util.IsOptionSet(c.parser, "ipv6") {
		c.CmdBase.ipv6 = nil
	}
	// just so generic parsing works - not relevant for functionality
	if !util.IsOptionSet(c.parser, "eon-mode") {
		c.CmdBase.isEon = nil
	}

	// parses host list and ipv6 - eon is irrelevant but handled
	return c.validateParse(log)
}

// all validations of the arguments should go in here
func (c *CmdScrutinize) validateParse(log vlog.Printer) error {
	log.Info("Called validateParse()")
	return c.ValidateParseBaseOptions(&c.sOptions.DatabaseOptions)
}

func (c *CmdScrutinize) Analyze(log vlog.Printer) error {
	log.Info("Called method Analyze()")

	// set cert/key values from env k8s
	err := c.updateCertTextsFromk8s(log)
	if err != nil {
		return err
	}

	var allErrs error
	port, found := os.LookupEnv(kubernetesPort)
	if found && port != "" && *c.sOptions.HonorUserInput {
		log.Info(kubernetesPort, " is set, k8s environment detected", found)
		dbName, found := os.LookupEnv(databaseName)
		if !found || dbName == "" {
			allErrs = errors.Join(allErrs, fmt.Errorf("unable to get database name from environment variable. "))
		} else {
			c.sOptions.DBName = &dbName
			log.Info("Setting database name from env as", "DBName", *c.sOptions.DBName)
		}

		catPrefix, found := os.LookupEnv(catalogPathPref)
		if !found || catPrefix == "" {
			allErrs = errors.Join(allErrs, fmt.Errorf("unable to get catalog path from environment variable. "))
		} else {
			c.sOptions.CatalogPrefix = &catPrefix
			log.Info("Setting catalog path from env as", "CatalogPrefix", *c.sOptions.CatalogPrefix)
		}
		if allErrs != nil {
			return allErrs
		}
	}
	return nil
}

func (c *CmdScrutinize) Run(vcc vclusterops.VClusterCommands) error {
	vcc.Log.PrintInfo("Running scrutinize") // TODO remove when no longer needed for tests
	vcc.Log.Info("Calling method Run()")

	// get config from vertica_cluster.yaml (if exists)
	config, err := c.sOptions.GetDBConfig(vcc)
	if err != nil {
		return err
	}
	c.sOptions.Config = config

	err = vcc.VScrutinize(&c.sOptions)
	if err != nil {
		vcc.Log.Error(err, "scrutinize run failed")
		return err
	}
	vcc.Log.PrintInfo("Successfully completed scrutinize run for the database %s", *c.sOptions.DBName)
	return err
}

// RetrieveSecret retrieves a secret from Kubernetes and returns its data.
func (k8sSecretRetrieverStruct) RetrieveSecret(namespace, secretName string) (ca, cert, key []byte, err error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, nil, nil, err
	}
	secretsClient := clientset.CoreV1().Secrets(namespace)
	certData, err := secretsClient.Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, nil, err
	}
	const (
		CACertName  = "ca.crt"
		TLSCertName = "tls.crt"
		TLSKeyName  = "tls.key"
	)
	caCertVal, exists := certData.Data[CACertName]
	if !exists {
		return nil, nil, nil, fmt.Errorf("missing key %s in secret", CACertName)
	}
	tlsCertVal, exists := certData.Data[TLSCertName]
	if !exists {
		return nil, nil, nil, fmt.Errorf("missing key %s in secret", TLSCertName)
	}
	tlsKeyVal, exists := certData.Data[TLSKeyName]
	if !exists {
		return nil, nil, nil, fmt.Errorf("missing key %s in secret", TLSKeyName)
	}
	return caCertVal, tlsCertVal, tlsKeyVal, nil
}

// updateCertTextsFromk8s retrieves PEM-encoded text of CA certs, the server cert, and
// the server key from kubernetes.
func (c *CmdScrutinize) updateCertTextsFromk8s(log vlog.Printer) error {
	_, portSet := os.LookupEnv(kubernetesPort)
	if !portSet {
		return nil
	}
	log.Info("K8s environment")
	secretNameSpace, nameSpaceSet := os.LookupEnv(secretNameSpaceEnvVar)
	secretName, nameSet := os.LookupEnv(secretNameEnvVar)

	// either secret namespace/name must be set, or none at all
	if !((nameSpaceSet && nameSet) || (!nameSpaceSet && !nameSet)) {
		missingParamError := constructMissingParamsMsg([]bool{nameSpaceSet, nameSet}, []string{kubernetesPort,
			secretNameSpaceEnvVar, secretNameEnvVar})
		return fmt.Errorf("all or none of the environment variables %s and %s must be set. %s",
			secretNameSpaceEnvVar, secretNameEnvVar, missingParamError)
	}

	if !nameSpaceSet {
		log.Info("Secret name not set in env. Failback to other cert retieval methods.")
		return nil
	}

	caCert, cert, key, err := c.k8secretRetreiver.RetrieveSecret(secretNameSpace, secretName)
	if err != nil {
		return fmt.Errorf("failed to read certs from k8s secret %s in namespace %s: %w", secretName, secretNameSpace, err)
	}
	if len(caCert) != 0 && len(cert) != 0 && len(key) != 0 {
		log.Info("Successfully read cert from k8s secret ", "secretName", secretName, "secretNameSpace", secretNameSpace)
	} else {
		return fmt.Errorf("failed to read CA, cert or key (sizes = %d/%d/%d)",
			len(caCert), len(cert), len(key))
	}
	c.sOptions.CaCert = string(caCert)
	c.sOptions.Cert = string(cert)
	c.sOptions.Key = string(key)

	return nil
}

// constructMissingParamsMsg builds a warning string listing each
// param in params flagged as not existing in exists.  The input
// slice order is assumed to match.
func constructMissingParamsMsg(exists []bool, params []string) string {
	needComma := false
	var warningBuilder strings.Builder
	warningBuilder.WriteString("Missing parameters: ")
	for paramIndex, paramExists := range exists {
		if !paramExists {
			if needComma {
				warningBuilder.WriteString(", ")
			}
			warningBuilder.WriteString(params[paramIndex])
			needComma = true
		}
	}
	return warningBuilder.String()
}
