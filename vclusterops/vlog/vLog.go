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

package vlog

import (
	"fmt"
	"log"
	"os"

	"runtime/debug"
)

const (
	LogDir = "/opt/vertica/log"

	// LogFile defines the default log path
	LogFile       = LogDir + "/vcluster.log"
	LogPermission = 0644

	InfoLog    = "[INFO] "
	WarningLog = "[WARNING] "
	ErrorLog   = "[ERROR] "
	DebugLog   = "[DEBUG] "
)

// expected to be used by both client lib and vcluster CLI
// so have logFile string to allow different log files for the two use cases
func SetupOrDie(logFile string) {
	logFileObj, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(LogPermission))
	LogFatal(err)
	log.Printf("Successfully opened file %s. Setting log output to that file.\n", logFileObj.Name())

	// start to setup log and log start up msg
	log.SetOutput(logFileObj)
	startupErr := logStartupMessage()
	LogFatal(startupErr)
}

func logStartupMessage() error {
	// all INFO level log
	LogInfo("New log for process %d", os.Getpid())
	LogInfo("Called with args %s", os.Args)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	LogInfo("Hostname %s, User id %d", hostname, os.Getuid())
	return nil
}

func LogFatal(err error) {
	if err == nil {
		return
	}
	stackBytes := debug.Stack()
	LogInfo("Fatal error occurred. Backtrace:\n%s\n", string(stackBytes))
	log.Fatal(err)
}

// basic log functions starts here: log plain string
// following log.Println naming convention
func LogInfoln(info string) {
	log.Println(InfoLog + info)
}

// log Warning
func LogWarningln(info string) {
	log.Println(WarningLog + info)
}

// log error
func LogErrorln(info string) {
	log.Println(ErrorLog + info)
}

// log info with formatting
func LogInfo(info string, v ...any) {
	log.Printf(InfoLog+info, v...)
}

func LogWarning(info string, v ...any) {
	log.Printf(WarningLog+info, v...)
}

func LogError(info string, v ...any) {
	log.Printf(ErrorLog+info, v...)
}

func LogDebug(info string, v ...any) {
	log.Printf(DebugLog+info, v...)
}

// output to both log and console
// another possible way is to use io.MultiWriter, but that needs set a different output:
// mw := io.MultiWriter(os.Stdout, logFile)
// log.SetOutput(mw)
// so to avoid setting output back and forth, we just do a log and fmt
// log and print msg of the format "levelPrefix msg"
// e.g., [Info] this is a sample log info
func logPrintInternal(msg string) {
	log.Println(msg)
	fmt.Println(msg)
}

func LogPrintInfo(msg string, v ...any) {
	completeMsg := fmt.Sprintf(InfoLog+msg, v...)
	logPrintInternal(completeMsg)
}

func LogPrintError(msg string, v ...any) {
	completeMsg := fmt.Errorf(ErrorLog+msg, v...)
	logPrintInternal(completeMsg.Error())
}

func LogPrintDebug(msg string, v ...any) {
	completeMsg := fmt.Sprintf(DebugLog+msg, v...)
	logPrintInternal(completeMsg)
}

func LogPrintWarning(msg string, v ...any) {
	completeMsg := fmt.Sprintf(WarningLog+msg, v...)
	logPrintInternal(completeMsg)
}

func LogPrintInfoln(msg string) {
	logPrintInternal(InfoLog + msg)
}

func LogPrintWarningln(msg string) {
	logPrintInternal(WarningLog + msg)
}

func LogPrintErrorln(msg string) {
	logPrintInternal(ErrorLog + msg)
}

// log functions for specific cases
func LogArgParse(inputArgv *[]string) {
	inputArgMsg := fmt.Sprintf("Called method Parse with args: %q.", *inputArgv)
	LogInfoln(inputArgMsg)
}
