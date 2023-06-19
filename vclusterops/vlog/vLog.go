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
	"sync"

	"runtime/debug"
)

const (
	DefaultLogPath = "/opt/vertica/log/vcluster.log"
	LogPermission  = 0644

	InfoLog    = "[INFO] "
	WarningLog = "[WARNING] "
	ErrorLog   = "[ERROR] "
	DebugLog   = "[DEBUG] "
)

type Vlogger struct {
	LogPath string
}

var (
	logInstance Vlogger
	once        sync.Once
)

// return a singleton instance of the GlobalLogger
func GetGlobalLogger() Vlogger {
	/* if once.Do(f) is called multiple times,
	 * only the first call will invoke f,
	 * even if f has a different value in each invocation.
	 * Reference: https://pkg.go.dev/sync#Once
	 */
	once.Do(func() {
		logInstance = makeGlobalLogger()
	})

	return logInstance
}

func makeGlobalLogger() Vlogger {
	newGlobalLogger := Vlogger{}
	return newGlobalLogger
}

func ParseLogPathArg(argInput []string, defaultPath string) string {
	logger := GetGlobalLogger()
	return logger.parseLogPathArg(argInput, defaultPath)
}
func (logger *Vlogger) parseLogPathArg(argInput []string, defaultPath string) string {
	checkLogDir := true
	for idx, arg := range argInput {
		if arg == "--log-path" {
			logger.LogPath = argInput[idx+1]
			checkLogDir = false
		}
	}
	if checkLogDir {
		logger.LogPath = defaultPath
	}
	return logger.LogPath
}

func SetupOrDie(logFile string) {
	logger := GetGlobalLogger()
	logger.setupOrDie(logFile)
}

// expected to be used by both client lib and vcluster CLI
// so have logFile string to allow different log files for the two use cases
func (logger *Vlogger) setupOrDie(logFile string) {
	logFileObj, err := os.OpenFile(logFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(LogPermission))
	if err != nil {
		fmt.Println(err)
	}
	logger.logFatal(err)
	log.Printf("Successfully opened file %s. Setting log output to that file.\n", logFileObj.Name())

	// start to setup log and log start up msg
	log.SetOutput(logFileObj)
	startupErr := logger.logStartupMessage()
	logger.logFatal(startupErr)
}

func LogStartupMessage() error {
	logger := GetGlobalLogger()
	return logger.logStartupMessage()
}

func (logger *Vlogger) logStartupMessage() error {
	// all INFO level log
	logger.logInfo("New log for process %d", os.Getpid())
	logger.logInfo("Called with args %s", os.Args)
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}

	logger.logInfo("Hostname %s, User id %d", hostname, os.Getuid())
	return nil
}

func LogFatal(err error) {
	logger := GetGlobalLogger()
	logger.logFatal(err)
}

func (logger *Vlogger) logFatal(err error) {
	if err == nil {
		return
	}
	stackBytes := debug.Stack()
	logger.logInfo("Fatal error occurred. Backtrace:\n%s\n", string(stackBytes))
	log.Fatal(err)
}

func LogInfoln(info string) {
	logger := GetGlobalLogger()
	logger.logInfoln(info)
}

// basic log functions starts here: log plain string
// following log.Println naming convention
func (logger *Vlogger) logInfoln(info string) {
	log.Println(InfoLog + info)
}

func LogWarningln(info string) {
	logger := GetGlobalLogger()
	logger.logWarningln(info)
}

// log Warning
func (logger *Vlogger) logWarningln(info string) {
	log.Println(WarningLog + info)
}

func LogErrorln(info string) {
	logger := GetGlobalLogger()
	logger.logErrorln(info)
}

// log error
func (logger *Vlogger) logErrorln(info string) {
	log.Println(ErrorLog + info)
}

func LogInfo(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logInfo(info, v...)
}

// log info with formatting
func (logger *Vlogger) logInfo(info string, v ...any) {
	log.Printf(InfoLog+info, v...)
}

func LogWarning(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logWarning(info, v...)
}
func (logger *Vlogger) logWarning(info string, v ...any) {
	log.Printf(WarningLog+info, v...)
}

func LogError(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logError(info, v...)
}
func (logger *Vlogger) logError(info string, v ...any) {
	log.Printf(ErrorLog+info, v...)
}

func LogDebug(info string, v ...any) {
	logger := GetGlobalLogger()
	logger.logDebug(info, v...)
}
func (logger *Vlogger) logDebug(info string, v ...any) {
	log.Printf(DebugLog+info, v...)
}

// output to both log and console
// another possible way is to use io.MultiWriter, but that needs set a different output:
// mw := io.MultiWriter(os.Stdout, logFile)
// log.SetOutput(mw)
// so to avoid setting output back and forth, we just do a log and fmt
// log and print msg of the format "levelPrefix msg"
// e.g., [Info] this is a sample log info
func (logger *Vlogger) logPrintInternal(msg string) {
	log.Println(msg)
	fmt.Println(msg)
}

func LogPrintInfo(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintInfo(msg, v...)
}
func (logger *Vlogger) logPrintInfo(msg string, v ...any) {
	completeMsg := fmt.Sprintf(InfoLog+msg, v...)
	logger.logPrintInternal(completeMsg)
}

func LogPrintError(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintError(msg, v...)
}
func (logger *Vlogger) logPrintError(msg string, v ...any) {
	completeMsg := fmt.Errorf(ErrorLog+msg, v...)
	logger.logPrintInternal(completeMsg.Error())
}

func LogPrintDebug(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintDebug(msg, v...)
}
func (logger *Vlogger) logPrintDebug(msg string, v ...any) {
	completeMsg := fmt.Sprintf(DebugLog+msg, v...)
	logger.logPrintInternal(completeMsg)
}

func LogPrintWarning(msg string, v ...any) {
	logger := GetGlobalLogger()
	logger.logPrintWarning(msg, v...)
}
func (logger *Vlogger) logPrintWarning(msg string, v ...any) {
	completeMsg := fmt.Sprintf(WarningLog+msg, v...)
	logger.logPrintInternal(completeMsg)
}

func LogPrintInfoln(msg string) {
	logger := GetGlobalLogger()
	logger.logPrintInfoln(msg)
}
func (logger *Vlogger) logPrintInfoln(msg string) {
	logger.logPrintInternal(InfoLog + msg)
}

func LogPrintWarningln(msg string) {
	logger := GetGlobalLogger()
	logger.logPrintWarningln(msg)
}
func (logger *Vlogger) logPrintWarningln(msg string) {
	logger.logPrintInternal(WarningLog + msg)
}

func LogPrintErrorln(msg string) {
	logger := GetGlobalLogger()
	logger.logPrintErrorln(msg)
}
func (logger *Vlogger) logPrintErrorln(msg string) {
	logger.logPrintInternal(ErrorLog + msg)
}

func LogArgParse(inputArgv *[]string) {
	logger := GetGlobalLogger()
	logger.logArgParse(inputArgv)
}

// log functions for specific cases
func (logger *Vlogger) logArgParse(inputArgv *[]string) {
	inputArgMsg := fmt.Sprintf("Called method Parse with args: %q.", *inputArgv)
	logger.logInfoln(inputArgMsg)
}
