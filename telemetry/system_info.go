package telemetry

import (
	"os"
	"runtime"
	"strings"
	"sync"
)

// sysInfoOnce caches the parts of system configuration that are invariant across calls
// (OS info, runtime, process name) to avoid repeated os.ReadFile on every metric.
var (
	sysInfoOnce    sync.Once
	cachedOSName   string
	cachedOSVer    string
	cachedArch     string
	cachedRuntime  string
	cachedLocale   string
	cachedProcess  string
)

func initSysInfo() {
	sysInfoOnce.Do(func() {
		cachedOSName  = getOSName()
		cachedOSVer   = getOSVersion()
		cachedArch    = runtime.GOARCH
		cachedRuntime = runtime.Version()
		cachedLocale  = getLocaleName()
		cachedProcess = getProcessName()
	})
}

func getSystemConfiguration(driverVersion string) *DriverSystemConfiguration {
	initSysInfo()
	return &DriverSystemConfiguration{
		OSName:          cachedOSName,
		OSVersion:       cachedOSVer,
		OSArch:          cachedArch,
		DriverName:      "databricks-sql-go",
		DriverVersion:   driverVersion,
		RuntimeName:     "go",
		RuntimeVersion:  cachedRuntime,
		RuntimeVendor:   "",
		LocaleName:      cachedLocale,
		CharSetEncoding: "UTF-8",
		ProcessName:     cachedProcess,
	}
}

func getOSName() string {
	switch runtime.GOOS {
	case "darwin":
		return "macOS"
	case "windows":
		return "Windows"
	case "linux":
		return "Linux"
	default:
		return runtime.GOOS
	}
}

func getOSVersion() string {
	switch runtime.GOOS {
	case "linux":
		if data, err := os.ReadFile("/etc/os-release"); err == nil {
			lines := strings.Split(string(data), "\n")
			for _, line := range lines {
				if strings.HasPrefix(line, "VERSION=") {
					version := strings.TrimPrefix(line, "VERSION=")
					version = strings.Trim(version, "\"")
					return version
				}
			}
		}
		if data, err := os.ReadFile("/proc/version"); err == nil {
			return strings.Split(string(data), " ")[2]
		}
	}
	return ""
}

func getLocaleName() string {
	if lang := os.Getenv("LANG"); lang != "" {
		parts := strings.Split(lang, ".")
		if len(parts) > 0 {
			return parts[0]
		}
	}
	return "en_US"
}

func getProcessName() string {
	if len(os.Args) > 0 {
		processPath := os.Args[0]
		lastSlash := strings.LastIndex(processPath, "/")
		if lastSlash == -1 {
			lastSlash = strings.LastIndex(processPath, "\\")
		}
		if lastSlash >= 0 {
			processPath = processPath[lastSlash+1:]
		}
		dotIndex := strings.LastIndex(processPath, ".")
		if dotIndex > 0 {
			processPath = processPath[:dotIndex]
		}
		return processPath
	}
	return ""
}
