package telemetry

import (
	"os"
	"runtime"
	"strings"
)

func getSystemConfiguration(driverVersion string) *DriverSystemConfiguration {
	return &DriverSystemConfiguration{
		OSName:          getOSName(),
		OSVersion:       getOSVersion(),
		OSArch:          runtime.GOARCH,
		DriverName:      "databricks-sql-go",
		DriverVersion:   driverVersion,
		RuntimeName:     "go",
		RuntimeVersion:  runtime.Version(),
		RuntimeVendor:   "",
		LocaleName:      getLocaleName(),
		CharSetEncoding: "UTF-8",
		ProcessName:     getProcessName(),
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
