package main

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"runtime"
	"text/template"
)

func getTemplateFilePath() string {
	_, filename, _, _ := runtime.Caller(0) // nolint:dogsled
	dir := path.Dir(filename)
	return path.Join(dir, "version.go.tmpl")
}

func main() {
	tag := os.Args[1] // The first argument is the tag
	// If the tag is not a valid semantic version, then it is not a release tag
	re := regexp.MustCompile(`^v\d+\.\d+\.\d+$`)
	if !re.MatchString(tag) {
		panic(fmt.Errorf("tag %s is not a valid semantic version", tag))
	}
	tmpl, err := template.ParseFiles(getTemplateFilePath())
	if err != nil {
		panic(err)
	}
	f, err := os.Create("version.go")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	if err := tmpl.Execute(f, map[string]string{
		"Version": string(tag),
	}); err != nil {
		panic(err)
	}
}
