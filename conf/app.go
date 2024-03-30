package conf

import (
	"bytes"
	"os"

	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/yaml"
	"github.com/kataras/golog"
)

var appCfgPath string
var App *config.Config

func LoadAppConfig(path string) {
	cfg := config.New("appCfg", config.ParseTime)
	App = cfg
	appCfgPath = path
	cfg.AddDriver(yaml.Driver)

	err := cfg.LoadFiles(path)
	if err != nil {
		golog.Fatalf("Error loading config: %v", err)
	}
}

func WriteAppConfig() {
	buf := new(bytes.Buffer)

	_, err := App.DumpTo(buf, config.Yaml)
	if err != nil {
		golog.Fatalf("Failed to dump config into buffer: %v", err)
	}

	err = os.WriteFile(appCfgPath, buf.Bytes(), 0755)
	if err != nil {
		golog.Fatalf("Failed to write config file: %v", err)
	}
}
