package global

type Author struct {
	Name    string `json:"name"`
	Twitter string `json:"twitter"`
}

type DlInfo struct {
	Windows string `json:"windows"`
	Linux   string `json:"linux"`
	Darwin  string `json:"darwin"`
}

type Web struct {
	Address    string `json:"address"`    // Address for the web server to listen on.
	ConfigPage string `json:"configPage"` // Url to the config page. For example "/config".
}

type Ctl struct {
	Address string `json:"address"` // Address for the web server to listen on.
}

type Manifest struct {
	ID         string `json:"id"`         // Unique ID of the plugin.
	Type       string `json:"type"`       // Type of the plugin. Currently "Source" and "Report" are supported.
	Label      string `json:"label"`      // Name of the plugin as presented in the apps UI.
	Author     Author `json:"author"`     // Author of the plugin. Can include a social media link as well. See Author struct.
	Version    string `json:"version"`    // Version of the plugin.
	Icon       string `json:"icon"`       // Icon to show in the plugin section and in the transaction table.
	Bin        string `json:"bin"`        // Name of the binary that f-taxes should start (must be the same for each operating system. The file extension should be omitted here. F-Taxes will add ".exe" on windows automatically).
	NoSpawn    bool   `json:"noSpawn"`    // If true, F-Taxes won't try to spawn the plugin. Useful to run a plugin manually for development.
	Repository string `json:"repository"` // Url of the repository with the plugin's source code.
	Download   DlInfo `json:"download"`   // List of download urls. Should supply one for each operating system if possible.
	Web        Web    `json:"web"`        // If set F-Taxes will allow the plugin to display a web ui.
	Ctl        Ctl    `json:"ctl"`        // Settings for the plugin's grpc server that allows for control via F-Taxes.
}
