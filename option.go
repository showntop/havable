package ha

type Option struct {
	ZkHosts  []string
	RootPath string `json:"root_path"`
}
