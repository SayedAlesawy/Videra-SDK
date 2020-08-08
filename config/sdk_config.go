package config

// SDKConfig Houses the configurations of the SDK
type SDKConfig struct {
	NameNodeEndpoint string `yaml:"name_node_endpoint"` //Upload endpoint
	ChunkSize        int64  `yaml:"chunk_size"`         //Size of chunk uploaded at a time
	MaxRetries       int    `yaml:"max_retries"`        //Max number of retries when failure
	WaitingTime      int    `yaml:"waiting_time"`       //Waiting time between consecutive retries
}

// SDKConfig A function to return the healthcheck monitor config
func (manager *ConfigurationManager) SDKConfig(filename string) SDKConfig {
	var configObj SDKConfig
	filePath := manager.getFilePath(filename)

	manager.retrieveConfig(&configObj, filePath)

	return configObj
}
