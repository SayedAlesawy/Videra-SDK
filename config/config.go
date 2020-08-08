package config

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"

	"gopkg.in/yaml.v2"
)

// logPrefix Used for hierarchical logging
var logPrefix = "[Configuration-Manager]"

// configManagerOnce Used to garauntee thread safety for singleton instances
var configManagerOnce sync.Once

// monitorInstance A singleton instance of the config manager object
var configManagerInstance *ConfigurationManager

// ConfigurationManagerInstance A function to return a configuration manager instance
func ConfigurationManagerInstance(configFilesDir string) *ConfigurationManager {
	configManagerOnce.Do(func() {
		manager := ConfigurationManager{configFilesDir: configFilesDir}

		configManagerInstance = &manager
	})

	return configManagerInstance
}

// retrieveConfig A function to read a config file
func (manager *ConfigurationManager) retrieveConfig(configObj interface{}, filePath string) {
	configFileContent, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Println(fmt.Sprintf("%s %s\n", logPrefix, fmt.Sprintf("%s %s", "Unable to read config file:", filePath)))
		log.Panic(err)
	}

	err = yaml.Unmarshal([]byte(configFileContent), configObj)
	if err != nil {
		log.Println(fmt.Sprintf("%s %s\n", logPrefix, fmt.Sprintf("%s %s", "Unable to unmarshal config file:", filePath)))
		log.Panic(err)
	}
}

// getFilePath A function to get the file path given the name
func (manager *ConfigurationManager) getFilePath(filename string) string {
	filePath := fmt.Sprintf("%s%s", os.ExpandEnv(fmt.Sprintf("%s/", manager.configFilesDir)), filename)

	return filePath
}
