package viderasdk

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/SayedAlesawy/Videra-SDK/utils"
)

// sendModelInitialRequest is a function responsible for sending initial upload request for model
func (sdk VideraSDK) sendModelInitialRequest(modelPath string, configPath string, codePath string) (string, error) {
	modelSize, err := utils.GetFileSize(modelPath)
	if err != nil {
		return "", err
	}
	configSize, err := utils.GetFileSize(configPath)
	if err != nil {
		return "", err
	}
	codeSize, err := utils.GetFileSize(codePath)
	if err != nil {
		return "", err
	}

	headers := map[string]string{
		"Filesize":    fmt.Sprintf("%v", modelSize+configSize+codeSize),
		"Model-Size":  fmt.Sprintf("%v", modelSize),
		"Config-Size": fmt.Sprintf("%v", configSize),
		"Code-Size":   fmt.Sprintf("%v", codeSize),
	}

	return sdk.sendInitialRequest(modelPath, "model", headers)
}

// UploadModel is a function responsible for uploading model
func (sdk VideraSDK) uploadModel(modelPath string, configPath string, codePath string) error {
	ticker := time.NewTicker(time.Duration(sdk.defaultWaitingTime) * time.Second)

	for trial := 0; trial <= sdk.defaultMaxRetries; trial, _ = trial+1, <-ticker.C {
		err := sdk.updateUploadURL()
		if err != nil {
			log.Println("Can't contact master")
			log.Println(err)
			continue
		}

		modelID, err := sdk.sendModelInitialRequest(modelPath, configPath, codePath)
		if err != nil {
			log.Println("Can't connect to node")
			log.Println(err)
			continue
		}

		log.Println("Sent inital request for model with ID =", modelID)
		uploadFilesPaths := map[string]string{
			"model":  modelPath,
			"config": configPath,
			"code":   codePath,
		}
		err = sdk.uploadFiles(modelID, uploadFilesPaths, modelUploadOrder)
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Upload successful")
		return nil
	}
	return errors.New("An error has occurred")
}
