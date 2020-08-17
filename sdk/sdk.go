package viderasdk

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/SayedAlesawy/Videra-SDK/config"
	"github.com/SayedAlesawy/Videra-SDK/utils"
)

// logPrefix Used for hierarchical logging
var logPrefix = "[Videra-SDK]"

// sdkOnce Used to garauntee thread safety for singleton instances
var sdkOnce sync.Once

// sdkInstance A singleton instance of the server object
var sdkInstance *VideraSDK

// SDKInstance A function to return a singleton server instance
func SDKInstance() *VideraSDK {

	sdkOnce.Do(func() {
		configManager := config.ConfigurationManagerInstance("config/config_files")
		configObj := configManager.SDKConfig("sdk_config.yaml")

		sdk := VideraSDK{
			masterURL:          configObj.NameNodeEndpoint,
			chunkSize:          configObj.ChunkSize,
			defaultMaxRetries:  configObj.MaxRetries,
			defaultWaitingTime: configObj.WaitingTime,
		}

		sdkInstance = &sdk
	})

	return sdkInstance
}

var uploadURL string

var modelUploadOrder = []string{"model", "config", "code"}

// updateUploadURL is a function responsible for asking master node for data node upload url
func (sdk VideraSDK) updateUploadURL() error {
	// send request to master node to get data node upload ip
	// if success, set the new upload URL
	// if fail, return error
	client := utils.NewClient(sdk.defaultMaxRetries, sdk.defaultWaitingTime)
	req, _ := http.NewRequest(http.MethodGet, sdk.masterURL, nil)
	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}

	defer res.Body.Close()

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	body := string(bodyBytes)

	if res.StatusCode != http.StatusOK {
		log.Println(body)
		return errors.New(body)
	}

	uploadURL = body
	log.Println(fmt.Sprintf("Updated upload url to %s", uploadURL))
	return nil
}

// sendInitialRequest is a function responsible for starting upload process with data node
// default set headers are filename and filetype
func (sdk VideraSDK) sendInitialRequest(filepath string, filetype string, extraHeaders map[string]string) (string, error) {
	filename := path.Base(filepath)

	client := utils.NewClient(sdk.defaultMaxRetries, sdk.defaultWaitingTime)
	req, _ := http.NewRequest(http.MethodPost, uploadURL, nil)
	req.Header.Set("Request-Type", "init")
	req.Header.Set("Filename", filename)
	req.Header.Set("Filetype", filetype)

	for key, val := range extraHeaders {
		req.Header.Set(key, val)
	}
	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return "", err
	}

	if res.StatusCode != http.StatusCreated {
		return "", errors.New("An error has occurred")
	}

	id := res.Header.Get("ID")
	if res.Header.Get("Max-Request-Size") != "" {
		sdk.chunkSize, _ = strconv.ParseInt(res.Header.Get("Max-Request-Size"), 10, 64)
		log.Println(fmt.Sprintf("Chunk size %v", sdk.chunkSize))
	}
	return id, nil
}

// UploadJob is a function responsible for uploading a model and a video into videra system
func (sdk VideraSDK) UploadJob(videoPath string, modelPath string, configPath string, codePath string) error {
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

		log.Println("Upload Model successful")

		videoID, err := sdk.sendVideoInitialRequest(videoPath, modelID)
		if err != nil {
			log.Println("Can't connect to node")
			log.Println(err)
			continue
		}

		log.Println("Sent inital request with ID =", videoID)
		videoPathMap := map[string]string{
			"video": videoPath,
		}

		err = sdk.uploadFiles(videoID, videoPathMap, []string{"video"})
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Video was upload successfully")
		return nil
	}
	return errors.New("An error has occurred")
}
