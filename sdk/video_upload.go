package viderasdk

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/SayedAlesawy/Videra-SDK/utils"
)

// sendVideoInitialRequest is a function responsible for sending initial upload request for video
func (sdk VideraSDK) sendVideoInitialRequest(videoPath string, associatedModelID string) (string, error) {
	videoSize, err := utils.GetFileSize(videoPath)
	if err != nil {
		return "", err
	}

	headers := map[string]string{
		"Filesize":            fmt.Sprintf("%v", videoSize),
		"Associated-Model-ID": associatedModelID,
	}
	return sdk.sendInitialRequest(videoPath, "video", headers)
}

// uploadVideo is a function responsible for uploading video
func (sdk VideraSDK) uploadVideo(videoPath string, associatedModelID string) error {
	ticker := time.NewTicker(time.Duration(sdk.defaultWaitingTime) * time.Second)

	for trial := 0; trial <= sdk.defaultMaxRetries; trial, _ = trial+1, <-ticker.C {
		err := sdk.updateUploadURL()
		if err != nil {
			log.Println("Can't contact master")
			log.Println(err)
			continue
		}

		id, err := sdk.sendVideoInitialRequest(videoPath, associatedModelID)
		if err != nil {
			log.Println("Can't connect to node")
			log.Println(err)
			continue
		}

		log.Println("Sent inital request with ID =", id)
		videoPathMap := map[string]string{
			"video": videoPath,
		}

		err = sdk.uploadFiles(id, videoPathMap, []string{"video"})
		if err == nil {
			log.Println("Upload successful")
			return nil
		}
	}
	return errors.New("An error has occurred")
}
