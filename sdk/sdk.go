package viderasdk

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// VideraSDK Handles communication between clients and videra system
type VideraSDK struct {
	masterURL          string //IP of master
	chunkSize          int64  //Upload chunk size
	defaultMaxRetries  int    //Max number of request retrials
	defaultWaitingTime int    //waiting time between failed request and new one
}

// logPrefix Used for hierarchical logging
var logPrefix = "[Videra-SDK]"

// sdkOnce Used to garauntee thread safety for singleton instances
var sdkOnce sync.Once

// sdkInstance A singleton instance of the server object
var sdkInstance *VideraSDK

// SDKInstance A function to return a singleton server instance
func SDKInstance(masterURL string) *VideraSDK {

	sdkOnce.Do(func() {
		sdk := VideraSDK{
			masterURL:          masterURL,
			chunkSize:          int64(4 << 20),
			defaultMaxRetries:  3,
			defaultWaitingTime: 10,
		}

		sdkInstance = &sdk
	})

	return sdkInstance
}

var uploadURL string

var modelUploadOrder = []string{"model", "config", "code"}

// newClient is a function that returns customized http client
func newClient(maxRetries int, waitingTime int) *http.Client {
	clientretry := retryablehttp.NewClient()
	clientretry.RetryMax = maxRetries
	clientretry.RetryWaitMin = time.Duration(time.Duration(waitingTime) * time.Second)
	clientretry.RetryWaitMax = time.Duration(time.Duration(waitingTime) * time.Second)

	return clientretry.StandardClient()
}

// updateUploadURL is a function responsible for asking master node for data node upload url
func (sdk VideraSDK) updateUploadURL() error {
	// send request to master node to get data node upload ip
	// if success, set the new upload URL
	// if fail, return error
	client := newClient(sdk.defaultMaxRetries, sdk.defaultWaitingTime)
	req, _ := http.NewRequest(http.MethodGet, sdk.masterURL, nil)
	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return err
	}

	defer res.Body.Close()

	bodyBytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
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

// getFileSize is a function to get file size
func getFileSize(filepath string) (int64, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

// sendInitialRequest is a function responsible for starting upload process with data node
// default set headers are filename and filetype
func (sdk VideraSDK) sendInitialRequest(filepath string, filetype string, extraHeaders map[string]string) (string, error) {
	filename := path.Base(filepath)

	client := newClient(sdk.defaultMaxRetries, sdk.defaultWaitingTime)
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
		log.Fatal(res.StatusCode)
	}

	id := res.Header.Get("ID")
	if res.Header.Get("Max-Request-Size") != "" {
		sdk.chunkSize, _ = strconv.ParseInt(res.Header.Get("Max-Request-Size"), 10, 64)
		log.Println(fmt.Sprintf("Chunk size %v", sdk.chunkSize))
	}
	return id, nil
}

// sendVideoInitialRequest is a function responsible for sending initial upload request for video
func (sdk VideraSDK) sendVideoInitialRequest(videoPath string) (string, error) {
	videoSize, err := getFileSize(videoPath)
	if err != nil {
		log.Fatal(err)
	}

	headers := map[string]string{
		"Filesize": fmt.Sprintf("%v", videoSize),
	}
	return sdk.sendInitialRequest(videoPath, "video", headers)
}

// sendModelInitialRequest is a function responsible for sending initial upload request for model
func (sdk VideraSDK) sendModelInitialRequest(modelPath string, configPath string, codePath string) (string, error) {
	modelSize, err := getFileSize(modelPath)
	if err != nil {
		log.Fatal(err)
	}
	configSize, err := getFileSize(configPath)
	if err != nil {
		log.Fatal(err)
	}
	codeSize, err := getFileSize(codePath)
	if err != nil {
		log.Fatal(err)
	}

	headers := map[string]string{
		"Filesize":    fmt.Sprintf("%v", modelSize+configSize+codeSize),
		"Model-Size":  fmt.Sprintf("%v", modelSize),
		"Config-Size": fmt.Sprintf("%v", configSize),
		"Code-Size":   fmt.Sprintf("%v", codeSize),
	}

	return sdk.sendInitialRequest(modelPath, "model", headers)
}

// uploadFiles is a function responsible for uploading files contents to data node
func (sdk VideraSDK) uploadFiles(id string, filesPaths map[string]string, uploadOrder []string) error {
	client := newClient(sdk.defaultMaxRetries, sdk.defaultWaitingTime)

	buffer := make([]byte, sdk.chunkSize)
	offset := int64(0)
	readOffset := int64(-1) //for re-reading file file content, in case of failure

	filesSizes := make([]int64, len(uploadOrder))
	for idx := 0; idx < len(uploadOrder); idx++ {
		fileName := uploadOrder[idx]
		fileSize, _ := getFileSize(filesPaths[fileName])
		filesSizes[idx] = fileSize
	}
	for idx := 0; idx < len(uploadOrder); idx++ {
		fileName := uploadOrder[idx]
		file, err := os.Open(filesPaths[fileName])
		if readOffset != -1 {
			file.Seek(readOffset, 0)
		}
		readOffset = -1
		log.Println("Uploading", fileName, file.Name())
		defer file.Close()
		if err != nil {
			fmt.Println(err)
			return err
		}

		for {
			bytesread, err := file.Read(buffer)

			if err != nil {
				if err == io.EOF {
					if idx == len(uploadOrder)-1 {
						log.Println(err)
						// reached the end of last file, but didn't receive ack from server
						return err
					}
					// finished current file
					file.Close()
					break
				}
				return err
			}

			r := bytes.NewReader(buffer[:bytesread])

			req, _ := http.NewRequest(http.MethodPost, uploadURL, r)
			req.Header.Set("Request-Type", "APPEND")
			req.Header.Set("ID", id)
			req.Header.Set("Offset", strconv.FormatInt(offset, 10))

			res, err := client.Do(req)
			if err != nil {
				log.Println(err)
				return err
			}
			if res.StatusCode != http.StatusOK {
				if res.StatusCode == http.StatusCreated {
					file.Close()
					return nil
				} else if res.Header.Get("Offset") != "" {
					newOffset, _ := strconv.ParseInt(res.Header.Get("Offset"), 10, 64)
					log.Println(fmt.Sprintf("Offset error: changing from %v to %v", offset, newOffset))
					offset = newOffset
					var newIdx int
					newIdx, readOffset, err = getFileFromOffset(filesSizes, offset)
					if err != nil {
						log.Println(err)
						return err
					}

					// file completly uploaded
					if newIdx == len(filesSizes) {
						return nil
					}

					idx = newIdx - 1 //subtracted 1 to cancel the 1 added by loop
					file.Close()
					break
				} else if res.Header.Get("Max-Request-Size") != "" {
					newChunkSize, _ := strconv.ParseInt(res.Header.Get("Max-Request-Size"), 10, 64)
					log.Println(fmt.Sprintf("Chunk size error: changing from %v to %v", sdk.chunkSize, newChunkSize))
					sdk.chunkSize = newChunkSize
					buffer = make([]byte, sdk.chunkSize)
					file.Seek(-int64(bytesread), 1) //revert current read bytes, 1 means relative to current offset
					continue
				}

				return err
			}
			offset += int64(bytesread)
			log.Println(res.Status)
		}
	}

	return nil
}

// getFileFromOffset gets which file contains the current offset
// returns index of file, and offset from file to read at
// if offset equals sum of files sizes, it retruns length of filesSizes as an index
// it returns error in case the offset is larger than all files
func getFileFromOffset(filesSizes []int64, offset int64) (int, int64, error) {
	readOffset := offset
	uploadedFilesSize := int64(0)
	idx := len(filesSizes)
	// detect which file will be resumed upload at
	for i, size := range filesSizes {
		uploadedFilesSize += size
		// the current file should be restarted at
		if uploadedFilesSize > offset {
			idx = i
			break
		}
		// subtract prev uploaded files sizes to get absolute offset at file
		readOffset -= size
	}
	if offset > uploadedFilesSize {
		return 0, int64(0), errors.New("offset is larger than all files")
	}
	return idx, readOffset, nil
}

// UploadVideo is a function responsible for uploading video
func (sdk VideraSDK) UploadVideo(videoPath string) {
	ticker := time.NewTicker(time.Duration(sdk.defaultWaitingTime) * time.Second)

	for trial := 0; trial <= sdk.defaultMaxRetries; trial, _ = trial+1, <-ticker.C {
		err := sdk.updateUploadURL()
		if err != nil {
			log.Println("Can't contact master")
			log.Println(err)
			continue
		}

		id, err := sdk.sendVideoInitialRequest(videoPath)
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
			return
		}
	}
	log.Fatal("File not uploaded")
}

// UploadModel is a function responsible for uploading model
func (sdk VideraSDK) UploadModel(modelPath string, configPath string, codePath string) {
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
		return
	}
	log.Fatal("File not uploaded")
}
