package main

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
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const chunkSize = 4 << 20 // 4 MB

var masterURL string
var uploadURL string
var id string

const defaultMaxRetries = 3
const defaultWaitingTime = 10

var mastersURLS []string
var lastUsedMaster = -1
// newClient is a function that returns customized http client
func newClient(maxRetries int, waitingTime int) *http.Client {
	clientretry := retryablehttp.NewClient()
	clientretry.RetryMax = maxRetries
	clientretry.RetryWaitMin = time.Duration(time.Duration(waitingTime) * time.Second)
	clientretry.RetryWaitMax = time.Duration(time.Duration(waitingTime) * time.Second)

	return clientretry.StandardClient()
}

// updateMasterURL is a function responsible for updating master node ip for future requests
func updateMasterURL() {
	// Should do master discovery by looping on nodes IPs in configuration file
	// and when a node responds with the master IP, it'll be set in master URL
	lastUsedMaster=(lastUsedMaster+1)%len(mastersURLS)
	masterURL = mastersURLS[lastUsedMaster]
}

// updateUploadURL is a function responsible for asking master node for data node upload url
func updateUploadURL() error {
	// send request to master node to get data node upload ip
	// if success, set the new upload URL
	// if fail, return error
	client := newClient(defaultMaxRetries, defaultWaitingTime)
	req, _ := http.NewRequest(http.MethodGet, masterURL, nil)
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
func sendInitialRequest(filepath string) (string, error) {
	fileSize, err := getFileSize(filepath)
	if err != nil {
		log.Fatal(err)
	}

	filename := path.Base(filepath)

	client := newClient(defaultMaxRetries, defaultWaitingTime)
	req, _ := http.NewRequest(http.MethodPost, uploadURL, nil)
	req.Header.Set("Request-Type", "INIT")
	req.Header.Set("Filename", filename)
	req.Header.Set("Filesize", strconv.FormatInt(fileSize, 10))

	res, err := client.Do(req)
	if err != nil {
		log.Println(err)
		return "", err
	}

	if res.StatusCode != http.StatusCreated {
		log.Fatal(res.StatusCode)
	}

	id := res.Header.Get("ID")
	return id, nil
}

// uploadFile is a function responsible for uploading file contents to data node
func uploadFile(filepath string, id string) error {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer file.Close()

	client := &http.Client{}

	buffer := make([]byte, chunkSize)
	offset := int64(0)

	for {
		bytesread, err := file.Read(buffer)

		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
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
			return err
		}
		if res.StatusCode != http.StatusOK {
			if res.StatusCode == http.StatusCreated {
				return nil
			}
			// todo, handle chunk size and offset errors
			return err
		}
		offset += int64(bytesread)
		log.Println(res.Status)
	}
}

func main() {

	if len(os.Args) < 3 {
		log.Fatalln("Error parsing args\nArgs: filename master1 master2 ........")
	}
	filepath := os.Args[1]
	mastersURLS = append(mastersURLS,os.Args[2:]...)
	ticker := time.NewTicker(defaultWaitingTime * time.Second)

	updateMasterURL()
	for trial := 0; trial <= defaultMaxRetries; trial, _ = trial+1, <-ticker.C {
		err := updateUploadURL()
		if err != nil {
			updateMasterURL()
			continue
		}

		id, err := sendInitialRequest(filepath)
		if err != nil {
			continue
		}

		log.Println("Sent inital request with ID =", id)
		err = uploadFile(filepath, id)
		if err == nil {
			log.Println("Upload successful")
			return
		}
	}
	log.Fatal("File not uploaded")
}
