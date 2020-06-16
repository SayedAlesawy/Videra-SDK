package main

import (
	"bytes"
	"fmt"
	"io"
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

const maxRetries = 3
const waitingTime = 10

// updateMasterURL is a function responsible for updating master node ip for future requests
func updateMasterURL() {
	// Should do master discovery by looping on nodes IPs in configuration file
	// and when a node responds with the master IP, it'll be set in master URL

	masterURL = ""
}

// updateUploadURL is a function responsible for asking master node for data node upload url
func updateUploadURL() error {

	// send request to master node to get data node upload ip
	// if success, set the new upload URL
	// if fail, return error

	uploadURL = "http://localhost:8080/upload"
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

	clientretry := retryablehttp.NewClient()
	clientretry.RetryMax = maxRetries
	clientretry.RetryWaitMin = time.Duration(waitingTime * time.Second)
	clientretry.RetryWaitMax = time.Duration(waitingTime * time.Second)

	client := clientretry.StandardClient()
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

	filepath := os.Args[1]

	ticker := time.NewTicker(waitingTime * time.Second)
	for trial := 0; trial <= maxRetries; trial, _ = trial+1, <-ticker.C {
		updateMasterURL()
		err := updateUploadURL()
		if err == nil {
			continue
		}
		id, err := sendInitialRequest(filepath)
		if err == nil {
			continue
		}
		log.Println("Sent inital request with ID =", id)
		err = uploadFile(filepath, id)
		if err != nil {
			log.Println("Upload successful")
			return
		}
	}
	log.Fatal("File not uploaded")
}
