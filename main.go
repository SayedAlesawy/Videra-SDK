package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
)

const chunkSize = 4 << 20 // 4 MB

func getFileSize(filepath string) (int64, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

func sendInitialRequest(filepath string, url string) string {
	fileSize, err := getFileSize(filepath)
	if err != nil {
		log.Fatal(err)
	}

	filename := path.Base(filepath)
	fmt.Println(filename)

	client := &http.Client{}
	req, _ := http.NewRequest(http.MethodPost, url, nil)
	req.Header.Set("Request-Type", "INIT")
	req.Header.Set("Filename", filename)
	req.Header.Set("Filesize", strconv.FormatInt(fileSize, 10))

	// should be put in a loop to check connection error
	res, err := client.Do(req)
	if err != nil {
		// some error should be returned to retry another node
		log.Fatal(err)
	}
	if res.StatusCode != http.StatusCreated {
		log.Fatal(res.StatusCode)
	}

	id := res.Header.Get("ID")
	return id
}

func uploadFile(filepath string, id string, url string) bool {
	file, err := os.Open(filepath)
	if err != nil {
		fmt.Println(err)
		return false
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

			break
		}

		r := bytes.NewReader(buffer[:bytesread])

		req, _ := http.NewRequest(http.MethodPost, url, r)
		req.Header.Set("Request-Type", "APPEND")
		req.Header.Set("ID", id)
		req.Header.Set("Offset", strconv.FormatInt(offset, 10))

		res, err := client.Do(req)
		if err != nil {
			log.Fatal(err)
		}
		if res.StatusCode != http.StatusOK {
			if res.StatusCode == http.StatusAccepted {
				return true
			}
			body, _ := ioutil.ReadAll(res.Body)
			bodyString := string(body)

			log.Fatal(bodyString)
		}
		offset += int64(bytesread)
		fmt.Println(res.Status)
	}

	return false
}

func main() {

	filepath := os.Args[1]
	url := "http://localhost:8080/upload"

	id := sendInitialRequest(filepath, url)
	log.Println("Sent inital request with ID =", id)
	state := uploadFile(filepath, id, url)
	if !state {
		log.Fatal("File not uploaded")
	} else {
		log.Println("File uploaded successfully")
	}

}
