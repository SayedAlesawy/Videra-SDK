package viderasdk

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/SayedAlesawy/Videra-SDK/utils"
)

// uploadFiles is a function responsible for uploading files contents to data node
func (sdk VideraSDK) uploadFiles(id string, filesPaths map[string]string, uploadOrder []string) error {
	client := utils.NewClient(sdk.defaultMaxRetries, sdk.defaultWaitingTime)

	buffer := make([]byte, sdk.chunkSize)
	offset := int64(0)
	readOffset := int64(-1) //for re-reading file file content, in case of failure

	filesSizes := make([]int64, len(uploadOrder))
	for idx := 0; idx < len(uploadOrder); idx++ {
		fileName := uploadOrder[idx]
		fileSize, _ := utils.GetFileSize(filesPaths[fileName])
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
					newIdx, readOffset, err = utils.GetFileFromOffset(filesSizes, offset)
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
