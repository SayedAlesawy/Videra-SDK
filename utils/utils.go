package utils

import (
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// ValidateFlags validates if are flags exist
func ValidateFlags(args ...string) error {
	for _, arg := range args {
		if arg == "" {
			return errors.New("Missing flag")
		}
	}
	return nil
}

// NewClient is a function that returns customized http client
func NewClient(maxRetries int, waitingTime int) *http.Client {
	clientretry := retryablehttp.NewClient()
	clientretry.RetryMax = maxRetries
	clientretry.RetryWaitMin = time.Duration(time.Duration(waitingTime) * time.Second)
	clientretry.RetryWaitMax = time.Duration(time.Duration(waitingTime) * time.Second)

	return clientretry.StandardClient()
}

// GetFileSize is a function to get file size
func GetFileSize(filepath string) (int64, error) {
	fi, err := os.Stat(filepath)
	if err != nil {
		return 0, err
	}

	return fi.Size(), nil
}

// GetFileFromOffset gets which file contains the current offset
// returns index of file, and offset from file to read at
// if offset equals sum of files sizes, it retruns length of filesSizes as an index
// it returns error in case the offset is larger than all files
func GetFileFromOffset(filesSizes []int64, offset int64) (int, int64, error) {
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
