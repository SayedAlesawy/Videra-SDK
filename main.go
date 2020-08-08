package main

import (
	"flag"

	viderasdk "github.com/SayedAlesawy/Videra-SDK/sdk"
	"github.com/SayedAlesawy/Videra-SDK/utils"
)

func main() {
	videoPath := flag.String("video", "", "Path to video file")
	modelPath := flag.String("model", "", "Path to model file")
	configPath := flag.String("config", "", "Path to config file")
	codePath := flag.String("code", "", "Path to code file")
	flag.Parse()

	flags := []string{*videoPath, *modelPath, *configPath, *codePath}
	err := utils.ValidateFlags(flags...)
	if err != nil {
		flag.PrintDefaults()
		return
	}

	vSDK := viderasdk.SDKInstance()
	vSDK.UploadJob(*videoPath, *modelPath, *configPath, *codePath)
}
