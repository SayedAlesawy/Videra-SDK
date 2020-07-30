package main

import (
	"flag"
	"log"

	viderasdk "github.com/SayedAlesawy/Videra-SDK/sdk"
)

func main() {
	videoPath := flag.String("video", "", "Path to video file")
	modelPath := flag.String("model", "", "Path to model file")
	configPath := flag.String("config", "", "Path to config file")
	codePath := flag.String("code", "", "Path to code file")
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatal("No master ip provided")
	}

	masterURL := flag.Args()[0]
	vSDK := viderasdk.SDKInstance(masterURL)
	vSDK.UploadModel(*modelPath, *configPath, *codePath)
	vSDK.UploadVideo(*videoPath)
}
