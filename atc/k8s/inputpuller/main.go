package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"code.cloudfoundry.org/lager"
	"github.com/concourse/concourse/atc/k8s"
)

func main() {
	bucketType := flag.String("bucketType", "", "bucket type, allowed values are s3 and gcs")
	sourceKey := flag.String("sourceKey", "", "source to be downloaded")
	destionationPath := flag.String("destionationPath", "", "location to place the downloaded artifact")

	flag.Parse()

	fmt.Println("ready to download input")
	url := os.Getenv("BUCKET_URL")
	bucketConfig := k8s.BucketConfig{
	  Type: *bucketType,
	  URL: url,
	  Secret: "notasecret",
	}
	k8s.Pull(lager.NewLogger("input_puller"), context.Background(), bucketConfig, *sourceKey, *destionationPath)
}

