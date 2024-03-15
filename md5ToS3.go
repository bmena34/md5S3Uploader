package md5S3Uploader

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Uploader struct {
	File   []byte
	Bucket string
	Key    string
}

func getConfig(ctx context.Context) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return aws.Config{}, err
	}
	return cfg, err
}

func makeMd5(ctx context.Context, file []byte) (checksum string, newFile []byte) {
	if ctx.Done() != nil {
		h := md5.New()
		h.Write(file)
		checksum = base64.StdEncoding.EncodeToString(h.Sum(nil))
	}

	return checksum, file
}

func uploadS3(ctx context.Context, cfg aws.Config, file []byte, bucket string, key string) error {
	// Create an Amazon S3 service client
	client := s3.NewFromConfig(cfg)

	checksum, file := makeMd5(ctx, file)

	// Create an object
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:     &bucket,
		Key:        &key,
		Body:       bytes.NewReader(file),
		ContentMD5: &checksum,
	})
	if err != nil {
		log.Printf("Got an error uploading the file: %v", err)
	}
	log.Printf("Successfully uploaded %s to %s\n", key, bucket)

	return err
}

func (u *Uploader) Upload(ctx context.Context) error {
	cfg, err := getConfig(ctx)
	if err != nil {
		log.Printf("unable to load AWS config, %v", err)
	}
	err = uploadS3(ctx, cfg, u.File, u.Bucket, u.Key)
	if err != nil {
		log.Printf("unable to upload to S3, %v", err)
	}
	return err
}
