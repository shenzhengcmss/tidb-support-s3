package s3storage

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/sessionctx"
	"strings"
)

func RemoveDataIfStoreInS3(ctx sessionctx.Context, s3opt, dbname, tablename string, isDropTable bool) bool {
	dom:=domain.GetDomain(ctx)
	if rec,ok := dom.S3server[s3opt]; ok {
		useSSL := false
		host:=strings.TrimLeft(rec.Host,"http://")

		// Initialize minio client object.
		minioClient, err := minio.New(host, &minio.Options{
			Creds:  credentials.NewStaticV4(rec.Accessid, rec.Accesss3key, ""),
			Secure: useSSL,
		})
		if err != nil {
			fmt.Println("make minio client failed: ", err)
		}

		var pprex string
		if isDropTable {
			pprex = fmt.Sprintf("%s/%s/",dbname,tablename)
		} else {
			pprex = fmt.Sprintf("%s/%s/result",dbname,tablename)
		}

		CleanData(minioClient, rec.Bucketfors3, pprex)
		//if isDropTable {
		//	_ = minioClient.RemoveObject(context.Background(), rec.Bucketfors3, fmt.Sprintf("%s/%s/metadata",dbname,tablename), minio.RemoveObjectOptions{GovernanceBypass: true})
		//	_ = minioClient.RemoveObject(context.Background(), rec.Bucketfors3, fmt.Sprintf("%s/%s",dbname,tablename), minio.RemoveObjectOptions{GovernanceBypass: true})
		//}
		return true
	}
	return false
}

func CleanData(c *minio.Client, bucket, prefix string) {
	objectsVersions := make(chan minio.ObjectInfo)
	go func() {
		objectsVersionsInfo := c.ListObjects(context.Background(), bucket, minio.ListObjectsOptions{
			Prefix: prefix,
			Recursive: true,
		})
		for info := range objectsVersionsInfo {
			if info.Err != nil {
				return
			}
			objectsVersions <- info
		}
		close(objectsVersions)
	}()

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}
	for rErr := range c.RemoveObjects(context.Background(), bucket, objectsVersions, opts) {
		fmt.Println("Error detected during deletion: ", rErr)
	}
}


func RemoveObjectsForDB(ctx sessionctx.Context, s3opts []string, dbname string) {
	dom:=domain.GetDomain(ctx)
	for _, s3opt := range s3opts {
		if rec,ok := dom.S3server[s3opt]; ok {
			useSSL := false
			host := strings.TrimLeft(rec.Host, "http://")

			// Initialize minio client object.
			minioClient, err := minio.New(host, &minio.Options{
				Creds:  credentials.NewStaticV4(rec.Accessid, rec.Accesss3key, ""),
				Secure: useSSL,
			})
			if err != nil {
				fmt.Println("make minio client failed: ", err)
			}

			CleanData(minioClient, rec.Bucketfors3, fmt.Sprintf("%s/",dbname))
			err = minioClient.RemoveObject(context.Background(), rec.Bucketfors3, fmt.Sprintf("%s",dbname),
				minio.RemoveObjectOptions{GovernanceBypass: true, ForceDelete: true})
			fmt.Println("remove  db err is ", err)
		}
	}
}
