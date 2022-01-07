package s3storage

import (
	"context"
	"fmt"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/tidb/domain"

	//"github.com/pingcap/tidb/domain"
)
//obj domain.S3UserRecord
func Loaddata(s3opt domain.S3UserRecord,database string,tablename string,query string) error {
	conf := export.DefaultConfig()
   /* conf.Host=obj.Host
    conf.BackendOptions.S3.Endpoint=obj.Host
	conf.BackendOptions.S3.AccessKey=obj.Accesss3key
	conf.BackendOptions.S3.SecretAccessKey=obj.Accessid*/
	//version,_ := semver.NewVersion("5.2.0")
   //conf.ServerInfo = export.ServerInfo{
	//   ServerType:    export.ServerTypeTiDB,
	//   ServerVersion: version,
   //}

	conf.Host="127.0.0.1"
	conf.Port=4000
	conf.User="root"
	conf.FileType="csv"
	conf.DBName=database
	conf.BackendOptions.S3.Endpoint=s3opt.Host
	conf.BackendOptions.S3.AccessKey=s3opt.Accessid
	conf.BackendOptions.S3.SecretAccessKey=s3opt.Accesss3key
	conf.BackendOptions.S3.ForcePathStyle=true
    path:=fmt.Sprintf("s3://%s/%s/%s",s3opt.Bucketfors3,database,tablename)

    conf.SQL=query
   // conf.OutputFileTemplate="{{.DB}}.{{.Table}}.{{.Index}}"
	tmpl, _ := export.ParseOutputFileTemplate("result.{{.Index}}")
	conf.OutputFileTemplate=tmpl
    conf.NoSchemas=true
    conf.NoHeader=false
	conf.OutputDirPath=path
	//conf.Rows=1000000
	conf.Threads = 128
	conf.CsvSeparator="|"
	conf.CsvDelimiter = "\""
	conf.EscapeBackslash = false
	conf.FileSize = 1 << 28
	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		return err
	}
	err = dumper.Dump()
	dumper.Close()
	if err != nil {
		return err
	}
	dumper.L().Info("dump data successfully, dumpling will exit now")
	return nil
}




