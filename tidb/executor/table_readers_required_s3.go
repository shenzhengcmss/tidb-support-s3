// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package executor

import (
	"bufio"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/json"
	"github.com/pingcap/tidb/util/chunk"
	"go.uber.org/zap"
	"strconv"
	"strings"
	"sync"
	gotime "time"
)

type s3RowsSelectResult struct {
	retTypes        []*types.FieldType
	totalRows       int
	count           int
	objch  chan minio.ObjectInfo
	rowChunk chan *chunk.Chunk
	minc *minio.Client
	bucketName string
	s3query string
}

func (r *s3RowsSelectResult) NextRaw(context.Context) ([]byte, error) { return nil, nil }
func (r *s3RowsSelectResult) Close() error                            { return nil }

func (r *s3RowsSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	chunkcp,receive :=  <-r.rowChunk
	if !receive {
		return nil
	}
	chk.Append(chunkcp, 0, chunkcp.NumRows())
	return nil
}

/*func (r *s3RowsSelectResult) genOneRow() chunk.Row {
	row := chunk.MutRowFromTypes(r.retTypes)
	for i := range r.retTypes {
		row.SetValue(i, r.genValue(r.retTypes[i]))
	}
	return row.ToRow()
}
*/

func (r *s3RowsSelectResult)ProductChunk(ctx context.Context,e Executor) error {
	if r.objch == nil {
		fmt.Println("object is nil")
		return nil
	}
	threadNum := 10
	wg := sync.WaitGroup{}
	wg.Add(threadNum)
	var end bool
	for i := 0; i < threadNum; i++ {
		go func(i int) {
			defer wg.Done()
			for {
				s, received := <- r.objch
				if !received {
					end = true
					break
				}
				var scanner *bufio.Scanner
				var isQuery = false
				fmt.Println("object is  ", s.Key)
				fmt.Println(r.s3query, " start time is ", gotime.Now())
				if r.s3query != "" {
					results, err := r.minc.SelectObjectContent(context.Background(), r.bucketName, s.Key, minio.SelectObjectOptions{
						Expression:         r.s3query,
						ExpressionType:      minio.QueryExpressionTypeSQL,
						InputSerialization:  minio.SelectObjectInputSerialization{CSV: &minio.CSVInputOptions{
							FieldDelimiter:  "|",
							FileHeaderInfo: minio.CSVFileHeaderInfoUse,
						}},
						OutputSerialization: minio.SelectObjectOutputSerialization{CSV: &minio.CSVOutputOptions{
							FieldDelimiter:  "|",
						}},
					})
					if results!= nil {
						defer results.Close()
					}
					if err != nil {
						fmt.Println("select object err: ", err)
						return
					}
					scanner = bufio.NewScanner(results)
					isQuery = true
				} else {
					ob1, err := r.minc.GetObject(context.Background(), r.bucketName, s.Key, minio.GetObjectOptions{})
					if err != nil {
						fmt.Println("get object err: ", err)
					}

					scanner = bufio.NewScanner(ob1)
				}
				fmt.Println(r.s3query, " end time is ", gotime.Now(), " and scan start")
				var isFirst = true
				var count uint64
				var chk *chunk.Chunk
				var chunkcp *chunk.Chunk
				//4k one chunk
				var lastPush bool
				chk = newFirstChunk(e)
				for scanner.Scan(){
					//fmt.Println("good")
					if !isQuery && isFirst {
						isFirst = false
						continue
					}
					lastPush = false
					if count % 1024 == 0 {
						chk.Reset()
						chunkcp = chk.CopyConstruct()
					}
					ab:=strings.Split(scanner.Text(),"|")
					chunkcp.AppendRow(r.genOneRow(ab))
					count++
					if count % 1024 == 0 {
						r.rowChunk <- chunkcp
						lastPush = true
					}
				}
				if lastPush == false {
					r.rowChunk <- chunkcp
				}
				fmt.Println(r.s3query," end scan time is ", gotime.Now())
			}
		}(i)
	}
	wg.Wait()
	if end == true {
		close(r.rowChunk)
	}
	return nil
}

func (r *s3RowsSelectResult) genOneRow(line []string) chunk.Row {
	row := chunk.MutRowFromTypes(r.retTypes)
	for i := range r.retTypes {
		row.SetValue(i, r.genValue(line[i],r.retTypes[i]))
	}
	return row.ToRow()
}

/*
func (r *s3RowsSelectResult) genValue(valType *types.FieldType) interface{} {

	switch valType.Tp {
	case mysql.TypeLong, mysql.TypeLonglong:
		//return int64(rand.Int())
		return int64(r.count)
	case mysql.TypeDouble:
		//return rand.Float64()
		return float64(r.count)/float64(100)
	default:
		panic("not implement")
	}
}
*/

func (r *s3RowsSelectResult) genValue(val string,valType *types.FieldType) interface{} {
	//fmt.Println("value is ", val, valType.Tp, valType.Elems)
	switch valType.Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		//return int64(rand.Int())
		fin, err := strconv.ParseInt(val, 10, 64)
		if err!=nil{
			fmt.Println("parse int failed: ", err)
			return nil
		}
		return fin
	case mysql.TypeDouble:
		//return rand.Float64()
		//return float64(r.count)/float64(100)
		f, err := strconv.ParseFloat(val, 64)
		if err!=nil{
			fmt.Println("parse float64 failed: ", err)
			return nil
		}
		return f
	case mysql.TypeFloat:
		f, err := strconv.ParseFloat(val, 32)
		if err!=nil{
			fmt.Println("parse float32 failed: ", err)
			return nil
		}
		return float32(f)
	case mysql.TypeVarchar, mysql.TypeTinyBlob,	mysql.TypeMediumBlob, mysql.TypeLongBlob,
	mysql.TypeBlob, mysql.TypeVarString, mysql.TypeString:
		if string(val[0]) == "\"" {
			l := len(val)
			val = val[1:l-1]
		}
		val = strings.ReplaceAll(val, "\"\"", "\"")
		return val
	case mysql.TypeDuration:
		timeLayoutStr := "15:04:05"
		t, err := gotime.Parse(timeLayoutStr, val)
		if err!=nil{
			fmt.Println("parse time failed: ", err)
			return nil
		}
		duration ,converr := types.NewTime(types.FromGoTime(t), mysql.TypeDuration, types.MaxFsp).ConvertToDuration()
		if converr!=nil{
			fmt.Println("convert time to duration failed: ", err)
			return nil
		}
		return duration
	case mysql.TypeDatetime, mysql.TypeTimestamp:
		val = strings.ReplaceAll(val, "\"\"", "\"")
		timeLayoutStr := "2006-01-02 15:04:05"
		t, err := gotime.Parse(timeLayoutStr, val)
		if err!=nil{
			fmt.Println("parse time failed: ", err)
			return nil
		}
		return types.NewTime(types.FromGoTime(t), valType.Tp, types.MaxFsp)
	case mysql.TypeDate:
		if string(val[0]) == "\"" {
			l := len(val)
			val = val[1:l-1]
		}

		timeLayoutStr := "2006-01-02"
		t, err := gotime.Parse(timeLayoutStr, val)
		if err!=nil{
			fmt.Println("parse time failed: ", err)
			return nil
		}
		return types.NewTime(types.FromGoTime(t), mysql.TypeDate, types.DefaultFsp)
	case mysql.TypeEnum:
		val = strings.ReplaceAll(val, "\"\"", "\"")
		enum, err := types.ParseEnum(valType.Elems, val, mysql.DefaultCollationName)
		if err!=nil{
			fmt.Println("parse enum failed: ", err)
			return nil
		}
		return enum
	case mysql.TypeSet:
		val = strings.ReplaceAll(val, "\"\"", "\"")
		set, err := types.ParseSet(valType.Elems, val, mysql.DefaultCollationName)
		if err!=nil{
			fmt.Println("parse set failed: ", err)
			return nil
		}
		return set
	case mysql.TypeJSON:
		j, err := json.ParseBinaryFromString(val)
		if err!=nil{
			fmt.Println("parse json failed: ", err)
			return nil
		}
		return j
	case mysql.TypeNewDecimal:
		dec := new(types.MyDecimal)
		err := dec.FromString([]byte(val))
		if err != nil {
			fmt.Println("encountered error", zap.Error(err), zap.String("DecimalStr", val))
		}
		return dec
	case mysql.TypeBit:
		bit, err := types.NewBitLiteral(val)
		if err!=nil{
			fmt.Println("parse bit failed: ", err)
			return nil
		}
		return bit
	default:
		panic("not implement")
	}
}


func s3SelectResult(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []int) (distsql.SelectResult, error) {

	return &s3RowsSelectResult{
		retTypes:        fieldTypes,
		totalRows:       1024,
	}, nil
}



