// Copyright 2018 PingCAP, Inc.
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
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tipb/go-tipb"
	"runtime"
	"sort"
	"strings"

	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/ranger"
	"github.com/pingcap/tidb/util/stringutil"
)

// make sure `TableReaderExecutor` implements `Executor`.
var _ Executor = &TableReaderExecutor{}

// selectResultHook is used to hack distsql.SelectWithRuntimeStats safely for testing.
type selectResultHook struct {
	selectResultFunc func(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
		fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []int) (distsql.SelectResult, error)
}

func (sr selectResultHook) SelectResult(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []int, rootPlanID int) (distsql.SelectResult, error) {
	if sr.selectResultFunc == nil {
		return distsql.SelectWithRuntimeStats(ctx, sctx, kvReq, fieldTypes, fb, copPlanIDs, rootPlanID)
	}
	return sr.selectResultFunc(ctx, sctx, kvReq, fieldTypes, fb, copPlanIDs)
}

type kvRangeBuilder interface {
	buildKeyRange(pid int64, ranges []*ranger.Range) ([]kv.KeyRange, error)
	buildKeyRangeSeparately(ranges []*ranger.Range) ([]int64, [][]kv.KeyRange, error)
}

// TableReaderExecutor sends DAG request and reads table data from kv layer.
type TableReaderExecutor struct {
	baseExecutor
	objch     chan minio.ObjectInfo
	rowChunk chan *chunk.Chunk
	minclient *minio.Client
	s3bucket  string
	s3query   string
	table     table.Table

	// The source of key ranges varies from case to case.
	// It may be calculated from PhysicalPlan by executorBuilder, or calculated from argument by dataBuilder;
	// It may be calculated from ranger.Ranger, or calculated from handles.
	// The table ID may also change because of the partition table, and causes the key range to change.
	// So instead of keeping a `range` struct field, it's better to define a interface.
	kvRangeBuilder
	// TODO: remove this field, use the kvRangeBuilder interface.
	ranges []*ranger.Range

	// kvRanges are only use for union scan.
	kvRanges    []kv.KeyRange
	dagPB       *tipb.DAGRequest
	startTS     uint64
	txnScope    string
	isStaleness bool
	// columns are only required by union scan and virtual column.
	columns []*model.ColumnInfo

	// resultHandler handles the order of the result. Since (MAXInt64, MAXUint64] stores before [0, MaxInt64] physically
	// for unsigned int.
	resultHandler *tableResultHandler
	feedback      *statistics.QueryFeedback
	plans         []plannercore.PhysicalPlan
	tablePlan     plannercore.PhysicalPlan

	memTracker       *memory.Tracker
	selectResultHook // for testing

	keepOrder bool
	desc      bool
	streaming bool
	storeType kv.StoreType
	// corColInFilter tells whether there's correlated column in filter.
	corColInFilter bool
	// corColInAccess tells whether there's correlated column in access conditions.
	corColInAccess bool
	// virtualColumnIndex records all the indices of virtual columns and sort them in definition
	// to make sure we can compute the virtual column in right order.
	virtualColumnIndex []int
	// virtualColumnRetFieldTypes records the RetFieldTypes of virtual columns.
	virtualColumnRetFieldTypes []*types.FieldType
	// batchCop indicates whether use super batch coprocessor request, only works for TiFlash engine.
	batchCop bool

	// extraPIDColumnIndex is used for partition reader to add an extra partition ID column.
	extraPIDColumnIndex offsetOptional
}

// offsetOptional may be a positive integer, or invalid.
type offsetOptional int

func newOffset(i int) offsetOptional {
	return offsetOptional(i + 1)
}

func (i offsetOptional) valid() bool {
	return i != 0
}

func (i offsetOptional) value() int {
	return int(i - 1)
}

// Open initializes necessary variables for using this executor.
func (e *TableReaderExecutor) Open(ctx context.Context) error {
	if span := opentracing.SpanFromContext(ctx); span != nil && span.Tracer() != nil {
		span1 := span.Tracer().StartSpan("TableReaderExecutor.Open", opentracing.ChildOf(span.Context()))
		defer span1.Finish()
		ctx = opentracing.ContextWithSpan(ctx, span1)
	}

	if t2, ok := e.plans[0].(*plannercore.PhysicalTableScan); ok && t2.Table != nil {
		dom := domain.GetDomain(e.ctx)


		p, pid := t2.IsPartition()
		//fmt.Println("table is ", t2.Table.Name, p, t2.ExplainInfo())
		var pname string
		var rec domain.S3UserRecord
		var exist bool
		if p {

			//fmt.Println("partition is ", t2.Table.GetPartitionInfo().Definitions, "adding partition ", t2.Table.GetPartitionInfo().AddingDefinitions)
			//if pname == "p0" {
			//	t2.Table.Partition.Definitions[0].S3opt = "testspx"
			//}
			pname = t2.Table.GetPartitionInfo().GetNameByID(pid)
			rec, exist = dom.S3server[t2.Table.GetPartitionInfo().GetS3PartitionByID(pid)]
		} else {
			rec, exist = dom.S3server[t2.Table.S3opt]
		}

		fmt.Println("t2 is ", t2.TP(), t2.ExplainInfo(), t2.Table.Name, "table is partion ", p, pname)
		if exist {

			useSSL := false

			host := strings.TrimLeft(rec.Host, "http://")

			// Initialize minio client object.
			minioClient, err := minio.New(host, &minio.Options{
				Creds:  credentials.NewStaticV4(rec.Accessid, rec.Accesss3key, ""),
				Secure: useSSL,
			})
			if err != nil {
				fmt.Println(err)
			}
			dbname := t2.DBName.L
			if dbname == "" {
				dbname = e.ctx.GetSessionVars().CurrentDB
			}
			tablename := t2.Table.Name.String()
			e.s3bucket = rec.Bucketfors3

			//idx := fmt.Sprintf("%s.%s", dbname, tablename)
			//var s3finalquery string
			//fmt.Println("s3query idx is ", idx, " s3query is ", e.ctx.GetSessionVars().S3querys[idx])
			//if query, ok := e.ctx.GetSessionVars().S3querys[idx]; ok {
			//	if agg, exist := query.S3query["agg"]; exist {
			//		s3finalquery = "SELECT " + agg
			//	} else {
			//		s3finalquery = fmt.Sprintf("%s", "SELECT * ")
			//	}
			//
			//	s3finalquery = fmt.Sprintf("%s FROM s3object", s3finalquery)
			//
			//	if pred, exist := query.S3query["where"]; exist {
			//		s3finalquery = fmt.Sprintf("%s WHERE %s", s3finalquery, pred)
			//	}
			//
			//	if limit, exist := query.S3query["limit"]; exist {
			//		s3finalquery = fmt.Sprintf("%s %s", s3finalquery, limit)
			//	}
			//}
			//ctx, cancel := context.WithCancel(context.TODO())

			e.objch = make(chan minio.ObjectInfo, 1000)
			//product object
			go func() {
				defer func() {
					if err := recover(); err != nil && (err.(runtime.Error)).Error() == "send on closed channel" {
						fmt.Println("send on closed channel")
					} else {
						close(e.objch)
					}
				}()

				var pprex string
				if p {
					pprex = fmt.Sprintf("%s/%s/%s/result", dbname, tablename, pname)
				} else {
					pprex = fmt.Sprintf("%s/%s/result", dbname, tablename)
				}
				for objectCh := range minioClient.ListObjects(context.Background(), rec.Bucketfors3, minio.ListObjectsOptions{
					Prefix:    pprex,
					Recursive: true,
				}) {
					e.objch <- objectCh
				}
			}()


			r := make(map[string]string)
			res := GetS3query(e.tablePlan, r)

			e.s3query = fmt.Sprintf("%v %v %v", res["select"], res["where"], res["limit"])

			e.minclient = minioClient

			e.selectResultHook = selectResultHook{s3SelectResult}
			// product table row data
 			e.rowChunk = make(chan *chunk.Chunk,1024)

		} else {
			if t2.Table.S3opt != "" {
				return dbterror.ClassDDL.NewStdErr(errno.ErrErrorOnRead, mysql.Message("s3options is not valid in mysql.serverobject", nil))
			}
		}

		//	fmt.Printf("open table is %s \n",t2.Table.Name.String())
		//	fmt.Printf("scan table is %s. \n",t2.Table.Name)
		//	e.selectResultHook=selectResultHook{mockSelectResult}
	}

	e.memTracker = memory.NewTracker(e.id, -1)
	e.memTracker.AttachTo(e.ctx.GetSessionVars().StmtCtx.MemTracker)

	var err error
	if e.corColInFilter {
		if e.storeType == kv.TiFlash {
			execs, _, err := constructDistExecForTiFlash(e.ctx, e.tablePlan)
			if err != nil {
				return err
			}
			e.dagPB.RootExecutor = execs[0]
		} else {
			e.dagPB.Executors, _, err = constructDistExec(e.ctx, e.plans)
			if err != nil {
				return err
			}
		}
	}
	if e.runtimeStats != nil {
		collExec := true
		e.dagPB.CollectExecutionSummaries = &collExec
	}
	if e.corColInAccess {
		ts := e.plans[0].(*plannercore.PhysicalTableScan)
		e.ranges, err = ts.ResolveCorrelatedColumns()
		if err != nil {
			return err
		}
	}

	e.resultHandler = &tableResultHandler{}
	if e.feedback != nil && e.feedback.Hist != nil {
		// EncodeInt don't need *statement.Context.
		var ok bool
		e.ranges, ok = e.feedback.Hist.SplitRange(nil, e.ranges, false)
		if !ok {
			e.feedback.Invalidate()
		}
	}
	firstPartRanges, secondPartRanges := distsql.SplitRangesAcrossInt64Boundary(e.ranges, e.keepOrder, e.desc, e.table.Meta() != nil && e.table.Meta().IsCommonHandle)

	// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
	// Calculate the kv ranges here, UnionScan rely on this kv ranges.
	if e.table.Meta() != nil && e.table.Meta().TempTableType != model.TempTableNone {
		kvReq, err := e.buildKVReq(ctx, firstPartRanges)
		if err != nil {
			return err
		}
		e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)
		if len(secondPartRanges) != 0 {
			kvReq, err = e.buildKVReq(ctx, secondPartRanges)
			if err != nil {
				return err
			}
			e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)
		}
		return nil
	}

	firstResult, err := e.buildResp(ctx, firstPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	if len(secondPartRanges) == 0 {
		e.resultHandler.open(nil, firstResult)
		return nil
	}
	var secondResult distsql.SelectResult
	secondResult, err = e.buildResp(ctx, secondPartRanges)
	if err != nil {
		e.feedback.Invalidate()
		return err
	}
	e.resultHandler.open(firstResult, secondResult)
	return nil
}

// Next fills data into the chunk passed by its caller.
// The task was actually done by tableReaderHandler.
func (e *TableReaderExecutor) Next(ctx context.Context, req *chunk.Chunk) error {
	if e.table.Meta() != nil && e.table.Meta().TempTableType != model.TempTableNone {
		// Treat temporary table as dummy table, avoid sending distsql request to TiKV.
		req.Reset()
		return nil
	}

	logutil.Eventf(ctx, "table scan table: %s, range: %v", stringutil.MemoizeStr(func() string {
		var tableName string
		if meta := e.table.Meta(); meta != nil {
			tableName = meta.Name.L
		}
		return tableName
	}), e.ranges)
	if err := e.resultHandler.nextChunk(ctx, req); err != nil {
		e.feedback.Invalidate()
		return err
	}

	err := FillVirtualColumnValue(e.virtualColumnRetFieldTypes, e.virtualColumnIndex, e.schema, e.columns, e.ctx, req)
	if err != nil {
		return err
	}

	// When 'select ... for update' work on a partitioned table, the table reader should
	// add the partition ID as an extra column. The SelectLockExec need this information
	// to construct the lock key.
	physicalID := getPhysicalTableID(e.table)
	if e.extraPIDColumnIndex.valid() {
		fillExtraPIDColumn(req, e.extraPIDColumnIndex.value(), physicalID)
	}

	return nil
}

func fillExtraPIDColumn(req *chunk.Chunk, extraPIDColumnIndex int, physicalID int64) {
	numRows := req.NumRows()
	pidColumn := chunk.NewColumn(types.NewFieldType(mysql.TypeLonglong), numRows)
	for i := 0; i < numRows; i++ {
		pidColumn.AppendInt64(physicalID)
	}
	req.SetCol(extraPIDColumnIndex, pidColumn)
}

// Close implements the Executor Close interface.
func (e *TableReaderExecutor) Close() error {
	if e.table.Meta() != nil && e.table.Meta().TempTableType != model.TempTableNone {
		return nil
	}

	var err error
	if e.resultHandler != nil {
		err = e.resultHandler.Close()
	}
	e.kvRanges = e.kvRanges[:0]
	e.ctx.StoreQueryFeedback(e.feedback)
	return err
}

// buildResp first builds request and sends it to tikv using distsql.Select. It uses SelectResult returned by the callee
// to fetch all results.
func (e *TableReaderExecutor) buildResp(ctx context.Context, ranges []*ranger.Range) (distsql.SelectResult, error) {
	if e.storeType == kv.TiFlash && e.kvRangeBuilder != nil {
		// TiFlash cannot support to access multiple tables/partitions within one KVReq, so we have to build KVReq for each partition separately.
		kvReqs, err := e.buildKVReqSeparately(ctx, ranges)
		if err != nil {
			return nil, err
		}
		var results []distsql.SelectResult
		for _, kvReq := range kvReqs {
			result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}
		return distsql.NewSerialSelectResults(results), nil
	}

	kvReq, err := e.buildKVReq(ctx, ranges)
	if err != nil {
		return nil, err
	}
	e.kvRanges = append(e.kvRanges, kvReq.KeyRanges...)

	result, err := e.SelectResult(ctx, e.ctx, kvReq, retTypes(e), e.feedback, getPhysicalPlanIDs(e.plans), e.id)
	if err != nil {
		return nil, err
	}

	if se, ok := result.(*s3RowsSelectResult); ok {
		var fields []*types.FieldType
		for idx, col := range e.schema.Columns {
			if col.ID != model.ExtraHandleID {
				fields = append(fields, e.retFieldTypes[idx])
			}
		}

		//fmt.Println("plan is ", e.tablePlan.ExplainInfo())
		se.objch = e.objch
		se.retTypes = fields
		se.minc = e.minclient
		se.bucketName = e.s3bucket
		se.s3query = e.s3query
		se.rowChunk = e.rowChunk
		go se.ProductChunk(ctx,e)
		//fmt.Printf("len is %d,retype is %d \n",len(se.objch),len(se.retTypes))
	}

	return result, nil
}


func (e *TableReaderExecutor) buildKVReqSeparately(ctx context.Context, ranges []*ranger.Range) ([]*kv.Request, error) {
	pids, kvRanges, err := e.kvRangeBuilder.buildKeyRangeSeparately(ranges)
	if err != nil {
		return nil, err
	}
	var kvReqs []*kv.Request
	for i, kvRange := range kvRanges {
		e.kvRanges = append(e.kvRanges, kvRange...)
		if err := updateExecutorTableID(ctx, e.dagPB.RootExecutor, pids[i], true); err != nil {
			return nil, err
		}
		var builder distsql.RequestBuilder
		reqBuilder := builder.SetKeyRanges(kvRange)
		kvReq, err := reqBuilder.
			SetDAGRequest(e.dagPB).
			SetStartTS(e.startTS).
			SetDesc(e.desc).
			SetKeepOrder(e.keepOrder).
			SetStreaming(e.streaming).
			SetTxnScope(e.txnScope).
			SetFromSessionVars(e.ctx.GetSessionVars()).
			SetFromInfoSchema(e.ctx.GetInfoSchema()).
			SetMemTracker(e.memTracker).
			SetStoreType(e.storeType).
			SetAllowBatchCop(e.batchCop).Build()
		if err != nil {
			return nil, err
		}
		kvReqs = append(kvReqs, kvReq)
	}
	return kvReqs, nil
}

func (e *TableReaderExecutor) buildKVReq(ctx context.Context, ranges []*ranger.Range) (*kv.Request, error) {
	var builder distsql.RequestBuilder
	var reqBuilder *distsql.RequestBuilder
	if e.kvRangeBuilder != nil {
		kvRange, err := e.kvRangeBuilder.buildKeyRange(getPhysicalTableID(e.table), ranges)
		if err != nil {
			return nil, err
		}
		reqBuilder = builder.SetKeyRanges(kvRange)
	} else {
		reqBuilder = builder.SetHandleRanges(e.ctx.GetSessionVars().StmtCtx, getPhysicalTableID(e.table), e.table.Meta() != nil && e.table.Meta().IsCommonHandle, ranges, e.feedback)
	}
	reqBuilder.
		SetDAGRequest(e.dagPB).
		SetStartTS(e.startTS).
		SetDesc(e.desc).
		SetKeepOrder(e.keepOrder).
		SetStreaming(e.streaming).
		SetTxnScope(e.txnScope).
		SetIsStaleness(e.isStaleness).
		SetFromSessionVars(e.ctx.GetSessionVars()).
		SetFromInfoSchema(e.ctx.GetInfoSchema()).
		SetMemTracker(e.memTracker).
		SetStoreType(e.storeType).
		SetAllowBatchCop(e.batchCop)
	return reqBuilder.Build()
}

func buildVirtualColumnIndex(schema *expression.Schema, columns []*model.ColumnInfo) []int {
	virtualColumnIndex := make([]int, 0, len(columns))
	for i, col := range schema.Columns {
		if col.VirtualExpr != nil {
			virtualColumnIndex = append(virtualColumnIndex, i)
		}
	}
	sort.Slice(virtualColumnIndex, func(i, j int) bool {
		return plannercore.FindColumnInfoByID(columns, schema.Columns[virtualColumnIndex[i]].ID).Offset <
			plannercore.FindColumnInfoByID(columns, schema.Columns[virtualColumnIndex[j]].ID).Offset
	})
	return virtualColumnIndex
}

// buildVirtualColumnInfo saves virtual column indices and sort them in definition order
func (e *TableReaderExecutor) buildVirtualColumnInfo() {
	e.virtualColumnIndex = buildVirtualColumnIndex(e.Schema(), e.columns)
	if len(e.virtualColumnIndex) > 0 {
		e.virtualColumnRetFieldTypes = make([]*types.FieldType, len(e.virtualColumnIndex))
		for i, idx := range e.virtualColumnIndex {
			e.virtualColumnRetFieldTypes[i] = e.schema.Columns[idx].RetType
		}
	}
}

type tableResultHandler struct {
	// If the pk is unsigned and we have KeepOrder=true and want ascending order,
	// `optionalResult` will handles the request whose range is in signed int range, and
	// `result` will handle the request whose range is exceed signed int range.
	// If we want descending order, `optionalResult` will handles the request whose range is exceed signed, and
	// the `result` will handle the request whose range is in signed.
	// Otherwise, we just set `optionalFinished` true and the `result` handles the whole ranges.
	optionalResult distsql.SelectResult
	result         distsql.SelectResult

	optionalFinished bool
}

func (tr *tableResultHandler) open(optionalResult, result distsql.SelectResult) {
	if optionalResult == nil {
		tr.optionalFinished = true
		tr.result = result
		return
	}
	tr.optionalResult = optionalResult
	tr.result = result
	tr.optionalFinished = false
}

func (tr *tableResultHandler) nextChunk(ctx context.Context, chk *chunk.Chunk) error {
	if !tr.optionalFinished {
		err := tr.optionalResult.Next(ctx, chk)
		if err != nil {
			return err
		}
		if chk.NumRows() > 0 {
			return nil
		}
		tr.optionalFinished = true
	}
	return tr.result.Next(ctx, chk)
}

func (tr *tableResultHandler) nextRaw(ctx context.Context) (data []byte, err error) {
	if !tr.optionalFinished {
		data, err = tr.optionalResult.NextRaw(ctx)
		if err != nil {
			return nil, err
		}
		if data != nil {
			return data, nil
		}
		tr.optionalFinished = true
	}
	data, err = tr.result.NextRaw(ctx)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (tr *tableResultHandler) Close() error {
	err := closeAll(tr.optionalResult, tr.result)
	tr.optionalResult, tr.result = nil, nil
	return err
}

func GetS3query(tablePlan plannercore.PhysicalPlan, result map[string]string) map[string]string {
	switch plan := tablePlan.(type) {
	case *plannercore.PhysicalSelection :
		fmt.Println(plan.Conditions)
		var exprs string
		for _, pushedDownCond := range plan.Conditions {
			fmt.Println("pushed down is ", pushedDownCond.String())
			var expr string
			switch ps := pushedDownCond.(type) {
			case *expression.ScalarFunction:
				var symbol string
				switch ps.FuncName.L {
				case ast.LT:
					symbol = "<"
				case ast.GT:
					symbol = ">"
				case ast.EQ:
					symbol = "="
				case ast.LE:
					symbol = "<="
				case ast.GE:
					symbol = ">="
				case ast.In:
					var arg string
					if s, ok := ps.GetArgs()[0].(*expression.ScalarFunction); ok {
						switch s.FuncName.L {
						case ast.Substring:
							a := strings.Split(s.GetArgs()[0].String(), ".")[2]
							arg = fmt.Sprintf("substring(%v, %v, %v)", a, s.GetArgs()[1], s.GetArgs()[2])
						default:
							continue
						}
					} else {
						arg = strings.Split(ps.GetArgs()[0].String(), ".")[2]
					}


					sets := ps.GetArgs()[1:]
					var maps string
					tp := mysql.TypeString
					//for _, col := range plan.Schema().Columns {
					//	if col.OrigName == arg {
					//		tp = col.RetType
					//		break
					//	}
					//}
					switch tp {
					case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
						mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
						expr = "'%v'"
					default:
						expr = "%v"
					}
					for _, set := range sets {
						if maps == "" {
							maps = fmt.Sprintf(expr, set.String())
						} else {
							maps = maps + "," + fmt.Sprintf(expr, set.String())
						}
					}
					symbol = fmt.Sprintf("%v in [%v]", arg, maps)
				case ast.NE:
					symbol = "!="
				case ast.UnaryNot:
					if s, ok := ps.GetArgs()[0].(*expression.ScalarFunction); ok {
						switch s.FuncName.L {
						case ast.In:
							arg := strings.Split(s.GetArgs()[0].String(), ".")[2]
							symbol = fmt.Sprintf("%v not in (%v)", arg, s.GetArgs()[1:])
						case ast.Like:
							arg := strings.Split(s.GetArgs()[0].String(), ".")[2]
							symbol = fmt.Sprintf("%v not like '%v'", arg, s.GetArgs()[1:])
						default:
							continue
						}
					}
				case ast.Like:
					arg := strings.Split(ps.GetArgs()[0].String(), ".")[2]
					l := len(ps.GetArgs())
					symbol = fmt.Sprintf("%v like '%v'", arg, ps.GetArgs()[l-2].String())
				case ast.LogicOr:
					var larg, rarg string
					if s, ok := ps.GetArgs()[0].(*expression.ScalarFunction); ok {
						switch s.FuncName.L {
						case ast.EQ:
							a := strings.Split(s.GetArgs()[0].String(), ".")[2]
							larg = fmt.Sprintf("%v = '%v'", a, s.GetArgs()[1])
						default:
							continue
						}
					} else {
						larg = strings.Split(ps.GetArgs()[0].String(), ".")[2]
					}
					if s, ok := ps.GetArgs()[1].(*expression.ScalarFunction); ok {
						switch s.FuncName.L {
						case ast.EQ:
							a := strings.Split(s.GetArgs()[0].String(), ".")[2]
							rarg = fmt.Sprintf("%v = '%v'", a, s.GetArgs()[1])
						default:
							continue
						}
					} else {
						rarg = strings.Split(ps.GetArgs()[0].String(), ".")[2]
					}
					symbol = fmt.Sprintf("( %v or %v )", larg, rarg)

				default:
					continue
				}
				args := ps.GetArgs()
				arg := strings.Split(args[0].String(), ".")[2]
				tp := args[0].GetType().Tp

				if ps.FuncName.L != ast.In && ps.FuncName.L != ast.UnaryNot && ps.FuncName.L != ast.Like && ps.FuncName.L != ast.LogicOr {
					var value string
					var valueIsColumn bool
					switch p := args[1].(type) {
					case *expression.Column:
						//fmt.Println("column is ", p.OrigName)
						value = strings.Split(p.OrigName, ".")[2]
						valueIsColumn = true
					default:
						value = p.String()
					}
					switch tp {
					case mysql.TypeDate:
						if valueIsColumn {
							expr = fmt.Sprintf(" %v %v %v", arg, symbol, value)
						} else {
							expr = fmt.Sprintf(" %v %v '%v'", arg, symbol, strings.Split(value, " ")[0])
						}
					case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString, mysql.TypeBlob,
						mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob:
						if valueIsColumn {
							expr = fmt.Sprintf(" %v %v %v", arg, symbol, value)
						} else {
							expr = fmt.Sprintf(" %v %v '%v'", arg, symbol, value)
						}
					default:
						expr = fmt.Sprintf(" %v %v %v ", arg, symbol, value)
					}
				} else {
					expr = symbol
				}

			default:
				continue
			}
			if exprs == "" {
				exprs = expr
			} else {
				if strings.Contains(expr, "in [") {
					exprs = fmt.Sprintf("%v and %v", exprs, expr)
				} else {
					exprs = fmt.Sprintf("%v and %v", expr, exprs)
				}
			}
		}
		result["where"] = fmt.Sprintf("WHERE %v", exprs)
		return GetS3query(plan.Children()[0], result)
	case *plannercore.PhysicalHashAgg:
		var aggquery string
		for _, agg := range plan.AggFuncs {
			var expr string
			fmt.Println("partial agg func is ", agg.Name, agg.Args, agg.Mode, agg.RetTp.String())
			//fmt.Println("col is ", p.TP(), p.children, p.ctx, p.Schema(), p.OutputNames(), p.Stats(), p.ExplainNormalizedInfo())
			if s, ok := agg.Args[0].(*expression.ScalarFunction); ok {
				switch s.FuncName.L {
				case ast.Mul:
					var right string
					left := strings.Split(s.GetArgs()[0].String(), ".")[2]
					if r, ok := s.GetArgs()[1].(*expression.ScalarFunction); ok {
						var l, ri string
						if r.FuncName.L == ast.Minus {
							if rleft, ok := r.GetArgs()[0].(*expression.Constant); ok {
								l = rleft.String()
							}
							if rright, ok := r.GetArgs()[1].(*expression.Column); ok {
								ri = strings.Split(rright.OrigName, ".")[2]
							}
							right = fmt.Sprintf("(%v - %v)", l, ri)
						}
					} else {
						right = strings.Split(s.GetArgs()[1].String(), ".")[2]
					}

					expr = fmt.Sprintf("%v(%v * %v)", agg.Name, left, right)
				}
			} else {
				args := strings.Split(agg.Args[0].String(), ".")
				if agg.Name == ast.AggFuncCount && len(args) == 1 {
					expr = fmt.Sprintf("COUNT(1)")
				} else {
					expr = agg.Name + "(" + args[2] + ")"
				}

			}

			if aggquery == "" {
				aggquery = expr
			} else {
				aggquery = fmt.Sprintf("%v, %v", aggquery, expr)
			}

		}
		query := fmt.Sprintf("SELECT %v FROM s3object", aggquery)
		result["select"] = query
		return GetS3query(plan.Children()[0], result)
	case *plannercore.PhysicalStreamAgg:
		var aggquery string
		for _, agg := range plan.AggFuncs {
			var expr string
			fmt.Println("partial agg func is ", agg.Name, agg.Args, agg.Mode, agg.RetTp.String())
			//fmt.Println("col is ", p.TP(), p.children, p.ctx, p.Schema(), p.OutputNames(), p.Stats(), p.ExplainNormalizedInfo())
			if s, ok := agg.Args[0].(*expression.ScalarFunction); ok {
				switch s.FuncName.L {
				case ast.Mul:
					var right string
					left := strings.Split(s.GetArgs()[0].String(), ".")[2]
					if r, ok := s.GetArgs()[1].(*expression.ScalarFunction); ok {
						var l, ri string
						if r.FuncName.L == ast.Minus {
							if rleft, ok := r.GetArgs()[0].(*expression.Constant); ok {
								l = rleft.String()
							}
							if rright, ok := r.GetArgs()[1].(*expression.Column); ok {
								ri = strings.Split(rright.OrigName, ".")[2]
							}
							right = fmt.Sprintf("(%v - %v)", l, ri)
						}
					} else {
						right = strings.Split(s.GetArgs()[1].String(), ".")[2]
					}

					expr = fmt.Sprintf("%v(%v * %v)", agg.Name, left, right)
				}
			} else {
				args := strings.Split(agg.Args[0].String(), ".")
				if agg.Name == ast.AggFuncCount && len(args) == 1 {
					expr = fmt.Sprintf("COUNT(1)")
				} else {
					expr = agg.Name + "(" + args[2] + ")"
				}

			}

			if aggquery == "" {
				aggquery = expr
			} else {
				aggquery = fmt.Sprintf("%v, %v", aggquery, expr)
			}

		}
		query := fmt.Sprintf("SELECT %v FROM s3object", aggquery)
		result["select"] = query
		return GetS3query(plan.Children()[0], result)
	case *plannercore.PhysicalLimit:
		newcount := plan.Count + plan.Offset
		limit := fmt.Sprintf("Limit %v", newcount)
		result["limit"] = limit
		return GetS3query(plan.Children()[0], result)
	case *plannercore.PhysicalTableScan:
		if _, ok := result["select"]; !ok {
			result["select"] = "SELECT * FROM s3object "
		}
		return result
	default:
		return result
	}
}