package doris

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/timescale/tsbs/pkg/targets"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StreamLoadResp struct {
	TxnID                  int    `json:"TxnId"`
	Label                  string `json:"Label"`
	Status                 string `json:"Status"`
	Message                string `json:"Message"`
	NumberTotalRows        int    `json:"NumberTotalRows"`
	NumberLoadedRows       int    `json:"NumberLoadedRows"`
	NumberFilteredRows     int    `json:"NumberFilteredRows"`
	NumberUnselectedRows   int    `json:"NumberUnselectedRows"`
	LoadBytes              int    `json:"LoadBytes"`
	LoadTimeMs             int    `json:"LoadTimeMs"`
	BeginTxnTimeMs         int    `json:"BeginTxnTimeMs"`
	StreamLoadPutTimeMs    int    `json:"StreamLoadPutTimeMs"`
	ReadDataTimeMs         int    `json:"ReadDataTimeMs"`
	WriteDataTimeMs        int    `json:"WriteDataTimeMs"`
	CommitAndPublishTimeMs int    `json:"CommitAndPublishTimeMs"`
	ErrorURL               string `json:"ErrorURL"`
}

// load.Processor interface implementation
type processor struct {
	db   *sqlx.DB
	csi  *syncCSI
	conf *DorisConfig
}

// Init load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	if doLoad {
		p.db = sqlx.MustConnect(dbType, getConnectString(p.conf, true))
		if hashWorkers {
			p.csi = newSyncCSI()
		} else {
			p.csi = globalSyncCSI
		}
	}
}

// Close load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		err := p.db.Close()
		if err != nil {
			return
		}
	}
}

// ProcessBatch load.Processor interface implementation
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*tableArr)
	rowCnt := 0
	metricCnt := uint64(0)

	for tableName, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(tableName, rows)
			if p.conf.LogBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/took.Seconds(), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0

	return metricCnt, uint64(rowCnt)
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

type syncCSI struct {
	// Map hostname to tags.id for this host
	m     map[string]int64
	mutex *sync.RWMutex
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

// Process part of incoming data - insert into tables
func (p *processor) processCSI(tableName string, rows []*insertData) uint64 {
	// start doris group commit suit
	dorisGroupCommitSql := "set group_commit = async_mode;"
	_, err := p.db.Exec(dorisGroupCommitSql)
	if err != nil {
		panic(err)
	}
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	ret := uint64(0)
	commonTagsLen := len(tableCols["tags"])

	colLen := len(tableCols[tableName]) + 2
	if p.conf.InTableTag {
		colLen++
	}

	for _, row := range rows {
		// Split the tags into individual common tags and
		// an extra bit leftover for non-common tags that need to be added separately.
		// For each of the common tags, remove everything after = in the form <label>=<val>
		// since we won't need it.
		// tags line ex.:
		// hostname=host_0,region=eu-west-1,datacenter=eu-west-1b,rack=67,os=Ubuntu16.10,arch=x86,team=NYC,service=7,service_version=0,service_environment=production
		tags := strings.SplitN(row.tags, ",", commonTagsLen+1)
		// tags = (
		//	hostname=host_0
		//	region=eu-west-1
		//	datacenter=eu-west-1b
		// )
		// extract value of each tag
		// tags = (
		//	host_0
		//	eu-west-1
		//	eu-west-1b
		// )
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}

		// fields line ex.:
		// 1451606400000000000,58,2,24,61,22,63,6,44,80,38
		metrics := strings.Split(row.fields, ",")

		// Count number of metrics processed
		ret += uint64(len(metrics) - 1) // 1-st field is timestamp, do not count it
		// metrics = (
		// 	1451606400000000000,
		// 	58,
		// )

		timestampNano, err := strconv.ParseInt(metrics[0], 10, 64)
		if err != nil {
			panic(err)
		}
		timeUTC := time.Unix(0, timestampNano)
		tmpCreatedAt := timeUTC.UTC().Format("2006-01-02 15:04:05")
		// use nil at 2-nd position as placeholder for tagKey
		r := make([]interface{}, 0, colLen)
		r = append(r,
			nil,          // tags_id
			tmpCreatedAt, // created_at
			timeUTC,      // created_date
			timestampNano) // time
		if p.conf.InTableTag {
			r = append(r, tags[0]) // tags[0] = hostname
		}
		for _, v := range metrics[1:] {
			if v == "" {
				r = append(r, nil)
				continue
			}
			f64, err := strconv.ParseFloat(v, 64)
			if err != nil {
				panic(err)
			}
			r = append(r, f64)
		}

		dataRows = append(dataRows, r)
		tagRows = append(tagRows, tags)
	}

	// Check if any of these tags has yet to be inserted
	// New tags in this batch, need to be inserted
	newTags := make([][]string, 0, len(rows))
	p.csi.mutex.RLock()
	// judge by device hostname(unique)
	seen := make(map[string]bool)
	for _, tagRow := range tagRows {
		// tagRow contains what was called `tags` earlier - see one screen higher
		// tagRow[0] = hostname
		if _, ok := p.csi.m[tagRow[0]]; !ok {
			// Tags of this hostname are not listed as inserted - new tags line, add it for creation
			if !seen[tagRow[0]] {
				newTags = append(newTags, tagRow)
				seen[tagRow[0]] = true
			}
		}
	}
	p.csi.mutex.RUnlock()

	// Deal with new tags
	if len(newTags) > 0 {
		// We have new tags to insert
		p.csi.mutex.Lock()
		hostnameToTags := insertTags(p.db, len(p.csi.m), newTags, true)
		// Insert new tags into map as well
		for hostName, tagsId := range hostnameToTags {
			p.csi.m[hostName] = tagsId
		}
		p.csi.mutex.Unlock()
	}

	// Prepare column names
	cols := make([]string, 0, colLen)
	cols = append(cols, "tags_id", "created_at", "created_date", "time")
	if p.conf.InTableTag {
		cols = append(cols, tableCols["tags"][0]) // hostname
	}
	cols = append(cols, tableCols[tableName]...)

	// 生成占位符模板（每个值的占位符）
	rowPlaceholder := "(" + strings.Repeat("?,", len(cols)-1) + "?)"
	var placeholders []string
	for range dataRows {
		placeholders = append(placeholders, rowPlaceholder)
	}

	// Deal with tag ids for each data row
	var tagsIdPosition = 0
	p.csi.mutex.RLock()
	for i := range dataRows {
		// tagKey = hostname
		tagKey := tagRows[i][0]
		// Insert id of the tag (tags.id) for this host into tags_id position of the dataRows record
		// refers to
		// nil,		// tags_id
		dataRows[i][tagsIdPosition] = p.csi.m[tagKey]
	}
	p.csi.mutex.RUnlock()

	// 准备参数（需要展开所有行的数据）
	//args := make([]interface{}, 0, len(dataRows)*len(cols))

	bs := make([]byte, 0, len(dataRows)*len(cols))

	//1,2024-01-01 00:00:00,2024-01-01,1704067200000000000,58,2,24,61,22,63,6,44,80,38
	//2,2024-01-01 00:00:00,2024-01-01,1704067200000000000,84,11,53,87,29,20,54,77,53,74
	for j, r := range dataRows {
		for i, row := range r {
			bs = append(bs, []byte(fmt.Sprintf("%v", row))...)
			if i != len(r)-1 {
				bs = append(bs, ',')
			}
		}
		if j != len(dataRows)-1 {
			bs = append(bs, '\n')
		}
	}

	if len(dataRows) == 0 {
		return ret
	}
	sendRequest(bytes.NewReader(bs), p.conf, tableName, cols)
	//_, err = p.db.Exec(sql, args...)
	if err != nil {
		panic(err)
	}

	return ret
}

//	response=$(curl --silent --location-trusted -u root:"" \
//	       -H "Expect: 100-continue" \
//	       -H "column_separator: ," \
//	       -H "columns: tags_id,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment" \
//	       -T "$doris_tags_file" \
//	       ${DORIS_URL}/api/benchmark/tags/_stream_load)
//	   echo "Tags 加载结果: $response"
func sendRequest(reader io.Reader, conf *DorisConfig, tableName string, cols []string) {
	url := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load", conf.Host, conf.FeHttpPort, conf.DbName, tableName)
	for i := 0; i < 3; i++ {
		// 如果是重定向响应，很可能是会被调用一次 close，避免多次 close
		req, err := http.NewRequest("PUT", url, reader)
		if err != nil {
			panic(err)
		}

		// set auth
		req.Header.Set("Content-Type", "application/json")
		result := "root" + ":" + ""
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s", base64.StdEncoding.EncodeToString([]byte(result))))
		req.Header.Set("Expect", "100-continue")
		// UTC Timezone
		req.Header.Set("timezone", "Africa/Abidjan")
		req.Header.Set("group_commit", "async_mode")
		req.Header.Set("column_separator", ",")
		req.Header.Set("columns", strings.Join(cols, ","))

		// create client
		client := &http.Client{
			Timeout: time.Duration(36000) * time.Second,
		}

		// send request
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}

		// read response
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			panic(err)
		}
		// check response
		if resp.StatusCode != 200 {
			// print response headers
			if resp.StatusCode == 307 {
				url = resp.Header.Get("Location")
				//.Printf("redirect to %s \n", url)
				resp.Body.Close()
				continue
			}
			//log.Printf("response headers: %v \n", resp.Header)
			//log.Fatalf("response code is not 200, code: %d, response: %s \n", resp.StatusCode, string(body))
		}

		// parse response
		var respMsg StreamLoadResp
		err = json.Unmarshal(body, &respMsg)
		if err != nil {
			panic(err)
		}

		if respMsg.Status != "Success" {
			//fmt.Println(string(body))
			if respMsg.Status != "Publish Timeout" {
				panic(respMsg.Status)
			}
		}
		resp.Body.Close()
		return
	}
	panic("redirect too much")
}

// insertTags fills tags table with values
func insertTags(db *sqlx.DB, startID int, rows [][]string, returnResults bool) map[string]int64 {
	// Map hostname to tags_id
	ret := make(map[string]int64)

	cols := tableCols["tags"]

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}

	id := startID
	var values []interface{}
	var valueStrings []string

	for _, row := range rows {
		id++

		// 构造一行数据
		variadicArgs := make([]interface{}, len(row)+1)
		variadicArgs[0] = id
		for i, value := range row {
			variadicArgs[i+1] = convertBasedOnType(tagColumnTypes[i], value)
		}

		// 构造占位符部分，如 "(?, ?, ?)"
		placeholders := strings.Repeat("?,", len(variadicArgs))
		placeholders = placeholders[:len(placeholders)-1]
		valueStrings = append(valueStrings, "("+placeholders+")")

		// 添加参数
		values = append(values, variadicArgs...)

		if returnResults {
			ret[row[0]] = int64(id)
		}
	}

	batchSQL := fmt.Sprintf(
		"INSERT INTO tags (tags_id, %s) VALUES %s",
		strings.Join(cols, ","),
		strings.Join(valueStrings, ","))
	stmt, err := tx.Prepare(batchSQL)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	_, err = tx.Exec(batchSQL, values...)
	if err != nil {
		err := tx.Rollback()
		if err != nil {
			return nil
		}
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	if returnResults {
		return ret
	}

	return nil
}

func convertBasedOnType(serializedType, value string) interface{} {
	if value == "" {
		return nil
	}

	switch serializedType {
	case "string":
		return value
	case "float32":
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float32", value))
		}
		return float32(f)
	case "float64":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float64", value))
		}
		return f
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return i
	case "int32":
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return int32(i)
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}

// subsystemTagsToJSON converts equations as
// a=b
// c=d
// into JSON STRING '{"a": "b", "c": "d"}'
func subsystemTagsToJSON(tags []string) string {
	json := "{"
	for i, t := range tags {
		args := strings.Split(t, "=")
		if i > 0 {
			json += ","
		}
		json += fmt.Sprintf("\"%s\": \"%s\"", args[0], args[1])
	}
	json += "}"
	return json
}
