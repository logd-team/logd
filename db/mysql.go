/*******************
 *
 * 对database/sql做简单封装，便于使用
 * mysql驱动使用go-sql-driver
 *
 *********************/

package db

import (
    "fmt"
    "log"
    "regexp"
    "strings"
    "database/sql"
    "reflect"
    _ "github.com/go-sql-driver/mysql"
)

type Mysql struct {
    db *sql.DB
}

func NewMysql(host string, port string, uname string, passwd string, db string, charset string) *Mysql{
    dsn := fmt.Sprintf("%s:%s@(%s:%s)/%s?charset=%s", uname, passwd, host, port, db, charset)
    dbObj, err := sql.Open("mysql", dsn)
    if err != nil {
        log.Fatalf("create db obj error for %s:%s/%s", host, port, db)
    }
    err = dbObj.Ping()
    if err != nil {
        log.Fatalf("connect to %s:%s/%s failed. quit...", host, port, db)
    }
    return &Mysql{dbObj}
}

func (my *Mysql) Query(sqlStr string, args ...interface{} ) (*MysqlResult, error) {
    rows, err := my.db.Query(sqlStr, args...)
    defer func(){
        if rows != nil {
            rows.Close()
        }
    }()

    result := new(MysqlResult)
    if err != nil {
       log.Println(err) 
       return result, err
    }
    
    columns, err := rows.Columns()
    nCols := len(columns)

    //列名与列号的映射
    colIndexMap := make(map[string]int)
    for i, v := range columns {
        colIndexMap[v] = i
    }
    result.ColIndexMap = colIndexMap

    row := make([]interface{}, nCols)
    valueArgs := make([]interface{}, nCols)
    for i, _ := range valueArgs {
        valueArgs[i] = &row[i]       //用于存数据的参数
    }
    var i uint = 0
    for rows.Next() {
        err = rows.Scan(valueArgs...)
        if err == nil {
            strRow := make([]string, nCols)
            for i, v := range row {
                newv, ok := v.([]byte)
                if ok {
                    strRow[i] = string(newv)
                }else{
                    strRow[i] = "<nil>"
                }
            }
            result.Rows = append(result.Rows, strRow)
            i++
        }else{
            log.Printf("scan row %d error\n", i)
        }
    }
    result.NumRows = i
    return result, err
}

func (my *Mysql) Exec(sqlStr string, args ...interface{}) (MysqlResult, error) {
    if isInsert(sqlStr) {
        sqlStr, args = makeMultiInsert(sqlStr, args...)
    }
    res, err := my.db.Exec(sqlStr, args...)
    var result MysqlResult
    if err != nil {
        log.Println("Mysql.Exec", err)
    }else{
        iid, _ := res.LastInsertId()
        nRows, _ := res.RowsAffected()
        result.InsertId = uint(iid)
        result.NumRows = uint(nRows)
    }
    return result,err
}

func isInsert(s string) bool {
    s = strings.ToLower(strings.Trim(s, " "))
    return strings.HasPrefix(s, "insert ") || strings.HasPrefix(s, "replace ")
}


func makeMultiInsert(sqlStr string, args ...interface{}) (string, []interface{}) {
    head := ""
    placeHolder := ""
    tail := ""           //on duplicate
    nValues := 0         //有多少组值

    re, _ := regexp.Compile("\\([?, ]+\\)")   //匹配占位符
    loc := re.FindStringIndex(sqlStr)
    if loc != nil {
        head = sqlStr[: loc[0]]
        placeHolder = sqlStr[ loc[0] : loc[1] ]
        tail = sqlStr[ loc[1]: ]
    }
    
    var data []interface{}
    nArgs := len(args)
    //检查是否二维数组
    if nArgs == 1 {
        value := reflect.ValueOf(args[0]) 
        kind := value.Kind()
        vLen := value.Len()
        // []byte 要当成一个整体处理
        if (kind == reflect.Slice || kind == reflect.Array) && value.Type().String() != "[]uint8"{
            for i:=0; i<vLen; i++ {
                val := value.Index(i)
                if (reflect.Slice == val.Kind() || reflect.Array == val.Kind()) && val.Type().String() != "[]uint8" {
                    // 处理二维的情况
                    vLen1 := val.Len()
                    for j:=0; j<vLen1; j++{
                        val1 := val.Index(j)
                        data = append(data, val1.Interface())
                    }
                    nValues++
                }else{
                    // 处理一维的情况
                    data = append(data, val.Interface())
                    nValues = 1
                }
            }
        }else{
            // 处理单个值的情况
            data = append(data, value.Interface())
            nValues = 1
        }
    }else if nArgs > 1{
        //如果是多个参数，就当成一维的
        nValues = 1
        data = args
    }
    if nValues > 1 {
        placeHolder = strings.Trim(strings.Repeat(placeHolder + ", ", nValues), ", ")
    }
    sqlStr = fmt.Sprintf("%s %s %s", head, placeHolder, tail)
    return sqlStr, data
}
/***************************
 *
 * mysql查询结果类型
 *
 ****************************/
type MysqlResult struct {
    Rows [][]string //存结果
    ColIndexMap map[string]int
    InsertId uint       //select时为0
    NumRows uint        //select时是结果行数，insert或update时是影响的行数
}

