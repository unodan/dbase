/*
# File dbase.go
# Author: Dan Huckson
# Date: 20160325
*/
package dbase

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"
	"unicode"

	_ "github.com/go-sql-driver/mysql"

	"github.com/unodan/cache"
)

var cStore *cache.Store

type Cache struct {
	Driver     string
	User       string
	UserPass   string
	Database   string
	ProjectID  string
	InstanceID string
	ServerIP   string
	ServerName string
	ServerPort string
	DBHandle   *sql.DB
}

func inList(a string, list *[]string) bool {
	for _, b := range *list {
		if b == a {
			return true
		}
	}
	return false
}

func Server(c *Cache) (*cache.Store, error) {
	var (
		err    error
		hdl    *sql.DB
		sqlcmd string

		ca        = cStore.New()
		cacheData = Cache{
			Driver:     "mysql",
			ServerIP:   "127.0.0.1",
			ServerName: "localhost",
			ServerPort: "3306",
		}
	)

	dflt := reflect.Indirect(reflect.ValueOf(&cacheData))
	if c != nil {
		vCache := reflect.Indirect(reflect.ValueOf(c))
		for i := 0; i < dflt.NumField(); i++ {
			if vCache.Field(i).Interface() == nil || vCache.Field(i).Interface() == "" {
				ca.Set(dflt.Type().Field(i).Name, dflt.Field(i).Interface())
			} else {
				ca.Set(vCache.Type().Field(i).Name, vCache.Field(i).Interface())
			}
		}

		var (
			user       = ca.Get("User").(string)
			userPass   = ca.Get("UserPass").(string)
			projectID  = ca.Get("ProjectID").(string)
			instanceID = ca.Get("InstanceID").(string)
			driver     = ca.Get("Driver").(string)
			database   = ca.Get("Database").(string)
			serverIP   = ca.Get("ServerIP").(string)
			serverName = ca.Get("ServerName").(string)
			serverPort = ca.Get("ServerPort").(string)
		)

		if serverName == "localhost" || serverIP == "127.0.0.1" {
			sqlcmd = user + ":" + userPass + "@tcp(" + serverName + ":" + serverPort + ")/"
		} else {
			sqlcmd = user + "@cloudsql(" + projectID + ":" + instanceID + ")/" + database
		}
		hdl, err = sql.Open(driver, sqlcmd)

		if err != nil {
			log.Println("dbase, Error: could not connect to database server [ " + serverIP + " ] as user [ " + user + " ] ")
		} else {
			results, err := hdl.Query("SELECT CURRENT_USER()")
			defer results.Close()

			if err == nil {
				ca.Set("DBHandle", hdl)
				current_user := ""
				for results.Next() {
					results.Scan(&current_user)
				}
				log.Println("dbase, Info: " + current_user + " login [ " + time.Now().Format(time.RFC850) + " ] ")
				log.Printf("dbase, Info: connected to datebase server [ %s, %s ] ", serverName, serverIP)
			}
		}
	}
	return ca, err
}
func GetFieldNames(hdl *sql.DB, database string, table string) []string {
	var (
		err    error
		name   string
		names  []string
		rows   *sql.Rows
		stmt   *sql.Stmt
		sqlcmd = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?"
	)

	if stmt, err = hdl.Prepare(sqlcmd); err == nil {
		if rows, err = stmt.Query(database, table); err != nil {
			log.Println("dbase,", err)
		} else {
			rows.Next()
			for rows.Next() {
				rows.Scan(&name)
				names = append(names, name)
			}
		}
		Use(hdl, database)
		stmt.Close()
	} else {
		log.Println("dbase,", err)
	}

	return names
}
func GetDatabaseName(hdl *sql.DB) string {
	var dbName string

	rows, err := hdl.Query("SELECT DATABASE()")
	defer rows.Close()

	if err != nil {
		log.Println("dbase, Error: could not connect to database")
	}

	for rows.Next() {
		rows.Scan(&dbName)
	}
	return dbName
}
func GetDatabaseNamesList(hdl *sql.DB) *[]string {
	var (
		dbName  string
		dbNames []string
	)
	rows, err := hdl.Query("SHOW DATABASES;")
	defer rows.Close()

	if err != nil {
		log.Println("dbase, Error: could not connect to database")
	}

	for rows.Next() {
		rows.Scan(&dbName)
		dbNames = append(dbNames, dbName)
	}
	return &dbNames
}
func SanatizeWhiteSpace(in string) (out string) {
	white := false
	for _, c := range in {
		if unicode.IsSpace(c) {
			if !white {
				out = out + " "
			}
			white = true
		} else {
			out = out + string(c)
			white = false
		}
	}
	return
}

func Use(hdl *sql.DB, name string) bool {
	var result = true

	if GetDatabaseName(hdl) != name {
		if _, err := hdl.Exec("USE " + name); err == nil {
			log.Println("dbase, Info: USE [ " + name + " ]")
		} else {
			result = false
			log.Println("dbase, Error: USE [ " + name + " ] command has failed")
			log.Println("dbase,", err)
		}
	}
	return result
}
func Exec(hdl *sql.DB, sqlcmd string, args ...interface{}) (sql.Result, error) {
	var (
		err  error
		stmt *sql.Stmt
		res  sql.Result
	)
	if stmt, err = hdl.Prepare(sqlcmd); err == nil {
		res, err = stmt.Exec(args...)
	}
	return res, err
}
func Query(hdl *sql.DB, sqlcmd string, args ...interface{}) (*sql.Rows, error) {
	return hdl.Query(sqlcmd, args...)
}

func RowExist(hdl *sql.DB, table string, id int64) bool {
	result := false

	rows, err := Query(hdl, "SELECT id FROM "+table+" WHERE id=?", id)
	if err == nil && rows.Next() {
		result = true
	}
	return result
}
func InsertRow(hdl *sql.DB, table string, d interface{}) (int64, error) {
	var (
		id   int64
		err  error
		stmt *sql.Stmt
		sqlcmd, sqlcmd1,
		sqlcmd2 string
		columns []interface{}
	)

	if d != nil {
		sqlcmd = "INSERT INTO " + table + " ("

		r := reflect.Indirect(reflect.ValueOf(d))
		columns = make([]interface{}, r.NumField())

		for i := 0; i < r.NumField(); i++ {
			columns[i] = r.Field(i).Interface()
			sqlcmd1 += r.Type().Field(i).Name + ","
			sqlcmd2 += "?,"
		}
		sqlcmd += strings.ToLower(sqlcmd1[:len(sqlcmd1)-1]) + ") VALUES(" + sqlcmd2[:len(sqlcmd2)-1] + ") "

		if stmt, err = hdl.Prepare(sqlcmd); err == nil {
			log.Println("dbase, Info:", sqlcmd)
			if r, e := stmt.Exec(columns...); e == nil {
				if id, err = r.LastInsertId(); err == nil {
					log.Printf("dbase, Info: inserted %d columns, %v", len(columns), columns)
				}
			} else {
				err = e
				log.Println("dbase, Error: insert row in table [ " + table + " ] has failed")
				log.Println("dbase,", err)
			}
			stmt.Close()
		}
	}
	return id, err
}
func UpdateRow(hdl *sql.DB, table string, id int64, d *map[string]interface{}) error {
	var (
		err     error
		sqlcmd  string
		stmt    *sql.Stmt
		columns []interface{}
	)
	if d != nil {
		sqlcmd = "UPDATE " + table + " SET "

		cnt := 0
		columns = make([]interface{}, len(*d)+1)
		for k, v := range *d {
			columns[cnt] = v
			sqlcmd += strings.ToLower(k) + "=?,"
			cnt++
		}
		columns[cnt] = id
		sqlcmd = sqlcmd[:len(sqlcmd)-1] + " WHERE id=?"

		if stmt, err = hdl.Prepare(sqlcmd); err == nil {
			log.Println("dbase, Info:", sqlcmd)
			if _, err = stmt.Exec(columns...); err == nil {
				str := ""
				for _, value := range columns[:len(columns)-1] {
					str += fmt.Sprintf("%v,", value)
				}
				log.Print("dbase, Info: updated ", len(columns)-1, " columns, [", str[:len(str)-1], "]")
			} else {
				log.Printf("dbase, Error: could not update row, %v", columns)
				log.Println("dbase,", err)
			}
		} else {
			log.Println("dbase, Error: preparing sql statement [" + sqlcmd + "]")
			log.Println("dbase,", err)
		}
		stmt.Close()
	}
	return err
}
func DeleteRow(hdl *sql.DB, table string, id int64) (int64, error) {
	res, err := hdl.Exec(fmt.Sprintf("DELETE FROM %s WHERE id=%d;", table, id))

	if err != nil {
		log.Printf("dbase, Error: Could not delete row id=%d\n%s", id, err)
		return 0, err
	}
	rows, err := res.RowsAffected()
	log.Println(fmt.Sprintf("dbase, Info: DELETE FROM %s WHERE id=%d, Rows affected=%d", table, id, rows))
	return rows, err
}

func Exist(hdl *sql.DB, name string) bool {
	return inList(name, GetDatabaseNamesList(hdl))
}
func TableExist(hdl *sql.DB, name string) bool {
	result := false
	if _, err := hdl.Exec("SELECT 1 FROM " + name + " LIMIT 1;"); err == nil {
		result = true
	}
	return result
}
func CreateTable(hdl *sql.DB, name string, sqlcmd string) bool {
	var success = true
	sqlcmd = SanatizeWhiteSpace(sqlcmd)

	if !TableExist(hdl, name) {
		_, err := hdl.Exec("CREATE TABLE " + name + " ( " + sqlcmd + " )")
		if err == nil {
			log.Println("dbase, Info: CREATE TABLE [ " + name + " ]")
		} else {
			success = false
			log.Println("dbase, Error: CREATE TABLE [ "+name+" ] failed. ", err)
		}
	}
	return success
}
func CreateDatabase(hdl *sql.DB, name string) bool {
	var success = true
	_, err := hdl.Exec("CREATE DATABASE " + name)
	if err == nil {
		log.Println("dbase, Info: CREATE DATABASE [ " + name + " ]")
	} else {
		success = false
		log.Println("dbase, Error: CREATE DATABASE [ "+name+" ] failed. ", err)
	}
	return success
}
