package tablestogo

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// MySQLDatabase satisfy the database interface
type MySQLDatabase struct{}

// CreateDataSourceName creates the DSN String to connect to this database
func (mysql *MySQLDatabase) CreateDataSourceName(settings *Settings) string {
	return fmt.Sprintf("%v:%v@tcp(%v:%v)/%v",
		settings.User, settings.Pswd, settings.Host, settings.Port, settings.DbName)
}

// GetTables gets all tables for a given database by name
func (mysql *MySQLDatabase) FetchTables(s *Settings) (tables []*Table, err error) {

	err = db.Select(&tables, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		AND table_schema = ?
		ORDER BY table_name
	`, s.DbName)

	if s.Verbose {
		if err != nil {
			fmt.Println("> Error at GetTables()")
			fmt.Printf("> schema: %q\r\n", s.DbName)
		}
	}

	return tables, err
}

// PrepareGetColumnsOfTableStmt prepares the statement for retrieving the columns of a specific table for a given database
func (mysql *MySQLDatabase) GetColumnsOfTableQuery() string {

	return `
		SELECT
		  ordinal_position,
		  column_name,
		  data_type,
		  column_default,
		  is_nullable,
		  character_maximum_length,
		  numeric_precision,
		  datetime_precision,
		  column_key,
		  extra
		FROM information_schema.columns
		WHERE table_name = ?
		AND table_schema = ?
		ORDER BY ordinal_position
	`
}

// GetColumnsOfTable executes the statement for retrieving the columns of a specific table for a given database
func (mysql *MySQLDatabase) FetchColumnsOfTable(s *Settings, stmt *sqlx.Stmt, table *Table) (err error) {

	err = stmt.Select(&table.Columns, table.TableName, s.DbName)

	if s.Verbose && err != nil {
		fmt.Printf("> Error at GetColumnsOfTable(%v)\r\n", table.TableName)
		fmt.Printf("> schema: %q\r\n", s.Schema)
		fmt.Printf("> dbName: %q\r\n", s.DbName)
	}

	return err
}

// IsPrimaryKey checks if column belongs to primary key
func (mysql *MySQLDatabase) IsPrimaryKey(column Column) bool {
	return strings.Contains(column.ColumnKey, "PRI")
}

// IsAutoIncrement checks if column is a auto_increment column
func (mysql *MySQLDatabase) IsAutoIncrement(column Column) bool {
	return strings.Contains(column.Extra, "auto_increment")
}

// IsNullable returns true if column is a nullable one
func (mysql *MySQLDatabase) IsNullable(column Column) bool {
	return column.IsNullable == "YES"
}

// GetStringDatatypes returns the string datatypes for the mysql database
func (mysql *MySQLDatabase) GetStringDatatypes() []string {
	return []string{
		"char",
		"varchar",
		"binary",
		"varbinary",
	}
}

// GetTextDatatypes returns the text datatypes for the mysql database
func (mysql *MySQLDatabase) GetTextDatatypes() []string {
	return []string{
		"text",
		"blob",
	}
}

// GetIntegerDatatypes returns the integer datatypes for the mysql database
func (mysql *MySQLDatabase) GetIntegerDatatypes() []string {
	return []string{
		"tinyint",
		"smallint",
		"mediumint",
		"int",
		"bigint",
	}
}

// GetFloatDatatypes returns the float datatypes for the mysql database
func (mysql *MySQLDatabase) GetFloatDatatypes() []string {
	return []string{
		"numeric",
		"decimal",
		"float",
		"real",
		"double precision",
	}
}

// GetTemporalDatatypes returns the temporal datatypes for the mysql database
func (mysql *MySQLDatabase) GetTemporalDatatypes() []string {
	return []string{
		"time",
		"timestamp",
		"date",
		"datetime",
		"year",
	}
}
