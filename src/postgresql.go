package tablestogo

import (
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

// PostgreDatabase satisfy the database interface
type PostgreDatabase struct{}

// CreateDataSourceName creates the DSN String to connect to this database
func (pg *PostgreDatabase) CreateDataSourceName(settings *Settings) string {
	return fmt.Sprintf("host=%v port=%v user=%v dbname=%v password=%v sslmode=disable",
		settings.Host, settings.Port, settings.User, settings.DbName, settings.Pswd)
}

// GetTables gets all tables for a given schema by name
func (pg *PostgreDatabase) FetchTables(s *Settings) (tables []*Table, err error) {

	err = db.Select(&tables, `
		SELECT table_name
		FROM information_schema.tables
		WHERE table_type = 'BASE TABLE'
		AND table_schema = $1
		ORDER BY table_name
	`, s.Schema)

	if s.Verbose {
		if err != nil {
			fmt.Println("> Error at GetTables()")
			fmt.Printf("> schema: %q\r\n", s.Schema)
		}
	}

	return tables, err
}

// PrepareGetColumnsOfTableStmt prepares the statement for retrieving the columns of a specific table for a given database
func (pg *PostgreDatabase) GetColumnsOfTableQuery() string {

	return `
		SELECT
			ic.ordinal_position,
			ic.column_name,
			ic.data_type,
			ic.column_default,
			ic.is_nullable,
			ic.character_maximum_length,
			ic.numeric_precision,
			ic.datetime_precision,
			itc.constraint_name,
			itc.constraint_type
		FROM information_schema.columns AS ic
			LEFT JOIN information_schema.key_column_usage AS ikcu ON ic.table_name = ikcu.table_name
			AND ic.table_schema = ikcu.table_schema
			AND ic.column_name = ikcu.column_name
			LEFT JOIN information_schema.table_constraints AS itc ON ic.table_name = itc.table_name
			AND ic.table_schema = itc.table_schema
			AND ikcu.constraint_name = itc.constraint_name
		WHERE ic.table_name = $1
		AND ic.table_schema = $2
		ORDER BY ic.ordinal_position
	`
}

// GetColumnsOfTable executes the statement for retrieving the columns of a specific table in a given schema
func (pg *PostgreDatabase) FetchColumnsOfTable(s *Settings, stmt *sqlx.Stmt, table *Table) (err error) {

	err = stmt.Select(&table.Columns, table.TableName, s.Schema)

	if s.Verbose && err != nil {
		fmt.Printf("> Error at GetColumnsOfTable(%v)\r\n", table.TableName)
		fmt.Printf("> schema: %q\r\n", s.Schema)
	}

	return err
}

// IsPrimaryKey checks if column belongs to primary key
func (pg *PostgreDatabase) IsPrimaryKey(column Column) bool {
	return strings.Contains(column.ConstraintType.String, "PRIMARY KEY")
}

// IsAutoIncrement checks if column is a serial column
func (pg *PostgreDatabase) IsAutoIncrement(column Column) bool {
	return strings.Contains(column.ColumnDefault.String, "nextval")
}

// IsNullable returns true if column is a nullable one
func (pg *PostgreDatabase) IsNullable(column Column) bool {
	return column.IsNullable == "YES"
}

// GetStringDatatypes returns the string datatypes for the postgre database
func (pg *PostgreDatabase) GetStringDatatypes() []string {
	return []string{
		"character varying",
		"varchar",
		"character",
		"char",
	}
}

// GetTextDatatypes returns the text datatypes for the postgre database
func (pg *PostgreDatabase) GetTextDatatypes() []string {
	return []string{
		"text",
	}
}

// GetIntegerDatatypes returns the integer datatypes for the postgre database
func (pg *PostgreDatabase) GetIntegerDatatypes() []string {
	return []string{
		"smallint",
		"integer",
		"bigint",
		"smallserial",
		"serial",
		"bigserial",
	}
}

// GetFloatDatatypes returns the float datatypes for the postgre database
func (pg *PostgreDatabase) GetFloatDatatypes() []string {
	return []string{
		"numeric",
		"decimal",
		"real",
		"double precision",
	}
}

// GetTemporalDatatypes returns the temporal datatypes for the postgre database
func (pg *PostgreDatabase) GetTemporalDatatypes() []string {
	return []string{
		"time",
		"timestamp",
		"time with time zone",
		"timestamp with time zone",
		"time without time zone",
		"timestamp without time zone",
		"date",
	}
}
