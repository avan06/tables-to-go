package tablestogo

import "github.com/jmoiron/sqlx"

// database interface for the general database
type database interface {
	GetDataSourceName() string

	GetTables() (tables []*Table, err error)
	PrepareGetColumnsOfTableStmt() (err error)
	GetColumnsOfTable(table *Table) (err error)

	IsString(column Column) bool
	IsText(column Column) bool
	IsInteger(column Column) bool
	IsFloat(column Column) bool
	IsTemporal(column Column) bool

	ConcreteDatabase
}

// ConcreteDatabase interface for the concrete databases
type ConcreteDatabase interface {
	CreateDataSourceName(settings *Settings) string

	FetchTables(s *Settings) (tables []*Table, err error)
	GetColumnsOfTableQuery() string
	FetchColumnsOfTable(s *Settings, stmt *sqlx.Stmt, table *Table) (err error)

	IsPrimaryKey(column Column) bool
	IsAutoIncrement(column Column) bool
	IsNullable(column Column) bool

	GetStringDatatypes() []string
	GetTextDatatypes() []string
	GetIntegerDatatypes() []string
	GetFloatDatatypes() []string
	GetTemporalDatatypes() []string

	// TODO pg: bitstrings, enum, range, other special types
	// TODO mysql: bit, enums, set
}

// Database represents a generic database, implements Database interface & holds a Concrete Database
type Database struct {
	ConcreteDatabase
	GetColumnsOfTableStmt *sqlx.Stmt
	*Settings
}

func (gdb *Database) GetDataSourceName() string {
	return gdb.ConcreteDatabase.CreateDataSourceName(gdb.Settings)
}

func (gdb *Database) GetTables() (tables []*Table, err error) {
	return gdb.ConcreteDatabase.FetchTables(gdb.Settings)
}

func (gdb *Database) PrepareGetColumnsOfTableStmt() (err error) {
	gdb.GetColumnsOfTableStmt, err = dbh.Preparex(gdb.ConcreteDatabase.GetColumnsOfTableQuery())
	return err
}

func (gdb *Database) GetColumnsOfTable(table *Table) (err error) {
	return gdb.ConcreteDatabase.FetchColumnsOfTable(gdb.Settings, gdb.GetColumnsOfTableStmt, table)
}

// IsString returns true if colum is of type string
func (gdb *Database) IsString(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetStringDatatypes())
}

// IsText returns true if colum is of type text
func (gdb *Database) IsText(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetTextDatatypes())
}

// IsInteger returns true if colum is of type integer
func (gdb *Database) IsInteger(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetIntegerDatatypes())
}

// IsFloat returns true if colum is of type float
func (gdb *Database) IsFloat(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetFloatDatatypes())
}

// IsTemporal returns true if colum is of type temporal
func (gdb *Database) IsTemporal(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetTemporalDatatypes())
}
