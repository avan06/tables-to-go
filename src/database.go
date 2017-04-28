package tablestogo

import "github.com/jmoiron/sqlx"

// Database interface for the concrete databases
type Database interface {
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

// GeneralDatabase represents a generic database - like a parent/base class of all other concrete databases
type GeneralDatabase struct {
	ConcreteDatabase
	GetColumnsOfTableStmt *sqlx.Stmt
	*Settings
}

func (gdb *GeneralDatabase) GetDataSourceName() string {
	return gdb.ConcreteDatabase.CreateDataSourceName(gdb.Settings)
}

func (gdb *GeneralDatabase) GetTables() (tables []*Table, err error) {
	return gdb.ConcreteDatabase.FetchTables(gdb.Settings)
}

func (gdb *GeneralDatabase) PrepareGetColumnsOfTableStmt() (err error) {
	gdb.GetColumnsOfTableStmt, err = db.Preparex(gdb.ConcreteDatabase.GetColumnsOfTableQuery())
	return err
}

func (gdb *GeneralDatabase) GetColumnsOfTable(table *Table) (err error) {
	return gdb.ConcreteDatabase.FetchColumnsOfTable(gdb.Settings, gdb.GetColumnsOfTableStmt, table)
}

// IsString returns true if colum is of type string
func (gdb *GeneralDatabase) IsString(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetStringDatatypes())
}

// IsText returns true if colum is of type text
func (gdb *GeneralDatabase) IsText(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetTextDatatypes())
}

// IsInteger returns true if colum is of type integer
func (gdb *GeneralDatabase) IsInteger(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetIntegerDatatypes())
}

// IsFloat returns true if colum is of type float
func (gdb *GeneralDatabase) IsFloat(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetFloatDatatypes())
}

// IsTemporal returns true if colum is of type temporal
func (gdb *GeneralDatabase) IsTemporal(column Column) bool {
	return IsStringInSlice(column.DataType, gdb.ConcreteDatabase.GetTemporalDatatypes())
}
