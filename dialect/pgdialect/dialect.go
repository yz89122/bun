package pgdialect

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect"
	"github.com/uptrace/bun/dialect/feature"
	"github.com/uptrace/bun/dialect/sqltype"
	"github.com/uptrace/bun/schema"
)

var pgDialect = New()

func init() {
	if Version() != bun.Version() {
		panic(fmt.Errorf("pgdialect and Bun must have the same version: v%s != v%s",
			Version(), bun.Version()))
	}
}

type Option interface {
	apply(d *Dialect)
}

type optionFunc func(d *Dialect)

func (f optionFunc) apply(d *Dialect) {
	f(d)
}

var _ Option = (*optionFunc)(nil)

func WithPrepared(prepared bool) Option {
	return optionFunc(func(d *Dialect) {
		d.prepared = prepared
	})
}

type Dialect struct {
	schema.BaseDialect

	tables   *schema.Tables
	features feature.Feature
	prepared bool
}

func New(opts ...Option) *Dialect {
	d := new(Dialect)
	d.tables = schema.NewTables(d)
	d.features = feature.CTE |
		feature.WithValues |
		feature.Returning |
		feature.InsertReturning |
		feature.DefaultPlaceholder |
		feature.DoubleColonCast |
		feature.InsertTableAlias |
		feature.UpdateTableAlias |
		feature.DeleteTableAlias |
		feature.TableCascade |
		feature.TableIdentity |
		feature.TableTruncate |
		feature.TableNotExists |
		feature.InsertOnConflict |
		feature.SelectExists |
		feature.GeneratedIdentity |
		feature.CompositeIn
	for _, opt := range opts {
		opt.apply(d)
	}
	return d
}

func (d *Dialect) Init(*sql.DB) {}

func (d *Dialect) Name() dialect.Name {
	return dialect.PG
}

func (d *Dialect) Features() feature.Feature {
	return d.features
}

func (d *Dialect) Tables() *schema.Tables {
	return d.tables
}

func (d *Dialect) OnTable(table *schema.Table) {
	for _, field := range table.FieldMap {
		d.onField(field)
	}
}

func (d *Dialect) Prepared() bool {
	return d.prepared
}

func (d *Dialect) onField(field *schema.Field) {
	field.DiscoveredSQLType = fieldSQLType(field)

	if field.AutoIncrement && !field.Identity {
		switch field.DiscoveredSQLType {
		case sqltype.SmallInt:
			field.CreateTableSQLType = pgTypeSmallSerial
		case sqltype.Integer:
			field.CreateTableSQLType = pgTypeSerial
		case sqltype.BigInt:
			field.CreateTableSQLType = pgTypeBigSerial
		}
	}

	if field.Tag.HasOption("array") || strings.HasSuffix(field.UserSQLType, "[]") {
		field.Append = d.arrayAppender(field.StructField.Type)
		field.Scan = arrayScanner(field.StructField.Type)
	}

	if field.DiscoveredSQLType == sqltype.HSTORE {
		field.Append = d.hstoreAppender(field.StructField.Type)
		field.Scan = hstoreScanner(field.StructField.Type)
	}
}

func (d *Dialect) IdentQuote() byte {
	return '"'
}

func (d *Dialect) AppendUint32(b []byte, n uint32) []byte {
	return strconv.AppendInt(b, int64(int32(n)), 10)
}

func (d *Dialect) AppendUint64(b []byte, n uint64) []byte {
	return strconv.AppendInt(b, int64(n), 10)
}

func (d *Dialect) AppendPlaceholder(b []byte, i int) []byte {
	b = append(b, '$')
	b = append(b, strconv.FormatInt(int64(i), 10)...)
	return b
}
