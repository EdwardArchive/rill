package starrocks

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	runtimev1 "github.com/rilldata/rill/proto/gen/rill/runtime/v1"
	"github.com/rilldata/rill/runtime/drivers"
	"go.uber.org/zap"
)

var _ drivers.OLAPStore = (*connection)(nil)

// Dialect implements drivers.OLAPStore.
func (c *connection) Dialect() drivers.Dialect {
	return drivers.DialectStarRocks
}

// MayBeScaledToZero implements drivers.OLAPStore.
func (c *connection) MayBeScaledToZero(ctx context.Context) bool {
	return false
}

// WithConnection implements drivers.OLAPStore.
func (c *connection) WithConnection(ctx context.Context, priority int, fn drivers.WithConnectionFunc) error {
	// Acquire semaphore for connection affinity
	if err := c.querySem.Acquire(ctx, 1); err != nil {
		return err
	}
	defer c.querySem.Release(1)

	db, err := c.getDB(ctx)
	if err != nil {
		return err
	}

	conn, err := db.Connx(ctx)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer conn.Close()

	// Create ensured context that won't be cancelled
	ensuredCtx := context.WithoutCancel(ctx)

	return fn(ctx, ensuredCtx)
}

// Exec implements drivers.OLAPStore.
func (c *connection) Exec(ctx context.Context, stmt *drivers.Statement) error {
	db, err := c.getDB(ctx)
	if err != nil {
		return err
	}

	if c.configProp.LogQueries {
		c.logger.Info("StarRocks exec",
			zap.String("query", stmt.Query),
			zap.Any("args", stmt.Args))
	}

	// Remove deadline but preserve cancellation (ClickHouse pattern)
	execCtx := contextWithoutDeadline(ctx)

	_, err = db.ExecContext(execCtx, stmt.Query, stmt.Args...)
	return err
}

// Query implements drivers.OLAPStore.
func (c *connection) Query(ctx context.Context, stmt *drivers.Statement) (*drivers.Result, error) {
	db, err := c.getDB(ctx)
	if err != nil {
		return nil, err
	}

	if c.configProp.LogQueries {
		c.logger.Info("StarRocks query",
			zap.String("query", stmt.Query),
			zap.Any("args", stmt.Args))
	}

	// Remove deadline but preserve cancellation (ClickHouse pattern)
	queryCtx := contextWithoutDeadline(ctx)

	rows, err := db.QueryxContext(queryCtx, stmt.Query, stmt.Args...)
	if err != nil {
		return nil, err
	}

	schema, err := c.rowsToSchema(rows)
	if err != nil {
		rows.Close()
		return nil, err
	}

	// Get column types for proper type handling in MapScan
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		return nil, err
	}

	// Create properly initialized row wrapper with typed scan destinations
	starrocksRows := &starrocksRows{
		Rows:     rows,
		scanDest: prepareScanDest(schema),
		colTypes: colTypes,
	}

	return &drivers.Result{
		Rows:   starrocksRows,
		Schema: schema,
	}, nil
}

// QuerySchema implements drivers.OLAPStore.
func (c *connection) QuerySchema(ctx context.Context, query string, args []any) (*runtimev1.StructType, error) {
	db, err := c.getDB(ctx)
	if err != nil {
		return nil, err
	}

	// Use LIMIT 0 to get schema without data
	schemaQuery := fmt.Sprintf("SELECT * FROM (%s) AS _schema_query LIMIT 0", query)

	// Remove deadline but preserve cancellation (ClickHouse pattern)
	queryCtx := contextWithoutDeadline(ctx)

	rows, err := db.QueryxContext(queryCtx, schemaQuery, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return c.rowsToSchema(rows)
}

// InformationSchema implements drivers.OLAPStore.
func (c *connection) InformationSchema() drivers.OLAPInformationSchema {
	return &informationSchema{c: c}
}

// rowsToSchema converts SQL rows to StructType schema.
func (c *connection) rowsToSchema(rows *sqlx.Rows) (*runtimev1.StructType, error) {
	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	fields := make([]*runtimev1.StructType_Field, len(colTypes))
	for i, ct := range colTypes {
		fields[i] = &runtimev1.StructType_Field{
			Name: ct.Name(),
			Type: c.databaseTypeToRuntimeType(ct.DatabaseTypeName()),
		}
	}

	return &runtimev1.StructType{Fields: fields}, nil
}

// databaseTypeToRuntimeType converts StarRocks/MySQL types to runtime types.
func (c *connection) databaseTypeToRuntimeType(dbType string) *runtimev1.Type {
	dbType = strings.ToUpper(dbType)

	// Handle nullable types
	if strings.HasPrefix(dbType, "NULLABLE(") {
		dbType = strings.TrimPrefix(dbType, "NULLABLE(")
		dbType = strings.TrimSuffix(dbType, ")")
	}

	// Handle parameterized types
	if idx := strings.Index(dbType, "("); idx != -1 {
		dbType = dbType[:idx]
	}

	switch dbType {
	case "BOOLEAN", "BOOL":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_BOOL}
	case "TINYINT":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_INT8}
	case "SMALLINT":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_INT16}
	case "INT", "INTEGER":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_INT32}
	case "BIGINT":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_INT64}
	case "LARGEINT":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_INT128}
	case "FLOAT":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_FLOAT32}
	case "DOUBLE":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_FLOAT64}
	case "DECIMAL", "DECIMALV2", "DECIMAL32", "DECIMAL64", "DECIMAL128":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_DECIMAL}
	case "CHAR", "VARCHAR", "STRING", "TEXT":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_STRING}
	case "DATE":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_DATE}
	case "DATETIME", "TIMESTAMP":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_TIMESTAMP}
	case "JSON", "JSONB":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_JSON}
	case "ARRAY":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_ARRAY}
	case "MAP":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_MAP}
	case "STRUCT":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_STRUCT}
	case "BINARY", "VARBINARY":
		return &runtimev1.Type{Code: runtimev1.Type_CODE_BYTES}
	default:
		return &runtimev1.Type{Code: runtimev1.Type_CODE_STRING}
	}
}

// prepareScanDest creates a slice of typed scan destinations based on the schema.
// This prevents the MySQL driver (used by StarRocks) from returning []byte for
// string/decimal types when scanning into interface{}.
func prepareScanDest(schema *runtimev1.StructType) []any {
	scanList := make([]any, len(schema.Fields))
	for i, field := range schema.Fields {
		var dest any
		switch field.Type.Code {
		case runtimev1.Type_CODE_BOOL:
			dest = &sql.NullBool{}
		case runtimev1.Type_CODE_INT8:
			// TINYINT in StarRocks is signed (-128 to 127), but Go's sql.NullByte is uint8
			// So we scan as int16 to handle negative values
			dest = &sql.NullInt16{}
		case runtimev1.Type_CODE_INT16:
			dest = &sql.NullInt16{}
		case runtimev1.Type_CODE_INT32:
			dest = &sql.NullInt32{}
		case runtimev1.Type_CODE_INT64:
			dest = &sql.NullInt64{}
		case runtimev1.Type_CODE_INT128:
			// LARGEINT - scan as string to avoid overflow
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_FLOAT32:
			// FLOAT - scan as float64 (Go's sql package doesn't have NullFloat32)
			dest = &sql.NullFloat64{}
		case runtimev1.Type_CODE_FLOAT64:
			dest = &sql.NullFloat64{}
		case runtimev1.Type_CODE_DECIMAL:
			// DECIMAL - scan as string to preserve precision
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_STRING:
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_DATE:
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_TIMESTAMP:
			dest = &sql.NullTime{}
		case runtimev1.Type_CODE_JSON:
			// JSON - scan as string
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_ARRAY:
			// ARRAY - scan as string (JSON representation)
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_MAP:
			// MAP - scan as string (JSON representation)
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_STRUCT:
			// STRUCT - scan as string (JSON representation)
			dest = &sql.NullString{}
		case runtimev1.Type_CODE_BYTES:
			// BINARY/VARBINARY - keep as any for base64 encoding
			dest = new(any)
		default:
			dest = new(any)
		}
		scanList[i] = dest
	}
	return scanList
}

// starrocksRows wraps sqlx.Rows to provide MapScan method.
// This is required because if the correct type is not provided to Scan
// the MySQL driver (used by StarRocks) returns byte arrays for string/decimal types.
type starrocksRows struct {
	*sqlx.Rows
	scanDest []any
	colTypes []*sql.ColumnType
}

// MapScan implements drivers.Rows.
func (r *starrocksRows) MapScan(dest map[string]any) error {
	// Scan into pre-allocated typed destinations
	err := r.Rows.Scan(r.scanDest...)
	if err != nil {
		return err
	}

	// Convert sql.Null* types to actual values and populate map
	cols, err := r.Rows.Columns()
	if err != nil {
		return err
	}

	for i, col := range cols {
		if i >= len(r.scanDest) {
			break
		}

		var value any
		switch v := r.scanDest[i].(type) {
		case *sql.NullBool:
			if v.Valid {
				value = v.Bool
			}
		case *sql.NullInt16:
			if v.Valid {
				value = v.Int16
			}
		case *sql.NullInt32:
			if v.Valid {
				value = v.Int32
			}
		case *sql.NullInt64:
			if v.Valid {
				value = v.Int64
			}
		case *sql.NullFloat64:
			if v.Valid {
				value = v.Float64
			}
		case *sql.NullString:
			if v.Valid {
				value = v.String
			}
		case *sql.NullTime:
			if v.Valid {
				value = v.Time
			}
		case *any:
			value = *v
		}
		dest[col] = value
	}

	return nil
}

// contextWithoutDeadline removes the deadline from the context but preserves cancellation.
// This prevents queries from being cancelled due to tight client timeouts while still
// respecting explicit cancellation signals.
func contextWithoutDeadline(parent context.Context) context.Context {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	go func() {
		<-parent.Done()
		cancel()
	}()
	return ctx
}