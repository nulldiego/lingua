package records

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/nulldiego/lingua/internal/datasets"
	"gofr.dev/pkg/gofr"
	"strconv"
)

const (
	querySelectDataset = "SELECT * FROM dataset WHERE id = ?"
	queryCountContent  = "SELECT COUNT(line_number) FROM dataset_%d"
	querySelectContent = "SELECT * FROM dataset_%d LIMIT %d,%d"
	querySelectRecord  = "SELECT * from dataset_%d WHERE line_number = ?"
)

var errGetDataset = errors.New("couldn't get dataset")
var errGetRecord = errors.New("couldn't get record")

type DatasetContent struct {
	datasets.Dataset
	TotalItems int           `json:"total_items"`
	Content    []interface{} `json:"content"`
}

type Record interface{}

func GetRecord(ctx *gofr.Context) (Record, error) {
	datasetId, err := strconv.Atoi(ctx.PathParam("id"))
	if err != nil {
		ctx.Logger.Errorf("error path param id: %v", err)
		return nil, errGetRecord
	}
	recordId, err := strconv.Atoi(ctx.PathParam("recordId"))
	if err != nil {
		ctx.Logger.Errorf("error path param record id: %v", err)
		return nil, errGetRecord
	}

	row, err := ctx.SQL.QueryContext(ctx, fmt.Sprintf(querySelectRecord, datasetId), recordId)
	if err != nil {
		ctx.Logger.Errorf("error query dataset record: %v", err)
		return nil, errGetRecord
	}
	var record Record
	record = rowsToJson(ctx, row)[0]

	return record, nil
}

func UpdateRecord(ctx *gofr.Context) (interface{}, error) {

	return nil, nil
}

func GetDatasetRecords(ctx *gofr.Context) (*DatasetContent, error) {
	var datasetContent DatasetContent

	datasetId, err := strconv.Atoi(ctx.PathParam("id"))
	if err != nil {
		ctx.Logger.Errorf("error path param id: %v", err)
		return nil, errGetDataset
	}
	page, err := strconv.Atoi(ctx.Param("page"))
	if err != nil {
		ctx.Logger.Errorf("error param page: %v", err)
		page = 1
	}
	items, err := strconv.Atoi(ctx.Param("items"))
	if err != nil {
		ctx.Logger.Errorf("error param items: %v", err)
		items = 10
	}

	ctx.SQL.Select(ctx, &datasetContent.Dataset, querySelectDataset, datasetId)

	totalItems := ctx.SQL.QueryRowContext(ctx, fmt.Sprintf(queryCountContent, datasetId))
	if err := totalItems.Scan(&datasetContent.TotalItems); err != nil {
		ctx.Logger.Errorf("error count dataset content: %v", err)
		return nil, errGetDataset
	}

	rows, err := ctx.SQL.QueryContext(ctx, fmt.Sprintf(querySelectContent, datasetId, (page-1)*items, items))
	if err != nil {
		ctx.Logger.Errorf("error query dataset content: %v", err)
		return nil, errGetDataset
	}

	datasetContent.Content = rowsToJson(ctx, rows)

	return &datasetContent, nil
}

func rowsToJson(ctx *gofr.Context, rows *sql.Rows) []interface{} {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		ctx.Logger.Errorf("error column types: %v", err)
		return nil
	}

	count := len(columnTypes)
	finalRows := []interface{}{}

	for rows.Next() {
		scanArgs := make([]interface{}, count)
		for i, v := range columnTypes {
			switch v.DatabaseTypeName() {
			case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
				scanArgs[i] = new(sql.NullString)
				break
			case "BOOL":
				scanArgs[i] = new(sql.NullBool)
				break
			case "INT4":
				scanArgs[i] = new(sql.NullInt64)
				break
			default:
				scanArgs[i] = new(sql.NullString)
			}
		}
		err := rows.Scan(scanArgs...)

		if err != nil {
			ctx.Logger.Errorf("error scan row: %v", err)
			return nil
		}

		masterData := map[string]interface{}{}
		for i, v := range columnTypes {

			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				masterData[v.Name()] = z.Bool
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[v.Name()] = z.String
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[v.Name()] = z.Int64
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				masterData[v.Name()] = z.Float64
				continue
			}
			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				masterData[v.Name()] = z.Int32
				continue
			}
			masterData[v.Name()] = scanArgs[i]
		}

		finalRows = append(finalRows, masterData)
	}

	return finalRows
}
