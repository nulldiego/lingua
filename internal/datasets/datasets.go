package datasets

import (
	"errors"
	"fmt"
	"gofr.dev/pkg/gofr"
	"io"
	"mime/multipart"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	queryInsertDataset = "INSERT INTO dataset (name, authors) VALUES (?, ?)"
	querySelectAll     = "SELECT * FROM dataset"
	queryDatasetFields = "SELECT column_name, column_type, column_comment FROM information_schema.columns WHERE table_name = ? order by ordinal_position"
	queryInsertColumn  = "alter table dataset_%d add column (%s)"
)

var errSavingFile = errors.New("error saving file")
var errObtainingDataset = errors.New("error obtaining dataset")
var errInvalidBody = errors.New("error invalid body")
var errCreateField = errors.New("error creating field")

type Dataset struct {
	Id      int                   `json:"id"`
	Name    string                `json:"name"`
	Authors string                `json:"authors"`
	File    *multipart.FileHeader `file:"file" json:"-"`
}

type Field struct {
	Name       string   `json:"name"`
	Options    []string `json:"options,omitempty"` // options in case field is enum
	Annotate   bool     `json:"annotate,omitempty"`
	ColumnType string   `json:"-"`
}

func CreateDatasetField(ctx *gofr.Context) ([]Field, error) {
	datasetId, err := strconv.Atoi(ctx.PathParam("id"))
	if err != nil {
		ctx.Logger.Errorf("error path param id: %v", err)
		return nil, errObtainingDataset
	}

	var fields []Field
	if err := ctx.Bind(&fields); err != nil {
		ctx.Logger.Errorf("error binding fields: %v", err)
		return nil, errInvalidBody
	}
	var columns []string
	for _, field := range fields {
		// TODO: Validate field name and options, potential sql injection (?)
		columnName := strings.ReplaceAll(field.Name, " ", "_")
		columnType := "VARCHAR(4000)"
		if len(field.Options) > 0 {
			columnType = fmt.Sprintf("ENUM('%s')", strings.Join(field.Options, "','"))
		}
		columns = append(columns, fmt.Sprintf("%s %s COMMENT 'user_defined'", columnName, columnType))
	}
	query := fmt.Sprintf(queryInsertColumn, datasetId, strings.Join(columns, ","))
	_, err = ctx.SQL.ExecContext(ctx, query)
	if err != nil {
		ctx.Logger.Errorf("error insert columns: %v", err)
		return nil, errCreateField
	}

	return GetDatasetFields(ctx)
}

func GetDatasetFields(ctx *gofr.Context) ([]Field, error) {
	datasetId := ctx.PathParam("id")
	var fields []Field
	rows, err := ctx.SQL.Query(queryDatasetFields, fmt.Sprintf("dataset_%s", datasetId))
	if err != nil {
		return nil, errObtainingDataset
	}
	for rows.Next() {
		var field Field
		var comment string
		if err := rows.Scan(&field.Name, &field.ColumnType, &comment); err != nil {
			return nil, errObtainingDataset
		}
		field.Annotate = comment == "user_defined"
		if strings.HasPrefix(field.ColumnType, "enum") {
			columnType := strings.ReplaceAll(field.ColumnType[5:len(field.ColumnType)-1], "'", "")
			field.Options = strings.Split(columnType, ",")
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// Create Inserts a new dataset
func Create(ctx *gofr.Context) (*Dataset, error) {
	var dataset Dataset
	if err := ctx.Bind(&dataset); err != nil {
		ctx.Logger.Errorf("error binding dataset: %v", err)
		return nil, errors.New("invalid body")
	}

	// TODO: As form data instead of params (https://github.com/gofr-dev/gofr/issues/623)
	dataset.Name = ctx.Param("name")
	dataset.Authors = ctx.Param("authors")

	var err error
	if dataset.Id, err = insert(ctx, dataset); err != nil {
		return nil, errors.New("connection error")
	}

	err = createDatasetTable(ctx, dataset.Id, dataset.File)
	return &dataset, err
}

// GetAll Get all datasets
func GetAll(ctx *gofr.Context) ([]Dataset, error) {
	var datasets []Dataset
	ctx.SQL.Select(ctx, &datasets, querySelectAll)
	return datasets, nil
}

func insert(ctx *gofr.Context, dataset Dataset) (int, error) {
	res, err := ctx.SQL.ExecContext(ctx, queryInsertDataset, dataset.Name, dataset.Authors)
	if err != nil {
		ctx.Logger.Errorf("error insert dataset: %v", err)
		return 0, err
	}
	id, err := res.LastInsertId()
	if err != nil {
		ctx.Logger.Errorf("error last insert id: %v", err)
		return 0, err
	}
	return int(id), nil
}

// TODO: Works for basic dataset, improve for handling tab-separated files, malformed files, etc.
// TODO: ¿Avoid using csvkit and process through go code?
func createDatasetTable(ctx *gofr.Context, datasetId int, file *multipart.FileHeader) error {
	// 1. Write csv file
	// 1.1 Open input file
	inputFile, err := file.Open()
	if err != nil {
		ctx.Logger.Errorf("error opening input file: %v", err)
		return errSavingFile
	}
	defer inputFile.Close()
	// 1.2 Create destination file
	destFile, err := os.Create("./tmp-data/input.csv")
	if err != nil {
		ctx.Logger.Errorf("error creating file: %v", err)
		return errSavingFile
	}
	defer destFile.Close()
	// 1.3 Copy input into destination file
	if _, err := io.Copy(destFile, inputFile); err != nil {
		ctx.Logger.Errorf("error copying input file: %v", err)
		return errSavingFile
	}

	// 2. Create sql table from csv (csvsql command form csvkit)
	// 2.1 Add line numbers to dataset
	// TODO: "-t" argument is for tab separated files, remove argument if it's not a tsv
	cmd := exec.Command("./venv/bin/csvformat", "-l", destFile.Name())

	outfile, err := os.Create(fmt.Sprintf("./tmp-data/dataset_%d.csv", datasetId))
	if err != nil {
		ctx.Logger.Errorf("error creating file for csvcut output: %v", err)
		return errSavingFile
	}
	defer outfile.Close()
	cmd.Stdout = outfile
	if err := cmd.Run(); err != nil {
		ctx.Logger.Errorf("error adding line numbers to csv: %v", err)
		return errSavingFile
	}

	// 2.2 Import to SQL
	// csvsql --dialect mysql --snifflimit 100000 bigdatafile.csv > maketable.sql
	// csvsql --db mysql://user:password@localhost:3306/dbschema --tables mytable --insert file.csv
	cmd = exec.Command("./venv/bin/csvsql", "--db", "mysql://root:root123@127.0.0.1:3306/test_db", "--insert", outfile.Name())
	if info, err := cmd.Output(); err != nil {
		ctx.Logger.Errorf("error import csv to mysql: %v, %s", err, info)
		return errSavingFile
	}

	// 3. ¿Delete csv file?
	return err
}
