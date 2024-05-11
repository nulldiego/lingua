package dataset

import (
	"errors"
	"fmt"
	"gofr.dev/pkg/gofr"
	"io"
	"mime/multipart"
	"os"
	"os/exec"
)

const (
	queryInsertDataset = "INSERT INTO dataset (name, authors) VALUES (?, ?)"
	querySelectAll     = "SELECT * FROM dataset"
)

var errSavingFile = errors.New("error saving file")

type Dataset struct {
	Id      int                   `json:"id"`
	Name    string                `json:"name"`
	Authors string                `json:"authors"`
	File    *multipart.FileHeader `file:"file"`
}

// Create Inserts a new dataset
func Create(ctx *gofr.Context) (*Dataset, error) {
	var dataset Dataset
	if err := ctx.Bind(&dataset); err != nil {
		ctx.Logger.Errorf("error binding: %v", err)
		return nil, errors.New("invalid body")
	}

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
	destFile, err := os.Create(fmt.Sprintf("dataset_%d", datasetId))
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
	cmd := exec.Command("csvcut", "-l", "-t", fmt.Sprintf("%s > %s", destFile.Name(), destFile.Name()))
	if info, err := cmd.Output(); err != nil {
		ctx.Logger.Errorf("error adding line numbers to csv: %v, %s", err, info)
		return errSavingFile
	}

	// 2.2 Import to SQL
	// csvsql --dialect mysql --snifflimit 100000 bigdatafile.csv > maketable.sql
	// csvsql --db mysql://user:password@localhost:3306/dbschema --tables mytable --insert file.csv
	cmd = exec.Command("csvsql", "--db", "mysql://root:root123@localhost:3306/test_db", "--insert", destFile.Name())
	err = cmd.Run()

	// 3. ¿Delete csv file?
	return err
}
