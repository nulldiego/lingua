package api

import (
	"errors"
	"github.com/nulldiego/lingua/internal/datasets"
	"github.com/nulldiego/lingua/internal/records"
	"gofr.dev/pkg/gofr"
)

func RegisterRoutes(app *gofr.App) {
	app.POST("/api/datasets", postDataset)
	app.GET("/api/datasets", getDatasets)
	app.POST("/api/datasets/{id}/fields", postDatasetField) // name, type
	app.GET("/api/datasets/{id}/fields", getDatasetFields)  // name, type
	app.GET("/api/datasets/{id}/records", getDatasetRecords)
	app.PUT("/api/datasets/{id}/records/{id}", putDatasetRecord)
}

func postDataset(ctx *gofr.Context) (interface{}, error) {
	return datasets.Create(ctx)
}

func getDatasets(ctx *gofr.Context) (interface{}, error) {
	return datasets.GetAll(ctx)
}

func postDatasetField(ctx *gofr.Context) (interface{}, error) {
	return datasets.CreateDatasetField(ctx)
}

func getDatasetFields(ctx *gofr.Context) (interface{}, error) {
	return datasets.GetDatasetFields(ctx)
}

func getDatasetRecords(ctx *gofr.Context) (interface{}, error) {
	return records.GetDatasetRecords(ctx)
}

func putDatasetRecord(_ *gofr.Context) (interface{}, error) {
	return nil, errors.New("not implemented")
}
