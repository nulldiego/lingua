package api

import (
	"errors"
	"github.com/nulldiego/lingua/internal/dataset"
	"gofr.dev/pkg/gofr"
)

func RegisterRoutes(app *gofr.App) {
	app.POST("/datasets", postDataset)
	app.GET("/datasets", getDatasets)
	app.POST("/datasets/{id}/fields", postDatasetField)  // name, type
	app.GET("/datasets/{id}/fields", getDatasetFields)   // name, type
	app.GET("/datasets/{id}/records", getDatasetRecords) // paginated
	app.PUT("/datasets/{id}/records/{id}", putDatasetRecord)
}

func postDataset(ctx *gofr.Context) (interface{}, error) {
	return dataset.Create(ctx)
}

func getDatasets(ctx *gofr.Context) (interface{}, error) {
	return dataset.GetAll(ctx)
}

func postDatasetField(_ *gofr.Context) (interface{}, error) {
	return nil, errors.New("not implemented")
}

func getDatasetFields(_ *gofr.Context) (interface{}, error) {
	return nil, errors.New("not implemented")
}

func getDatasetRecords(_ *gofr.Context) (interface{}, error) {
	return nil, errors.New("not implemented")
}

func putDatasetRecord(_ *gofr.Context) (interface{}, error) {
	return nil, errors.New("not implemented")
}
