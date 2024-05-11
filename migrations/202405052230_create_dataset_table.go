package migrations

import "gofr.dev/pkg/gofr/migration"

const createTable = `CREATE TABLE IF NOT EXISTS dataset
(
    id int not null auto_increment primary key,
    name varchar(50) not null,
    authors varchar(50) not null
);`

func createTableDataset() migration.Migrate {
	return migration.Migrate{
		UP: func(d migration.Datasource) error {
			_, err := d.SQL.Exec(createTable)
			if err != nil {
				return err
			}
			return nil
		},
	}
}
