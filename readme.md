# Golang Schema Migration

This is a simple schema-migration-utility project.

## Usage
Put SQL-Scripts into `./migrations` directory. They will be executed in increasing order of simple string comparison.

```
db, err := sql.Open("...", ...)
if err != nil {
    log.Fatal(err)
}
defer  db.Close()

err = migrations.Migrate(db)
if err != nil {
    ...
}
```