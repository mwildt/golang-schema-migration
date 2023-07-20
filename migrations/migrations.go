package migrations

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type scriptSource struct {
	path string
	name string
}

func (src scriptSource) join() string {
	return filepath.Join(src.path, src.name)
}

func Migrate(db *sql.DB) error {

	createExecutedScriptsTable(db)

	scriptDir := "./migrations"

	fmt.Printf("start running schema-migrations (src: %s)\n", scriptDir)

	err := runScripts(context.Background(), db, scriptDir)
	if err != nil {
		return err
	}

	fmt.Println("successfully finished migrations.")
	return nil
}

func runScripts(ctx context.Context, db *sql.DB, scriptDir string) error {

	fileInfos, err := ioutil.ReadDir(scriptDir)
	if err != nil {
		return err
	}

	// Skripte in aufsteigender Reihenfolge nach Dateinamen sortieren
	scripts := sortScripts(fileInfos)

	// SQL-Skripte ausführen
	for _, script := range scripts {

		if scriptError := runScript(ctx, db, scriptSource{scriptDir, script.Name()}); scriptError != nil {
			return scriptError
		}
	}

	return nil
}

func runScript(ctx context.Context, db *sql.DB, src scriptSource) error {

	scriptPath := src.join()
	content, err := ioutil.ReadFile(scriptPath)

	fail := func(err error) error {
		return fmt.Errorf("error running migtaion script %s: %v\n", scriptPath, err)
	}

	if err != nil {
		return err
	}

	checksum := calculateChecksum(content)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fail(err)
	}
	fmt.Printf("begin transaction for file %s\n", scriptPath)
	defer func() {
		fmt.Printf("rollback transaction for file %s\n", scriptPath)
		tx.Rollback()
	}()

	// Überprüfen, ob das Skript bereits ausgeführt wurde
	executed, err := isScriptExecuted(tx, src, checksum)
	if err != nil {
		return fail(err)
	} else if executed {
		fmt.Printf("migration '%s' has already run, skip\n", scriptPath)
		return nil
	}

	// SQL-Skriptinhalt ausführen
	if err = executeCommands(tx, ctx, content); err != nil {
		return fail(err)
	}

	// Speichern, der Ausführung
	if err = saveScriptChecksum(tx, ctx, src, checksum); err != nil {
		fmt.Printf("unable to save script metadata and checksum, rollback transaction'%s'\n", err.Error())
		return fail(err)
	}

	fmt.Printf("migration '%s' has been applied.\n", scriptPath)
	if err = tx.Commit(); err != nil {
		return fail(err)
	}

	return nil
}

func executeCommands(tx *sql.Tx, ctx context.Context, content []byte) error {
	statements := strings.Split(string(content), ";")

	for _, statement := range statements {
		trimmedStatement := strings.TrimSpace(statement)

		// Leere oder Kommentarzeilen überspringen
		if trimmedStatement == "" || strings.HasPrefix(trimmedStatement, "--") {
			continue
		}

		_, err := tx.ExecContext(ctx, trimmedStatement)
		if err != nil {
			return err
		}

		fmt.Printf("statement executed: %s\n", trimmedStatement)
	}
	return nil

}

func sortScripts(fileInfos []os.FileInfo) []os.FileInfo {
	sort.Slice(fileInfos, func(i, j int) bool {
		return fileInfos[i].Name() < fileInfos[j].Name()
	})
	return fileInfos
}

func calculateChecksum(content []byte) string {
	hash := md5.Sum(content)
	return hex.EncodeToString(hash[:])
}

func isScriptExecuted(tx *sql.Tx, src scriptSource, checksum string) (bool, error) {
	query := "SELECT checksum FROM executed_scripts WHERE script_name = $1"
	var storedChecksum string
	err := tx.QueryRow(query, src.join()).Scan(&storedChecksum)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil // Skript wurde noch nicht ausgeführt
		}
		return false, err // Fehler bei der Datenbankabfrage
	}

	if storedChecksum != checksum {
		return false, fmt.Errorf("invalid checksum for migration '%s' (%s != %s)", src.join(), checksum, storedChecksum)
	}

	return true, nil
}

func saveScriptChecksum(tx *sql.Tx, ctx context.Context, src scriptSource, checksum string) error {
	query := "INSERT INTO executed_scripts (script_name, checksum) VALUES ($1, $2)"
	_, err := tx.ExecContext(ctx, query, src.join(), checksum)
	return err
}

func createExecutedScriptsTable(db *sql.DB) error {
	query := `
		CREATE TABLE IF NOT EXISTS executed_scripts (
			id serial PRIMARY KEY,
			script_name VARCHAR(255) NOT NULL UNIQUE,
			checksum VARCHAR(32) NOT NULL,
			executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`
	_, err := db.Exec(query)
	return err
}
