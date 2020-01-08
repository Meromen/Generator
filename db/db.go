package db

import (
	"database/sql"
	_ "github.com/jackc/pgx"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"time"
)

const (
	CreateSchemaQuery = `CREATE SCHEMA IF NOT EXISTS "generator";`

	CreateQuery string = `
		DROP TABLE IF EXISTS  public.categories CASCADE;
		DROP TABLE IF EXISTS  public.users CASCADE;
		DROP TABLE IF EXISTS  public.messages CASCADE;

-- 		CREATE SCHEMA IF NOT EXISTS "generator";
		CREATE UNLOGGED TABLE IF NOT EXISTS public.messages (
		"id" uuid NOT NULL,
		"text" TEXT NOT NULL,
		"category_id" uuid NOT NULL,
		"posted_at" TIME NOT NULL,
		"author_id" uuid NOT NULL
	) WITH (
		OIDS=FALSE
	);	
	
	CREATE UNLOGGED TABLE IF NOT EXISTS  public.categories (
		"id" uuid NOT NULL,
		"name" varchar(255) NOT NULL,
		"parent_id" uuid NOT NULL
	) WITH (
		OIDS=FALSE
	);	
	
	CREATE UNLOGGED TABLE IF NOT EXISTS  public.users (
		"id" uuid NOT NULL,
		"name" varchar(255) NOT NULL
	) WITH (
		OIDS=FALSE
	);

	ALTER TABLE public.users SET (autovacuum_enabled = false);
	ALTER TABLE public.categories SET (autovacuum_enabled = false);
	ALTER TABLE public.messages SET (autovacuum_enabled = false);
	`
)

type Message struct {
	Id         string
	Text       string
	CategoryId string
	PostedAt   time.Time
	AuthorId   string
}

type Category struct {
	Id       string
	Name     string
	ParentId string
}

type User struct {
	Id   string
	Name string
}

var defaultPgUrl = "postgres://postgres@127.0.0.1:5432/badge?sslmode=disable"

func Connect(connStr *string) (*sql.DB, error) {
	if connStr == nil {
		connStr = &defaultPgUrl
	}

	db, err := sql.Open("postgres", *connStr)
	return db, err
}

func CreateTables(conn *sql.DB) error {
	bdTx, err := conn.Begin()
	if err != nil {
		return err
	}

	_, err = bdTx.Exec(CreateQuery)
	if err != nil {
		return err
	}

	err = bdTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func InsertUsers(conn *sql.DB, users *[]User) error {
	bdTx, err := conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := bdTx.Prepare(pq.CopyInSchema("public", "users", "id", "name"))
	if err != nil {
		return err
	}

	for _, row := range *users {
		if _, err := stmt.Exec((row).Id, (row).Name);
			err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = bdTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func InsertCategories(conn *sql.DB, categories *[]Category) error {
	bdTx, err := conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := bdTx.Prepare(pq.CopyInSchema("public", "categories", "id", "name", "parent_id"))
	if err != nil {
		return err
	}

	for _, row := range *categories {
		if _, err := stmt.Exec(row.Id, row.Name, row.ParentId);
			err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = bdTx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func InsertMessages(conn *sql.DB, messages *[]Message) error {
	bdTx, err := conn.Begin()
	if err != nil {
		return err
	}

	stmt, err := bdTx.Prepare(pq.CopyInSchema("public", "messages", "id", "text", "category_id", "posted_at", "author_id"))
	if err != nil {
		return err
	}

	for _, row := range *messages {
		if _, err := stmt.Exec(row.Id, row.Text, row.CategoryId, row.PostedAt, row.AuthorId);
			err != nil {
			return err
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	err = stmt.Close()
	if err != nil {
		return err
	}

	err = bdTx.Commit()
	if err != nil {
		return err
	}

	return nil
}
