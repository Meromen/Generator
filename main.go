package main

import (
	"database/sql"
	"github.com/brianvoe/gofakeit"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"log"
	"math/rand"
	"time"
)

type Message struct {
	Id         string `fake:"{ misc.uuid}"`
	Text       string `fake:"{ string.letter }"`
	CategoryId string
	PostedAt   time.Time `fake:"{ date.date }"`
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

func main() {

	startGenerationTime := time.Now().Unix()

	dbInst, err := sql.Open("postgres", "postgres://postgres@127.0.0.1:5432/generator?sslmode=disable")
	if err != nil {
		panic(err)
	}

	dbTx, err := dbInst.Begin()
	if err != nil {
		panic(err)
	}

	_, err = dbTx.Exec(CreateQuery)
	if err != nil {
		panic(err)
	}

	err = dbTx.Commit()
	if err != nil {
		panic(err)
	}

	gofakeit.Seed(time.Now().UnixNano())

	var categoryIds []string
	var categories []Category
	for i := 0; i < 5000; i++ {
		category := Category{
			Id:   gofakeit.UUID(),
			Name: gofakeit.Company(),
		}
		if i == 0 {
			category.ParentId = category.Id
		} else {
			category.ParentId = categoryIds[len(categoryIds)-1]
		}
		categoryIds = append(categoryIds, category.Id)
		categories = append(categories, category)
	}

	var userIds []string
	var users []User

	for i := 0; i < 500000; i++ {
		user := User{
			Id:   gofakeit.UUID(),
			Name: gofakeit.Name(),
		}

		userIds = append(userIds, user.Id)
		users = append(users, user)
	}

	var messages []Message

	for i := 0; i < 10000000; i++ {
		message := Message{
			Id:         gofakeit.UUID(),
			Text:       gofakeit.ProgrammingLanguage(),
			CategoryId: categoryIds[rand.Intn(len(categoryIds))],
			PostedAt:   gofakeit.Date(),
			AuthorId:   userIds[rand.Intn(len(userIds))],
		}

		messages = append(messages, message)
	}

	endGenerationTime := time.Now().Unix()

	log.Printf("Generation data: %d seconds" , endGenerationTime - startGenerationTime)

	startInsertingTime := time.Now().Unix()

	bdTx, err := dbInst.Begin()
	if err != nil {
		panic(err)
	}

	stmt, err := bdTx.Prepare(pq.CopyInSchema("public", "users", "id", "name"))
	if err != nil {
		panic(err)
	}

	for _, row := range users {
		if _, err := stmt.Exec((row).Id, (row).Name);
			err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}

	err = stmt.Close()
	if err != nil {
		panic(err)
	}

	err = bdTx.Commit()
	if err != nil {
		panic(err)
	}

	bdTx, err = dbInst.Begin()
	if err != nil {
		panic(err)
	}

	stmt, err = bdTx.Prepare(pq.CopyInSchema("public", "categories", "id", "name", "parent_id"))
	if err != nil {
		panic(err)
	}

	for _, row := range categories {
		if _, err := stmt.Exec(row.Id, row.Name, row.ParentId);
			err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}

	err = stmt.Close()
	if err != nil {
		panic(err)
	}

	err = bdTx.Commit()
	if err != nil {
		panic(err)
	}

	bdTx, err = dbInst.Begin()
	if err != nil {
		panic(err)
	}

	stmt, err = bdTx.Prepare(pq.CopyInSchema("public", "messages", "id", "text", "category_id", "posted_at", "author_id"))
	if err != nil {
		panic(err)
	}

	for _, row := range messages {
		if _, err := stmt.Exec(row.Id, row.Text, row.CategoryId, row.PostedAt, row.AuthorId);
			err != nil {
			panic(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		panic(err)
	}

	err = stmt.Close()
	if err != nil {
		panic(err)
	}

	err = bdTx.Commit()
	if err != nil {
		panic(err)
	}

	endInsertingTime := time.Now().Unix()

	log.Printf("Inserting data: %d seconds", endInsertingTime - startInsertingTime)
}
