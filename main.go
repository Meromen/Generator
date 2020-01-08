package main

import (
	"github.com/brianvoe/gofakeit"
	_ "github.com/lib/pq"
	dbpkg "github.com/meromen/generator/db"
	"log"
	"math/rand"
	"time"
)

func main() {

	startGenerationTime := time.Now().Unix()

	dbInst, err := dbpkg.Connect(nil)
	if err != nil {
		panic(err)
	}

	err = dbpkg.CreateTables(dbInst)
	if err != nil {
		panic(err)
	}

	gofakeit.Seed(time.Now().UnixNano())

	var categoryIds []string
	var categories []dbpkg.Category
	for i := 0; i < 5000; i++ {
		category := dbpkg.Category{
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
	var users []dbpkg.User

	for i := 0; i < 500000; i++ {
		user := dbpkg.User{
			Id:   gofakeit.UUID(),
			Name: gofakeit.Name(),
		}

		userIds = append(userIds, user.Id)
		users = append(users, user)
	}

	var messages []dbpkg.Message

	for i := 0; i < 10000000; i++ {
		message := dbpkg.Message{
			Id:         gofakeit.UUID(),
			Text:       gofakeit.ProgrammingLanguage(),
			CategoryId: categoryIds[rand.Intn(len(categoryIds))],
			PostedAt:   gofakeit.Date(),
			AuthorId:   userIds[rand.Intn(len(userIds))],
		}

		messages = append(messages, message)
	}

	endGenerationTime := time.Now().Unix()

	log.Printf("Generation data: %d seconds", endGenerationTime-startGenerationTime)

	startInsertingTime := time.Now().Unix()

	err = dbpkg.InsertUsers(dbInst, &users)
	if err != nil {
		panic(err)
	}

	err = dbpkg.InsertCategories(dbInst, &categories)
	if err != nil {
		panic(err)
	}

	err = dbpkg.InsertMessages(dbInst, &messages)
	if err != nil {
		panic(err)
	}

	endInsertingTime := time.Now().Unix()

	log.Printf("Inserting data: %d seconds", endInsertingTime-startInsertingTime)
}
