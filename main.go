package main

import (
	"context"
	"github.com/brianvoe/gofakeit"
	_ "github.com/lib/pq"
	dbpkg "github.com/meromen/generator/db"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	WORKERS_COUNT    = 32
	USERS_COUNT      = 500000
	MESSAGES_COUNT   = 10000000
	CATEGORIES_COUNT = 5000
)

func main() {
	categoryChan := make(chan dbpkg.Category)
	usersChan := make(chan dbpkg.User)
	messagesChan := make(chan dbpkg.Message)
	ctxWriter, cancelWriter := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	dbInst, err := dbpkg.Connect(nil)
	if err != nil {
		panic(err)
	}

	//err = dbpkg.CreateTables(dbInst)
	//if err != nil {
	//	panic(err)
	//}

	dbController := dbpkg.DataBaseController{
		UsersChan:      usersChan,
		CategoriesChan: categoryChan,
		MessagesChan:   messagesChan,
		Conn:           dbInst,
	}

	for i := 0; i < WORKERS_COUNT; i++ {
		wg.Add(1)
		go func() {
			err := dbController.InsertData(ctxWriter, &wg)
			if err != nil {
				panic(err)
			}
		}()
	}

	startGenerationTime := time.Now().Unix()

	gofakeit.Seed(time.Now().UnixNano())

	var categoryIds []string

	for i := 0; i < CATEGORIES_COUNT; i++ {
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
		categoryChan <- category
	}

	var userIds []string

	for i := 0; i < USERS_COUNT; i++ {
		user := dbpkg.User{
			Id:   gofakeit.UUID(),
			Name: gofakeit.Name(),
		}

		userIds = append(userIds, user.Id)
		usersChan <- user
	}

	for i := 0; i < MESSAGES_COUNT; i++ {
		message := dbpkg.Message{
			Id:         gofakeit.UUID(),
			Text:       gofakeit.ProgrammingLanguage(),
			CategoryId: categoryIds[rand.Intn(len(categoryIds))],
			PostedAt:   gofakeit.Date(),
			AuthorId:   userIds[rand.Intn(len(userIds))],
		}

		messagesChan <- message
	}

	for i := 0; i < WORKERS_COUNT; i++ {
		cancelWriter()
	}

	wg.Wait()

	endGenerationTime := time.Now().Unix()
	log.Printf("Generation data: %d seconds", endGenerationTime-startGenerationTime)
}
