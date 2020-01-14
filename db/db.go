package db

import (
	"context"
	"database/sql"
	//"github.com/golang-migrate/migrate"

	"github.com/golang-migrate/migrate"
	_ "github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	_ "github.com/jackc/pgx"
	"github.com/joho/godotenv"
	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"log"
	"os"
	"sync"
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

type DataBaseController struct {
	UsersChan      chan User
	CategoriesChan chan Category
	MessagesChan   chan Message
	Conn           *sql.DB
}

func Connect(connStr *string) (*sql.DB, error) {
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	if connStr == nil {
		pgUrl := os.Getenv("DEFAULT_PG_URL")
		connStr = &pgUrl
	}

	m, err := migrate.New(
		"file://db/migrations",
		*connStr)
	if err != nil {
		log.Fatal(err)
	}
	if err := m.Up(); err != nil {
		log.Fatal(err)
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

func (dbc *DataBaseController) InsertData(ctx context.Context, wg *sync.WaitGroup) error {
	defer func() {
		wg.Done()
		log.Println("Worker stop")
	} ()

	log.Println("Worker Start")

	categoriesBdTx, err := dbc.Conn.Begin()
	if err != nil {
		err := categoriesBdTx.Rollback()
		if err != nil {
			return err
		}
	}

	categoriesStmt, err := categoriesBdTx.Prepare(pq.CopyInSchema("public", "categories", "id", "name", "parent_id"))
	if err != nil {
		err := categoriesBdTx.Rollback()
		if err != nil {
			return err
		}
	}

	usersBdTx, err := dbc.Conn.Begin()
	if err != nil {
		err := usersBdTx.Rollback()
		if err != nil {
			return err
		}
	}

	usersStmt, err := usersBdTx.Prepare(pq.CopyInSchema("public", "users", "id", "name"))
	if err != nil {
		err := usersBdTx.Rollback()
		if err != nil {
			return err
		}
	}

	messagesDdTx, err := dbc.Conn.Begin()
	if err != nil {
		err := messagesDdTx.Rollback()
		if err != nil {
			return err
		}
	}

	messagesStmt, err := messagesDdTx.Prepare(pq.CopyInSchema("public", "messages", "id", "text", "category_id", "posted_at", "author_id"))
	if err != nil {
		err := messagesDdTx.Rollback()
		if err != nil {
			return err
		}
	}

	for {
		select {
		case category := <-dbc.CategoriesChan:
			{
				if _, err := categoriesStmt.Exec(category.Id, category.Name, category.ParentId);
					err != nil {
					err := categoriesBdTx.Rollback()
					if err != nil {
						return err
					}
				}
			}
		case user := <-dbc.UsersChan:
			{
				if _, err := usersStmt.Exec((user).Id, (user).Name);
					err != nil {
					err := usersBdTx.Rollback()
					if err != nil {
						return err
					}
				}
			}
		case message := <-dbc.MessagesChan:
			{
				if _, err := messagesStmt.Exec(message.Id, message.Text, message.CategoryId, message.PostedAt, message.AuthorId);
					err != nil {
					err := messagesDdTx.Rollback()
					if err != nil {
						return err
					}
				}
			}
		case <-ctx.Done():
			_, err = categoriesStmt.Exec()
			if err != nil {
				err := categoriesBdTx.Rollback()
				if err != nil {
					return err
				}
			}

			err = categoriesStmt.Close()
			if err != nil {
				err := categoriesBdTx.Rollback()
				if err != nil {
					return err
				}
			}

			err = categoriesBdTx.Commit()
			if err != nil {
				err := categoriesBdTx.Rollback()
				if err != nil {
					return err
				}
			}

			_, err = usersStmt.Exec()
			if err != nil {
				err := usersBdTx.Rollback()
				if err != nil {
					return err
				}
			}

			err = usersStmt.Close()
			if err != nil {
				err := usersBdTx.Rollback()
				if err != nil {
					return err
				}
			}

			err = usersBdTx.Commit()
			if err != nil {
				err := usersBdTx.Rollback()
				if err != nil {
					return err
				}
			}

			_, err = messagesStmt.Exec()
			if err != nil {
				err := messagesDdTx.Rollback()
				if err != nil {
					return err
				}
			}

			err = messagesStmt.Close()
			if err != nil {
				err := messagesDdTx.Rollback()
				if err != nil {
					return err
				}
			}

			err = messagesDdTx.Commit()
			if err != nil {
				err := messagesDdTx.Rollback()
				if err != nil {
					return err
				}
			}

			return nil
		}
	}

	return nil
}
