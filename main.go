package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/labstack/gommon/log"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type MessageUnmarshal struct {
	Worker            int    `json:"worker"`
	DestinationWorker int    `json:"destination_worker"`
	Interval          int    `json:"interval"`
	Message           string `json:"message"`
}

func main() {

	var err error

	dir := filepath.Join(".", "db")

	err = os.MkdirAll(dir, 0755)
	if err != nil {
		log.Fatal(err)
		return
	}

	db, err := gorm.Open(sqlite.Open(filepath.Join(dir, "workers.db")), &gorm.Config{
		PrepareStmt:            true,
		SkipDefaultTransaction: true,
	})
	if err != nil {
		log.Fatal("Error starting GORM: ", err)
		return
	}

	defer func() {
		sqlDbCloser, err := db.DB()
		if err != nil {
			log.Fatal("Error preparing the database to close: ", err)
			return
		}
		err = sqlDbCloser.Close()
		if err != nil {
			log.Fatal("Error closing the database: ", err)
			return
		}
	}()

	//LIMPAR O BANCO
	// err = db.Exec(`DELETE FROM messages_table;`).Error
	// if err != nil {
	// 	log.Errorf("Clean DB")
	// }

	type Message struct {
		Data []byte `json:"data"`
	}

	var unparsedJSON []Message
	result := db.Raw(`SELECT data FROM messages_table;`).Scan(&unparsedJSON)
	err = result.Error
	if err != nil {
		log.Error("Error getting messages in main: ", err)
		return
	}
	var parsedMessages []MessageUnmarshal
	for _, message := range unparsedJSON {
		var unmarshalledMsg MessageUnmarshal
		if err := json.Unmarshal(message.Data, &unmarshalledMsg); err != nil {
			log.Error("Error unmarshalling JSON: ", err)
			continue
		}
		parsedMessages = append(parsedMessages, unmarshalledMsg)
	}

	// Imprimir saida dos dados organizados
	for _, msg := range parsedMessages {
		fmt.Printf("%+v\n", msg)
	}

	// SEGUNDA PARTE
}
