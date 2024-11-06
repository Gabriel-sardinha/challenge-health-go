package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

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

type LastMessage struct {
	Sender  int
	Message string
}

var waitGroup sync.WaitGroup

var lastMessages = [5]LastMessage{}

type MessageOutput struct {
	worker           int
	worker_listened  int
	message          string
	message_listened string
}

func PrintMessageOfAWorker(worker int, messages []MessageUnmarshal) {
	// Espera todas as rotinas iniciarem para iniciar ao mesmo tempo
	waitGroup.Done()
	waitGroup.Wait()
	messageOutput := MessageOutput{}
	for _, msg := range messages {

		messageOutput.worker = msg.Worker
		messageOutput.worker_listened = lastMessages[msg.DestinationWorker-1].Sender
		messageOutput.message = msg.Message
		messageOutput.message_listened = lastMessages[msg.DestinationWorker-1].Message

		fmt.Printf("worker: %d\nworker_listened: %d\nmessage: \"%s\"\nmessage_listened: \"%s\"\n\n",
			messageOutput.worker, messageOutput.worker_listened, messageOutput.message, messageOutput.message_listened)

		lastMessages[msg.DestinationWorker-1] = LastMessage{Sender: msg.DestinationWorker, Message: msg.Message}

		// * 30 porque acho que o intervalo entre a inicialização de cada rotina está atrapalhando a ordem de impressão
		<-time.After(time.Duration(msg.Interval) * time.Millisecond * 30)
	}
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
	// for _, msg := range parsedMessages {
	// 	fmt.Printf("%+v\n", msg)
	// }

	//// Imprimindo os dados no formato final
	//for _, msg := range parsedMessages {
	//	fmt.Printf("worker: %d\nDestinationWorker: %d\nmessage: \"%s\"\nmessage_listened: \"%s\"\n\n",
	//		msg.Worker, msg.DestinationWorker, msg.Message, getMessageListened(msg, parsedMessages))
	//}

	// Uma lista de mensagens para cada worker
	worker1Messages := make([]MessageUnmarshal, 0)
	worker2Messages := make([]MessageUnmarshal, 0)
	worker3Messages := make([]MessageUnmarshal, 0)
	worker4Messages := make([]MessageUnmarshal, 0)
	worker5Messages := make([]MessageUnmarshal, 0)

	// O tempo total de espera
	amountOfTimeToWait := 0
	// Separar as mensagens em suas respectivas listas
	for _, msg := range parsedMessages {
		amountOfTimeToWait += msg.Interval
		switch msg.Worker {
		case 1:
			worker1Messages = append(worker1Messages, msg)
		case 2:
			worker2Messages = append(worker2Messages, msg)
		case 3:
			worker3Messages = append(worker3Messages, msg)
		case 4:
			worker4Messages = append(worker4Messages, msg)
		case 5:
			worker5Messages = append(worker5Messages, msg)
		}
	}

	// Inicia 6 esperas (5 workers + 1 para sincronizar)
	waitGroup.Add(6)
	// Imprimir as mensagens de cada worker
	go PrintMessageOfAWorker(1, worker1Messages)
	go PrintMessageOfAWorker(2, worker2Messages)
	go PrintMessageOfAWorker(3, worker3Messages)
	go PrintMessageOfAWorker(4, worker4Messages)
	go PrintMessageOfAWorker(5, worker5Messages)

	// Esperar 1 segundo para sincronizar
	<-time.After(time.Second)
	// Sincronizar as rotinas
	waitGroup.Done()

	// c é um canal que recebe sinais do sistema operacional como o interrupt
	var c chan os.Signal

	// Como a soma de todos os tempos está maior que os tempos paralelos, é melhor lidar com o interrupt
	select {
	case <-time.After(time.Duration(amountOfTimeToWait) * time.Millisecond):
		return
	case <-c:
		return
	}
}

//func getMessageListened(msg MessageUnmarshal, messages []MessageUnmarshal) string {
//	for _, m := range messages {
//		if m.Worker == msg.DestinationWorker {
//			return m.Message
//		}
//	}
//	return "N/A" // Caso não seja encontrada, retorna "N/A"
//}
