package main

import (
	"./config"
	"encoding/json"
	"flag"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/influxdata/influxdb/client/v2"
)

var conf config.Config
var addr = flag.String("addr", "api.gemini.com", "ws service address")

type Message struct {
	ID          int     `json:"eventID"`
	Type        string  `json:"type"` // I only want to look at `update` type
	Events      []Event `json:"events"`
	Timestampms int     `json:"timestampms"`
}

type Event struct {
	Type      string `json:"type"` // I only want to look at `trade` type
	Price     string `json:"price"`
	Amount    string `json:"amount"`
	MakerSIDe string `json:"makerSIDe"`
}

func main() {
	var price float64
	var amt float64

	conf = config.LoadConfiguration("./configs.yaml")
	events := make(chan Event)

	flag.Parse()
	log.SetFlags(0)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	u := url.URL{Scheme: "wss", Host: *addr, Path: "/v1/marketdata/BTCUSD"}
	log.Printf("connecting to %s", u.String())

	// Create a new HTTPClient
	ic, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://localhost:8086",
		Username: conf.Db.Username,
		Password: conf.Db.Password,
	})
	if err != nil {
		log.Fatal(err)
	}
	// Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  conf.Db.Name,
		Precision: "s",
	})
	if err != nil {
		log.Fatal(err)
	}

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}

	defer c.Close()
	done := make(chan struct{})

	go func() {
		defer c.Close()
		defer close(done)
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}

			m := Message{}
			json.Unmarshal(message, &m)

			if m.Type == "update" && m.Events[0].Type == "trade" {
				events <- m.Events[0]
			}
		}
	}()

	ticker := time.NewTicker(3600 * time.Second)
	insertTicker := time.NewTicker(1 * time.Second)

	defer ticker.Stop()

	for {
		select {
		// Write points
		case _ = <-insertTicker.C:
			if len(bp.Points()) != 0 {
				if err := ic.Write(bp); err != nil {
					log.Fatal(err)
				}

				bp, err = client.NewBatchPoints(client.BatchPointsConfig{
					Database:  conf.Db.Name,
					Precision: "s",
				})
				if err != nil {
					log.Fatal(err)
				}
			}

		// Add into points
		case tm := <-events:
			price, _ = strconv.ParseFloat(tm.Price, 64)
			amt, _ = strconv.ParseFloat(tm.Amount, 64)

			fields := map[string]interface{}{
				"price":  price,
				"amount": amt,
			}
			log.Printf("Price: " + tm.Price + ", Amount: " + tm.Amount)
			pt, err := client.NewPoint("trades", nil, fields, time.Now())
			if err != nil {
				log.Fatal(err)
			}
			bp.AddPoint(pt)

		case t := <-ticker.C:
			err := c.WriteMessage(websocket.TextMessage, []byte(t.String()))
			if err != nil {
				log.Println("write:", err)
				return
			}
		case <-interrupt:
			log.Println("Interrupt")
			err := c.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))

			if err != nil {
				log.Println("write close:", err)
				return
			}
			select {
			case <-done:
			case <-time.After(3600 * time.Second):
			}
			c.Close()
			return
		}
	}
}
