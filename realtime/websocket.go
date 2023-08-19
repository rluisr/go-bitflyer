package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/rluisr/go-bitflyer/private/auth"

	"github.com/buger/jsonparser"
)

const (
	Endpoint  = "wss://ws.lightstream.bitflyer.com/json-rpc"
	PingTimer = 3 * time.Minute
)

type Client struct {
	conn *websocket.Conn
	ctx  context.Context
}

func New(ctx context.Context) (*Client, error) {
	conn, _, err := websocket.DefaultDialer.Dial(Endpoint, nil)

	return &Client{
		conn: conn,
		ctx:  ctx,
	}, err
}

func (p *Client) Close() error {
	if err := p.conn.Close(); err != nil {
		return err
	}

	return nil
}

func (p *Client) Connect(conf *auth.Client, channels, symbols []string, send chan Response) {
	defer log.Println("defer is end, completed websocket connect")
	defer p.Close()

	p.conn.SetPongHandler(func(data string) error {
		return nil
	})

	requests := _createRequester(conf, channels, symbols)
	if err := p.subscribe(
		conf,
		requests,
	); err != nil {
		log.Fatal("[FATAL] ", err.Error())
	}
	defer p.unsubscribe(requests)

	go func() {
		for {
			res := new(Response)
			_, msg, err := p.conn.ReadMessage()
			if err != nil {
				log.Printf("read message error: %v\n", err)

				if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					continue
				} else if strings.Contains(err.Error(), "scheduled maintenance") { // maintenance
					send <- res._set(fmt.Errorf("%v, closed read websocket", err))
					// can be detected by the receiver, so it tells the receiver to terminate.
					time.Sleep(time.Second)
					close(send)
					return
				}

				send <- res._set(err)
				continue
			}

			channelName, err := jsonparser.GetString(msg, "params", "channel")
			if err != nil {
				send <- res._set(err)
				continue
			}
			data, _, _, err := jsonparser.Get(msg, "params", "message")
			if err != nil {
				send <- res._set(err)
				continue
			}

			switch {
			case strings.HasPrefix(channelName, Ticker):
				// fmt.Println(Ticker)
				res.Types = TickerN
				res.ProductCode = strings.Replace(channelName, Ticker, "", 1)
				if err := json.Unmarshal(data, &res.Ticker); err != nil {
					res.Types = ErrorN
					res._set(fmt.Errorf("[WARN]: cant unmarshal ticker %+v", err))
				}
			case strings.HasPrefix(channelName, Executions):
				// fmt.Println(Executions)
				res.Types = ExecutionsN
				res.ProductCode = strings.Replace(channelName, Executions, "", 1)
				if err := json.Unmarshal(data, &res.Executions); err != nil {
					res.Types = ErrorN
					res._set(fmt.Errorf("[WARN]: cant unmarshal executions %+v", err))
				}
			case strings.HasPrefix(channelName, BoardSnap):
				// fmt.Println(BoardSnap)
				res.Types = BoardSnapN
				res.ProductCode = strings.Replace(channelName, BoardSnap, "", 1)
				if err := json.Unmarshal(data, &res.Board); err != nil {
					res.Types = ErrorN
					res._set(fmt.Errorf("[WARN]: cant unmarshal board snap %+v", err))
				}
			case strings.HasPrefix(channelName, Board):
				// fmt.Println(Board)
				res.Types = BoardN
				res.ProductCode = strings.Replace(channelName, Board, "", 1)
				if err := json.Unmarshal(data, &res.Board); err != nil {
					res.Types = ErrorN
					res._set(fmt.Errorf("[WARN]: cant unmarshal board update %+v", err))
				}
			case strings.HasPrefix(channelName, ChildOrders):
				// fmt.Println(ChildOrders)
				res.Types = ChildOrdersN
				if err := json.Unmarshal(data, &res.ChildOrders); err != nil {
					res.Types = ErrorN
					res._set(fmt.Errorf("[WARN]: cant unmarshal childorder %+v", err))
				}
			case strings.HasPrefix(channelName, ParentOrders):
				// fmt.Println(ParentOrders)
				res.Types = ParentOrdersN
				if err := json.Unmarshal(data, &res.ParentOrders); err != nil {
					res.Types = ErrorN
					res._set(fmt.Errorf("[WARN]: cant unmarshal parentorder %+v", err))
				}
			case strings.HasPrefix(channelName, Error):
				// fmt.Println("error!")
				res.Types = ErrorN
				res._set(err)
			default:
				// fmt.Println("undefined", data)
				res.Types = UndefinedN
			}

			send <- *res
		}
	}()

	go func() {
		t := time.NewTicker(PingTimer)
		defer t.Stop()
		for {
			select {
			case <-p.ctx.Done():
				return
			case <-t.C:
				err := p.conn.WriteMessage(websocket.PingMessage, []byte{})
				if err != nil {
					log.Println("ping error detected. will reconnect err: ", err.Error())
					p.Connect(conf, channels, symbols, send)
				}
			}
		}
	}()

	<-p.ctx.Done()
	log.Println("received context cancel from parent, websocket closed")
}
