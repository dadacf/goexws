package huobi

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	. "github.com/nntaoli-project/goex"
)

var _INERNAL_KLINE_PERIOD_CONVERTER = map[int]string{
	KLINE_PERIOD_1MIN:   "1min",
	KLINE_PERIOD_5MIN:   "5min",
	KLINE_PERIOD_15MIN:  "15min",
	KLINE_PERIOD_30MIN:  "30min",
	KLINE_PERIOD_60MIN:  "60min",
	KLINE_PERIOD_1DAY:   "1day",
	KLINE_PERIOD_1WEEK:  "1week",
	KLINE_PERIOD_1MONTH: "1mon",
	KLINE_PERIOD_1YEAR:  "1year",
}

type SpotWs struct {
	*WsBuilder
	sync.Once
	wsConn *WsConn

	tickerCallback func(*Ticker)
	depthCallback  func(*Depth)
	tradeCallback  func(*Trade)
	klineCallback  func(*Kline, int)
}

func NewSpotWs() *SpotWs {
	ws := &SpotWs{
		WsBuilder: NewWsBuilder(),
	}
	ws.WsBuilder = ws.WsBuilder.
		WsUrl("wss://api.huobi.pro/ws").
		AutoReconnect().
		DecompressFunc(GzipDecompress).
		ProtoHandleFunc(ws.handle)
	return ws
}

func (ws *SpotWs) DepthCallback(call func(depth *Depth)) {
	ws.depthCallback = call
}

func (ws *SpotWs) TickerCallback(call func(ticker *Ticker)) {
	ws.tickerCallback = call
}

func (ws *SpotWs) TradeCallback(call func(trade *Trade)) {
	ws.tradeCallback = call
}

func (ws *SpotWs) KlineCallback(call func(*Kline, int)) {
	ws.klineCallback = call
}

func (ws *SpotWs) connectWs() {
	ws.Do(func() {
		ws.wsConn = ws.WsBuilder.Build()
	})
}

func (ws *SpotWs) subscribe(sub map[string]interface{}) error {
	ws.connectWs()
	return ws.wsConn.Subscribe(sub)
}

// 市场深度MBP行情数据（全量推送）
func (ws *SpotWs) SubscribeDepth(pair CurrencyPair, size int) error {
	if ws.depthCallback == nil {
		return errors.New("please set depth callback func")
	}
	return ws.subscribe(map[string]interface{}{
		"id":  "spot.depth",
		"sub": fmt.Sprintf("market.%s.mbp.refresh.5", pair.ToLower().ToSymbol(""))})
}

// 提供24小时内最新市场概要快照。快照频率不超过每秒10次。
func (ws *SpotWs) SubscribeTicker(pair CurrencyPair) error {
	if ws.tickerCallback == nil {
		return errors.New("please set ticker call back func")
	}
	return ws.subscribe(map[string]interface{}{
		"id":  "spot.ticker",
		"sub": fmt.Sprintf("market.%s.detail", pair.ToLower().ToSymbol("")),
	})
	return nil
}

func (ws *SpotWs) SubscribeTrade(pair CurrencyPair) error {
	return nil
}

// 订阅现货K线数据
func (ws *SpotWs) SubscribeKline(pair CurrencyPair, period int) error {
	if ws.klineCallback == nil {
		return errors.New("please set kline call back func")
	}
	periodS, isOk := _INERNAL_KLINE_PERIOD_CONVERTER[period]
	if isOk != true {
		periodS = "1min"
	}
	return ws.subscribe(map[string]interface{}{
		"id": "spot.Kline",
		// "sub": "market.btcusdt.kline.1min",
		"sub": fmt.Sprintf("market.%s.kline.%s", pair.ToLower().ToSymbol(""), periodS),
	})
	return nil
}

// 处理Ws返回值
func (ws *SpotWs) handle(msg []byte) error {
	// fmt.Println(string(msg))
	if bytes.Contains(msg, []byte("ping")) {
		pong := bytes.ReplaceAll(msg, []byte("ping"), []byte("pong"))
		ws.wsConn.SendMessage(pong)
		return nil
	}

	var resp WsResponse
	err := json.Unmarshal(msg, &resp)
	if err != nil {
		return err
	}

	currencyPair := ParseCurrencyPairFromSpotWsCh(resp.Ch)
	if strings.Contains(resp.Ch, "mbp.refresh") {
		var (
			depthResp DepthResponse
		)

		err := json.Unmarshal(resp.Tick, &depthResp)
		if err != nil {
			return err
		}

		dep := ParseDepthFromResponse(depthResp)
		dep.Pair = currencyPair
		dep.UTime = time.Unix(0, resp.Ts*int64(time.Millisecond))
		ws.depthCallback(&dep)

		return nil
	}

	if strings.Contains(resp.Ch, ".detail") {
		var tickerResp DetailResponse
		err := json.Unmarshal(resp.Tick, &tickerResp)
		if err != nil {
			return err
		}
		ws.tickerCallback(&Ticker{
			Pair: currencyPair,
			Last: tickerResp.Close,
			High: tickerResp.High,
			Low:  tickerResp.Low,
			Vol:  tickerResp.Amount,
			Date: uint64(resp.Ts),
		})
		return nil
	}

	if strings.Contains(resp.Ch, ".kline") {
		var kinfoResp DetailResponse
		err := json.Unmarshal(resp.Tick, &kinfoResp)
		if err != nil {
			return err
		}
		ws.klineCallback(&Kline{
			Pair:      currencyPair,
			Open:      kinfoResp.Open,
			Close:     kinfoResp.Close,
			High:      kinfoResp.High,
			Low:       kinfoResp.Low,
			Vol:       kinfoResp.Amount,
			Timestamp: resp.Ts,
		}, 1)
		return nil
	}

	//fmt.Errorf("[%s] unknown message ch , msg=%s", ws.wsConn.WsUrl, string(msg))

	return nil
}
