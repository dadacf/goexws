package binance

import (
	"log"
	"testing"
	"time"
	"unsafe"

	"github.com/nntaoli-project/goex"
)

var bnWs = NewSpotWs()

func init() {
	bnWs.proxyUrl = "socks5://127.0.0.1:1080"
	//bnWs.SetBaseUrl("wss://fstream.binancezh.com/ws")
	//bnWs.SetCombinedBaseURL("wss://fstream.binancezh.com/stream?streams=")
	bnWs.SetCallbacks(printfTicker, printfDepth, printfTrade, printfKline)
}

func printfTicker(ticker *goex.Ticker) {
	log.Println("ticker:", ticker)
}

func printfDepth(depth *goex.Depth) {
	log.Println("depth:", depth)
}

func printfTrade(trade *goex.Trade) {
	log.Println("trade:", trade)
	log.Println("trade:", (*RawTrade)(unsafe.Pointer(trade)))
}

func printfAggTrade(aggTrade *goex.Trade) {
	log.Println("trade:", (*AggTrade)(unsafe.Pointer(aggTrade)))
}

func printfKline(kline *goex.Kline, period int) {
	log.Println("kline:", kline)
}

// func TestBinanceWs_SubscribeTicker(t *testing.T) {
// 	bnWs.SubscribeTicker(goex.BTC_USDT)
// 	time.Sleep(time.Second * 5)
// }

func TestBinanceWs_GetDepthWithWs(t *testing.T) {
	bnWs.SubscribeDepth(goex.BTC_USDT, 5)
	time.Sleep(time.Second * 10)
}

func TestBinanceWs_GetKLineWithWs(t *testing.T) {
	return
	bnWs.SubscribeKline(goex.BTC_USDT, goex.KLINE_PERIOD_1MIN)
	time.Sleep(time.Second * 10)
}

func TestBinanceWs_GetTradesWithWs(t *testing.T) {
	bnWs.SubscribeTrade(goex.BTC_USDT)
	time.Sleep(time.Second * 5)
}

func TestBinanceWs_SubscribeAggTrade(t *testing.T) {
	bnWs.SubscribeAggTrade(goex.BTC_USDT, printfAggTrade)
	time.Sleep(time.Second * 5)
}

func TestBinanceWs_SubscribeDiffDepth(t *testing.T) {
	bnWs.SubscribeDiffDepth(goex.BTC_USDT, printfDepth)
	time.Sleep(time.Second * 10)
}

func TestBinanceWs_SubscribeDepth(t *testing.T) {
	bnWs.SubscribeDepth(goex.BTC_USDT, 5)
	bnWs.SubscribeDepth(goex.LTC_USDT, 5)
	bnWs.SubscribeDepth(goex.ETC_USDT, 5)
	time.Sleep(time.Second * 60)
}
