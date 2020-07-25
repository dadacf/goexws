package goexws

import (
	"github.com/nntaoli-project/goex"
)

type FuturesWsApi interface {
	DepthCallback(func(depth *goex.Depth))
	TickerCallback(func(ticker *goex.FutureTicker))
	TradeCallback(func(trade *goex.Trade, contract string))
	KlineCallback(func(kline *goex.FutureKline, period int, contract string))
	SubscribeDepth(pair goex.CurrencyPair, size int, contractType string) error
	SubscribeTicker(pair goex.CurrencyPair, contractType string) error
	SubscribeTrade(pair goex.CurrencyPair, contractType string) error
	SubscribeKline(pair goex.CurrencyPair, period int, contractType string) error
}

type SpotWsApi interface {
	DepthCallback(func(depth *goex.Depth))
	TickerCallback(func(ticker *goex.Ticker))
	TradeCallback(func(trade *goex.Trade))
	KlineCallback(func(*goex.Kline, int))
	SubscribeDepth(pair goex.CurrencyPair, size int) error
	SubscribeTicker(pair goex.CurrencyPair) error
	SubscribeTrade(pair goex.CurrencyPair) error
	SubscribeKline(pair goex.CurrencyPair, period int) error
}
