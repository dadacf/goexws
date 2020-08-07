package goexws

import (
	"fmt"
	"time"

	"github.com/nntaoli-project/goex"
)

// 现货k线
func WsSpotKlineinfo(kinfo *goex.Kline, size int) {
	fmt.Println("打印")
	var stime string = time.Unix(int64(kinfo.Timestamp/1000), 0).Format("2006-01-02 15:04:05")
	fmt.Println(stime, kinfo.Pair, kinfo.Close)
}

// 现货深度数据
func WsSpotDepthinfo(depth *goex.Depth) {
	fmt.Println(depth.AskList.Len)
	// fmt.Println(kinfo)
}

// 合约k线
func WsHeYueKlineinfo(kinfo *goex.FutureKline, period int, contract string) {
	fmt.Println("打印")
	var stime string = time.Unix(int64(kinfo.Timestamp/1000), 0).Format("2006-01-02 15:04:05")
	fmt.Println(stime, kinfo.Pair, contract, kinfo.Close)
}

// 现货深度数据
func WsHeYueDepthinfo(depth *goex.Depth) {
	fmt.Println(depth.AskList.Len)
	// fmt.Println(kinfo)
}
func GetDeptInfo() {
	fmt.Println("Hello, World!")
	api := SpotBuild(Spot_OKEx)
	api.DepthCallback(WsSpotDepthinfo)
	err := api.SubscribeDepth(goex.BTC_USDT, goex.KLINE_PERIOD_1MIN)
	if err != nil {
		panic(err)
	}
	// api.DepthCallback(WsDepthinfo)
	for {
		time.Sleep(time.Second * 5)
		fmt.Println("循环一次")
	}
}

// 获取现货K
func GetKlineInfo() {
	fmt.Println("Hello, World!")
	api := SpotBuild(Spot_Binance)
	api.KlineCallback(WsSpotKlineinfo)
	err := api.SubscribeKline(goex.BTC_USDT, goex.KLINE_PERIOD_1MIN)
	if err != nil {
		panic(err)
	}
	// api.DepthCallback(WsDepthinfo)
	for {
		time.Sleep(time.Second * 5)
		fmt.Println("循环一次")
	}
}

// 获取合约K
func GetHYKlineInfo() {
	fmt.Println("Hello, World!")
	api := FuturesBuild(Futures_OKEx)
	api.KlineCallback(WsHeYueKlineinfo)
	err := api.SubscribeKline(goex.BTC_USDT, goex.KLINE_PERIOD_1MIN, goex.BI_QUARTER_CONTRACT)
	if err != nil {
		panic(err)
	}
	// api.DepthCallback(WsDepthinfo)
	for {
		time.Sleep(time.Second * 5)
		fmt.Println("循环一次")
	}
}
