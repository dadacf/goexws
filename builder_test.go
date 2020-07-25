package goexws

import (
	"testing"
	"time"

	"github.com/nntaoli-project/goex"
)

func TestBuild(t *testing.T) {
	api := SpotBuild(Spot_Huobi)
	err := api.SubscribeKline(goex.BTC_USDT, goex.KLINE_PERIOD_1MIN)
	if err != nil {
		panic(err)
	}
	for {
		// api.KlineCallback(service.Wsinfo)
		time.Sleep(time.Second * 5)
	}

}
