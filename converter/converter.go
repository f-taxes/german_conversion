package converter

import (
	"fmt"
	"time"

	"github.com/f-taxes/german_conversion/conf"
	"github.com/f-taxes/german_conversion/global"
	"github.com/f-taxes/german_conversion/pricesrc"
	"github.com/kataras/golog"
	"github.com/shopspring/decimal"
)

var germanTZ *time.Location

func init() {
	tz, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		golog.Fatalf("Failed to load timezone: %v", err)
	}
	germanTZ = tz
}

func PriceAtTime(asset, targetCurrency string, ts time.Time) (decimal.Decimal, error) {
	resolvedAsset := conf.App.String(fmt.Sprintf("symbolAliases.%s", asset), asset)

	ts = global.StartOfMinute(ts)

	localTs := ts.In(germanTZ)

	currencyMap, ok := pricesrc.Prices[resolvedAsset]
	if !ok {
		return decimal.Zero, nil
	}

	rates, ok := currencyMap[localTs.Year()]
	if !ok {
		return decimal.Zero, nil
	}

	rate, ok := rates[localTs.Month()]
	if !ok {
		return decimal.Zero, nil
	}

	return rate, nil
}
