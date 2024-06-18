package ctl

import (
	"context"
	"net"

	"github.com/f-taxes/german_conversion/conf"
	"github.com/f-taxes/german_conversion/converter"
	"github.com/f-taxes/german_conversion/global"
	"github.com/f-taxes/german_conversion/pricesrc"
	pb "github.com/f-taxes/german_conversion/proto"
	"github.com/kataras/golog"
	"github.com/shopspring/decimal"
	"google.golang.org/grpc"
)

type PluginCtl struct {
	pb.UnimplementedPluginCtlServer
}

func (s *PluginCtl) ConvertPricesInTrade(ctx context.Context, job *pb.TradeConversionJob) (*pb.Trade, error) {
	pricesrc.EnsurePrices()

	force := true
	priceC := global.StrToDecimal(job.Trade.PriceC, decimal.Zero)
	quotePriceC := global.StrToDecimal(job.Trade.QuotePriceC, decimal.Zero)
	feeC := global.StrToDecimal(job.Trade.FeeC, decimal.Zero)
	quoteFeeC := global.StrToDecimal(job.Trade.QuoteFeeC, decimal.Zero)

	if force || priceC.IsZero() {
		if job.Trade.Quote == job.TargetCurrency {
			job.Trade.PriceC = job.Trade.Price
			job.Trade.PriceConvertedBy = global.Plugin.ID
			job.Trade.ValueC = job.Trade.Value
		} else {
			priceC, err := converter.PriceAtTime(job.Trade.Asset, job.TargetCurrency, job.Trade.Ts.AsTime())
			if err != nil {
				return nil, err
			}

			if priceC.IsZero() && conf.App.Bool("attemptIndirectConversion") {
				priceC, err = converter.PriceAtTime(job.Trade.Quote, job.TargetCurrency, job.Trade.Ts.AsTime())
				if err != nil {
					return nil, err
				}

				priceC = global.StrToDecimal(job.Trade.Price).Mul(priceC).Round(4)
			}

			job.Trade.PriceC = priceC.String()
			job.Trade.ValueC = priceC.Mul(global.StrToDecimal(job.Trade.Amount)).Round(4).String()

			if !priceC.IsZero() {
				job.Trade.PriceConvertedBy = global.Plugin.ID
			}
		}
	}

	if force || quotePriceC.IsZero() {
		if job.Trade.Quote == job.TargetCurrency {
			job.Trade.QuotePriceC = "1"
			job.Trade.QuotePriceConvertedBy = global.Plugin.ID
		} else {
			quotePriceC, err := converter.PriceAtTime(job.Trade.Quote, job.TargetCurrency, job.Trade.Ts.AsTime())
			if err != nil {
				return nil, err
			}

			job.Trade.QuotePriceC = quotePriceC.String()
		}
	}

	if force || feeC.IsZero() {
		if job.Trade.FeeCurrency == job.TargetCurrency {
			job.Trade.FeeC = job.Trade.Fee
			job.Trade.FeePriceC = "1"
			job.Trade.FeeConvertedBy = global.Plugin.ID
		} else {
			var feePriceC decimal.Decimal

			if job.Trade.FeeCurrency == job.Trade.Asset {
				feePriceC = global.StrToDecimal(job.Trade.PriceC)
			} else {
				var err error
				feePriceC, err = converter.PriceAtTime(job.Trade.FeeCurrency, job.TargetCurrency, job.Trade.Ts.AsTime())
				if err != nil {
					return nil, err
				}
			}

			job.Trade.FeePriceC = feePriceC.String()

			if !feePriceC.IsZero() {
				job.Trade.FeeC = global.StrToDecimal(job.Trade.Fee).Mul(feePriceC).Round(4).String()
				job.Trade.FeeConvertedBy = global.Plugin.ID
			}
		}
	}

	if force || quoteFeeC.IsZero() {
		if job.Trade.QuoteFeeCurrency == job.Trade.Quote {
			job.Trade.QuoteFeePriceC = job.Trade.QuotePriceC
			job.Trade.QuoteFeeC = global.StrToDecimal(job.Trade.QuoteFee).Mul(global.StrToDecimal(job.Trade.QuotePriceC)).Round(4).String()
			job.Trade.QuoteFeeConvertedBy = global.Plugin.ID
		} else {
			var quoteFeePriceC decimal.Decimal

			if job.Trade.QuoteFeeCurrency == job.Trade.Asset {
				quoteFeePriceC = global.StrToDecimal(job.Trade.PriceC)
			} else {
				var err error
				quoteFeePriceC, err = converter.PriceAtTime(job.Trade.QuoteFeeCurrency, job.TargetCurrency, job.Trade.Ts.AsTime())
				if err != nil {
					return nil, err
				}
			}

			job.Trade.QuoteFeePriceC = quoteFeePriceC.String()

			if !quoteFeePriceC.IsZero() {
				job.Trade.QuoteFeeC = global.StrToDecimal(job.Trade.QuoteFee).Mul(quoteFeePriceC).Round(4).String()
				job.Trade.QuoteFeeConvertedBy = global.Plugin.ID
			}
		}
	}

	return job.Trade, nil
}

func (s *PluginCtl) ConvertPricesInTransfer(ctx context.Context, job *pb.TransferConversionJob) (*pb.Transfer, error) {
	pricesrc.EnsurePrices()

	if job.Transfer.FeeCurrency == job.TargetCurrency {
		job.Transfer.FeeC = job.Transfer.Fee
		job.Transfer.FeeConvertedBy = global.Plugin.ID
		return job.Transfer, nil
	}

	price, err := converter.PriceAtTime(job.Transfer.FeeCurrency, job.TargetCurrency, job.Transfer.Ts.AsTime())
	if err != nil {
		return nil, err
	}

	if price.IsZero() {
		return job.Transfer, nil
	}

	job.Transfer.FeePriceC = price.String()
	job.Transfer.FeeC = global.StrToDecimal(job.Transfer.Fee).Mul(price).Round(4).String()
	job.Transfer.FeeConvertedBy = global.Plugin.ID
	return job.Transfer, nil
}

func Start(address string) {
	srv := &PluginCtl{}
	lis, err := net.Listen("tcp", address)
	if err != nil {
		golog.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPluginCtlServer(s, srv)
	golog.Infof("Ctl server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		golog.Fatalf("failed to serve: %v", err)
	}
}
