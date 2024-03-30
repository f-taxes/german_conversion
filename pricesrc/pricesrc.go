package pricesrc

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/f-taxes/german_conversion/global"
	"github.com/f-taxes/german_conversion/grpc_client"
	"github.com/f-taxes/german_conversion/proto"
	"github.com/kataras/golog"
	"github.com/shopspring/decimal"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.uber.org/ratelimit"
)

/*
	Foreign Currency (string): {
		year (int): {
			month (time.Month): {
				price (decimal.Decimal)
			}
		}
	}
*/
var Prices map[string]map[int]map[time.Month]decimal.Decimal = make(map[string]map[int]map[time.Month]decimal.Decimal)
var germanTZ *time.Location
var limiter = ratelimit.New(60, ratelimit.Per(time.Minute))

var pricePattern = regexp.MustCompile("^([0-9]+(,[0-9]+)?) (.+)$")
var firstYear = 2015
var urlTpl = "https://www.bundesfinanzministerium.de/Datenportal/Daten/offene-daten/steuern-zoelle/umsatzsteuer-umrechnungskurse/datensaetze/uu-kurse-%d-csv.csv?__blob=publicationFile&v=4"

func init() {
	tz, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		golog.Fatalf("Failed to load timezone: %v", err)
	}
	germanTZ = tz
}

func EnsurePrices() error {
	if len(Prices) > 0 {
		return nil
	}

	folder, _ := filepath.Abs("./prices")

	err := os.MkdirAll(folder, 0755)
	if err != nil {
		return err
	}

	jobId := primitive.NewObjectID().Hex()
	grpc_client.GrpcClient.ShowJobProgress(context.Background(), &proto.JobProgress{
		ID:       jobId,
		Label:    "Downloading conversion rates from the Bundesfinanzministerium.",
		Progress: "-1",
	})

	defer grpc_client.GrpcClient.ShowJobProgress(context.Background(), &proto.JobProgress{
		ID:       jobId,
		Progress: "100",
	})

	golog.Info("Downloading Umsatzsteuer-Umrechnungskurse from the Bundesfinanzministerium...")
	year := firstYear
	client := &http.Client{}

	for {
		limiter.Take()
		req, err := http.NewRequest("GET", fmt.Sprintf(urlTpl, year), nil)
		if err != nil {
			return err
		}

		req.Header.Set("User-Agent", "Golang/3.0")

		response, err := client.Do(req)
		if err != nil {
			return err
		}

		defer response.Body.Close()

		// Read the content of the CSV file
		csvContent, err := io.ReadAll(response.Body)
		if err != nil {
			return err
		}

		reader := csv.NewReader(bytes.NewReader(csvContent))
		reader.Comma = ';'

		records, err := reader.ReadAll()
		if err != nil {
			return err
		}

		for i, row := range records {
			// First lines are useless.
			if i < 4 {
				continue
			}

			month := 1

			for c, txt := range row {
				if c < 2 {
					continue
				}

				txt := strings.ReplaceAll(txt, "\n", " ")

				if pricePattern.MatchString(txt) {
					matches := pricePattern.FindStringSubmatch(txt)

					if len(matches) == 4 {
						rate := strings.ReplaceAll(matches[1], ",", ".")
						currency := matches[3]

						if _, ok := Prices[currency]; !ok {
							Prices[currency] = make(map[int]map[time.Month]decimal.Decimal)
						}

						if _, ok := Prices[currency][year]; !ok {
							Prices[currency][year] = make(map[time.Month]decimal.Decimal)
						}

						Prices[currency][year][time.Month(month)] = global.StrToDecimal(rate, decimal.Zero)
					}

					month++
				}
			}
		}

		year++

		if time.Now().In(germanTZ).AddDate(0, -1, 0).Year() < year {
			break
		}
	}

	return nil
}
