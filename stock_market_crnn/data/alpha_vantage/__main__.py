from . import acquisition

import argparse
import datetime
import json
from pathlib import Path
import sys

parser = argparse.ArgumentParser(
    prog=".".join(__name__.split(".")[:-1]),
    description="Acquire or preprocess data from Alpha Vantage.",
)
subparsers = parser.add_subparsers(dest="command", required=True)
parser.add_argument("config_path", type=Path, help="path to the configuration file")

acquire_parser = subparsers.add_parser("acquire", help="acquire data")
acquire_parser.add_argument("apikey", type=Path, help="your Alpha Vantage API key")

args = parser.parse_args()

if args.command == "acquire":
    config_path = args.config_path
    apikey = args.apikey

    config = json.loads(config_path.read_text())
    symbols = config["symbols"]
    interval = config["interval"]
    adjusted = config["adjusted"]
    extended_hours = config["extended_hours"]
    topics = config.get("topics")
    time_from = datetime.datetime.fromisoformat(config["time_from"])
    time_to = datetime.datetime.fromisoformat(config["time_to"])
    max_requests = config.get("max_requests")
    data_path = Path(config["data_path"])

    time_series_intraday_path = data_path / "time_series_intraday"
    news_sentiment_path = data_path / "news_sentiment"

    data_path.mkdir(exist_ok=True)
    time_series_intraday_path.mkdir(exist_ok=True)
    news_sentiment_path.mkdir(exist_ok=True)

    for symbol in config["symbols"]:
        time_series_intraday = acquisition.all_time_series_intraday(
            symbol,
            interval,
            adjusted,
            extended_hours,
            time_from=time_from,
            time_to=time_to,
            apikey=apikey,
            max_requests=max_requests,
        )
        if time_series_intraday is not None:
            time_series_intraday.to_csv(time_series_intraday_path / f"{symbol}.csv")
        else:
            print(f"Failed to get time series intraday data for {symbol}")

        news_sentiment = acquisition.all_news_sentiment(
            symbol,
            topics,
            time_from=time_from,
            time_to=time_to,
            apikey=apikey,
            max_requests=max_requests,
        )

        if news_sentiment is not None:
            news_sentiment.to_csv(news_sentiment_path / f"{symbol}.csv")
        else:
            print(f"Failed to get news sentiment data for {symbol}")
