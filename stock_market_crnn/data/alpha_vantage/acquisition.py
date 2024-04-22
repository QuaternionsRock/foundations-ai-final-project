import datetime as _datetime
import io as _io
import time as _time
import typing as _typing

import frozendict as _frozendict
import pandas as _pd
import requests as _requests

if _typing.TYPE_CHECKING:
    from collections.abc import Iterable

    Interval = _typing.Literal["1min", "5min", "15min", "30min", "60min"]

    Topic = _typing.Literal[
        "blockchain",
        "earnings",
        "ipo",
        "mergers_and_acquisitions",
        "financial_markets",
        "economy_fiscal",
        "economy_monetary",
        "economy_macro",
        "energy_transportation",
        "finance",
        "life_sciences",
        "manufacturing",
        "real_estate",
        "retail_wholesale",
        "technology",
    ]

DEFAULT_MAX_REQUESTS = 5

_URL = "https://www.alphavantage.co/query"
_TOPIC_MAP = {
    "Blockchain": "blockchain",
    "Earnings": "earnings",
    "IPO": "ipo",
    "Mergers & Acquisitions": "mergers_and_acquisitions",
    "Financial Markets": "financial_markets",
    "Economy - Fiscal": "economy_fiscal",
    "Economy - Monetary": "economy_monetary",
    "Economy - Macro": "economy_macro",
    "Energy & Transportation": "energy_transportation",
    "Finance": "finance",
    "Life Sciences": "life_sciences",
    "Manufacturing": "manufacturing",
    "Real Estate & Construction": "real_estate",
    "Retail & Wholesale": "retail_wholesale",
    "Technology": "technology",
}


def get_time_series_intraday(
    symbol: str,
    interval: "Interval",
    adjusted: bool | None = None,
    extended_hours: bool | None = None,
    month: _datetime.date | None = None,
    outputsize: _typing.Literal["compact", "full"] | None = None,
    datatype: _typing.Literal["json", "csv"] | None = None,
    *,
    apikey: str,
    max_requests: int | None = None,
) -> _requests.Response | None:
    """Get time series intraday data from Alpha Vantage."""
    max_requests = max_requests if max_requests is not None else DEFAULT_MAX_REQUESTS
    assert max_requests > 0
    for _ in range(max_requests):
        r = _requests.get(
            _URL,
            params={
                "function": "TIME_SERIES_INTRADAY",
                "symbol": symbol,
                "interval": interval,
                "adjusted": (
                    "true" if adjusted else "false" if adjusted is not None else None
                ),
                "extended_hours": (
                    "true"
                    if extended_hours
                    else "false" if extended_hours is not None else None
                ),
                "month": month.strftime("%Y-%m") if month is not None else None,
                "outputsize": outputsize,
                "datatype": datatype,
                "apikey": apikey,
            },
        )
        print("TIME_SERIES_INTRADAY status code:", r.status_code)
        if r.status_code == 200:
            return r
        _time.sleep(60)


def all_time_series_intraday(
    symbol: str,
    interval: "Interval",
    adjusted: bool | None = None,
    extended_hours: bool | None = None,
    *,
    time_from: _datetime.datetime,
    time_to: _datetime.datetime,
    apikey: str,
    max_requests: int | None = None,
) -> _pd.DataFrame | None:
    """Get all time series intraday data from Alpha Vantage."""
    for year in range(time_from.year, time_to.year + 1):
        dfs = []
        for month in range(
            time_from.month if year == time_from.year else 1,
            time_to.month + 1 if year == time_to.year else 13,
        ):
            r = get_time_series_intraday(
                symbol,
                interval,
                adjusted,
                extended_hours,
                _datetime.date(year, month, 1),
                "full",
                "csv",
                apikey=apikey,
                max_requests=max_requests,
            )
            if r is None:
                return None
            dfs.append(
                _pd.read_csv(_io.StringIO(r.text), index_col="timestamp", parse_dates=True).iloc[::-1]
            )
    df = _pd.concat(dfs)
    return df.loc[time_from:time_to]


def get_news_sentiment(
    tickers: str | None = None,
    topics: "Iterable[Topic] | None" = None,
    time_from: _datetime.datetime | None = None,
    time_to: _datetime.datetime | None = None,
    sort: _typing.Literal["LATEST", "EARLIEST", "RELEVANCE"] | None = None,
    limit: int | None = None,
    *,
    apikey: str,
    max_requests: int | None = None,
) -> _requests.Response | None:
    """Get news sentiment data from Alpha Vantage."""
    max_requests = max_requests if max_requests is not None else DEFAULT_MAX_REQUESTS
    assert max_requests > 0
    if limit is not None:
        assert 0 < limit <= 1000
    for _ in range(max_requests):
        r = _requests.get(
            _URL,
            params={
                "function": "NEWS_SENTIMENT",
                "tickers": tickers,
                "topics": ",".join(topics) if topics is not None else None,
                "time_from": (
                    time_from.strftime("%Y%m%dT%H%M") if time_from is not None else None
                ),
                "time_to": (
                    time_to.strftime("%Y%m%dT%H%M") if time_to is not None else None
                ),
                "sort": sort,
                "limit": limit,
                "apikey": apikey,
            },
        )
        print("NEWS_SENTIMENT status code:", r.status_code)
        if r.status_code == 200:
            return r
        _time.sleep(60)


def all_news_sentiment(
    symbol: str | None = None,
    topics: "Iterable[Topic] | None" = None,
    *,
    time_from: _datetime.datetime,
    time_to: _datetime.datetime,
    apikey: str,
    max_requests: int | None = None,
):
    articles = {}
    while True:
        r = get_news_sentiment(
            symbol,
            topics,
            time_from,
            sort="EARLIEST",
            limit=1000,
            apikey=apikey,
            max_requests=max_requests,
        )
        if r is None:
            return None
        json_dict = r.json()
        for json_article in json_dict["feed"]:
            time_published = _datetime.datetime.strptime(
                json_article["time_published"], "%Y%m%dT%H%M%S"
            )
            time_from = time_published
            if time_from > time_to:
                break
            article = (
                {"time_published": time_published}
                | {
                    k: json_article[k]
                    for k in (
                        "time_published",
                        "source",
                        "category_within_source",
                        "overall_sentiment_score",
                    )
                }
                | {
                    _TOPIC_MAP[topic["topic"]]: topic["relevance_score"]
                    for topic in json_article["topics"]
                }
            )
            for ticker_sentiment in json_article["ticker_sentiment"]:
                if ticker_sentiment["ticker"] == symbol:
                    article |= {
                        k: ticker_sentiment[k]
                        for k in ("relevance_score", "ticker_sentiment_score")
                    }
                    break
            else:
                raise ValueError("No sentiment data for the symbol")
            articles[json_article["url"]] = article
        else:
            continue
        break
    df = _pd.DataFrame.from_dict(articles, orient="index")
    df.index.name = "url"
    return df
