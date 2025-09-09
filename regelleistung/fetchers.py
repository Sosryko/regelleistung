import re
import warnings
import pandas as pd
import requests as rq
from enum import Enum
from io import BytesIO
from pathlib import Path
from pydantic import BaseModel
from abc import ABC, abstractmethod

class FutureDate(Exception):
    pass

class ContentNotFoundError(Exception):
    pass

class Market(Enum):
    CAPACITY = "CAPACITY"
    ENERGY = "ENERGY"

class ProductType (Enum):
    FCR = "FCR"
    AFRR = "aFRR"

class QueryType(Enum):
    AUCTION_RESULTS = "resultsoverview"
    LIST_ANONYMOUS_BIDS = "anonymousresults"


class RegelleistungFetcher(ABC, BaseModel):
    product_type: ProductType
    market: Market
    query_type : QueryType

    @staticmethod
    def query_single_day(market: Market, product_type: ProductType, query_type: QueryType, date: pd.Timestamp) -> pd.DataFrame:
        if date > pd.Timestamp.now():
            raise FutureDate(date.strftime("%Y-%m-%d"))  
        base_url = "https://www.regelleistung.net/apps/cpp-publisher/api/v1/download/tenders/"
        parameter_segment = f"{query_type}?&productTypes={product_type}&market={market}&exportFormat=xlsx&date={date.strftime("%Y-%m-%d")}"
        response = rq.get(base_url + parameter_segment)
        match response.status_code:
            case 200:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", category=UserWarning, module=re.escape('openpyxl.styles.stylesheet'))
                    df = pd.read_excel(BytesIO(response.content), engine='openpyxl')
                return df
            case _:
                raise ContentNotFoundError(f"{date.strftime("%Y-%m-%d")} not available, status code: {response.status_code}")

    def query(self, date_range: pd.DatetimeIndex, postprocess: bool = True) -> pd.DataFrame:
        list_raw_data = []
        for date in date_range:
            list_raw_data.append(RegelleistungFetcher.query_single_day(market=self.market.value, product_type=self.product_type.value, query_type=self.query_type.value, date=date))
        if postprocess:
            return self.postprocess(pd.concat(list_raw_data, axis=0))
        return pd.concat(list_raw_data, axis=0)


    @abstractmethod
    def postprocess(self) -> pd.DataFrame:
        pass

class FCRResultsFetcher(RegelleistungFetcher):
    product_type: ProductType = ProductType.FCR
    market: Market = Market.CAPACITY
    query_type: QueryType = QueryType.AUCTION_RESULTS

    def postprocess(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        raw_data["hour"] = raw_data["PRODUCTNAME"].str.split("_").str[1]
        raw_data["datetime"] = pd.to_datetime(
            raw_data["DATE_FROM"].dt.strftime("%Y-%m-%d")
            + " "
            + raw_data["hour"]
            + ":00:00"
        )
        raw_data["datetime"] = raw_data["datetime"].dt.tz_localize("Europe/Berlin")
        raw_data.index = raw_data["datetime"]
        return raw_data

class FCRMOFetcher(RegelleistungFetcher):
    product_type:ProductType = ProductType.FCR
    market:Market = Market.CAPACITY
    query_type: QueryType = QueryType.LIST_ANONYMOUS_BIDS

    def postprocess(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        raw_data["hour"] = raw_data["PRODUCT"].str.split("_").str[1]
        raw_data["datetime"] = pd.to_datetime(
            raw_data["DATE_FROM"].dt.strftime("%Y-%m-%d")
            + " "
            + raw_data["hour"]
            + ":00:00"
        )
        raw_data["datetime"] = raw_data["datetime"].dt.tz_localize("Europe/Berlin")
        raw_data.index = raw_data["datetime"]
        return raw_data

class aFRRCapacityResultsFetcher(RegelleistungFetcher):
    product_type:ProductType = ProductType.AFRR
    market:Market  = Market.CAPACITY
    query_type: QueryType = QueryType.AUCTION_RESULTS

    def postprocess(raw_data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()

class aFRRCapacityMOFetchers(RegelleistungFetcher):
    product_type:ProductType = ProductType.AFRR
    market:Market  = Market.CAPACITY
    query_type: QueryType = QueryType.LIST_ANONYMOUS_BIDS

    def postprocess(raw_data: pd.DataFrame) -> pd.DataFrame:
        raise NotImplementedError()