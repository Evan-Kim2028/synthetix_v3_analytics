import os
import polars as pl
import pandas as pd

from dataclasses import dataclass
from dotenv import load_dotenv
from subgrounds import Subgrounds


@dataclass
class BasePerps:
    load_dotenv()

    sg = Subgrounds()
    subgraph = sg.load_subgraph(url=os.getenv("ENDPOINT"))

    def get_settled_orders(
        self, timestamp: int = 1696344937, limit: int = 2500
    ) -> pd.DataFrame:
        """
        timestamp: int = 1696344937   # October 3, 10:55 AM

        get settled orders. Merge with markets to get market names.

        Returns a dataframe with these columns:

            orderSettleds_id                   object
            orderSettleds_timestamp             int64
            markets_id                          int64
            orderSettleds_accountId           float64
            orderSettleds_fillPrice           float64
            orderSettleds_accruedFunding      float64
            orderSettleds_sizeDelta           float64
            orderSettleds_newSize             float64
            orderSettleds_totalFees           float64
            orderSettleds_referralFees        float64
            orderSettleds_collectedFees       float64
            orderSettleds_settlementReward    float64
            orderSettleds_trackingCode         object
            orderSettleds_settler              object
            markets_marketSymbol               object
        """
        orders = self.subgraph.Query.orderSettleds(
            first=limit, where=[self.subgraph.OrderSettled.timestamp > timestamp]
        )

        # query subgraph
        orders_df = self.sg.query_df(orders)
        markets_df = self.get_markets()[["markets_marketSymbol", "markets_id"]]

        # force numeric datatypes
        markets_df = markets_df.astype({"markets_id": "int64"})
        orders_df = orders_df.astype(
            {
                "orderSettleds_accountId": "float",
                "orderSettleds_fillPrice": "float",
                "orderSettleds_accruedFunding": "float",
                "orderSettleds_sizeDelta": "float",
                "orderSettleds_newSize": "float",
                "orderSettleds_totalFees": "float",
                "orderSettleds_referralFees": "float",
                "orderSettleds_collectedFees": "float",
                "orderSettleds_settlementReward": "float",
            }
        )

        # update column name for merge
        orders_df = orders_df.rename(columns={"orderSettleds_marketId": "markets_id"})
        markets_df = markets_df.astype({"markets_id": "int64"})

        return orders_df.merge(markets_df, on="markets_id", how="left")

    def get_markets(self) -> pd.DataFrame:
        """
        get perp market ids.

        Returns a pandas dataframe with these columns:

            markets_id                                           object
            markets_perpsMarketId                                 int64
            markets_marketName                                   object
            markets_marketSymbol                                 object
            markets_price                                        object
            markets_skew                                         object
            markets_size                                         object
            markets_sizeDelta                                    object
            markets_currentFundingRate                            int64
            markets_currentFundingVelocity                        int64
            markets_feedId                                       object
            markets_maxFundingVelocity                           object
            markets_skewScale                                    object
            markets_lockedOiPercent                               int64
            markets_marketOwner                                  object
            markets_owner                                        object
            markets_initialMarginRatioD18                         int64
            markets_maintenanceMarginRatioD18                     int64
            markets_liquidationRewardRatioD18                     int64
            markets_maxSecondsInLiquidationWindow                object
            markets_minimumPositionMargin                         int64
            markets_maxLiquidationLimitAccumulationMultiplier    object
            markets_makerFee                                      int64
            markets_takerFee                                      int64
            markets_factoryInitialized                           object
        """

        return self.sg.query_df(self.subgraph.Query.markets)

    def get_market_updates(self, limit: int = 5000) -> pd.DataFrame:
        """
        Get historical market stats.

        Returns a pandas dataframe with these columns:
            marketUpdateds_timestamp                   int64
            markets_id                                 int64
            marketUpdateds_price                     float64
            marketUpdateds_skew                      float64
            marketUpdateds_size                      float64
            marketUpdateds_sizeDelta                 float64
            marketUpdateds_currentFundingRate        float64
            marketUpdateds_currentFundingVelocity    float64
            markets_marketSymbol                      object
        """
        market_updates = self.subgraph.Query._select("marketUpdateds")(
            first=limit, orderBy="timestamp", orderDirection="desc"
        )

        # query subgraph
        market_updates_df = self.sg.query_df(
            [
                market_updates._select("timestamp"),
                market_updates._select("marketId"),
                market_updates._select("price"),
                market_updates._select("skew"),
                market_updates._select("size"),
                market_updates._select("sizeDelta"),
                market_updates._select("currentFundingRate"),
                market_updates._select("currentFundingVelocity"),
            ]
        )
        markets_df = self.get_markets()[["markets_marketSymbol", "markets_id"]]

        # force numeric datatypes
        market_updates_df = market_updates_df.astype(
            {
                "marketUpdateds_price": "float",
                "marketUpdateds_skew": "float",
                "marketUpdateds_size": "float",
                "marketUpdateds_sizeDelta": "float",
                "marketUpdateds_currentFundingRate": "float",
                "marketUpdateds_currentFundingVelocity": "float",
            }
        )
        markets_df = markets_df.astype({"markets_id": "int64"})

        # rename cols for merge
        market_updates_df = market_updates_df.rename(
            columns={"marketUpdateds_marketId": "markets_id"}
        )

        return market_updates_df.merge(markets_df, on="markets_id", how="left")