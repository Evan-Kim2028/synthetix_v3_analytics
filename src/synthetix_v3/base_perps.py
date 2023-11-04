import os
import polars as pl
import pandas as pd

from dataclasses import dataclass
from dotenv import load_dotenv
from subgrounds import Subgrounds


@dataclass
class TestBasePerps:
    load_dotenv()

    sg = Subgrounds()
    subgraph = sg.load_subgraph(url=os.getenv("PERPS_MARKET_BASE_TESTNET"))

    def get_accounts(self, limit=5000) -> pd.DataFrame:
        """
        get account schema information
        """
        accounts = self.subgraph.Query._select("accounts")(first=limit)

        accounts_df = self.sg.query_df([accounts])
        accounts_df = accounts_df.astype(
            {
                "accounts_owner": "str",
                "accounts_accountId": "str",
            }
        )
        return accounts_df

    def get_settled_orders(
        self, start_block: int = None, end_block: int = None, limit: int = 2500
    ) -> pd.DataFrame:
        """

        get settled orders. Merge with markets to get market names and returns combined dataframe

        Returns a dataframe with these columns:

            orderSettleds_id                   object
            orderSettleds_timestamp             int64
            markets_id                          int64
            orderSettleds_accountId           str
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

        match (start_block, end_block):
            case (None, None):
                orders = self.subgraph.Query.orderSettleds(first=limit)

            case (None, end_block) if end_block is not None:
                orders = self.subgraph.Query.orderSettleds(
                    first=limit, block={"number": end_block}
                )

            case (start_block, None) if start_block is not None:
                orders = self.subgraph.Query.orderSettleds(
                    first=limit, where={"_change_block": {"number_gte": start_block}}
                )

            case (start_block, end_block):
                orders = self.subgraph.Query.orderSettleds(
                    first=limit,
                    block={"number": end_block},
                    where={"_change_block": {"number_gte": start_block}},
                )

        # query subgraph
        orders_df = self.sg.query_df(
            [
                orders.accountId,
                orders.timestamp,
                orders.marketId,
                orders.fillPrice,
                orders.accruedFunding,
                orders.sizeDelta,
                orders.newSize,
                orders.totalFees,
                orders.referralFees,
                orders.collectedFees,
                orders.settlementReward,
            ]
        )
        markets_df = self.get_markets()[["markets_marketSymbol", "markets_id"]]

        # force numeric datatypes
        markets_df = markets_df.astype({"markets_id": "int64"})
        orders_df = orders_df.astype(
            {
                "orderSettleds_accountId": "str",
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

    def get_market_updates(
        self, start_block: int, end_block: int, limit: int = 5000
    ) -> pd.DataFrame:
        """
        Get historical market stats.
        Converts pandas objects to floats to avoid int overflow type errors.

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

        match (start_block, end_block):
            case (None, None):
                market_updates = self.subgraph.Query._select("marketUpdateds")(
                    first=limit
                )

            case (None, end_block) if end_block is not None:
                market_updates = self.subgraph.Query._select("marketUpdateds")(
                    first=limit, block={"number": end_block}
                )

            case (start_block, None) if start_block is not None:
                market_updates = self.subgraph.Query._select("marketUpdateds")(
                    first=limit, where={"_change_block": {"number_gte": start_block}}
                )

            case (start_block, end_block):
                market_updates = self.subgraph.Query._select("marketUpdateds")(
                    first=limit,
                    block={"number": end_block},
                    where={"_change_block": {"number_gte": start_block}},
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

    def get_position_liquidations(
        self, start_block: int = None, end_block: int = None, limit: int = 5000
    ) -> pd.DataFrame:
        """
        get position liquidations. Forces float type cols.
        Example dataframe output:

        positionLiquidateds_id                     object
        positionLiquidateds_timestamp               int64
        positionLiquidateds_accountId              str
        markets_id                                  int64
        positionLiquidateds_amountLiquidated       float64
        positionLiquidateds_currentPositionSize    float64
        markets_marketSymbol                       object
        """
        match (start_block, end_block):
            case (None, None):
                positions_liquidated = self.subgraph.Query.positionLiquidateds(
                    first=limit
                )

            case (None, end_block) if end_block is not None:
                positions_liquidated = self.subgraph.Query.positionLiquidateds(
                    first=limit,
                    block={"number": end_block},
                    orderBy="timestamp",
                    orderDirection="desc",
                )

            case (start_block, None) if start_block is not None:
                positions_liquidated = self.subgraph.Query.positionLiquidateds(
                    first=limit,
                    where={"_change_block": {"number_gte": start_block}},
                    orderBy="timestamp",
                    orderDirection="desc",
                )

            case (start_block, end_block):
                positions_liquidated = self.subgraph.Query.positionLiquidateds(
                    first=limit,
                    block={"number": end_block},
                    where={"_change_block": {"number_gte": start_block}},
                    orderBy="timestamp",
                    orderDirection="desc",
                )

        position_liquidation_df = self.sg.query_df(
            [
                positions_liquidated._select("id"),
                positions_liquidated.timestamp,
                positions_liquidated.accountId,
                positions_liquidated.marketId,
                positions_liquidated.amountLiquidated,
                positions_liquidated.currentPositionSize,
            ]
        )

        markets_df = self.get_markets()[["markets_marketSymbol", "markets_id"]].astype(
            {"markets_id": "int64"}
        )

        # rename cols for merge
        position_liquidation_df = position_liquidation_df.rename(
            columns={"positionLiquidateds_marketId": "markets_id"}
        )
        # force datatypes
        position_liquidation_df[
            "positionLiquidateds_accountId"
        ] = position_liquidation_df["positionLiquidateds_accountId"].astype(str)
        position_liquidation_df[
            "positionLiquidateds_amountLiquidated"
        ] = position_liquidation_df["positionLiquidateds_amountLiquidated"].astype(
            float
        )
        position_liquidation_df[
            "positionLiquidateds_currentPositionSize"
        ] = position_liquidation_df["positionLiquidateds_currentPositionSize"].astype(
            float
        )

        return position_liquidation_df.merge(markets_df, on="markets_id", how="left")

    def get_account_liquidations(self, limit: int = 5000) -> pd.DataFrame:
        """
        get account liquidations. Example dataframe output:

        accountLiquidateds_id                   object
        accountLiquidateds_timestamp             int64
        accountLiquidateds_accountId            str
        accountLiquidateds_liquidationReward    object
        accountLiquidateds_fullyLiquidated        bool
        """
        accounts_liquidated = self.subgraph.Query._select("accountLiquidateds")(
            first=limit, orderBy="timestamp", orderDirection="desc"
        )

        account_liquidation_df = self.sg.query_df([accounts_liquidated])

        return account_liquidation_df
