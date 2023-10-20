import os
import pandas as pd

from dataclasses import dataclass
from dotenv import load_dotenv
from subgrounds import Subgrounds


@dataclass
class TestMarketBase:
    load_dotenv()

    sg = Subgrounds()
    subgraph = sg.load_subgraph(url=os.getenv("MARKET_BASE_TESTNET"))

    def get_market_configurations(self) -> pd.DataFrame:
        field_path = self.subgraph.Query._select("marketConfigurations")
        df = self.sg.query_df(field_path)

        return df

    def get_markets(self) -> pd.DataFrame:
        field_path = self.subgraph.Query._select("markets")
        df = self.sg.query_df(field_path)

        return df
