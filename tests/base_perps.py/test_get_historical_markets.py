from synthetix_v3.base_perps import BasePerps
import polars as pl

base_perp = BasePerps()

market_updates_df = base_perp.get_market_updates()

print(market_updates_df)

print(market_updates_df.dtypes)

print("done")
