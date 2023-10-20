from synthetix_v3.market_base import TestMarketBase


base_perps = TestMarketBase()

df = base_perps.get_market_configurations()  # market identification info

print(df)
print(df.dtypes)
print("done")
