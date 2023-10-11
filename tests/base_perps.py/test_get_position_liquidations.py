from synthetix_v3.base_perps import BasePerps


base_perp = BasePerps()

df = base_perp.get_position_liquidations()

print(df)

print(df.dtypes)

print("done")
