from synthetix_v3.base_perps import TestBasePerps


base_perp = TestBasePerps()

df = base_perp.get_position_liquidations()

print(df)

print(df.dtypes)

print("done")
