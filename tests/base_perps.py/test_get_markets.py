from synthetix_v3.base_perps import TestBasePerps


base_perps = TestBasePerps()

df = base_perps.get_markets()  # market identification info

print(df)
print(df.dtypes)
print("done")
