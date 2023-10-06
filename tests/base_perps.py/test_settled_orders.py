from synthetix_v3.base_perps import BasePerps


base_perps = BasePerps()

df = base_perps.get_settled_orders()

print(df)
print(df.dtypes)
print("done")
