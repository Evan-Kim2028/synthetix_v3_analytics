from synthetix_v3.base_perps import TestBasePerps

# start and end blocks for the competition
start_block = 10536192
end_block = 11455992

base_perps = TestBasePerps()

df = base_perps.get_settled_orders(start_block=start_block, end_block=end_block)

print(df)
print(df.dtypes)
print("done")
