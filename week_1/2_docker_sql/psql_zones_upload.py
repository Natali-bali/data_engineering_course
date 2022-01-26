import pandas as pd
import sqlalchemy

df = pd.read_csv('/Users/nataliakonovalova/My Drive/projects/data_engineering_course/week_1/data/taxi+_zone_lookup.csv')

engine = sqlalchemy.create_engine('postgres://root:root@localhost:5432/ny_taxi')
df.to_sql(name = 'zones', con = engine, if_exists='replace')