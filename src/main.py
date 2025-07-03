from extract import load_products, load_sales, load_stores, init_spark, load_products_df, load_sales_df, load_stores_df
from utils import load_config

spark = init_spark()

config = load_config()
input_path = config.get("input_path", "data/input")

#df_products = load_products(spark,input_path)
#df_stores = load_stores(spark,input_path)
#df_sales = load_sales(spark,input_path)

df_products2 = load_products_df(input_path)
df_stores2 = load_stores_df(input_path)
df_sales2 = load_sales_df(input_path)


