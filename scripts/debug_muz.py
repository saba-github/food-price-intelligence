import os
import psycopg2

conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

print("DIM_PRODUCTS:")
cur.execute("""
select standardized_product_name
from dim_products
where standardized_product_name like '%muz%'
order by 1
limit 20;
""")
print(cur.fetchall())

print("\nALIASES:")
cur.execute("""
select normalized_alias, product_id
from dim_product_aliases
where normalized_alias like '%muz%'
order by 1
limit 20;
""")
print(cur.fetchall())

cur.close()
conn.close()