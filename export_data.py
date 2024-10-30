import pandas as pd
from sqlalchemy import create_engine

try:
    engine = create_engine('mysql://root:Duong2003@localhost/dim_fact')

    # Define the query
    query = "SELECT * FROM dim_fact.fact_delivery"

    # Output file path
    output_file = 'D:\\dw\\Dim and Fact\\fact_hii.csv'
    # Initialize an empty DataFrame
    df_chunk = pd.DataFrame()
for col in shipping_limit_date:
  df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %H:%M')
    # Read the data in chunks
    chunk_size = 10000  # Adjust the chunk size based on your memory capacity
    chunks = pd.read_sql(query, con=engine, chunksize=chunk_size)

    for i, chunk in enumerate(chunks):
        if i == 0:
            chunk.to_csv(output_file, mode='w', header=True, index=False)
        else:
            chunk.to_csv(output_file, mode='a', header=False, index=False)
        print(f"Chunk {i+1} processed.")

    print("Dữ liệu đã được export ra file CSV.")
except Exception as e:
    print(f"Một ngoại lệ đã xảy ra: {e}")