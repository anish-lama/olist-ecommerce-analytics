import pandas as pd, psycopg2

conn = psycopg2.connect(
    host = "localhost",
    database = "OlistEcommerce",
    user = "postgres",
    password = "9841"
)

print("StagingToSilver Job successfully started...")

cur = conn.cursor()

sql = open("C:/Users/anish/Downloads/Project/OlistProject/Postgres/Jobs/StagingToSilverJob.sql", "r").read()
cur.execute(sql)

conn.commit()
cur.close()
conn.close()

print("StagingToSilver Job ran successfully")