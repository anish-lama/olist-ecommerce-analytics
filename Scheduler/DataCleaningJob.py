import psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="OlistEcommerce",
    user="postgres",
    password="9841"
)

cur = conn.cursor()

print("Data Cleaning Job started")

base_path = "C:/Users/anish/Downloads/Project/OlistProject/Postgres/DataCleaning"

sql_files = [
    "1_Validate&Fix_DataType.sql",
    "2_StructuralCleaning.sql",
    "3_HandleMissing_Values.sql",
    "4_BusinessValidation.sql"
]

try:
    for file in sql_files:
        print(f"Running {file}")
        sql = open(f"{base_path}/{file}", "r").read()
        cur.execute(sql)

    conn.commit()
    print("Data Cleaning Job completed successfully")

except Exception as e:
    conn.rollback()
    print("Data Cleaning Job failed")
    print(e)

finally:
    cur.close()
    conn.close()
    print("Connection closed")
