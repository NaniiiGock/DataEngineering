import requests
import pandas as pd
import hashlib
from datetime import datetime
import duckdb
from cryptography.fernet import Fernet
from minio import Minio
import os


minio_access_key = os.getenv("MINIO_ACCESS_KEY")
minio_secret_key = os.getenv("MINIO_SECRET_KEY")
encryption_key = os.getenv("ENCRYPTION_KEY")

if not encryption_key:
    encryption_key = Fernet.generate_key().decode()  
    print("Generated encryption key:", encryption_key)
cipher = Fernet(encryption_key.encode())

# ============= #
# Getting users #
# ============= #

url = "https://randomuser.me/api/?results=100"
print("GETTING USERS...")
response = requests.get(url)
users = response.json()['results']
print("USERS:")
print(users[5], "...")

def mask_pii(value):
    return hashlib.sha256(value.encode()).hexdigest() if isinstance(value, str) else value

def process_user(user):
    flat_user = {
        "gender": user.get("gender"),
        "age": user.get("dob", {}).get("age"),
        "location_country": user.get("location", {}).get("country"),
        "location_city": user.get("location", {}).get("city"),
        "registered_date": user.get("registered", {}).get("date")
    }
    flat_user["email"] = mask_pii(user.get("email", ""))
    flat_user["phone"] = mask_pii(user.get("phone", ""))
    return flat_user

user_data = [process_user(user) for user in users]
df = pd.DataFrame(user_data)
df['registered_date'] = pd.to_datetime(df['registered_date'])

# ================= #
# SAVING AS PARQUET #
# ================= #

parquet_file = "user_data.parquet"
duckdb.sql("CREATE TABLE user_data AS SELECT * FROM df")
duckdb.sql(f"COPY user_data TO '{parquet_file}' (FORMAT 'PARQUET')")

with open(parquet_file, "rb") as file:
    file_data = file.read()
encrypted_data = cipher.encrypt(file_data)

encrypted_file = "encrypted_user_data.parquet"
with open(encrypted_file, "wb") as file:
    file.write(encrypted_data)

print("Encryption Key:", encryption_key)


# ==================================== #
# Uploading of encrypted file to minio #
# ==================================== #

minio_client = Minio(
    "localhost:9000",  
    access_key="minioadmin",  
    secret_key="minioadmin",
    secure=False
)

bucket_name = "user-data"

if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    print(f"Bucket '{bucket_name}' created.")
else:
    print(f"Bucket '{bucket_name}' already exists.")

object_name = "encrypted_user_data.parquet"
minio_client.fput_object(bucket_name, object_name, encrypted_file)
print("Encrypted file uploaded successfully to Minio.")

# ================== #
# Query with Duck DB #
# ================== #


with open(encrypted_file, "rb") as file:
    encrypted_data = file.read()
decrypted_data = cipher.decrypt(encrypted_data)

temp_file = "decrypted_user_data.parquet"
with open(temp_file, "wb") as file:
    file.write(decrypted_data)

query_df = duckdb.sql(f"SELECT * FROM parquet_scan('{temp_file}')").to_df()
query_df['registered_date'] = query_df['registered_date'].dt.tz_localize(None)

query_df['days_since_registration'] = (datetime.now() - query_df['registered_date']).dt.days
average_duration = query_df['days_since_registration'].mean()
print("Average duration since registration:", average_duration)

age_gender_distribution = query_df.groupby(['age', 'gender']).size().reset_index(name='count')
print("Age and Gender Distribution:\n", age_gender_distribution)

location_counts = query_df.groupby(['location_country', 'location_city']).size().reset_index(name='count')
most_common_location = location_counts.sort_values(by='count', ascending=False).iloc[0]
print("Most common location:", most_common_location)
