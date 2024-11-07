# DataEngineering


## Week 10 - Security and privacy | practice assignment

----------

### Assignment 1 

- Read user data from an API. Using the https://randomuser.me/ API you would need to read in 100 users.

- Mask or anonymize PII (Personally Identifiable Information)

- Save the data as encrypted parquet files on Minio. The data should have a flat structure (no arrays or structs)


### The following analytical questions should still be answerable with the data:

- What is the average duration since a user registered?
  
- What is the age and gender distribution of users?
  
- Which location (country and city) do most users come from?



____

Usage:

```
pip install -r requirements.txt
```

Put into .env file your keys and passwords

Start OMIO with your username and password:

docker run -p 9000:9000 -p 9001:9001 --name minio \
  -e "MINIO_ROOT_USER=<<<your_username>>>" \
  -e "MINIO_ROOT_PASSWORD=<<<your_password>>>" \
  quay.io/minio/minio server /data --console-address ":9001"

Duck DB usage is integrated into python, no special setup needed


-------------

# DESCRIPTION:

I retrieve 100 user profiles from the RandomUser API and mask sensitive data (email and phone) by hashing it with SHA-256 to protect user privacy.

Then I save the data as a Parquet file, where the data is encrypted with cryptography library, ensuring data is protected from unauthorized access.

Also, data is Uploaded to the encrypted file to Minio (an S3-compatible storage). As access keys and the encryption key are stored in environment variables, I am keeping sensitive information out of the code (each user can use it's own keys by simply exchanging the variables in the .env file)

DuckDB is decrypting the data for the further analysis, so I calculate needed metrics: average registration time, age/gender distribution, and the most common user locations.

Using environment variables, encryption, and anlnymization(hashing) ensures data and credentials are secure throughout the process.

--------
# RESULTS:
  
<img width="1027" alt="image" src="https://github.com/user-attachments/assets/4e8a72ea-54fc-4962-8bd6-52e96449f67b">


