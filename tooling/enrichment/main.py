# 
# Copyright 2025 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from google.cloud import bigquery, storage
import pandas as pd
import os
from dotenv import load_dotenv
import time
from imagen_client import generate_image
from gemini_client import get_image_description
from firestore_client import FirestoreClient

# Load environment variables
load_dotenv()

# Get environment variables
PROJECT_ID = os.getenv('project_id')
DATASET = os.getenv('bq_dataset')
TABLE = os.getenv('bq_table')
FIRESTORE_COLLECTION = os.getenv('firestore_collection')
BATCH_SIZE = 50  # Process 50 rows at a time

# Initialize Firestore client
firestore_client = FirestoreClient(PROJECT_ID, FIRESTORE_COLLECTION)

def fetch_bigquery_data(last_processed_id):
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT *
    FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    WHERE id > {last_processed_id}
    ORDER BY id
    LIMIT {BATCH_SIZE}
    """
    
    try:
        df = client.query(query).to_dataframe()
        print(f"\nProcessing {len(df)} rows starting from ID: {last_processed_id}")
        return df
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return None

def upload_to_gcs(image, filename):
    """Upload generated image to Google Cloud Storage bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(os.getenv('psearch_img_bucket'))
    blob = bucket.blob(filename)
    blob.upload_from_string(image.image_bytes)
    return f"gs://{os.getenv('psearch_img_bucket')}/{filename}"

def process_single_product(row):
    """Process a single product and return the result."""
    product_id = row['id']
    row_data = row.to_dict()
    
    try:
        # Start processing and mark in Firestore
        firestore_client.start_product_processing(product_id, row_data)
        
        # Generate safe filename using product ID
        safe_filename = f"product_{product_id}.png"
        
        # Generate image using Imagen client
        image = generate_image(row, PROJECT_ID)
        if not image:
            raise Exception("Failed to generate image")
            
        # Get image description using Gemini client - Updated to pass product_data
        description = get_image_description(image.image_bytes, PROJECT_ID, row_data)
        if not description:
            raise Exception("Failed to generate description")
        
        # Upload to GCS and get URI
        image_uri = upload_to_gcs(image, safe_filename)
        
        # Mark as completed in Firestore
        firestore_client.complete_product_processing(product_id, image_uri, description)
        
        # Add to processed rows for CSV
        row_data['image_uri'] = image_uri
        row_data['description'] = description
        
        print(f"Generated and uploaded image for product ID {product_id}: {image_uri}")
        print(f"Generated description: {description[:100]}...")
        
        return row_data, None
        
    except Exception as e:
        error_message = f"Error processing product: {str(e)}"
        print(error_message)
        firestore_client.mark_product_failed(product_id, error_message)
        return None, error_message

def process_products():
    processed_rows = []
    total_processed = 0
    last_id = firestore_client.get_last_processed_id()
    
    # First, try to process any previously failed items
    print("Checking for failed products to retry...")
    failed_products = firestore_client.get_failed_products()
    for failed_product in failed_products:
        product_data = failed_product['product_data']
        row = pd.Series(product_data)
        
        result, error = process_single_product(row)
        if result:
            processed_rows.append(result)
            total_processed += 1
        
        time.sleep(1)  # Rate limiting
    
    # Now process new items
    while total_processed < 30000:
        df = fetch_bigquery_data(last_id)
        
        if df is None or df.empty:
            print("No more rows to process")
            break
            
        for index, row in df.iterrows():
            product_id = row['id']
            
            # Skip if product already processed
            if firestore_client.is_product_processed(product_id):
                print(f"Product {product_id} already processed, skipping...")
                last_id = product_id
                continue
            
            result, error = process_single_product(row)
            if result:
                processed_rows.append(result)
                total_processed += 1
            
            # Update last processed ID
            last_id = product_id
            firestore_client.update_last_processed_id(last_id)
            
            # Add some delay to avoid rate limiting
            time.sleep(1)
        
        print(f"Processed {total_processed} products so far")
    
    # Create final DataFrame and export to CSV
    if processed_rows:
        final_df = pd.DataFrame(processed_rows)
        output_filename = f"processed_products_{int(time.time())}.csv"
        final_df.to_csv(output_filename, index=False)
        print(f"Exported results to {output_filename}")
        
        # Upload CSV to GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(os.getenv('psearch_img_bucket'))
        blob = bucket.blob(f"exports/{output_filename}")
        blob.upload_from_filename(output_filename)
        print(f"Uploaded CSV to gs://{os.getenv('psearch_img_bucket')}/exports/{output_filename}")

if __name__ == "__main__":
    process_products()
