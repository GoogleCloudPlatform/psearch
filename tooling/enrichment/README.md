# Product Enrichment Tool

This tool enriches product data by generating AI-powered product images and descriptions using Google Cloud's Vertex AI services. It processes products from BigQuery, generates images using Imagen, creates descriptions using Gemini, and stores the results in both Cloud Storage and Firestore.

## Features

- **Image Generation**: Uses Vertex AI Imagen to create product images based on product details
- **Description Generation**: Uses Vertex AI Gemini to generate detailed product descriptions from the generated images
- **State Management**: Uses Firestore to track processing status and handle retries
- **Fault Tolerance**: Includes retry mechanism for failed products (up to 3 attempts)
- **Progress Tracking**: Maintains processing state and can resume from interruptions
- **Batch Processing**: Processes products in configurable batch sizes
- **Export Capability**: Exports results to CSV and uploads to Cloud Storage
- **Result Consolidation**: Separate script to consolidate results from BigQuery and Firestore

## Prerequisites

- Google Cloud Project with the following APIs enabled:
  - Vertex AI API
  - BigQuery API
  - Firestore API
  - Cloud Storage API
- Python 3.11+
- Required IAM permissions:
  - BigQuery Data Viewer
  - Firestore User
  - Storage Object Creator
  - Vertex AI User

## Setup

1. Clone the repository and navigate to the enrichment directory:
```bash
cd tooling/enrichment
```

2. Create and configure your `.env` file:
```bash
cp .env.example .env
# Edit .env with your configuration
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Configure the following environment variables in your `.env` file:

```env
project_id=your-project-id
bq_dataset=your_dataset
bq_table=your_table
psearch_img_bucket=your-bucket-name
firestore_collection=your_collection
firestore_database=(default)
```

## Usage

### Local Development

Run the enrichment process:
```bash
python main.py
```

Generate consolidated report:
```bash
python consolidate_results.py
```

### Cloud Run Deployment

Deploy the enrichment job to Cloud Run:
```bash
chmod +x deploy.sh
./deploy.sh
```

## Architecture

The tool consists of several components:

- **main.py**: Orchestrates the enrichment process
- **imagen_client.py**: Handles product image generation with Gemini image models (`gemini-3.1-flash-image`) via the Google Gen AI SDK
- **gemini_client.py**: Generates descriptions with Gemini (`gemini-3.5-flash`) via the Google Gen AI SDK (`GENAI_LOCATION=global`)
- **firestore_client.py**: Manages state and progress tracking
- **consolidate_results.py**: Consolidates results from BigQuery and Firestore

### Process Flow

1. Enrichment Process (main.py):
   - Fetches products from BigQuery in batches
   - Generates product image using Imagen
   - Generates description using Gemini
   - Uploads image to Cloud Storage
   - Updates processing status in Firestore
   - Handles failures and retries automatically

2. Result Consolidation (consolidate_results.py):
   - Fetches all products from BigQuery
   - Retrieves processed products from Firestore
   - Retrieves failed products from Firestore
   - Merges data and adds processing status
   - Generates comprehensive CSV report
   - Uploads report to Cloud Storage

### Error Handling

- Tracks processing status in Firestore
- Automatically retries failed products up to 3 times
- Marks products as permanently failed after 3 failed attempts
- Stores detailed error messages for debugging
- Consolidation script includes failed products in final report

## Monitoring

Monitor processing progress through:
- Console output with detailed status updates
- Firestore documents containing processing status
- Generated CSV reports in Cloud Storage
- Consolidated reports showing overall progress

## Output

The tool generates:
- Product images in Cloud Storage
- Product descriptions in Firestore
- Incremental CSV exports with processed products
- Consolidated CSV reports including:
  - Original product data
  - Generated image URIs
  - Generated descriptions
  - Processing status
  - Error messages for failed items
  - Processing timestamps

## Troubleshooting

Common issues and solutions:

1. **Rate Limiting**: The tool includes built-in delays to handle API rate limits
2. **Failed Products**: Check Firestore for detailed error messages
3. **Interrupted Processing**: The tool can safely resume from the last processed ID
4. **Permanent Failures**: Products that fail 3 times are marked as 'permanently_failed'
5. **Missing Data**: Run consolidate_results.py to get a complete view of processing status

## Cloud Run Jobs

The tool is configured to run as a Cloud Run job with:
- 2GB memory
- 2 CPU cores
- 1-hour timeout
- 3 retry attempts
- Automatic environment variable configuration 