/*
 * Copyright 2025 Google LLC
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import axios from 'axios';
import config from '../config';
import * as genAiService from './genAiService';

// Base URL for the Source Ingestion API - direct to the port since we're running the backend directly
const INGESTION_SOURCE_URL = config.ingestionSourceUrl;

// Storage for mock job statuses
const mockJobStatuses = {};

// Mock implementations
const mockService = {
  // Generate a mock schema based on file type
  generateMockSchema: (fileType) => {
    if (fileType === 'csv') {
      return {
        schema_fields: [
          { name: 'id', type: 'INTEGER', mode: 'NULLABLE' },
          { name: 'name', type: 'STRING', mode: 'NULLABLE' },
          { name: 'description', type: 'STRING', mode: 'NULLABLE' },
          { name: 'price', type: 'FLOAT', mode: 'NULLABLE' },
          { name: 'category', type: 'STRING', mode: 'NULLABLE' },
          { name: 'in_stock', type: 'BOOLEAN', mode: 'NULLABLE' },
          { name: 'created_at', type: 'TIMESTAMP', mode: 'NULLABLE' }
        ],
        row_count_estimate: 1250,
        has_header: true
      };
    } else {
      return {
        schema_fields: [
          { name: 'id', type: 'INTEGER', mode: 'NULLABLE' },
          { name: 'name', type: 'STRING', mode: 'NULLABLE' },
          { name: 'description', type: 'STRING', mode: 'NULLABLE' },
          { name: 'price', type: 'FLOAT', mode: 'NULLABLE' },
          { name: 'category', type: 'STRING', mode: 'NULLABLE' },
          {
            name: 'attributes', type: 'RECORD', mode: 'NULLABLE', fields: [
              { name: 'color', type: 'STRING', mode: 'NULLABLE' },
              { name: 'size', type: 'STRING', mode: 'NULLABLE' },
              { name: 'weight', type: 'FLOAT', mode: 'NULLABLE' }
            ]
          },
          { name: 'tags', type: 'STRING', mode: 'REPEATED' },
          { name: 'in_stock', type: 'BOOLEAN', mode: 'NULLABLE' },
          { name: 'created_at', type: 'TIMESTAMP', mode: 'NULLABLE' }
        ],
        row_count_estimate: 1250,
        is_array: true
      };
    }
  },

  // Mock upload file
  uploadFile: async (file, onProgress) => {
    return new Promise((resolve) => {
      // Simulate progress
      let progress = 0;
      const progressInterval = setInterval(() => {
        progress += 10;
        if (onProgress) onProgress(progress);
        if (progress >= 100) {
          clearInterval(progressInterval);
        }
      }, 200);

      // Simulate API delay
      setTimeout(() => {
        const fileType = file.name.split('.').pop().toLowerCase();
        const mockSchema = mockService.generateMockSchema(fileType);

        resolve({
          file_id: 'mock-file-id-' + Date.now(),
          original_filename: file.name,
          gcs_uri: `gs://${config.projectId}_psearch_raw/${fileType}/${file.name}`,
          file_type: fileType,
          detected_schema: mockSchema,
          row_count_estimate: mockSchema.row_count_estimate,
          upload_timestamp: new Date().toISOString()
        });
      }, 1500);
    });
  },

  // Mock dataset creation
  createDataset: async (datasetRequest) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({
          dataset_id: datasetRequest.dataset_id,
          location: datasetRequest.location || "US",
          created: true,
          message: `Dataset ${datasetRequest.dataset_id} created successfully`
        });
      }, 1000);
    });
  },

  // Mock table creation
  createTable: async (tableRequest) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve({
          dataset_id: tableRequest.dataset_id,
          table_id: tableRequest.table_id,
          created: true,
          message: `Table ${tableRequest.dataset_id}.${tableRequest.table_id} created successfully`,
          schema_field_count: tableRequest.schema.length
        });
      }, 1000);
    });
  },

  // Mock data loading
  loadData: async (loadRequest, fileId, fileType) => {
    return new Promise((resolve) => {
      const jobId = 'mock-job-id-' + Date.now();
      const response = {
        job_id: jobId,
        status: 'RUNNING',
        message: 'Job started',
        created_at: new Date().toISOString(),
        metadata: {
          file_id: fileId,
          dataset_id: loadRequest.dataset_id,
          table_id: loadRequest.table_id,
          source_format: loadRequest.source_format,
          file_type: fileType,
          max_bad_records: loadRequest.max_bad_records || 0
        }
      };

      // Store the initial status
      mockJobStatuses[jobId] = { ...response };

      // Start a timer to simulate job completion after 3 seconds
      setTimeout(() => {
        mockJobStatuses[jobId] = {
          ...response,
          status: 'COMPLETED',
          message: `Loaded 1250 rows into ${loadRequest.dataset_id}.${loadRequest.table_id}`,
          completed_at: new Date().toISOString(),
          metadata: {
            ...response.metadata,
            row_count: 1250,
            bytes_processed: 1250 * 500
          }
        };
      }, 3000);

      resolve(response);
    });
  },

  // Mock get job status
  getJobStatus: async (jobId) => {
    return new Promise((resolve) => {
      setTimeout(() => {
        if (mockJobStatuses[jobId]) {
          resolve(mockJobStatuses[jobId]);
        } else {
          // If no status yet, make one up
          resolve({
            job_id: jobId,
            status: 'RUNNING',
            message: 'Job in progress',
            created_at: new Date().toISOString()
          });
        }
      }, 500);
    });
  },

  // Mock get buckets
  listBuckets: async () => {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve([
          {
            name: `${config.projectId}_psearch_raw`,
            location: 'us-central1',
            created: new Date().toISOString(),
            storage_class: 'STANDARD',
            is_default: true
          },
          {
            name: `${config.projectId}-tmp`,
            location: 'us-central1',
            created: new Date(Date.now() - 86400000).toISOString(),
            storage_class: 'STANDARD',
            is_default: false
          }
        ]);
      }, 500);
    });
  }
};

// Main service that delegates to real or mock based on mode
export const sourceIngestionService = {
  // Get project bucket name (for display purposes)
  getProjectBucketName: (projectId) => {
    return `${projectId}_psearch_raw`;
  },

  // Check if we should use mock mode
  useMockMode: () => {
    return localStorage.getItem('useMockIngestionApi') !== 'false'; // Default to true
  },

  // Set mock mode
  setMockMode: (useMock) => {
    localStorage.setItem('useMockIngestionApi', useMock.toString());
  },

  // Upload file and detect schema
  uploadFile: async (file, onProgress) => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.uploadFile(file, onProgress);
    }

    const formData = new FormData();
    formData.append('file', file);

    const response = await axios.post(`${INGESTION_SOURCE_URL}/upload`, formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      onUploadProgress: (progressEvent) => {
        if (onProgress) {
          const percentCompleted = Math.round(
            (progressEvent.loaded * 100) / progressEvent.total
          );
          onProgress(percentCompleted);
        }
      }
    });

    return response.data;
  },

  // List available buckets
  listBuckets: async () => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.listBuckets();
    }

    const response = await axios.get(`${INGESTION_SOURCE_URL}/buckets`);
    return response.data;
  },

  // Create a BigQuery dataset
  createDataset: async (datasetRequest) => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.createDataset(datasetRequest);
    }

    const response = await axios.post(`${INGESTION_SOURCE_URL}/datasets`, datasetRequest);
    return response.data;
  },

  // Create a BigQuery table with schema
  createTable: async (tableRequest) => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.createTable(tableRequest);
    }

    const response = await axios.post(`${INGESTION_SOURCE_URL}/tables`, tableRequest);
    return response.data;
  },

  // Create table and load data in one step with schema autodetection
  createAndLoadTable: async (loadRequest, fileId, fileType) => { // Added fileType
    if (sourceIngestionService.useMockMode()) {
      // Pass all parameters to the mock service
      return mockService.loadData(loadRequest, fileId, fileType);
    }

    // Add file_type to the query parameters
    const response = await axios.post(
      `${INGESTION_SOURCE_URL}/create_and_load?file_id=${fileId}&file_type=${fileType}`,
      loadRequest
    );
    return response.data;
  },

  // Load data from file into BigQuery table (assumes table already exists)
  loadData: async (loadRequest, fileId, fileType) => { // Added fileType
    if (sourceIngestionService.useMockMode()) {
      // Pass all parameters to the mock service
      return mockService.loadData(loadRequest, fileId, fileType);
    }

    // Add file_type to the query parameters
    const response = await axios.post(
      `${INGESTION_SOURCE_URL}/load?file_id=${fileId}&file_type=${fileType}`,
      loadRequest
    );
    return response.data;
  },

  // Get job status
  getJobStatus: async (jobId) => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.getJobStatus(jobId);
    }

    const response = await axios.get(`${INGESTION_SOURCE_URL}/jobs/${jobId}`);
    return response.data;
  },

  /**
   * Ensures a BigQuery dataset exists, creating it if necessary.
   * @param {string} datasetId The ID of the dataset to ensure exists
   * @returns {Promise<object>} Object indicating success or containing error details
   */
  ensureDatasetExists: async (datasetId) => {
    if (sourceIngestionService.useMockMode()) {
      console.log(`Mock ensuring dataset ${datasetId} exists`);
      return {
        dataset_id: datasetId,
        location: "US",
        created: false, // Pretend it already existed
        message: `Dataset ${datasetId} already exists`
      };
    }

    try {
      console.log(`Ensuring dataset ${datasetId} exists`);
      const response = await axios.post(`${INGESTION_SOURCE_URL}/ensure-dataset`, {
        dataset_id: datasetId,
        location: "US"
      });
      
      console.log("Dataset ensure response:", response.data);
      return response.data;
    } catch (error) {
      console.error(`Error ensuring dataset ${datasetId} exists:`, error);
      throw error;
    }
  },

  /**
   * Performs a BigQuery dry run for the given SQL script.
   * @param {string} sqlScript The SQL script to validate.
   * @returns {Promise<object>} Object indicating success or containing error details.
   *                            Example success: { valid: true, message: "SQL syntax validated successfully" }
   *                            Example error: { valid: false, error: "Error message..." }
   */
  // Generate a SQL fix using the new integrated AI-powered fix service
  generateSqlFix: async (originalSql, currentSql, errorMessage, attemptNumber = 1) => {
    if (sourceIngestionService.useMockMode()) {
      console.log("Using mock SQL fix for validation errors");
      
      return new Promise((resolve) => {
        setTimeout(() => {
          // Simulated diff showing what would be changed
          let diff = `--- current.sql\n+++ suggested.sql\n@@ -2,7 +2,7 @@\n SELECT\n   SAFE_CAST(source.id AS STRING) AS id,\n   SAFE_CAST(source.name AS STRING) AS name,\n-  SAFE_CAST(source.title AS STRING) AS title,\n+  /* Field 'title' not in source - using NULL */ NULL AS title,\n   IFNULL(source.brands, []) AS brands,`;
          
          // For the colorFamilies error, create a more specific mock response
          if (errorMessage.includes("colorFamilies")) {
            diff = `--- current.sql\n+++ suggested.sql\n@@ -7,8 +7,8 @@\n   IF(source.colorInfo IS NULL,\n     NULL,\n     STRUCT(\n-      ARRAY(SELECT CAST(colorFamily AS STRING) FROM UNNEST(colorFamilies) AS colorFamily) AS colorFamilies,\n-      ARRAY(SELECT CAST(color AS STRING) FROM UNNEST(colors) AS color) AS colors\n+      -- Default values for colorFamilies which doesn't exist in source\n+      ARRAY(SELECT CAST('Default Color' AS STRING)) AS colorFamilies\n     )\n   ) AS colorInfo,`;
            
            // Create a more sophisticated fix for the specific error
            const suggestedSql = currentSql.replace(
              /ARRAY\(SELECT CAST\(colorFamily AS STRING\) FROM UNNEST\(colorFamilies\) AS colorFamily\) AS colorFamilies/,
              "-- Default values for colorFamilies which doesn't exist in source\n      ARRAY(SELECT CAST('Default Color' AS STRING)) AS colorFamilies"
            );
            
            resolve({
              original_sql: originalSql,
              current_sql: currentSql,
              suggested_sql: suggestedSql,
              diff: diff,
              attempt_number: attemptNumber,
              valid: true, // Mock pretends the fix will be valid
              message: "AI-suggested fix should resolve the missing colorFamilies field"
            });
          } else {
            // Generic mock response for other errors
            const suggestedSql = currentSql.replace(
              /SAFE_CAST\(source\.title AS STRING\) AS title/,
              "/* Field 'title' not in source - using NULL */ NULL AS title"
            );
            
            resolve({
              original_sql: originalSql,
              current_sql: currentSql,
              suggested_sql: suggestedSql,
              diff: diff,
              attempt_number: attemptNumber,
              valid: Math.random() > 0.3, // 70% chance the mock fix is valid
              message: Math.random() > 0.3 ? "SQL fix appears to be valid" : null,
              error: Math.random() > 0.3 ? null : "Mock error: The fix might need further refinement"
            });
          }
        }, 1500); // Simulate network delay
      });
    }
    
    // Real implementation using the genAiService
    return await genAiService.generateSqlFix(originalSql, currentSql, errorMessage, attemptNumber);
  },
  
  // Validate a fixed SQL query
  validateSqlFix: async (sqlToApply, attemptNumber = 1) => {
    if (sourceIngestionService.useMockMode()) {
      console.log("Using mock validate SQL fix");
      
      return new Promise((resolve) => {
        setTimeout(() => {
          // 80% chance of success in mock mode
          const isValid = Math.random() > 0.2; 
          
          if (isValid) {
            resolve({
              valid: true,
              message: "SQL syntax validated successfully (Mock)",
              details: {
                estimated_bytes_processed: 1024 * 1024 * 5 // 5MB mock estimate
              }
            });
          } else {
            resolve({
              valid: false,
              error: "Mock error: Still having issues with the SQL syntax near line 10.",
              details: {
                error_type: "syntax_error",
                line_number: 10
              }
            });
          }
        }, 800); // Faster response for validation
      });
    }
    
    // Real implementation using the genAiService
    return await genAiService.validateSqlFix(sqlToApply, attemptNumber);
  },
  
  dryRunQuery: async (sqlScript) => {
    // Check if we should use mock mode
    if (sourceIngestionService.useMockMode()) {
      console.log("Using mock dry run for SQL validation");
      
      // Clear past refinement state to test fresh (keep mock behavior for testing)
      localStorage.removeItem('sqlRefined');
      
      return new Promise((resolve, reject) => {
        setTimeout(() => {
          // Check if SQL contains specific patterns to validate
          // This makes the mock service more realistic
          let mockSuccess = true;
          let mockError = "";
          
          // First dry run - fail with specific colorInfo struct error
          // Subsequent runs (after AI refinement) will succeed
          if (sqlScript.includes("STRUCT(") && sqlScript.includes("colorInfo") && 
              (sqlScript.includes("colorFamilies") || sqlScript.includes("colorFamily"))) {
            
            // Check if it's the initial SQL or already refined
            const isRefined = (
              sqlScript.includes("-- Default values for") || 
              sqlScript.includes("ARRAY(SELECT CAST('Default") || 
              sqlScript.includes("IFNULL(colors,") ||
              localStorage.getItem('sqlRefined') === 'completed'
            );
            
            if (sqlScript.includes("UNNEST(colorFamilies)") && !isRefined) {
              // First-time run with the issue
              mockSuccess = false;
              mockError = "Mock Dry Run Error: Invalid field reference 'colorFamilies'. Field does not exist in source table 'psearch_raw.example_catalog'.";
              localStorage.setItem('sqlRefined', 'pending');
            } else {
              // Already refined - SQL should be valid now
              mockSuccess = true;
              localStorage.setItem('sqlRefined', 'completed');
            }
          } else if (Math.random() > 0.95) { // Lower the chance of random errors to 5%
            // Randomly fail some other SQL with generic error
            mockSuccess = false;
            mockError = "Mock Dry Run Error: Invalid syntax near 'SOME_COLUMN'. Check column names and types.";
          }
          
          if (mockSuccess) {
            console.log("Mock dry run successful.");
            resolve({ valid: true, message: "SQL syntax validated successfully" });
          } else {
            console.error("Mock dry run failed:", mockError);
            // Resolve with error structure, don't reject the promise for expected API failures
            resolve({ valid: false, error: mockError });
          }
        }, 1200); // Simulate network delay
      });
    }

    // Extract target dataset IDs from SQL script to ensure they exist
    try {
      // Use a regex to extract dataset references from CREATE TABLE statements
      const createTablePattern = /CREATE\s+OR\s+REPLACE\s+TABLE\s+`([^`]*)`/i;
      const match = createTablePattern.exec(sqlScript);
      
      if (match && match[1]) {
        const tableRef = match[1];
        
        // Split the reference into its parts (project.dataset.table)
        const parts = tableRef.split('.');
        
        // If we have at least dataset and table parts
        if (parts.length >= 2) {
          // For dataset.table format, we need to get the dataset part
          let datasetId;
          if (parts.length === 2) {
            datasetId = parts[0]; // dataset.table format
          } else if (parts.length === 3) {
            datasetId = parts[1]; // project.dataset.table format
          }
          
          if (datasetId) {
            console.log(`Found dataset reference in SQL: ${datasetId}`);
            try {
              // Ensure the dataset exists before running the dry run
              await sourceIngestionService.ensureDatasetExists(datasetId);
              console.log(`Successfully ensured dataset ${datasetId} exists`);
            } catch (datasetError) {
              console.warn(`Failed to ensure dataset ${datasetId} exists, proceeding with dry run anyway:`, datasetError);
              // Continue with the dry run even if this fails, as BigQuery will give a more specific error
            }
          }
        }
      }
    } catch (parseError) {
      console.warn("Error parsing SQL for dataset references:", parseError);
      // Continue with the dry run even if parsing fails
    }

    // Real implementation using the actual BigQuery dry run endpoint
    try {
      console.log("Using real BigQuery dry run API");
      
      const response = await axios.post(`${INGESTION_SOURCE_URL}/dry-run-query`, {
        sql_script: sqlScript,
        max_timeout_seconds: 30  // 30 second timeout for dry run
      }, { 
        timeout: 35000 // Slightly longer axios timeout than the server-side timeout
      });

      console.log("Dry run response:", response.data);
      
      if (response.data.valid) {
        return {
          valid: true,
          message: response.data.message || "SQL syntax validated successfully",
          details: response.data.details || null
        };
      } else {
        // Check if we have a "not found" error with dataset information
        if (response.data.details?.error_type === "not_found" && response.data.details?.missing_dataset) {
          const missingDataset = response.data.details.missing_dataset;
          
          try {
            // Try to create the missing dataset and retry the dry run
            console.log(`Attempting to create missing dataset: ${missingDataset}`);
            await sourceIngestionService.ensureDatasetExists(missingDataset);
            
            // Retry the dry run
            console.log("Retrying dry run after creating dataset");
            return await sourceIngestionService.dryRunQuery(sqlScript);
          } catch (createError) {
            console.error(`Failed to create missing dataset ${missingDataset}:`, createError);
            // Return the original error if we can't create the dataset
            return {
              valid: false,
              error: response.data.error || "Unknown validation error from server",
              details: response.data.details || null
            };
          }
        } else {
          // Return the error as-is for other types of errors
          return {
            valid: false,
            error: response.data.error || "Unknown validation error from server",
            details: response.data.details || null
          };
        }
      }
    } catch (error) {
      console.error('Error performing dry run query:', error);
      
      // Provide more detailed error information
      const errorMessage = error.response?.data?.error || 
                          error.response?.data?.detail || 
                          error.message || 
                          "An unknown error occurred during the dry run.";
      
      return { 
        valid: false, 
        error: errorMessage,
        details: error.response?.data?.details || null
      };
    }
  }
};

export default sourceIngestionService;
