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

// Base URL for the Source Ingestion API - direct to the port since we're running the backend directly
const API_BASE_URL = config.apiUrl;

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
          { name: 'attributes', type: 'RECORD', mode: 'NULLABLE', fields: [
            { name: 'color', type: 'STRING', mode: 'NULLABLE' },
            { name: 'size', type: 'STRING', mode: 'NULLABLE' },
            { name: 'weight', type: 'FLOAT', mode: 'NULLABLE' }
          ]},
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
    
    const response = await axios.post(`${API_BASE_URL}/upload`, formData, {
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
    
    const response = await axios.get(`${API_BASE_URL}/buckets`);
    return response.data;
  },
  
  // Create a BigQuery dataset
  createDataset: async (datasetRequest) => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.createDataset(datasetRequest);
    }
    
    const response = await axios.post(`${API_BASE_URL}/datasets`, datasetRequest);
    return response.data;
  },
  
  // Create a BigQuery table with schema
  createTable: async (tableRequest) => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.createTable(tableRequest);
    }
    
    const response = await axios.post(`${API_BASE_URL}/tables`, tableRequest);
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
      `${API_BASE_URL}/create_and_load?file_id=${fileId}&file_type=${fileType}`, 
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
      `${API_BASE_URL}/load?file_id=${fileId}&file_type=${fileType}`, 
      loadRequest
    );
    return response.data;
  },
  
  // Get job status
  getJobStatus: async (jobId) => {
    if (sourceIngestionService.useMockMode()) {
      return mockService.getJobStatus(jobId);
    }
    
    const response = await axios.get(`${API_BASE_URL}/jobs/${jobId}`);
    return response.data;
  }
};

export default sourceIngestionService;
