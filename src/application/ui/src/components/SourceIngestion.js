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

import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Box,
  Container,
  Paper,
  Typography,
  Stepper,
  Step,
  StepLabel,
  Button,
  TextField,
  Grid,
  CircularProgress,
  Alert,
  Chip,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Divider,
  FormControl,
  FormControlLabel,
  InputLabel,
  Select,
  MenuItem,
  Stack,
  Tabs,
  Tab,
  Card,
  CardContent,
  IconButton,
  Tooltip,
  Switch,
  LinearProgress
} from '@mui/material';
import FileUploadIcon from '@mui/icons-material/FileUpload';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import StorageIcon from '@mui/icons-material/Storage';
import TableChartIcon from '@mui/icons-material/TableChart';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import VisibilityIcon from '@mui/icons-material/Visibility';
import InfoIcon from '@mui/icons-material/Info';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import config from '../config';
import { sourceIngestionService } from '../services/sourceIngestionService';

// Component for file upload and processing
const SourceIngestion = () => {
  // State for file upload and processing
  const [activeStep, setActiveStep] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [success, setSuccess] = useState(null);
  const [uploadProgress, setUploadProgress] = useState(0);
  
  // UI mode toggles
  const [useMockApi, setUseMockApi] = useState(() => sourceIngestionService.useMockMode());
  const [useAutodetect, setUseAutodetect] = useState(true); // Default to using schema autodetection
  const [isSchemaFile, setIsSchemaFile] = useState(false); // Track if file appears to be a schema definition
  
  // File upload state
  const [file, setFile] = useState(null);
  const [uploadResult, setUploadResult] = useState(null);
  
  // Schema state
  const [schema, setSchema] = useState([]);
  const [datasetId, setDatasetId] = useState('');
  const [tableId, setTableId] = useState('');
  
  // BigQuery job state
  const [jobId, setJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState(null);
  
  // Refs
  const fileInputRef = useRef(null);
  
  // Define steps
  const steps = [
    'Upload Data File',
    'Configure Schema',
    'Create BigQuery Table',
    'Load Data'
  ];

  // Helper to detect if a file might be a schema definition file
  const detectSchemaFile = (file) => {
    // If the file name contains 'schema', it's likely a schema definition
    const fileNameLower = file.name.toLowerCase();
    const hasSchemaInName = fileNameLower.includes('schema');
    
    // If it's a JSON file with 'schema' in the name, it's likely a schema file
    return file.name.endsWith('.json') && hasSchemaInName;
  };

  // Handle file selection
  const handleFileSelect = (event) => {
    const selectedFile = event.target.files[0];
    if (selectedFile) {
      // Check file type
      const fileType = selectedFile.name.split('.').pop().toLowerCase();
      if (fileType !== 'csv' && fileType !== 'json') {
        setError('Unsupported file type. Please upload a CSV or JSON file.');
        return;
      }
      
      // Check if it's potentially a schema file
      const schemaFile = detectSchemaFile(selectedFile);
      setIsSchemaFile(schemaFile);
      
      // If it's a schema file, set max_bad_records to 0 as default since schema should be valid
      if (schemaFile) {
        setMaxBadRecords(0);
      }
      
      setFile(selectedFile);
      setError(null);
    }
  };

  // Keep uploaded file ID in localStorage to persist across page refreshes
  const saveFileInfoToLocalStorage = (fileId, fileType) => {
    localStorage.setItem('psearch_last_uploaded_file_id', fileId);
    localStorage.setItem('psearch_last_uploaded_file_type', fileType);
  };
  
  // Handle file upload
  const handleFileUpload = async () => {
    if (!file) {
      setError('Please select a file to upload.');
      return;
    }
    
    setLoading(true);
    setError(null);
    setUploadProgress(0);
    
    try {
      // Log important data for debugging
      console.log("Starting file upload, file name:", file.name);
      
      // Use our service to upload the file
      const result = await sourceIngestionService.uploadFile(file, (progress) => {
        setUploadProgress(progress);
      });
      
      // Store the file ID and type for lookup later
      saveFileInfoToLocalStorage(result.file_id, result.file_type);
      console.log("Upload successful, file ID:", result.file_id, "file type:", result.file_type);
      
      setUploadResult(result);
      setSchema(result.detected_schema.schema_fields);
      setSuccess(`File ${file.name} uploaded successfully! File ID: ${result.file_id}`);
      
      // Set fixed dataset ID and generate default table ID
      const fixedDatasetId = 'psearch_raw'; // Always use psearch_raw
      const defaultTableId = file.name.split('.')[0].replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
      
      setDatasetId(fixedDatasetId);
      setTableId(defaultTableId);
      
    } catch (error) {
      console.error('Error uploading file:', error);
      
      // Enhanced error debugging
      let errorMessage = 'Failed to upload file. Please try again.';
      
      if (error.response) {
        // The request was made and the server responded with a status code
        console.log('Error response status:', error.response.status);
        console.log('Error response data:', error.response.data);
        // Ensure we're getting a string from the error detail
        const detail = error.response.data?.detail;
        errorMessage = typeof detail === 'string' ? detail : 
                       (detail ? JSON.stringify(detail) : `Server error: ${error.response.status}`);
      } else if (error.request) {
        // The request was made but no response was received
        console.log('Error request:', error.request);
        errorMessage = 'Network error - no response from server. Check if the backend is running on http://localhost:8080.';
      } else {
        // Something happened in setting up the request
        console.log('Error message:', error.message);
        errorMessage = `Request setup error: ${error.message}`;
      }
      
      // Ensure error is always a string
      setError(typeof errorMessage === 'object' ? JSON.stringify(errorMessage) : errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // Handle dataset creation
  const handleCreateDataset = async () => {
    if (!datasetId) {
      setError('Please enter a dataset ID.');
      return;
    }
    
    setLoading(true);
    setError(null);
    
    try {
      const datasetRequest = {
        dataset_id: datasetId,
        location: "US",
        description: `Dataset for ${file.name} created by PSearch Source Ingestion`
      };
      
      const result = await sourceIngestionService.createDataset(datasetRequest);
      setSuccess(result.message || `Dataset ${datasetId} created successfully!`);
    } catch (error) {
      console.error('Error creating dataset:', error);
      
      // Enhanced error debugging
      let errorMessage = 'Failed to create dataset. Please try again.';
      
      if (error.response) {
        // The request was made and the server responded with a status code
        console.log('Error response status:', error.response.status);
        console.log('Error response data:', error.response.data);
        const detail = error.response.data?.detail;
        errorMessage = typeof detail === 'string' ? detail : 
                       (detail ? JSON.stringify(detail) : `Server error: ${error.response.status}`);
      } else if (error.request) {
        // The request was made but no response was received
        console.log('Error request:', error.request);
        errorMessage = 'Network error - no response from server.';
      } else {
        // Something happened in setting up the request
        console.log('Error message:', error.message);
        errorMessage = `Request setup error: ${error.message}`;
      }
      
      // Ensure error is always a string
      setError(typeof errorMessage === 'object' ? JSON.stringify(errorMessage) : errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // Handle table creation
  const handleCreateTable = async () => {
    if (!tableId) {
      setError('Please enter a table ID.');
      return;
    }
    
    setLoading(true);
    setError(null);
    
    try {
      // Format schema field objects for API
      const formattedSchema = schema.map(field => ({
        name: field.name,
        type: field.type,
        mode: field.mode || "NULLABLE",
        description: field.description
      }));
      
      const tableRequest = {
        dataset_id: datasetId,
        table_id: tableId,
        schema: formattedSchema,
        description: `Table for ${file.name} created by PSearch Source Ingestion`
      };
      
      const result = await sourceIngestionService.createTable(tableRequest);
      setSuccess(result.message || `Table ${datasetId}.${tableId} created successfully!`);
    } catch (error) {
      console.error('Error creating table:', error);
      
      // Enhanced error debugging
      let errorMessage = 'Failed to create table. Please try again.';
      
      if (error.response) {
        // The request was made and the server responded with a status code
        console.log('Error response status:', error.response.status);
        console.log('Error response data:', error.response.data);
        const detail = error.response.data?.detail;
        errorMessage = typeof detail === 'string' ? detail : 
                       (detail ? JSON.stringify(detail) : `Server error: ${error.response.status}`);
      } else if (error.request) {
        // The request was made but no response was received
        console.log('Error request:', error.request);
        errorMessage = 'Network error - no response from server.';
      } else {
        // Something happened in setting up the request
        console.log('Error message:', error.message);
        errorMessage = `Request setup error: ${error.message}`;
      }
      
      // Ensure error is always a string
      setError(typeof errorMessage === 'object' ? JSON.stringify(errorMessage) : errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // State for job polling
  const [statusPollingInterval, setStatusPollingInterval] = useState(null);
  
  // Clean up polling interval on unmount
  useEffect(() => {
    return () => {
      if (statusPollingInterval) {
        clearInterval(statusPollingInterval);
      }
    };
  }, [statusPollingInterval]);
  
  // Start polling for job status
  const startJobStatusPolling = (jobId) => {
    // Clear any existing interval
    if (statusPollingInterval) {
      clearInterval(statusPollingInterval);
    }
    
    // Create a new polling interval
    const intervalId = setInterval(async () => {
      try {
        const status = await sourceIngestionService.getJobStatus(jobId);
        setJobStatus(status);
        
        // If job is completed or failed, stop polling
        if (status.status === 'COMPLETED' || status.status === 'FAILED') {
          clearInterval(intervalId);
          setStatusPollingInterval(null);
        }
      } catch (error) {
        console.error('Error polling job status:', error);
        clearInterval(intervalId);
        setStatusPollingInterval(null);
      }
    }, 3000); // Poll every 3 seconds
    
    setStatusPollingInterval(intervalId);
  };

  // State for max bad records (for JSON files)
  const [maxBadRecords, setMaxBadRecords] = useState(0);

  // Handle data loading
  const handleLoadData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      // Create common request payload
      const loadRequest = {
        dataset_id: datasetId,
        table_id: tableId,
        source_format: uploadResult.file_type.toUpperCase(),
        write_disposition: "WRITE_TRUNCATE",
        skip_leading_rows: uploadResult.file_type === 'csv' ? 1 : 0, // Use 0 instead of null for non-CSV files
        allow_jagged_rows: false,
        allow_quoted_newlines: true,
        field_delimiter: ",",
        max_bad_records: maxBadRecords  // Add max_bad_records parameter
      };
      
      // Use appropriate API based on autodetect setting
      let result;
      if (useAutodetect) {
        // Use the create_and_load endpoint with schema autodetection, passing file_type
        result = await sourceIngestionService.createAndLoadTable(
          loadRequest, 
          uploadResult.file_id, 
          uploadResult.file_type // Pass the file_type here
        );
      } else {
        // Use the traditional approach with manual schema
        // Pass file_type to loadData as well
        result = await sourceIngestionService.loadData(
          loadRequest, 
          uploadResult.file_id,
          uploadResult.file_type // Pass the file_type here too
        );
      }
      setJobId(result.job_id);
      setJobStatus(result);
      setSuccess(`Data load job started with ID: ${result.job_id}`);
      
      // Start polling for job status updates
      startJobStatusPolling(result.job_id);
    } catch (error) {
      console.error('Error loading data:', error);
      
      // Enhanced error debugging
      let errorMessage = 'Failed to load data. Please try again.';
      
      if (error.response) {
        // The request was made and the server responded with a status code
        console.log('Error response status:', error.response.status);
        console.log('Error response data:', error.response.data);
        const detail = error.response.data?.detail;
        errorMessage = typeof detail === 'string' ? detail : 
                       (detail ? JSON.stringify(detail) : `Server error: ${error.response.status}`);
      } else if (error.request) {
        // The request was made but no response was received
        console.log('Error request:', error.request);
        errorMessage = 'Network error - no response from server.';
      } else {
        // Something happened in setting up the request
        console.log('Error message:', error.message);
        errorMessage = `Request setup error: ${error.message}`;
      }
      
      // Ensure error is always a string
      setError(typeof errorMessage === 'object' ? JSON.stringify(errorMessage) : errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // Handle next step
  const handleNext = () => {
    if (activeStep === 0 && !uploadResult) {
      handleFileUpload();
      return;
    }
    
    if (activeStep === 1) {
      handleCreateDataset();
    }
    
    if (activeStep === 2) {
      // If autodetect is enabled, we skip table creation as it will be done 
      // automatically in the load step with schema detection
      if (useAutodetect) {
        setActiveStep((prevStep) => prevStep + 1);
        setError(null);
        setSuccess("Using schema autodetection - table will be created automatically during data load");
        return;
      } else {
        handleCreateTable();
      }
    }
    
    if (activeStep === 3) {
      handleLoadData();
      return;
    }
    
    setActiveStep((prevStep) => prevStep + 1);
    setError(null);
    setSuccess(null);
  };

  // Handle back step
  const handleBack = () => {
    setActiveStep((prevStep) => prevStep - 1);
    setError(null);
    setSuccess(null);
  };

  // Handle reset
  const handleReset = () => {
    setActiveStep(0);
    setFile(null);
    setUploadResult(null);
    setSchema([]);
    setDatasetId('');
    setTableId('');
    setJobId(null);
    setJobStatus(null);
    setError(null);
    setSuccess(null);
    
    // Reset file input
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  // Generate a mock schema based on file type
  const generateMockSchema = (fileType) => {
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
  };

  // Render current step content
  const getStepContent = (step) => {
    switch (step) {
      case 0:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Upload Data File
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Upload a CSV or JSON file to ingest into BigQuery. The system will automatically detect the schema.
            </Typography>
            
            {/* Display destination storage information */}
            <Alert severity="info" sx={{ mb: 3 }}>
              <Typography variant="subtitle2" gutterBottom>
                Storage Information
              </Typography>
              Files will be uploaded to bucket: <strong>{sourceIngestionService.getProjectBucketName(config.projectId || 'your-project')}</strong>
              <br />
              CSV files will be stored in the <strong>csv/</strong> folder
              <br />
              JSON files will be stored in the <strong>json/</strong> folder
            </Alert>
            
            <Box sx={{ 
              border: '2px dashed #ccc', 
              borderRadius: 2, 
              p: 3, 
              textAlign: 'center',
              mt: 2,
              mb: 3,
              backgroundColor: '#f8f9fa'
            }}>
              {/* Show upload progress when loading */}
              {loading && activeStep === 0 && (
                <Box sx={{ width: '100%', mt: 2, mb: 2 }}>
                  <LinearProgress variant="determinate" value={uploadProgress} />
                  <Typography variant="body2" color="text.secondary" align="center" sx={{ mt: 1 }}>
                    {uploadProgress}% Uploaded
                  </Typography>
                </Box>
              )}
              <input
                ref={fileInputRef}
                type="file"
                accept=".csv,.json"
                onChange={handleFileSelect}
                style={{ display: 'none' }}
                id="file-upload-input"
              />
              <label htmlFor="file-upload-input">
                <Button
                  variant="contained"
                  component="span"
                  startIcon={<FileUploadIcon />}
                >
                  Select File
                </Button>
              </label>
              
            {file && (
              <Box sx={{ mt: 2 }}>
                <Typography variant="body1">
                  Selected File: {file.name}
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Size: {(file.size / 1024).toFixed(2)} KB
                </Typography>
                <Typography variant="body2" color="text.secondary">
                  Type: {file.type || file.name.split('.').pop().toUpperCase()}
                  {isSchemaFile && (
                    <Chip 
                      label="Schema Definition" 
                      color="primary" 
                      size="small" 
                      sx={{ ml: 1 }}
                    />
                  )}
                </Typography>
                
                {isSchemaFile && (
                  <Alert severity="info" sx={{ mt: 2, mb: 2 }}>
                    <Typography variant="subtitle2">
                      Schema Definition File Detected
                    </Typography>
                    <Typography variant="body2">
                      This appears to be a BigQuery schema definition file. The system will 
                      automatically convert this to the required JSONL format for BigQuery ingestion.
                    </Typography>
                  </Alert>
                )}
              </Box>
            )}
            </Box>
            
            {uploadResult && (
              <Card variant="outlined" sx={{ mb: 3 }}>
                <CardContent>
                  <Typography variant="h6" gutterBottom>
                    File Upload Summary
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={6}>
                      <Typography variant="body2" color="text.secondary">
                        File Name:
                      </Typography>
                      <Typography variant="body1">
                        {uploadResult.original_filename}
                      </Typography>
                    </Grid>
                    <Grid item xs={6}>
                      <Typography variant="body2" color="text.secondary">
                        File Type:
                      </Typography>
                      <Typography variant="body1" sx={{ textTransform: 'uppercase' }}>
                        {uploadResult.file_type}
                      </Typography>
                    </Grid>
                    <Grid item xs={6}>
                      <Typography variant="body2" color="text.secondary">
                        Estimated Rows:
                      </Typography>
                      <Typography variant="body1">
                        {uploadResult.row_count_estimate.toLocaleString()}
                      </Typography>
                    </Grid>
                    <Grid item xs={6}>
                      <Typography variant="body2" color="text.secondary">
                        Upload Time:
                      </Typography>
                      <Typography variant="body1">
                        {new Date(uploadResult.upload_timestamp).toLocaleString()}
                      </Typography>
                    </Grid>
                  </Grid>
                </CardContent>
              </Card>
            )}
          </Box>
        );
      
      case 1:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Configure Schema
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Review and modify the detected schema before creating the BigQuery dataset and table.
            </Typography>
            
            <Grid container spacing={3} sx={{ mb: 3 }}>
              <Grid item xs={12} md={6}>
                <TextField
                  label="Dataset ID"
                  value={datasetId}
                  onChange={(e) => setDatasetId(e.target.value)}
                  fullWidth
                  required
                  helperText="Enter a unique ID for the BigQuery dataset"
                />
              </Grid>
              <Grid item xs={12} md={6}>
                <TextField
                  label="Table ID"
                  value={tableId}
                  onChange={(e) => setTableId(e.target.value)}
                  fullWidth
                  required
                  helperText="Enter a unique ID for the BigQuery table"
                />
              </Grid>
            </Grid>
            
            <Typography variant="subtitle1" gutterBottom>
              Detected Schema
            </Typography>
            
            <TableContainer component={Paper} variant="outlined" sx={{ mb: 3 }}>
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ backgroundColor: '#f5f5f5' }}>
                    <TableCell>Field Name</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Mode</TableCell>
                    <TableCell width="100">Actions</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {schema.map((field, index) => (
                    <TableRow key={index}>
                      <TableCell>{field.name}</TableCell>
                      <TableCell>
                        <Chip 
                          label={field.type} 
                          size="small" 
                          color={
                            field.type === 'STRING' ? 'primary' :
                            field.type === 'INTEGER' || field.type === 'FLOAT' ? 'success' :
                            field.type === 'BOOLEAN' ? 'secondary' :
                            field.type === 'TIMESTAMP' || field.type === 'DATE' ? 'info' :
                            field.type === 'RECORD' ? 'warning' : 'default'
                          }
                          variant="outlined"
                        />
                      </TableCell>
                      <TableCell>{field.mode}</TableCell>
                      <TableCell>
                        <IconButton size="small" color="primary" disabled>
                          <EditIcon fontSize="small" />
                        </IconButton>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
            
            <Alert severity="info">
              In the full implementation, you would be able to edit field types and add/remove fields.
            </Alert>
          </Box>
        );
      
      case 2:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Create BigQuery Table
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Create a BigQuery table using the configured schema.
            </Typography>
            
            <Alert severity="info" sx={{ mb: 3 }}>
              This will create a BigQuery table named <strong>{tableId}</strong> in the dataset <strong>{datasetId}</strong>.
            </Alert>
            
            <Card variant="outlined" sx={{ mb: 3 }}>
              <CardContent>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <Typography variant="body2" color="text.secondary">
                      Dataset ID:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      {datasetId}
                    </Typography>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                      Location:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      US (multi-region)
                    </Typography>
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <Typography variant="body2" color="text.secondary">
                      Table ID:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      {tableId}
                    </Typography>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                      Schema Fields:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      {schema.length} fields
                    </Typography>
                  </Grid>
                </Grid>
              </CardContent>
            </Card>
            
            <TableContainer component={Paper} variant="outlined" sx={{ mb: 3, maxHeight: '300px', overflow: 'auto' }}>
              <Table size="small">
                <TableHead>
                  <TableRow sx={{ backgroundColor: '#f5f5f5' }}>
                    <TableCell>Field Name</TableCell>
                    <TableCell>Type</TableCell>
                    <TableCell>Mode</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {schema.map((field, index) => (
                    <TableRow key={index}>
                      <TableCell>{field.name}</TableCell>
                      <TableCell>{field.type}</TableCell>
                      <TableCell>{field.mode}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </TableContainer>
          </Box>
        );
      
      case 3:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Load Data
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Load the uploaded data into the BigQuery table.
            </Typography>
            
            {/* Max Bad Records option - show for JSON files only */}
            {uploadResult?.file_type === 'json' && (
              <Box sx={{ mb: 3 }}>
                <Typography variant="subtitle2" color="primary" gutterBottom>
                  JSON Error Handling
                </Typography>
                <Grid container spacing={2} alignItems="center">
                  <Grid item xs={12} md={8}>
                    <TextField
                      label="Maximum Bad Records"
                      type="number"
                      value={maxBadRecords}
                      onChange={(e) => setMaxBadRecords(Math.max(0, parseInt(e.target.value) || 0))}
                      InputProps={{ inputProps: { min: 0 } }}
                      fullWidth
                      helperText="Number of bad JSON records to allow before failing (0 = no errors allowed)"
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <Button 
                      variant="outlined" 
                      color="info"
                      onClick={() => setMaxBadRecords(10)}
                      sx={{ mr: 1 }}
                    >
                      Allow 10
                    </Button>
                    <Button 
                      variant="outlined" 
                      color="info"
                      onClick={() => setMaxBadRecords(100)}
                    >
                      Allow 100
                    </Button>
                  </Grid>
                  <Grid item xs={12}>
                    <Alert severity="info" sx={{ mt: 1 }}>
                      BigQuery expects newline-delimited JSON. If your file has formatting issues, increasing this number will allow BigQuery to skip problematic records.
                    </Alert>
                  </Grid>
                </Grid>
              </Box>
            )}
            
            <Card variant="outlined" sx={{ mb: 3 }}>
              <CardContent>
                <Typography variant="subtitle1" gutterBottom>
                  Load Job Configuration
                </Typography>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <Typography variant="body2" color="text.secondary">
                      Source:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      {uploadResult?.original_filename || 'Unknown'}
                    </Typography>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                      Source Format:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      {uploadResult?.file_type ? uploadResult.file_type.toUpperCase() : 'Unknown'}
                    </Typography>
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <Typography variant="body2" color="text.secondary">
                      Destination:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      {datasetId}.{tableId}
                    </Typography>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                      Write Disposition:
                    </Typography>
                    <Typography variant="body1" gutterBottom>
                      WRITE_TRUNCATE (Replace existing data)
                    </Typography>
                  </Grid>
                </Grid>
              </CardContent>
            </Card>
            
            {jobStatus && (
              <Card variant="outlined" sx={{ mb: 3 }}>
                <CardContent>
                  <Typography variant="subtitle1" gutterBottom>
                    Load Job Status
                  </Typography>
                  <Grid container spacing={2}>
                    <Grid item xs={12} md={6}>
                      <Typography variant="body2" color="text.secondary">
                        Job ID:
                      </Typography>
                      <Typography variant="body1" gutterBottom>
                        {jobStatus.job_id}
                      </Typography>
                      
                      <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                        Status:
                      </Typography>
                      <Typography variant="body1" gutterBottom>
                        <Chip 
                          label={jobStatus.status} 
                          size="small" 
                          color={
                            jobStatus.status === 'COMPLETED' ? 'success' :
                            jobStatus.status === 'RUNNING' ? 'info' :
                            jobStatus.status === 'FAILED' ? 'error' : 'default'
                          }
                        />
                      </Typography>
                    </Grid>
                    <Grid item xs={12} md={6}>
                      <Typography variant="body2" color="text.secondary">
                        Start Time:
                      </Typography>
                      <Typography variant="body1" gutterBottom>
                        {new Date(jobStatus.created_at).toLocaleString()}
                      </Typography>
                      
                      {jobStatus.completed_at && (
                        <>
                          <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>
                            Completion Time:
                          </Typography>
                          <Typography variant="body1" gutterBottom>
                            {new Date(jobStatus.completed_at).toLocaleString()}
                          </Typography>
                        </>
                      )}
                    </Grid>
                    <Grid item xs={12}>
                      <Typography variant="body2" color="text.secondary">
                        Message:
                      </Typography>
                      <Typography variant="body1" gutterBottom>
                        {jobStatus.message}
                      </Typography>
                    </Grid>
                    
                    {jobStatus.status === 'COMPLETED' && jobStatus.metadata && (
                      <Grid item xs={12}>
                        <Divider sx={{ my: 1 }} />
                        <Typography variant="subtitle2" gutterBottom>
                          Job Results
                        </Typography>
                        <Grid container spacing={2}>
                          <Grid item xs={6}>
                            <Typography variant="body2" color="text.secondary">
                              Rows Loaded:
                            </Typography>
                            <Typography variant="body1">
                              {jobStatus.metadata.row_count?.toLocaleString() || 'Unknown'}
                            </Typography>
                          </Grid>
                          <Grid item xs={6}>
                            <Typography variant="body2" color="text.secondary">
                              Bytes Processed:
                            </Typography>
                            <Typography variant="body1">
                              {jobStatus.metadata.bytes_processed ? 
                                `${(jobStatus.metadata.bytes_processed / 1024 / 1024).toFixed(2)} MB` : 
                                'Unknown'}
                            </Typography>
                          </Grid>
                        </Grid>
                      </Grid>
                    )}
                  </Grid>
                </CardContent>
              </Card>
            )}
          </Box>
        );
      
      default:
        return 'Unknown step';
    }
  };

  // Calculate if the current step is completed
  const isStepCompleted = (step) => {
    switch (step) {
      case 0:
        return !!uploadResult;
      case 1:
        return !!datasetId && !!tableId && schema.length > 0;
      case 2:
        return true; // We go to this step only after completing step 1
      case 3:
        return jobStatus && jobStatus.status === 'COMPLETED';
      default:
        return false;
    }
  };

  return (
    <Container maxWidth="lg" sx={{ mt: 4 }}>
      <Paper sx={{ p: 3, mb: 4 }}>
        <Typography variant="h5" component="h1" gutterBottom>
          Source Ingestion
        </Typography>
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
          <Typography variant="body1" color="text.secondary">
            Upload CSV or JSON files to create BigQuery datasets and tables. This is the first phase of the two-phase ingestion process.
          </Typography>

          <FormControlLabel
            control={
              <Switch
                checked={useMockApi}
                onChange={(e) => {
                  const newValue = e.target.checked;
                  setUseMockApi(newValue);
                  sourceIngestionService.setMockMode(newValue);
                }}
                color="primary"
              />
            }
            label={useMockApi ? "Using Mock API" : "Using Live API"}
            sx={{ ml: 2 }}
          />
        </Box>
        
        {/* Mode indicators */}
        <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
          <FormControlLabel
            control={
              <Switch
                checked={useAutodetect}
                onChange={(e) => setUseAutodetect(e.target.checked)}
                color="primary"
              />
            }
            label={useAutodetect ? "Using Schema Autodetection" : "Manual Schema"}
          />
          
          <Box sx={{ display: 'flex', gap: 1 }}>
            <Chip
              label={useMockApi ? "Mock Mode" : "Live Mode"}
              color={useMockApi ? "default" : "primary"}
              variant="outlined"
              size="small"
            />
            <Chip
              label={useAutodetect ? "Autodetect" : "Manual Schema"}
              color={useAutodetect ? "success" : "info"}
              variant="outlined"
              size="small"
            />
          </Box>
        </Box>
        
        <Stepper activeStep={activeStep} sx={{ mb: 4 }}>
          {steps.map((label, index) => {
            const stepProps = {};
            const labelProps = {};
            
            // Add completed prop if step is completed
            if (index < activeStep && isStepCompleted(index)) {
              stepProps.completed = true;
            }
            
            return (
              <Step key={label} {...stepProps}>
                <StepLabel {...labelProps}>{label}</StepLabel>
              </Step>
            );
          })}
        </Stepper>
        
        {/* Error and success alerts */}
        {error && (
          <Alert severity="error" sx={{ mb: 2 }}>
            {error}
          </Alert>
        )}
        
        {success && (
          <Alert severity="success" sx={{ mb: 2 }}>
            {success}
          </Alert>
        )}
        
        {/* Step content */}
        {getStepContent(activeStep)}
        
        {/* Actions */}
        <Box sx={{ display: 'flex', flexDirection: 'row', pt: 2 }}>
          <Button
            color="inherit"
            onClick={handleReset}
            sx={{ mr: 1 }}
            startIcon={<RestartAltIcon />}
          >
            Start Over
          </Button>
          <Box sx={{ flex: '1 1 auto' }} />
          
          {activeStep > 0 && (
            <Button
              onClick={handleBack}
              sx={{ mr: 1 }}
              disabled={loading}
            >
              Back
            </Button>
          )}
          
          <Button 
            variant="contained" 
            onClick={handleNext}
            disabled={
              loading || 
              (activeStep === 0 && !file) ||
              (activeStep === 1 && (!datasetId || !tableId)) ||
              (activeStep === 3 && jobStatus && jobStatus.status === 'COMPLETED')
            }
            startIcon={loading ? <CircularProgress size={20} /> : null}
          >
            {activeStep === steps.length - 1 ? 'Load Data' : 'Next'}
          </Button>
        </Box>
      </Paper>
    </Container>
  );
};

export default SourceIngestion;
