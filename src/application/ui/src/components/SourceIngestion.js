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
import CodeIcon from '@mui/icons-material/Code'; // Moved from @mui/material
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import SqlErrorFix from './SqlErrorFix'; // Import our new component
import TableChartIcon from '@mui/icons-material/TableChart';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import VisibilityIcon from '@mui/icons-material/Visibility';
import OpenInNewIcon from '@mui/icons-material/OpenInNew'; // For BigQuery Studio link
import InfoIcon from '@mui/icons-material/Info';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import config from '../config';
import { sourceIngestionService } from '../services/sourceIngestionService';
import { generateTransformationSql, refineTransformationSql } from '../services/genAiService'; // Added GenAI service imports

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
  const [isLoadDataRunning, setIsLoadDataRunning] = useState(false); // Track load operation status

  // Manual Table Creation state (for Step 2 when autodetect is off)
  const [isTableCreationLoading, setIsTableCreationLoading] = useState(false);
  const [tableCreationError, setTableCreationError] = useState(null);
  const [isTableCreated, setIsTableCreated] = useState(false); // Tracks success of manual creation

  // GenAI SQL Generation state
  const [isGeneratingSql, setIsGeneratingSql] = useState(false);
  const [generatedSql, setGeneratedSql] = useState(null);
  const [sqlGenerationError, setSqlGenerationError] = useState(null);

  // Dry Run & Refine SQL state
  const [isDryRunning, setIsDryRunning] = useState(false);
  const [dryRunError, setDryRunError] = useState(null);
  const [dryRunSuccess, setDryRunSuccess] = useState(false);
  const [isRefiningSql, setIsRefiningSql] = useState(false);
  const [sqlFixAttemptNumber, setSqlFixAttemptNumber] = useState(1); // Add state for SQL fix attempt tracking
  // const [generationPrompt, setGenerationPrompt] = useState(''); // Add if prompt becomes dynamic/editable later
  
  // Refs
  const fileInputRef = useRef(null);
  
  // Define steps - Updated to 6 steps
  const steps = [
    'Upload Data File',        // Index 0
    'Configure Schema',      // Index 1
    'Create BigQuery Table', // Index 2
    'Load Data to Source',   // Index 3 (Label updated)
    'Generate SQL',          // Index 4 (New Step 5)
    'Dry Run & Refine SQL'   // Index 5 (New Step 6)
  ];

  // Helper to detect if a file might be a schema definition file
  const detectSchemaFile = (file) => {
    if (!file) return false;
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
      setSuccess(null);
      setUploadResult(null); // Reset upload result on new file selection
      setJobStatus(null); // Reset job status
      setGeneratedSql(null); // Reset SQL
      setDryRunSuccess(false);
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
    setSuccess(null);
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
      setSchema(result.detected_schema.schema_fields || []); // Ensure schema is an array
      setSuccess(`File ${file.name} uploaded successfully! File ID: ${result.file_id}`);
      
      // Set fixed dataset ID and generate default table ID
      const fixedDatasetId = 'psearch_raw'; // Always use psearch_raw
      const defaultTableId = file.name.split('.')[0].replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase();
      
      setDatasetId(fixedDatasetId);
      setTableId(defaultTableId);

      // Automatically move to the next step after successful upload
      setActiveStep((prevStep) => prevStep + 1); 
      
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
      setUploadProgress(0); // Reset progress after completion/error
    }
  };

  // Handle dataset creation (Not typically needed as we use a fixed 'psearch_raw' dataset)
  // const handleCreateDataset = async () => { ... };

  // Handle table creation - Updated for manual trigger and state
  const handleCreateTable = async () => {
    if (!tableId) {
      setTableCreationError('Please enter a table ID.'); // Use dedicated error state
      return;
    }
    
    setIsTableCreationLoading(true);
    setTableCreationError(null); // Clear previous table creation errors
    setError(null); // Clear general errors
    setSuccess(null); // Clear general success messages
    setIsTableCreated(false); // Reset success flag

    try {
      // Format schema field objects for API
      const formattedSchema = schema.map(field => ({
        name: field.name,
        type: field.type,
        mode: field.mode || "NULLABLE",
        description: field.description || "" // Ensure description is always a string
      }));
      
      const tableRequest = {
        dataset_id: datasetId,
        table_id: tableId,
        schema: formattedSchema,
        description: `Table for ${file.name} created by PSearch Source Ingestion`
      };
      
      console.log("Creating table with request:", tableRequest); // Debug log
      const result = await sourceIngestionService.createTable(tableRequest);
      console.log("Table creation result:", result); // Debug log

      // Use dedicated success state/flag instead of general success message
      setIsTableCreated(true);
      setSuccess(result.message || `Table ${datasetId}.${tableId} created successfully!`); 
      // Automatically move to the next step after successful table creation
      setActiveStep((prevStep) => prevStep + 1); 

    } catch (error) {
      console.error('Error creating table:', error);
      setIsTableCreated(false); // Ensure flag is false on error
      // Enhanced error debugging using dedicated state
      let errorMessage = 'Failed to create table. Please try again.';
      
      if (error.response) {
        console.log('Error response status:', error.response.status);
        console.log('Error response data:', error.response.data);
        const detail = error.response.data?.detail;
        errorMessage = typeof detail === 'string' ? detail : 
                       (detail ? JSON.stringify(detail) : `Server error: ${error.response.status}`);
      } else if (error.request) {
        console.log('Error request:', error.request);
        errorMessage = 'Network error - no response from server.';
      } else {
        console.log('Error message:', error.message);
        errorMessage = `Request setup error: ${error.message}`;
      }
      
      // Ensure error is always a string
      setTableCreationError(typeof errorMessage === 'object' ? JSON.stringify(errorMessage) : errorMessage);
      setError(`Table Creation Failed: ${errorMessage}`); // Set general error as well
    } finally {
      setIsTableCreationLoading(false);
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
    
    setJobStatus({ status: 'PENDING', message: 'Job submitted, waiting for status...' }); // Initial status
    
    // Create a new polling interval
    const intervalId = setInterval(async () => {
      try {
        console.log("Polling job status for job ID:", jobId); // Debug log
        const status = await sourceIngestionService.getJobStatus(jobId);
        console.log("Received job status:", status); // Debug log
        setJobStatus(status);
        
        // If job is completed or failed, stop polling
        if (status.status === 'COMPLETED' || status.status === 'FAILED') {
          console.log("Job finished, stopping polling for:", jobId); // Debug log
          clearInterval(intervalId);
          setStatusPollingInterval(null);
          if (status.status === 'COMPLETED') {
            setSuccess(status.message || 'Data load completed successfully!');
            // Automatically advance to next step on successful completion
            setActiveStep((prevStep) => prevStep + 1);
          } else { // FAILED
            setError(status.error || 'Data load job failed.');
          }
        }
      } catch (error) {
        console.error('Error polling job status:', error);
        setError('Failed to get job status update.');
        clearInterval(intervalId);
        setStatusPollingInterval(null);
      }
    }, 5000); // Poll every 5 seconds
    
    setStatusPollingInterval(intervalId);
  };

  // State for max bad records (for JSON files)
  const [maxBadRecords, setMaxBadRecords] = useState(0);

  // Handle data loading
  const handleLoadData = async () => {
    setIsLoadDataRunning(true);
    setError(null);
    setSuccess(null);
    setJobStatus(null); // Clear previous status
    
    try {
      // Create common request payload
      const loadRequest = {
        dataset_id: datasetId,
        table_id: tableId,
        source_format: uploadResult.file_type.toUpperCase(),
        write_disposition: "WRITE_TRUNCATE",
        skip_leading_rows: uploadResult.file_type === 'csv' ? 1 : 0, // Use 0 for JSON files
        allow_jagged_rows: uploadResult.file_type === 'csv', // Only relevant for CSV
        allow_quoted_newlines: uploadResult.file_type === 'csv', // Only relevant for CSV
        field_delimiter: ",", // Only relevant for CSV
        max_bad_records: uploadResult.file_type === 'json' ? maxBadRecords : 0 // Apply only for JSON
      };
      
      console.log("Starting data load with request:", loadRequest, "File ID:", uploadResult.file_id); // Debug log

      // Use appropriate API based on autodetect setting
      let result;
      if (useAutodetect) {
        // Use the create_and_load endpoint with schema autodetection, passing file_type
        // This endpoint handles table creation internally if needed.
        console.log("Using createAndLoadTable endpoint");
        result = await sourceIngestionService.createAndLoadTable(
          loadRequest, 
          uploadResult.file_id, 
          uploadResult.file_type // Pass the file_type here
        );
      } else {
        // Use the traditional approach - requires table to exist first.
        // We assume handleCreateTable was successful if we got here in manual mode.
        console.log("Using loadData endpoint");
        result = await sourceIngestionService.loadData(
          loadRequest, 
          uploadResult.file_id,
          uploadResult.file_type // Pass the file_type here too
        );
      }
      
      console.log("Data load job submission result:", result); // Debug log
      setJobId(result.job_id);
      setSuccess(`Data load job started with ID: ${result.job_id}. Monitoring progress...`);
      
      // Start polling for job status updates
      startJobStatusPolling(result.job_id);

    } catch (error) {
      console.error('Error starting data load job:', error);
      
      // Enhanced error debugging
      let errorMessage = 'Failed to start data load job. Please try again.';
      
      if (error.response) {
        console.log('Error response status:', error.response.status);
        console.log('Error response data:', error.response.data);
        const detail = error.response.data?.detail;
        errorMessage = typeof detail === 'string' ? detail : 
                       (detail ? JSON.stringify(detail) : `Server error: ${error.response.status}`);
      } else if (error.request) {
        console.log('Error request:', error.request);
        errorMessage = 'Network error - no response from server.';
      } else {
        console.log('Error message:', error.message);
        errorMessage = `Request setup error: ${error.message}`;
      }
      
      // Ensure error is always a string
      setError(typeof errorMessage === 'object' ? JSON.stringify(errorMessage) : errorMessage);
    } finally {
      setIsLoadDataRunning(false);
    }
  };

  // Handle next step navigation logic - primarily advances the step index
  const handleNext = () => {
    setError(null); // Clear errors on navigation
    setSuccess(null); // Clear success messages on navigation

    // Logic for advancing depends on the current step and conditions
    if (activeStep === 0 && uploadResult) {
      setActiveStep(1); // Move from Upload -> Configure Schema
    } else if (activeStep === 1) {
       if (!datasetId || !tableId) {
         setError("Dataset ID and Table ID are required.");
         return;
       }
       setActiveStep(2); // Move from Configure Schema -> Create Table
    } else if (activeStep === 2) {
      if (useAutodetect) {
          // If using autodetect, skip manual creation and go directly to Load Data
          setActiveStep(3);
          setSuccess("Using schema autodetection - table will be created during data load.");
      } else {
          // If manual mode, table needs to be created first.
          // Button logic should handle this, but double-check state.
          if (isTableCreated) {
              setActiveStep(3); // Move from Create Table -> Load Data
          } else {
              setError("Please create the table first or enable Autodetect.");
          }
      }
    } else if (activeStep === 3 && jobStatus?.status === 'COMPLETED') {
       // Clear SQL/Dry Run state when moving past Load Data
       setGeneratedSql(null);
       setSqlGenerationError(null);
       setDryRunError(null);
       setDryRunSuccess(false);
       setActiveStep(4); // Move from Load Data -> Generate SQL
    } else if (activeStep === 4 && generatedSql && !sqlGenerationError) {
       // Clear dry run state when moving past Generate SQL
       setDryRunError(null);
       setDryRunSuccess(false);
       setActiveStep(5); // Move from Generate SQL -> Dry Run/Refine
    } else if (activeStep === 5 && dryRunSuccess) {
       // End of flow (for now)
       console.log("Workflow complete up to Dry Run success.");
       setSuccess("SQL script validated successfully with Dry Run.");
       // Optionally, stay on this step or implement a final action/summary step
    } else {
       // Fallback or error condition - log if needed
       console.warn("Could not determine next step logic for activeStep:", activeStep, "Job Status:", jobStatus, "SQL:", generatedSql);
       // Potentially set an error message if conditions aren't met
       if (activeStep === 3 && jobStatus?.status !== 'COMPLETED') setError("Data load must complete successfully first.");
       if (activeStep === 4 && !generatedSql) setError("SQL must be generated first.");
       if (activeStep === 5 && !dryRunSuccess) setError("SQL Dry Run must succeed first.");
    }
  };

  // Handle back step
  const handleBack = () => {
    if (activeStep > 0) {
      setActiveStep((prevStep) => prevStep - 1);
    }
    setError(null); // Clear errors when navigating back/forward
    setSuccess(null); // Clear success messages
    // Optionally clear state specific to the step being *left*
    if (activeStep === 5) { // Leaving Dry Run step
        setDryRunError(null);
        setDryRunSuccess(false);
    }
    if (activeStep === 4) { // Leaving Generate SQL step
        setGeneratedSql(null);
        setSqlGenerationError(null);
    }
     if (activeStep === 3) { // Leaving Load Data step
        // Optional: Stop polling if going back? Or let it continue?
        // if (statusPollingInterval) clearInterval(statusPollingInterval);
        // setStatusPollingInterval(null);
        // setJobStatus(null);
    }
    if (activeStep === 2) { // Leaving Create Table step
        setTableCreationError(null);
        // Don't reset isTableCreated, user might go back and forth
    }
  };

  // Handle reset - Updated to reset new state variables
  const handleReset = () => {
    setActiveStep(0);
    setFile(null);
    setUploadResult(null);
    setSchema([]);
    setDatasetId('');
    setTableId('');
    setJobId(null);
    setJobStatus(null);
    setError(null); // General errors
    setSuccess(null); // General success messages
    setUploadProgress(0);
    setIsSchemaFile(false);
    
    // Stop polling if active
    if (statusPollingInterval) {
      clearInterval(statusPollingInterval);
      setStatusPollingInterval(null);
    }

    // Reset manual table creation state
    setIsTableCreationLoading(false);
    setTableCreationError(null);
    setIsTableCreated(false);

    // Reset new state variables for SQL generation, dry run, refine
    setIsGeneratingSql(false);
    setGeneratedSql(null);
    setSqlGenerationError(null);
    // setGenerationPrompt(''); // Reset prompt if state is added
    setIsDryRunning(false);
    setDryRunError(null);
    setDryRunSuccess(false);
    setIsRefiningSql(false);
    setIsLoadDataRunning(false);
    setMaxBadRecords(0);
    
    // Reset file input
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
    
    // Clear localStorage
    localStorage.removeItem('psearch_last_uploaded_file_id');
    localStorage.removeItem('psearch_last_uploaded_file_type');

    console.log("Workflow reset.");
  };

  // Generate a mock schema based on file type (only used in mock mode)
  // const generateMockSchema = (fileType) => { ... };

  // Handle GenAI SQL Generation
  const handleGenerateSql = async () => {
    if (!uploadResult || !datasetId || !tableId) {
      setSqlGenerationError("Missing source table information (file upload result, dataset ID, or table ID).");
      return;
    }

    // Check if projectId is configured
    const projectId = config.projectId;
    if (!projectId || projectId === 'your-gcp-project-id') {
        setSqlGenerationError("Project ID is not configured correctly in src/config.js. Cannot proceed.");
        console.error("Project ID configuration error in handleGenerateSql.");
        setError("Configuration Error: Project ID not set.");
        return;
    }

    setIsGeneratingSql(true);
    setGeneratedSql(null);
    setSqlGenerationError(null);
    setError(null);
    setSuccess(null);

    // Construct table IDs - service handles adding project ID if needed
    const sourceTable = `${datasetId}.${tableId}`;
    const destinationTable = `products_psearch.psearch`; // Target Dataset.Table format

    // Extract field names from the schema to pass to the SQL generator
    const sourceSchemaFields = schema.map(field => field.name);
    console.log("Requesting SQL generation for:", sourceTable, "->", destinationTable);
    console.log("Using source schema fields:", sourceSchemaFields);

    try {
      // The genAiService now handles SQL normalization internally
      const sqlScript = await generateTransformationSql(
        sourceTable,
        destinationTable,
        sourceSchemaFields // Pass the schema field names
      );
      
      console.log("SQL script received:", sqlScript.substring(0, 100) + "...");
      
      // Store the normalized SQL script
      setGeneratedSql(sqlScript);
      setSuccess("SQL script generated successfully!");
      
      // Automatically move to the next step after successful generation
      setActiveStep((prevStep) => prevStep + 1); 

    } catch (error) {
      console.error("SQL Generation failed:", error);
      let message = error.message || "An unknown error occurred during SQL generation.";
      if (error.response?.data?.detail) {
          message = error.response.data.detail;
      }
      setSqlGenerationError(message);
      setError(`SQL Generation Failed: ${message}`);
    } finally {
      setIsGeneratingSql(false);
    }
  };

  // Handle Dry Run SQL
  const handleDryRunSql = async () => {
    if (!generatedSql) {
      setDryRunError("No SQL script generated to perform a dry run.");
      setError("No SQL script generated to perform a dry run.");
      return;
    }

    setIsDryRunning(true);
    setDryRunError(null);
    setDryRunSuccess(false);
    setError(null);
    setSuccess(null);

    try {
      console.log("Attempting dry run for SQL:", generatedSql);
      const result = await sourceIngestionService.dryRunQuery(generatedSql);
      console.log("Dry run result:", result);

      if (result.valid) {
          setDryRunSuccess(true);
          setSuccess(`SQL Dry Run Successful: ${result.message || 'Query syntax is valid.'}`);
          // Automatically move to the next step (conceptually, the end for now)
          // setActiveStep((prevStep) => prevStep + 1); // Uncomment if there's a step 6
      } else {
          // If API returns valid=false but no specific error message, use a generic one
          const errorMessage = result.error || "Invalid query syntax detected during dry run.";
          setDryRunError(errorMessage);
          setError(`SQL Dry Run Failed: ${errorMessage}`);
      }

    } catch (error) {
      console.error("Error during SQL dry run:", error);
      let message = error.message || "An unknown error occurred during the dry run.";
       if (error.response?.data?.detail) {
           message = error.response.data.detail;
       }
      setDryRunError(message);
      setError(`SQL Dry Run Failed: ${message}`); // Update main error state
    } finally {
      setIsDryRunning(false);
    }
  };

  // Handle Refine SQL with AI
  const handleRefineSql = async () => {
    if (!generatedSql || !dryRunError) {
      setSqlGenerationError("Cannot refine SQL without an initial script and a dry run error.");
      setError("Cannot refine: Need SQL script and a dry run error.");
      return;
    }

    // Extract specific field name from error message if possible
    let missingField = null;
    let fieldMatch = /Invalid field reference '([^']+)'/.exec(dryRunError);
    if (fieldMatch && fieldMatch[1]) {
      missingField = fieldMatch[1];
    }

    // Parse error message to show more helpful UI feedback
    let errorType = "syntax error";
    let specificFix = "";
    
    if (missingField) {
      errorType = `missing field '${missingField}'`;
      specificFix = `. Will add default values for ${missingField}`;
    } else if (dryRunError.includes("Invalid syntax near")) {
      errorType = "column reference issue";
    } else if (dryRunError.includes("Column not found")) {
      errorType = "missing column";
    } else if (dryRunError.includes("Unrecognized name")) {
      errorType = "unrecognized field name";
    }

    setIsRefiningSql(true);
    setSqlGenerationError(null); // Clear previous errors
    setError(null);
    setSuccess(`AI is analyzing the ${errorType}${specificFix} and refining the SQL...`);

    try {
      console.log(`Attempting to refine SQL based on error: ${dryRunError}`);
      
      // Always use the real AI refinement service which we've enhanced to handle
      // specific errors like colorFamilies effectively
      const refinedScript = await refineTransformationSql(generatedSql, dryRunError);
      
      // Add additional logging about what changes were made
      console.log("SQL refinement completed. Analyzing changes...");
      
      // Check for specific pattern additions that indicate successful refinement
      let refinementSummary = "";
      if (missingField === "colorFamilies" && 
          refinedScript.includes("Default") && 
          !generatedSql.includes("Default")) {
        refinementSummary = " Added default values for missing colorFamilies field.";
      } else if (refinedScript.includes("IFNULL") && !generatedSql.includes("IFNULL")) {
        refinementSummary = " Added NULL handling for potentially missing fields.";
      }

      setGeneratedSql(refinedScript); // Update the SQL with the refined version
      setDryRunError(null); // Clear the error as we have a new script
      setDryRunSuccess(false); // Reset success state, need new dry run
      setSuccess(`SQL script refined by AI.${refinementSummary} Please review and try Dry Run again.`);

    } catch (error) {
      console.error("Error refining SQL:", error);
      let message = error.message || "An unknown error occurred during SQL refinement.";
      if (error.response?.data?.detail) {
        message = error.response.data.detail;
      }
      setSqlGenerationError(`AI Refinement Failed: ${message}`);
      setError(`AI Refinement Failed: ${message}`);
    } finally {
      setIsRefiningSql(false);
    }
  };

  // --- Schema Editing Functions ---
  const handleSchemaChange = (index, field, value) => {
    const newSchema = [...schema];
    newSchema[index] = { ...newSchema[index], [field]: value };
    setSchema(newSchema);
  };

  const handleAddField = () => {
    setSchema([...schema, { name: `new_field_${schema.length}`, type: 'STRING', mode: 'NULLABLE', description: '' }]);
  };

  const handleRemoveField = (index) => {
    const newSchema = schema.filter((_, i) => i !== index);
    setSchema(newSchema);
  };
  // --- End Schema Editing Functions ---

  // --- Render Helper Functions ---
  const renderSchemaFieldType = (type) => {
    let color = 'default';
    switch (type?.toUpperCase()) {
      case 'STRING': color = 'info'; break;
      case 'INTEGER':
      case 'INT64': color = 'success'; break;
      case 'FLOAT':
      case 'FLOAT64':
      case 'NUMERIC': color = 'secondary'; break;
      case 'BOOLEAN':
      case 'BOOL': color = 'warning'; break;
      case 'TIMESTAMP':
      case 'DATETIME':
      case 'DATE': color = 'primary'; break;
      case 'RECORD':
      case 'STRUCT': color = 'error'; break; // Or another distinct color
      default: color = 'default';
    }
    return <Chip label={type || 'N/A'} color={color} size="small" sx={{ textTransform: 'uppercase' }} />;
  };

  const renderJobStatus = () => {
    if (!jobStatus) return null;

    let severity = "info";
    let statusText = jobStatus.status || "Unknown";
    let message = jobStatus.message || jobStatus.error || "";
    let showProgress = false;

    switch (jobStatus.status) {
      case 'PENDING':
      case 'RUNNING':
        severity = "info";
        showProgress = true;
        break;
      case 'COMPLETED':
        severity = "success";
        break;
      case 'FAILED':
        severity = "error";
        break;
      default:
        severity = "warning"; // Unknown status
    }

    return (
      <Card variant="outlined" sx={{ mt: 2, mb: 2 }}>
        <CardContent>
           <Stack direction="row" spacing={1} alignItems="center" sx={{ mb: 1 }}>
             <Typography variant="subtitle2">Load Job Status:</Typography>
             <Chip 
               label={statusText} 
               color={severity === 'info' ? 'primary' : severity} 
               size="small" 
               icon={jobStatus.status === 'COMPLETED' ? <CheckCircleIcon /> : 
                     jobStatus.status === 'FAILED' ? <ErrorIcon /> : 
                     <CircularProgress size={16} color="inherit" />} 
             />
           </Stack>
           {showProgress && <LinearProgress sx={{ mb: 1 }} />}
           {message && <Typography variant="body2" color={severity === "error" ? "error" : "text.secondary"}>{message}</Typography>}
           {jobId && <Typography variant="caption" display="block" color="text.secondary">Job ID: {jobId}</Typography>}
        </CardContent>
      </Card>
    );
  };
  // --- End Render Helper Functions ---


  // Render current step content
  const getStepContent = (step) => {
    switch (step) {
      // --- Step 0: Upload Data File ---
      case 0:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Upload Data File
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Upload a CSV or JSON file containing your product data. The system will attempt to automatically detect the schema.
            </Typography>
            
            <Alert severity="info" sx={{ mb: 3 }}>
              The uploaded file will be stored temporarily in Google Cloud Storage bucket: <strong>{config.gcsBucket || 'N/A'}</strong>
            </Alert>
            
            <Grid container spacing={2} alignItems="center">
              <Grid item xs={12} md={8}>
                <Button
                  variant="outlined"
                  component="label"
                  startIcon={<FileUploadIcon />}
                  fullWidth
                  disabled={loading}
                >
                  {file ? `Selected: ${file.name}` : 'Select File (.csv or .json)'}
                  <input
                    type="file"
                    hidden
                    accept=".csv,.json"
                    onChange={handleFileSelect}
                    ref={fileInputRef}
                  />
                </Button>
              </Grid>
              <Grid item xs={12} md={4}>
                <Button
                  variant="contained"
                  onClick={handleFileUpload}
                  disabled={!file || loading}
                  startIcon={loading ? <CircularProgress size={20} color="inherit" /> : <CheckCircleIcon />}
                  fullWidth
                >
                  {loading ? 'Uploading...' : 'Upload & Proceed'}
                </Button>
              </Grid>
            </Grid>
             {loading && uploadProgress > 0 && (
               <Box sx={{ width: '100%', mt: 2 }}>
                 <LinearProgress variant="determinate" value={uploadProgress} />
                 <Typography variant="caption" display="block" textAlign="center">{`${Math.round(uploadProgress)}%`}</Typography>
               </Box>
             )}
             {file && isSchemaFile && (
                <Alert severity="warning" sx={{ mt: 2 }}>
                    This looks like a schema definition file (contains 'schema' in name). Are you sure you want to upload this as data? Schema files are usually used in Step 2 if Autodetect is off.
                </Alert>
             )}
          </Box>
        );

      // --- Step 1: Configure Schema ---
      case 1:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Configure Schema & Destination
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Review the detected schema. Define the target BigQuery dataset and table for the source data.
            </Typography>

            <Grid container spacing={3}>
               {/* Destination Settings */}
               <Grid item xs={12}>
                 <Card variant="outlined">
                   <CardContent>
                      <Typography variant="subtitle1" gutterBottom>BigQuery Destination</Typography>
                       <Grid container spacing={2}>
                          <Grid item xs={12} md={6}>
                            <TextField
                              label="Target Dataset ID"
                              value={datasetId}
                              onChange={(e) => setDatasetId(e.target.value)}
                              fullWidth
                              required
                              disabled // Keep dataset fixed to psearch_raw
                              helperText="Source data will land in this dataset (fixed)."
                            />
                          </Grid>
                          <Grid item xs={12} md={6}>
                            <TextField
                              label="Target Table ID"
                              value={tableId}
                              onChange={(e) => setTableId(e.target.value.replace(/[^a-zA-Z0-9_]/g, '_').toLowerCase())} // Basic sanitization
                              fullWidth
                              required
                              helperText="New table name (will be created)."
                            />
                          </Grid>
                       </Grid>
                   </CardContent>
                 </Card>
               </Grid>
            
               {/* Schema Review/Edit */}
               <Grid item xs={12}>
                  <Typography variant="subtitle1" gutterBottom sx={{ mt: 2 }}>Detected Schema</Typography>
                  {schema.length > 0 ? (
                     <TableContainer component={Paper} variant="outlined">
                       <Table size="small">
                         <TableHead>
                           <TableRow>
                             <TableCell>Field Name</TableCell>
                             <TableCell>Type</TableCell>
                             <TableCell>Mode</TableCell>
                             <TableCell>Description</TableCell>
                             {/* Add Actions header if editing is enabled */}
                             {/* <TableCell align="right">Actions</TableCell> */}
                           </TableRow>
                         </TableHead>
                         <TableBody>
                           {schema.map((field, index) => (
                             <TableRow key={index}>
                               <TableCell>{field.name}</TableCell>
                               <TableCell>{renderSchemaFieldType(field.type)}</TableCell>
                               <TableCell>
                                  <Chip label={field.mode || 'NULLABLE'} size="small" variant="outlined" />
                               </TableCell>
                               <TableCell sx={{ maxWidth: 300, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                  <Tooltip title={field.description || 'No description'}>
                                     <span>{field.description || '-'}</span>
                                  </Tooltip>
                               </TableCell>
                               {/* Add editing controls here if needed */}
                               {/* 
                               <TableCell align="right">
                                 <IconButton size="small" onClick={() => handleEditField(index)}><EditIcon fontSize="small" /></IconButton>
                                 <IconButton size="small" onClick={() => handleRemoveField(index)}><DeleteIcon fontSize="small" /></IconButton>
                               </TableCell> 
                               */}
                             </TableRow>
                           ))}
                         </TableBody>
                       </Table>
                     </TableContainer>
                  ) : (
                    <Alert severity="warning">No schema detected or provided.</Alert>
                  )}
                  {/* Add Button to add fields manually if needed */}
                  {/* <Button onClick={handleAddField} startIcon={<AddIcon />} sx={{ mt: 1 }}>Add Field</Button> */}
               </Grid>
            </Grid>
          </Box>
        );

      // --- Step 2: Create BigQuery Table ---
      case 2:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Create BigQuery Table
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Configure how the BigQuery table will be created. Use Autodetect or manually define the schema.
            </Typography>

            <FormControlLabel
                control={<Switch checked={useAutodetect} onChange={(e) => setUseAutodetect(e.target.checked)} />}
                label="Use Schema Autodetection during Load"
                sx={{ mb: 2 }}
            />

            {useAutodetect ? (
              <Alert severity="info" sx={{ mb: 2 }}>
                Schema Autodetection is ON. The table <strong>{datasetId}.{tableId}</strong> will be created automatically by BigQuery during the data loading step based on the source file. Proceed to the next step to start the load.
              </Alert>
            ) : (
              <Box>
                 <Alert severity="warning" sx={{ mb: 2 }}>
                   Schema Autodetection is OFF. You must manually create the table using the schema defined in the previous step before loading data. Ensure the schema is correct.
                 </Alert>
                 <Button
                   variant="contained"
                   onClick={handleCreateTable}
                   disabled={isTableCreationLoading || isTableCreated} // Disable if loading or already created
                   startIcon={isTableCreationLoading ? <CircularProgress size={20} color="inherit" /> : <TableChartIcon />}
                   sx={{ mb: 2 }}
                 >
                   {isTableCreationLoading ? 'Creating Table...' : (isTableCreated ? 'Table Created' : 'Create Table Now')}
                 </Button>
                 {tableCreationError && <Alert severity="error" sx={{ mt: 2 }}>{tableCreationError}</Alert>}
                 {isTableCreated && <Alert severity="success" sx={{ mt: 2 }}>Table {datasetId}.{tableId} created successfully. You can now proceed to load data.</Alert>}
              </Box>
            )}
          </Box>
        );

      // --- Step 3: Load Data to Source ---
      case 3:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Load Data into BigQuery Source Table
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Load the uploaded data from Cloud Storage into the target BigQuery table ({datasetId}.{tableId}). 
              {useAutodetect && " BigQuery will create the table and detect the schema."}
              {!useAutodetect && " The table structure must already exist."}
            </Typography>
            
            {/* Max Bad Records option - show for JSON files only */}
            {uploadResult?.file_type === 'json' && (
              <Box sx={{ mb: 3, p: 2, border: '1px solid', borderColor: 'divider', borderRadius: 1 }}>
                <Typography variant="subtitle2" color="primary" gutterBottom>
                  JSON Error Handling (Optional)
                </Typography>
                <Grid container spacing={2} alignItems="center">
                  <Grid item xs={12} md={8}>
                    <TextField
                      label="Maximum Bad Records Allowed"
                      type="number"
                      value={maxBadRecords}
                      onChange={(e) => setMaxBadRecords(Math.max(0, parseInt(e.target.value) || 0))}
                      InputProps={{ inputProps: { min: 0 } }}
                      fullWidth
                      size="small"
                      helperText="Number of invalid JSON records BigQuery should skip before failing the job (0 = strict, allow no errors)."
                      disabled={isLoadDataRunning || (jobStatus && jobStatus.status !== 'FAILED')} // Disable if running or completed
                    />
                  </Grid>
                  <Grid item xs={12} md={4}>
                    <Stack direction="row" spacing={1}>
                       <Button 
                         variant="outlined" 
                         size="small"
                         color="info"
                         onClick={() => setMaxBadRecords(0)}
                         disabled={isLoadDataRunning || (jobStatus && jobStatus.status !== 'FAILED')}
                       >
                         Set to 0
                       </Button>
                      <Button 
                        variant="outlined" 
                        size="small"
                        color="info"
                        onClick={() => setMaxBadRecords(100)}
                        disabled={isLoadDataRunning || (jobStatus && jobStatus.status !== 'FAILED')}
                      >
                        Allow 100
                      </Button>
                     </Stack>
                  </Grid>
                  <Grid item xs={12}>
                    <Alert severity="info" sx={{ mt: 1 }} icon={<InfoIcon fontSize="inherit" />}>
                      For JSON files, BigQuery expects newline-delimited JSON (NDJSON). If your file isn't strictly NDJSON or has minor formatting issues, allowing some bad records might help the load succeed by skipping problematic lines. Use with caution.
                    </Alert>
                  </Grid>
                </Grid>
              </Box>
            )}
            
            {/* Load Job Configuration Summary */}
            <Card variant="outlined" sx={{ mb: 3 }}>
              <CardContent>
                <Typography variant="subtitle1" gutterBottom>
                  Load Job Configuration Summary
                </Typography>
                <Grid container spacing={2}>
                  <Grid item xs={12} md={6}>
                    <Typography variant="body2" color="text.secondary">Source GCS Path:</Typography>
                    <Typography variant="body1" gutterBottom sx={{ wordBreak: 'break-all' }}>
                       gs://{config.gcsBucket}/{uploadResult?.file_id || 'Unknown'}
                    </Typography>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>Source Format:</Typography>
                    <Typography variant="body1" gutterBottom>
                      {uploadResult?.file_type ? uploadResult.file_type.toUpperCase() : 'Unknown'}
                       {uploadResult?.file_type === 'csv' && ' (Skip Header Row: Yes)'}
                       {uploadResult?.file_type === 'json' && ' (NDJSON expected)'}
                    </Typography>
                  </Grid>
                  <Grid item xs={12} md={6}>
                    <Typography variant="body2" color="text.secondary">Destination Table:</Typography>
                    <Typography variant="body1" gutterBottom>
                      {datasetId}.{tableId}
                    </Typography>
                    
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>Write Disposition:</Typography>
                    <Typography variant="body1" gutterBottom>
                      WRITE_TRUNCATE (Replace existing data)
                    </Typography>
                    
                    {uploadResult?.file_type === 'json' && (
                       <>
                         <Typography variant="body2" color="text.secondary" sx={{ mt: 2 }}>Max Bad Records:</Typography>
                         <Typography variant="body1" gutterBottom>
                           {maxBadRecords}
                         </Typography>
                       </>
                    )}
                  </Grid>
                   <Grid item xs={12}>
                       <FormControlLabel
                           control={<Switch checked={useAutodetect} disabled />} // Display only
                           label="Schema Autodetection Enabled"
                       />
                   </Grid>
                </Grid>
              </CardContent>
            </Card>
            
            {/* Load Data Button */}
            <Button
              variant="contained"
              onClick={handleLoadData}
              disabled={isLoadDataRunning || (jobStatus && jobStatus.status !== 'FAILED' && jobStatus.status !== null) } // Disable if running, completed, or pending (unless failed)
              startIcon={isLoadDataRunning ? <CircularProgress size={20} color="inherit" /> : <PlayArrowIcon />}
              sx={{ mb: 2 }}
            >
              {isLoadDataRunning ? 'Loading Data...' : (jobStatus?.status === 'COMPLETED' ? 'Load Complete' : (jobStatus?.status === 'FAILED' ? 'Retry Load Data' : 'Start Load Data'))}
            </Button>

            {/* Job Status Display */}
            {renderJobStatus()} 
            
          </Box>
        );

      // --- Step 4: Generate SQL ---
      case 4:
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Generate SQL Transformation
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Use GenAI to generate a SQL script to transform the data from the source table ({datasetId}.{tableId}) into the final `products_psearch.psearch` schema.
            </Typography>

            <Button
              variant="contained"
              onClick={handleGenerateSql}
              disabled={isGeneratingSql || generatedSql} // Disable if generating or already generated
              startIcon={isGeneratingSql ? <CircularProgress size={20} color="inherit" /> : <CodeIcon />}
              sx={{ mb: 2 }}
            >
              {isGeneratingSql ? 'Generating SQL...' : (generatedSql ? 'SQL Generated' : 'Generate SQL with AI')}
            </Button>

            {sqlGenerationError && <Alert severity="error" sx={{ mb: 2 }}>{sqlGenerationError}</Alert>}
            
            {generatedSql && (
              <Card variant="outlined">
                <CardContent>
                  <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
                      <Typography variant="subtitle1">Generated SQL Script</Typography>
                      <Tooltip title="Copy SQL to clipboard">
                          <IconButton onClick={() => navigator.clipboard.writeText(generatedSql)} size="small">
                              <ContentCopyIcon fontSize="small" />
                          </IconButton>
                      </Tooltip>
                  </Stack>
                  <Paper variant="outlined" sx={{ p: 2, maxHeight: '400px', overflowY: 'auto', backgroundColor: '#f5f5f5' }}>
                     <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
                       <code>{generatedSql}</code>
                     </pre>
                  </Paper>
                  <Typography variant="caption" color="text.secondary" sx={{ mt: 1, display: 'block' }}>
                    This SQL script transforms data from <strong>{datasetId}.{tableId}</strong> to match the product schema.
                  </Typography>
                </CardContent>
              </Card>
            )}
          </Box>
        );

      // --- Step 5: Dry Run & Refine SQL ---
      case 5:
        // Handler for when SQL is fixed by the component
        const handleSqlFixed = (fixedSql, validationResult) => {
          // Update the generated SQL with the fixed version
          setGeneratedSql(fixedSql);
          
          // Mark the dry run as successful
          setDryRunSuccess(true);
          
          // Clear any existing errors
          setDryRunError(null);
          setSqlGenerationError(null);
          
          // Show success message
          setSuccess("SQL fix successfully validated! The SQL is now ready to use.");
        };
        
        return (
          <Box sx={{ mt: 2 }}>
            <Typography variant="h6" gutterBottom>
              Dry Run & Refine SQL
            </Typography>
            <Typography variant="body2" color="text.secondary" paragraph>
              Validate the generated SQL script with a BigQuery dry run. If errors occur, use AI to refine the script.
            </Typography>

            {generatedSql ? (
              <>
                <Card variant="outlined" sx={{ mb: 2 }}>
                  <CardContent>
                    <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
                      <Typography variant="subtitle1">SQL Script to Validate</Typography>
                      <Tooltip title="Copy SQL to clipboard">
                        <IconButton onClick={() => navigator.clipboard.writeText(generatedSql)} size="small">
                          <ContentCopyIcon fontSize="small" />
                        </IconButton>
                      </Tooltip>
                    </Stack>
                    <Paper variant="outlined" sx={{ p: 2, maxHeight: '300px', overflowY: 'auto', backgroundColor: '#f5f5f5', mb: 2 }}>
                      <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
                        <code>{generatedSql}</code>
                      </pre>
                    </Paper>
                    
                    <Stack direction="row" spacing={1} sx={{ mt: 2 }}>
                      {/* Show dry run button if we haven't run it yet OR if it previously failed and isn't currently running */}
                      {(!dryRunSuccess || dryRunError) && (
                        <Button
                          variant="contained"
                          color="primary"
                          onClick={handleDryRunSql}
                          disabled={isDryRunning}
                          startIcon={isDryRunning ? <CircularProgress size={20} color="inherit" /> : <VisibilityIcon />}
                        >
                          {isDryRunning ? 'Validating...' : (dryRunError ? 'Retry Validation' : 'Validate SQL')}
                        </Button>
                      )}

                      {generatedSql && (
                        <Button
                          variant="outlined"
                          color="secondary"
                          startIcon={<OpenInNewIcon />}
                          onClick={() => {
                            const projectId = config.projectId;
                            const encodedSql = encodeURIComponent(generatedSql);
                            const bqUrl = `https://console.cloud.google.com/bigquery?sq=${encodedSql}&project=${projectId}`;
                            window.open(bqUrl, '_blank');
                          }}
                          disabled={!generatedSql}
                        >
                          Open in BigQuery Studio
                        </Button>
                      )}
                    </Stack>
                    
                    {dryRunSuccess && <Alert severity="success" sx={{ mt: 2 }}>Dry Run Successful! The SQL syntax is valid.</Alert>}
                  </CardContent>
                </Card>
                
                <Alert severity="info" sx={{ mt: 2, mb: 2 }}>
                  Ensure the destination dataset <strong>products_psearch</strong> exists in your BigQuery project: <code>{config.projectId}</code>.
                  If not, please create it before running the query in BigQuery Studio.
                  You can use the command: <code>gcloud bq mk --dataset {config.projectId}:products_psearch</code>
                </Alert>
                
                {/* Only show the SQL Error Fix component if there was an error */}
                {dryRunError && (
                  <SqlErrorFix
                    originalSql={generatedSql} // The first SQL version
                    currentSql={generatedSql}  // The current SQL (may be refined in future attempts)
                    errorMessage={dryRunError}
                    onSqlFixed={handleSqlFixed}
                    attemptNumber={sqlFixAttemptNumber}
                    maxAttempts={3}
                  />
                )}
              </>
            ) : (
              <Alert severity="warning">No SQL script generated yet. Go back to the previous step.</Alert>
            )}
          </Box>
        );

      default:
        return <Typography>Unknown step</Typography>;
    }
  };

  // Main component render
  return (
    <Container maxWidth="lg" sx={{ mt: 4, mb: 4 }}>
      {/* Mock Mode Toggle */}
       <Box sx={{ display: 'flex', justifyContent: 'flex-end', mb: 1 }}>
          <FormControlLabel
             control={<Switch checked={useMockApi} onChange={(e) => { 
                 setUseMockApi(e.target.checked); 
                 sourceIngestionService.setMockMode(e.target.checked);
                 handleReset(); // Reset workflow when changing mode
             }} />}
             label="Use Mock API"
           />
       </Box>

      <Paper elevation={3} sx={{ p: { xs: 2, md: 4 } }}>
        <Typography variant="h4" gutterBottom align="center">
          Source Data Ingestion Workflow
        </Typography>
        
        <Stepper activeStep={activeStep} alternativeLabel sx={{ mb: 4 }}>
          {steps.map((label) => (
            <Step key={label}>
              <StepLabel>{label}</StepLabel>
            </Step>
          ))}
        </Stepper>
        
        {/* Global Error/Success Alerts */}
        {error && <Alert severity="error" sx={{ mb: 2 }}>{error}</Alert>}
        {success && <Alert severity="success" sx={{ mb: 2 }}>{success}</Alert>}

        {/* Step Content Area */}
        <Box sx={{ minHeight: 300 }}> {/* Ensure minimum height for content */}
          {getStepContent(activeStep)}
        </Box>

        {/* Navigation Buttons */}
        <Divider sx={{ mt: 4, mb: 3 }} />
        <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
           <Button
              onClick={handleReset}
              variant="outlined"
              color="warning"
              startIcon={<RestartAltIcon />}
            >
              Reset Workflow
            </Button>
          <Box>
            <Button
              disabled={activeStep === 0}
              onClick={handleBack}
              sx={{ mr: 1 }}
            >
              Back
            </Button>
            <Button
              variant="contained"
              onClick={handleNext}
              // Disable "Next" based on step-specific conditions
              disabled={
                 (activeStep === 0 && !uploadResult) || 
                 (activeStep === 1 && (!datasetId || !tableId)) || 
                 (activeStep === 2 && !useAutodetect && !isTableCreated) ||
                 (activeStep === 3 && jobStatus?.status !== 'COMPLETED') ||
                 (activeStep === 4 && (!generatedSql || sqlGenerationError)) ||
                 (activeStep === 5 && !dryRunSuccess) ||
                 (activeStep === steps.length - 1 && !dryRunSuccess) // Disable on last step unless dry run succeeded
              }
            >
              {activeStep === steps.length - 1 ? 'Finish (Validated)' : 'Next'}
            </Button>
          </Box>
        </Box>
      </Paper>
    </Container>
  );
};

export default SourceIngestion;
