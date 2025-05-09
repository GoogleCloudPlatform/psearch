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
import { GEN_AI_URL } from '../config';
import productSchema from '../products.json'; // Updated import path for moved file
import config from '../config'; // Import config to get projectId

/**
 * Service for handling interactions with the Gen AI API
 */

// Function to handle conversational search
export const getConversationalSearch = async (query, conversationHistory, productContext, maxResults = 5) => {
    try {
        const response = await axios.post(`${GEN_AI_URL}/conversational-search`, {
            query,
            conversation_history: conversationHistory,
            product_context: productContext,
            max_results: maxResults
        }, {
            timeout: 25000, // 25 second timeout
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        // Validate the response data
        if (!response.data || !response.data.answer) {
            throw new Error('Incomplete data received from AI service');
        }

        return response.data;
    } catch (error) {
        console.error('Error in conversational search:', error);

        // Enhance error message based on error type
        if (error.code === 'ECONNABORTED') {
            throw new Error('Connection timeout. The search request took too long to process.');
        } else if (error.response) {
            // Server responded with a non-2xx status code
            const status = error.response.status;
            const message = error.response.data?.message || 'Unknown server error';
            throw new Error(`Server error (${status}): ${message}`);
        } else if (error.request) {
            // Request was made but no response received
            throw new Error('No response received from server. Please check your connection.');
        }

        throw error;
    }
};

// Function to handle product enrichment
export const getProductEnrichment = async (productId, productData, fieldsToEnrich) => {
    try {
        // Set a timeout for the API request to prevent hanging UI
        const response = await axios.post(`${GEN_AI_URL}/enrichment`, {
            product_id: productId,
            product_data: productData,
            fields_to_enrich: fieldsToEnrich
        }, {
            timeout: 25000, // 25 second timeout - shorter than the UI timeout
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        // Debug logging of the entire response
        console.log('API Response from enrichment service:', response.data);
        console.log('Fields to enrich requested:', fieldsToEnrich);

        if (!response.data) {
            console.error('No data received from enrichment API');
            throw new Error('No data received from AI service');
        }

        if (!response.data.enriched_fields) {
            console.error('No enriched_fields found in response:', response.data);
            throw new Error('Response missing enriched_fields property');
        }

        // Log specific fields for debugging
        console.log('Enriched fields received:', Object.keys(response.data.enriched_fields));

        // Detailed validation with specific error messages
        if (fieldsToEnrich.includes('technical_specs') && !response.data.enriched_fields.technical_specs) {
            console.error('Missing technical_specs in enriched_fields:', response.data.enriched_fields);
            throw new Error('Technical specifications data missing from AI response');
        }

        if (fieldsToEnrich.includes('description') && !response.data.enriched_fields.description) {
            console.error('Missing description in enriched_fields:', response.data.enriched_fields);
            throw new Error('Description data missing from AI response');
        }

        return response.data;
    } catch (error) {
        console.error('Error in product enrichment:', error);
        // Enhance error message based on error type
        if (error.code === 'ECONNABORTED') {
            throw new Error('Connection timeout. The server took too long to respond.');
        } else if (error.response) {
            // Server responded with a non-2xx status code
            const status = error.response.status;
            const message = error.response.data?.message || 'Unknown server error';
            throw new Error(`Server error (${status}): ${message}`);
        } else if (error.request) {
            // Request was made but no response received
            throw new Error('No response received from server. Please check your connection.');
        }
        // For other errors, pass through the original error
        throw error;
    }
};

// Function to generate marketing content
export const generateMarketingContent = async (
    productId,
    productData,
    contentType,
    tone = 'professional',
    targetAudience = null,
    maxLength = 500
) => {
    try {
        const response = await axios.post(`${GEN_AI_URL}/marketing`, {
            product_id: productId,
            product_data: productData,
            content_type: contentType,
            tone,
            target_audience: targetAudience,
            max_length: maxLength
        }, {
            timeout: 25000, // 25 second timeout
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        // Validate response data
        if (!response.data || !response.data.content) {
            throw new Error('Incomplete data received from marketing AI service');
        }

        return response.data;
    } catch (error) {
        console.error('Error generating marketing content:', error);

        // Enhance error message based on error type
        if (error.code === 'ECONNABORTED') {
            throw new Error('Connection timeout. Marketing content generation took too long.');
        } else if (error.response) {
            // Server responded with a non-2xx status code
            const status = error.response.status;
            const message = error.response.data?.message || 'Unknown server error';
            throw new Error(`Server error (${status}): ${message}`);
        } else if (error.request) {
            // Request was made but no response received
            throw new Error('No response received from server. Please check your connection.');
        }

        throw error;
    }
};

// Function to generate enhanced images using Gemini
export const generateEnhancedImage = async (payload) => {
    // payload should contain: { product_id, product_data, image_base64, background_prompt, person_description, style }
    try {
        const response = await axios.post(`${GEN_AI_URL}/generate-enhanced-image`, payload, {
            timeout: 40000, // 40 second timeout (image generation takes longer)
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
        });

        // Validate response data
        if (!response.data || !response.data.generated_image_base64) {
            throw new Error('Incomplete image data received from AI service');
        }

        return response.data;
    } catch (error) {
        console.error('Error generating enhanced image:', error);

        // Enhance error message based on error type
        if (error.code === 'ECONNABORTED') {
            throw new Error('Connection timeout. Image generation took too long to complete.');
        } else if (error.response) {
            // Server responded with a non-2xx status code
            const status = error.response.status;
            const message = error.response.data?.message || 'Unknown server error';
            throw new Error(`Server error (${status}): ${message}`);
        } else if (error.request) {
            // Request was made but no response received
            throw new Error('No response received from server. Please check your connection.');
        }

        throw error;
    }
};

/**
 * Generates a BigQuery SQL script to transform data from a source table
 * to the target product schema.
 * @param {string} sourceTableId Full BigQuery source table ID (e.g., project.dataset.table)
 * @param {string} destinationTableId Full BigQuery destination table ID (e.g., project.dataset.table)
 * @returns {Promise<string>} The generated SQL script.
 */
export const generateTransformationSql = async (sourceTableId, destinationTableId) => {
  try {
    console.log(`Requesting SQL generation from ${sourceTableId} to ${destinationTableId}`);
    console.log("Using destination schema defined in products.json"); // Avoid logging the whole schema

    // Ensure projectId is available
    const projectId = config.projectId;
    if (!projectId || projectId === 'your-gcp-project-id') {
        console.error("Project ID is not configured correctly in src/config.js");
        throw new Error("Project ID is not configured. Please set REACT_APP_GCP_PROJECT_ID.");
    }

    // Adjust table IDs if they don't already include the project ID
    const fullSourceTableId = sourceTableId.includes('.') ? sourceTableId : `${projectId}.${sourceTableId}`;
    const fullDestinationTableId = destinationTableId.includes('.') ? destinationTableId : `${projectId}.${destinationTableId}`;


    const response = await axios.post(`${GEN_AI_URL}/generate-sql`, {
      source_table: fullSourceTableId,
      destination_table: fullDestinationTableId,
      destination_schema: productSchema // Send the imported JSON schema
    }, {
      timeout: 60000, // 60 second timeout, as SQL generation might take longer
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });

    // Validate response data
    if (!response.data || !response.data.sql_script) {
      console.error("Invalid response from /generate-sql:", response.data);
      throw new Error('Incomplete or invalid SQL script data received from AI service');
    }

    console.log("Generated SQL script received.");
    
    // Process the SQL script to ensure it's properly formatted for display
    let sqlScript = response.data.sql_script.trim();
    
    // Remove any markdown code formatting that might have slipped through
    if (sqlScript.startsWith("```") && sqlScript.endsWith("```")) {
      sqlScript = sqlScript.substring(3, sqlScript.length - 3).trim();
      // Also remove any language identifier like "sql"
      if (sqlScript.startsWith("sql") || sqlScript.startsWith("SQL")) {
        sqlScript = sqlScript.substring(3).trim();
      }
    }
    
    // Fix double backticks issue in table references
    // This regex matches ``table_name`` pattern and replaces with `table_name`
    sqlScript = sqlScript.replace(/``([^`]+)``/g, '`$1`');
    
    // Fix any other potential quoting issues
    // 1. Remove extra spaces between backticks and table names
    sqlScript = sqlScript.replace(/`\s+/g, '`');
    sqlScript = sqlScript.replace(/\s+`/g, '`');
    
    // 2. Ensure project.dataset.table has proper quoting
    const tableRefPattern = /`?([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)\.([a-zA-Z0-9_-]+)`?/g;
    sqlScript = sqlScript.replace(tableRefPattern, function(match, project, dataset, table) {
      // Properly quote the full reference
      if (!match.startsWith('`') || !match.endsWith('`')) {
        return `\`${project}.${dataset}.${table}\``;
      }
      return match;
    });
    
    console.log("Processed SQL script ready for display:", sqlScript.substring(0, 100) + "...");
    return sqlScript; // Return the cleaned SQL string

  } catch (error) {
    console.error('Error generating transformation SQL:', error);

    // Enhance error message based on error type
    if (error.code === 'ECONNABORTED') {
      throw new Error('Connection timeout. SQL generation took too long to complete.');
    } else if (error.response) {
      const status = error.response.status;
      // Attempt to get detailed error message from backend
      const message = error.response.data?.detail || error.response.data?.message || 'Unknown server error';
      console.error(`Server error (${status}): ${message}`, error.response.data);
      // Provide a user-friendly message, potentially including details if available
      let userMessage = `Server error (${status}) while generating SQL.`;
      if (typeof message === 'string' && message.length < 100) { // Avoid overly long technical details
          userMessage += ` Details: ${message}`;
      }
      throw new Error(userMessage);
    } else if (error.request) {
      throw new Error('No response received from GenAI server for SQL generation. Please check connection and if the service is running.');
    }

    // Rethrow other errors, including the projectId configuration error
    throw error;
  }
};

/**
 * Calls the backend GenAI service to refine a SQL script based on an error message.
 * @param {string} sqlScript The current SQL script that needs refinement.
 * @param {string} errorMessage The error message received from the dry run.
 * @returns {Promise<string>} The refined SQL script.
 */
export const refineTransformationSql = async (sqlScript, errorMessage) => {
  console.log("Attempting SQL refinement based on error:", errorMessage);
  
  try {
    const response = await axios.post(`${GEN_AI_URL}/refine-sql`, {
      sql_script: sqlScript,
      error_message: errorMessage
      // Optionally add source/destination context if needed by backend
    }, {
      timeout: 60000, // 60 second timeout for refinement
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });

    if (response.data && response.data.refined_sql_script) {
      console.log("SQL refinement successful.");
      return response.data.refined_sql_script;
    } else {
      console.error("Invalid response from SQL refinement service:", response.data);
      throw new Error("Invalid or incomplete response from SQL refinement service.");
    }
  } catch (error) {
    console.error('Error refining transformation SQL:', error);
    const message = error.response?.data?.detail || error.message || "An unknown error occurred during SQL refinement.";
    throw new Error(`SQL Refinement Failed: ${message}`);
  }
};

/**
 * Uses the SQL fix endpoint in the Gen AI API to fix SQL errors.
 * 
 * @param {string} originalSql The original SQL that started the process
 * @param {string} currentSql The current SQL version (may be previously refined)
 * @param {string} errorMessage The error message from BigQuery
 * @param {number} attemptNumber The current attempt number (for tracking)
 * @returns {Promise<Object>} Object containing the suggested SQL fix, diff, and validation status
 */
export const generateSqlFix = async (originalSql, currentSql, errorMessage, attemptNumber = 1) => {
  console.log(`Requesting SQL fix for attempt #${attemptNumber}`, {errorLength: errorMessage.length});
  
  try {
    // Use the GEN_AI_URL instead of ingestionSourceUrl
    const response = await axios.post(`${GEN_AI_URL}/sql/fix`, {
      original_sql: originalSql,
      current_sql: currentSql,
      error_message: errorMessage,
      attempt_number: attemptNumber
    }, {
      timeout: 60000,  // 60 second timeout
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });
    
    // Validate response data
    if (!response.data) {
      throw new Error("No data received from SQL fix service");
    }
    
    console.log("SQL fix generated successfully", {
      success: response.data.success,
      diffLength: response.data.diff?.length || 0
    });
    
    return response.data;
  } catch (error) {
    console.error("Error generating SQL fix:", error);
    
    // Enhanced error handling
    let errorMessage = "Failed to generate SQL fix: ";
    
    if (error.response) {
      const status = error.response.status;
      const detail = error.response.data?.detail || "Unknown server error";
      errorMessage += `Server error (${status}): ${detail}`;
    } else if (error.request) {
      errorMessage += "No response received from server. Check if the Gen AI API is running.";
    } else {
      errorMessage += error.message || "Unknown error";
    }
    
    throw new Error(errorMessage);
  }
};

/**
 * Validates a fixed SQL query without executing it using the Gen AI API.
 * 
 * @param {string} sqlToApply The SQL to validate
 * @param {number} attemptNumber The current attempt number (for tracking)
 * @returns {Promise<Object>} Object with validation result
 */
export const validateSqlFix = async (sqlToApply, attemptNumber = 1) => {
  console.log(`Validating SQL fix for attempt #${attemptNumber}`);
  
  try {
    // Use the GEN_AI_URL instead of ingestionSourceUrl
    const response = await axios.post(`${GEN_AI_URL}/sql/validate`, {
      sql_script: sqlToApply,
      timeout_seconds: 30
    }, {
      timeout: 35000,  // 35 second timeout
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });
    
    // Validation response should match the expected structure
    if (!response.data) {
      throw new Error("No data received from SQL validation service");
    }
    
    console.log("SQL validation completed:", {
      valid: response.data.valid,
      hasError: !!response.data.error
    });
    
    return response.data;
  } catch (error) {
    console.error("Error validating SQL fix:", error);
    
    // Enhanced error handling
    let errorMessage = "Failed to validate SQL fix: ";
    
    if (error.response) {
      const status = error.response.status;
      const detail = error.response.data?.detail || "Unknown server error";
      errorMessage += `Server error (${status}): ${detail}`;
    } else if (error.request) {
      errorMessage += "No response received from server. Check if the Gen AI API is running.";
    } else {
      errorMessage += error.message || "Unknown error";
    }
    
    throw new Error(errorMessage);
  }
};

/**
 * Analyzes differences between original and fixed SQL scripts.
 * 
 * @param {string} originalSql The original SQL with errors
 * @param {string} fixedSql The fixed SQL script
 * @returns {Promise<Object>} Object with analysis result
 */
export const analyzeSqlDifferences = async (originalSql, fixedSql) => {
  console.log(`Analyzing SQL differences between scripts`);
  
  try {
    const response = await axios.post(`${GEN_AI_URL}/sql/analyze`, {
      original_sql: originalSql,
      fixed_sql: fixedSql
    }, {
      timeout: 30000,  // 30 second timeout
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      }
    });
    
    if (!response.data) {
      throw new Error("No data received from SQL analysis service");
    }
    
    console.log("SQL analysis completed:", {
      changesCount: response.data.changes?.length || 0
    });
    
    return response.data;
  } catch (error) {
    console.error("Error analyzing SQL differences:", error);
    
    let errorMessage = "Failed to analyze SQL differences: ";
    
    if (error.response) {
      const status = error.response.status;
      const detail = error.response.data?.detail || "Unknown server error";
      errorMessage += `Server error (${status}): ${detail}`;
    } else if (error.request) {
      errorMessage += "No response received from server. Check if the Gen AI API is running.";
    } else {
      errorMessage += error.message || "Unknown error";
    }
    
    throw new Error(errorMessage);
  }
};

// Old function to generate images (removed as it's replaced by generateEnhancedImage)
/*
export const generateProductImage = async (
    productId,
    productData,
    prompt = null,
    imageType = 'lifestyle',
    style = 'photorealistic'
) => {
    try {
        const response = await axios.post(`${GEN_AI_URL}/imagen`, {
            product_id: productId,
            product_data: productData,
            prompt,
            image_type: imageType,
            style
        });
        return response.data;
    } catch (error) {
        console.error('Error generating image:', error);
        throw error;
    }
};
*/
