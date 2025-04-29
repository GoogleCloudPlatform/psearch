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

// In production, this will be provided by the environment variable at build time
// In development, we use the provided URL for testing
const apiUrl = process.env.REACT_APP_API_URL || 'http://localhost:8080';
const genAiUrl = process.env.REACT_APP_GEN_AI_URL || 'http://localhost:8080';
const ingestionSourceUrl = process.env.REACT_APP_INGESTION_SOURCE_API_URL || 'http://localhost:8082';

const config = {
  // Store the base API URLs
  apiUrl: apiUrl,
  genAiUrl: genAiUrl,
  ingestionSourceUrl: ingestionSourceUrl,
  projectId: process.env.REACT_APP_PROJECT_ID || null, // Use null fallback, env var is required
};

// Log the configuration
console.log(`API URL: ${apiUrl}`);
console.log(`Gen AI URL: ${genAiUrl}`);
console.log(`Ingestion Source URL: ${ingestionSourceUrl}`);

export const API_URL = config.apiUrl;
export const GEN_AI_URL = config.genAiUrl;
export const INGESTION_SOURCE_URL = config.ingestionSourceUrl
export default config;
