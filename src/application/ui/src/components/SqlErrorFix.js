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

import React, { useState, useEffect } from 'react';
import {
  Box,
  Paper,
  Typography,
  Button,
  CircularProgress,
  Alert,
  Divider,
  Card,
  CardContent,
  IconButton,
  Tooltip,
  Stack,
  Collapse,
  Fade
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import EditIcon from '@mui/icons-material/Edit';
import VisibilityIcon from '@mui/icons-material/Visibility';
import SyncIcon from '@mui/icons-material/Sync';
import { sourceIngestionService } from '../services/sourceIngestionService';

/**
 * Component for handling SQL errors with AI-powered fixes
 * Displays diffs between original and fixed SQL and allows applying fixes
 */
const SqlErrorFix = ({ 
  originalSql,
  currentSql,
  errorMessage,
  onSqlFixed,
  attemptNumber = 1,
  maxAttempts = 3,
  disabled = false
}) => {
  // States
  const [isGeneratingSqlFix, setIsGeneratingSqlFix] = useState(false);
  const [suggestedSql, setSuggestedSql] = useState(null);
  const [sqlFixError, setSqlFixError] = useState(null);
  const [sqlDiff, setSqlDiff] = useState(null);
  const [isValidating, setIsValidating] = useState(false);
  const [validationResult, setValidationResult] = useState(null);
  const [showDiff, setShowDiff] = useState(true); // Start with diff visible
  
  // Clear states when inputs change
  useEffect(() => {
    setSuggestedSql(null);
    setSqlFixError(null);
    setSqlDiff(null);
    setValidationResult(null);
  }, [originalSql, currentSql, errorMessage, attemptNumber]);
  
  // Handle generating SQL fix
  const handleGenerateFix = async () => {
    if (!currentSql || !errorMessage) {
      setSqlFixError("Missing SQL or error message to generate a fix");
      return;
    }
    
    setIsGeneratingSqlFix(true);
    setSqlFixError(null);
    
    try {
      // Ensure we have the original SQL to reference
      const origSql = originalSql || currentSql;
      
      const result = await sourceIngestionService.generateSqlFix(
        origSql,
        currentSql,
        errorMessage,
        attemptNumber
      );
      
      if (result) {
        setSuggestedSql(result.suggested_sql);
        setSqlDiff(result.diff);
        
        // If the fix has a validation result already, store it
        if (result.valid !== undefined) {
          setValidationResult({
            valid: result.valid,
            message: result.message,
            error: result.error
          });
        }
      } else {
        setSqlFixError("No fix suggestion received from the AI");
      }
    } catch (error) {
      console.error("Error generating SQL fix:", error);
      setSqlFixError(error.message || "Failed to generate SQL fix");
    } finally {
      setIsGeneratingSqlFix(false);
    }
  };
  
  // Handle applying the fix
  const handleApplyFix = async () => {
    if (!suggestedSql) {
      setSqlFixError("No SQL fix to apply");
      return;
    }
    
    setIsValidating(true);
    
    try {
      const result = await sourceIngestionService.validateSqlFix(suggestedSql, attemptNumber);
      
      setValidationResult(result);
      
      if (result.valid) {
        // If valid, inform the parent component
        if (onSqlFixed) {
          onSqlFixed(suggestedSql, result);
        }
      }
    } catch (error) {
      console.error("Error validating SQL fix:", error);
      setValidationResult({
        valid: false,
        error: error.message || "Failed to validate SQL fix"
      });
    } finally {
      setIsValidating(false);
    }
  };
  
  // Helper to copy text to clipboard
  const copyToClipboard = (text) => {
    navigator.clipboard.writeText(text);
  };
  
  // Render the diff view
  const renderDiff = () => {
    if (!sqlDiff) return null;
    
    // Process the diff to style additions and removals
    const diffLines = sqlDiff.split('\n');
    
    // Skip the first few lines that contain file names and such
    const contentStartIndex = diffLines.findIndex(line => line.startsWith('@@'));
    const contentDiffLines = diffLines.slice(contentStartIndex > 0 ? contentStartIndex + 1 : 0);
    
    return (
      <Paper variant="outlined" sx={{ p: 2, overflowX: 'auto', bgcolor: '#f8f9fa', mb: 2 }}>
        <Typography variant="subtitle2" sx={{ mb: 1, fontFamily: 'monospace' }}>
          SQL Fix Diff:
        </Typography>
        <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
          {contentDiffLines.map((line, index) => {
            let style = {};
            
            if (line.startsWith('+')) {
              style.backgroundColor = 'rgba(0, 255, 0, 0.1)';
              style.color = '#1e8e3e';
            } else if (line.startsWith('-')) {
              style.backgroundColor = 'rgba(255, 0, 0, 0.1)';
              style.color = '#d32f2f';
            }
            
            return (
              <div key={index} style={style}>
                {line}
              </div>
            );
          })}
        </pre>
      </Paper>
    );
  };
  
  // Render the fixed SQL
  const renderFixedSql = () => {
    if (!suggestedSql) return null;
    
    return (
      <Paper variant="outlined" sx={{ p: 2, overflowX: 'auto', bgcolor: '#f8f9fa', mb: 2 }}>
        <Stack direction="row" justifyContent="space-between" alignItems="center" sx={{ mb: 1 }}>
          <Typography variant="subtitle2" sx={{ fontFamily: 'monospace' }}>
            Fixed SQL:
          </Typography>
          <Tooltip title="Copy SQL to clipboard">
            <IconButton onClick={() => copyToClipboard(suggestedSql)} size="small">
              <ContentCopyIcon fontSize="small" />
            </IconButton>
          </Tooltip>
        </Stack>
        <pre style={{ margin: 0, whiteSpace: 'pre-wrap', wordWrap: 'break-word' }}>
          <code>{suggestedSql}</code>
        </pre>
      </Paper>
    );
  };
  
  // Render validation result
  const renderValidationResult = () => {
    if (!validationResult) return null;
    
    const { valid, message, error } = validationResult;
    
    return (
      <Alert 
        severity={valid ? "success" : "error"} 
        sx={{ mt: 2 }}
        icon={valid ? <CheckCircleIcon /> : <ErrorIcon />}
      >
        {valid 
          ? (message || "SQL fix is valid! The issue has been resolved.") 
          : (error || "SQL fix is not valid. Further fixes may be needed.")}
      </Alert>
    );
  };
  
  return (
    <Box sx={{ mt: 2, mb: 3 }}>
      <Card variant="outlined">
        <CardContent>
          <Typography variant="h6" gutterBottom>
            AI-Powered SQL Error Fix
          </Typography>
          
          {errorMessage && (
            <Alert severity="error" sx={{ mb: 2 }}>
              <Typography variant="caption" sx={{ display: 'block', fontWeight: 'bold', mb: 0.5 }}>
                SQL Error:
              </Typography>
              {errorMessage}
            </Alert>
          )}
          
          <Stack direction="row" spacing={2} sx={{ mb: 2 }}>
            <Button
              variant="contained"
              color="secondary"
              onClick={handleGenerateFix}
              disabled={isGeneratingSqlFix || !errorMessage || !currentSql || disabled || attemptNumber > maxAttempts}
              startIcon={isGeneratingSqlFix ? <CircularProgress size={20} color="inherit" /> : <EditIcon />}
            >
              {isGeneratingSqlFix 
                ? 'Generating Fix...' 
                : (attemptNumber > 1 ? `Generate Fix (Attempt ${attemptNumber}/${maxAttempts})` : 'Fix with AI')}
            </Button>
            
            {suggestedSql && (
              <Button
                variant="outlined"
                color="primary"
                onClick={handleApplyFix}
                disabled={isValidating || !suggestedSql || disabled || (validationResult && validationResult.valid)}
                startIcon={isValidating ? <CircularProgress size={20} color="inherit" /> : <PlayArrowIcon />}
              >
                {isValidating ? 'Validating...' : 'Apply & Validate Fix'}
              </Button>
            )}
            
            {suggestedSql && (
              <Button
                variant="text"
                onClick={() => setShowDiff(!showDiff)}
                startIcon={<VisibilityIcon />}
                size="small"
              >
                {showDiff ? 'Hide Diff' : 'Show Diff'}
              </Button>
            )}
          </Stack>
          
          {sqlFixError && (
            <Alert severity="error" sx={{ mb: 2 }}>
              {sqlFixError}
            </Alert>
          )}
          
          <Collapse in={showDiff && !!sqlDiff}>
            {renderDiff()}
          </Collapse>
          
          <Fade in={!!suggestedSql}>
            {suggestedSql ? renderFixedSql() : <Box />}
          </Fade>
          
          {renderValidationResult()}
          
          {attemptNumber > maxAttempts && (
            <Alert severity="warning" sx={{ mt: 2 }}>
              Maximum fix attempts reached ({maxAttempts}). Consider manually editing the SQL or contact support.
            </Alert>
          )}
        </CardContent>
      </Card>
    </Box>
  );
};

export default SqlErrorFix;
