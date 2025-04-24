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
  Button,
  Card,
  CardContent,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControl,
  Grid,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  TextField,
  Typography,
  Alert,
  Snackbar,
  CircularProgress,
  Chip,
} from '@mui/material';
import { LoadingButton } from '@mui/lab';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import { ruleService } from '../services/ruleService';
import config from '../config';

const RULE_TYPES = {
  BOOST: 'boost',
  BURY: 'bury'
};

const CONDITION_TYPES = {
  CATEGORY: 'category',
  BRAND: 'brand',
  PRICE_RANGE: 'price_range',
  PRODUCT_ID: 'product_id'
};

export default function RuleManager() {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(false);
  const [openDialog, setOpenDialog] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [error, setError] = useState(null);
  const [apiHealth, setApiHealth] = useState(null);
  const [currentRule, setCurrentRule] = useState({
    type: RULE_TYPES.BOOST,
    conditionType: CONDITION_TYPES.CATEGORY,
    condition: '',
    score: 1.0,
  });

  const checkApiHealth = async () => {
    try {
      const health = await ruleService.checkHealth();
      setApiHealth(health);
      setError(null);
    } catch (error) {
      setApiHealth(null);
      setError(`API Connection Error: Mock rule service unavailable`);
    }
  };

  const fetchRules = async () => {
    setLoading(true);
    try {
      const rulesList = await ruleService.getRules();
      setRules(rulesList || []);
      setError(null);
    } catch (error) {
      setError('Failed to fetch mock rules. Please try again.');
      console.error('Error fetching rules:', error);
      setRules([]);
    }
    setLoading(false);
  };

  useEffect(() => {
    checkApiHealth();
    fetchRules();
  }, []);

  const handleOpenDialog = (rule = null) => {
    if (rule) {
      setEditingRule(rule);
      setCurrentRule(rule);
    } else {
      setEditingRule(null);
      setCurrentRule({
        type: RULE_TYPES.BOOST,
        conditionType: CONDITION_TYPES.CATEGORY,
        condition: '',
        score: 1.0,
      });
    }
    setOpenDialog(true);
  };

  const handleCloseDialog = () => {
    setOpenDialog(false);
    setEditingRule(null);
    setError(null);
  };

  const validateRule = (rule) => {
    if (!rule.condition.trim()) {
      throw new Error('Condition cannot be empty');
    }
    if (rule.conditionType === CONDITION_TYPES.PRICE_RANGE) {
      try {
        const [min, max] = rule.condition.split('-').map(Number);
        if (isNaN(min) || isNaN(max) || min >= max) {
          throw new Error();
        }
      } catch {
        throw new Error('Price range must be in format min-max (e.g., 0-100)');
      }
    }
    if (!rule.score || rule.score <= 0) {
      throw new Error('Score must be greater than 0');
    }
  };

  const handleSaveRule = async () => {
    setLoading(true);
    try {
      validateRule(currentRule);
      
      if (editingRule) {
        await ruleService.updateRule(editingRule.id, currentRule);
      } else {
        await ruleService.createRule(currentRule);
      }
      
      await fetchRules();
      handleCloseDialog();
      setError(null);
    } catch (error) {
      setError(error.message || 'Failed to save mock rule. Please try again.');
      console.error('Error saving rule:', error);
    }
    setLoading(false);
  };

  const handleDeleteRule = async (ruleId) => {
    if (window.confirm('Are you sure you want to delete this rule?')) {
      setLoading(true);
      try {
        await ruleService.deleteRule(ruleId);
        await fetchRules();
        setError(null);
      } catch (error) {
        setError('Failed to delete mock rule. Please try again.');
        console.error('Error deleting rule:', error);
      }
      setLoading(false);
    }
  };

  if (loading && rules.length === 0) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh' }}>
        <CircularProgress />
      </Box>
    );
  }

  return (
    <Box sx={{ p: 3 }}>
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3 }}>
        <Box>
          <Typography variant="h5" sx={{ mb: 1 }}>Search Rules Management</Typography>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexDirection: 'column', alignItems: 'flex-start' }}>
            {apiHealth && (
              <Chip
                label={`Status: ${apiHealth.status}`}
                color={apiHealth.status === 'healthy' ? 'success' : 'error'}
                size="small"
              />
            )}
            <Typography variant="caption" color="text.secondary" sx={{ fontStyle: 'italic' }}>
              Using mock data (Spanner implementation coming soon)
            </Typography>
          </Box>
        </Box>
        <Button
          variant="contained"
          onClick={() => handleOpenDialog()}
          disabled={loading || !apiHealth}
        >
          Add New Rule
        </Button>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }}>
          {error}
        </Alert>
      )}

      <Grid container spacing={3}>
        {rules.map((rule) => (
          <Grid item xs={12} md={6} key={rule.id}>
            <Card>
              <CardContent>
                <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                  <Typography 
                    variant="h6" 
                    color={rule.type === RULE_TYPES.BOOST ? 'success.main' : 'error.main'}
                  >
                    {(rule.type || '').toUpperCase()}
                  </Typography>
                  <Box>
                    <IconButton onClick={() => handleOpenDialog(rule)}>
                      <EditIcon />
                    </IconButton>
                    <IconButton onClick={() => handleDeleteRule(rule.id)}>
                      <DeleteIcon />
                    </IconButton>
                  </Box>
                </Box>
                <Typography variant="body1">
                  Condition Type: {rule.conditionType || 'N/A'}
                </Typography>
                <Typography variant="body1">
                  Condition: {rule.condition || 'N/A'}
                </Typography>
                <Typography variant="body1">
                  Score: {rule.score || 'N/A'}
                </Typography>
              </CardContent>
            </Card>
          </Grid>
        ))}
      </Grid>

      <Dialog open={openDialog} onClose={handleCloseDialog} maxWidth="sm" fullWidth>
        <DialogTitle>{editingRule ? 'Edit Rule' : 'Create New Rule'}</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 2 }}>
            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel>Rule Type</InputLabel>
              <Select
                value={currentRule.type}
                label="Rule Type"
                onChange={(e) => setCurrentRule({ ...currentRule, type: e.target.value })}
              >
                <MenuItem value={RULE_TYPES.BOOST}>Boost</MenuItem>
                <MenuItem value={RULE_TYPES.BURY}>Bury</MenuItem>
              </Select>
            </FormControl>

            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel>Condition Type</InputLabel>
              <Select
                value={currentRule.conditionType}
                label="Condition Type"
                onChange={(e) => setCurrentRule({ ...currentRule, conditionType: e.target.value })}
              >
                <MenuItem value={CONDITION_TYPES.CATEGORY}>Category</MenuItem>
                <MenuItem value={CONDITION_TYPES.BRAND}>Brand</MenuItem>
                <MenuItem value={CONDITION_TYPES.PRICE_RANGE}>Price Range</MenuItem>
                <MenuItem value={CONDITION_TYPES.PRODUCT_ID}>Product ID</MenuItem>
              </Select>
            </FormControl>

            <TextField
              fullWidth
              label="Condition"
              value={currentRule.condition}
              onChange={(e) => setCurrentRule({ ...currentRule, condition: e.target.value })}
              sx={{ mb: 2 }}
              helperText={
                currentRule.conditionType === CONDITION_TYPES.PRICE_RANGE
                  ? 'Format: min-max (e.g., 0-100)'
                  : ''
              }
            />

            <TextField
              fullWidth
              type="number"
              label="Score"
              value={currentRule.score}
              onChange={(e) => setCurrentRule({ ...currentRule, score: parseFloat(e.target.value) })}
              inputProps={{ step: 0.1, min: 0.1 }}
            />
          </Box>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleCloseDialog}>Cancel</Button>
          <LoadingButton
            loading={loading}
            onClick={handleSaveRule}
            variant="contained"
          >
            Save
          </LoadingButton>
        </DialogActions>
      </Dialog>
    </Box>
  );
}
