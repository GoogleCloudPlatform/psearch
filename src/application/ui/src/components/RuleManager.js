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

import React, { useState, useEffect, useCallback } from 'react';
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
  FormControlLabel,
  Switch,
  Grid,
  IconButton,
  InputLabel,
  MenuItem,
  Select,
  TextField,
  Typography,
  Alert,
  CircularProgress,
  Chip,
} from '@mui/material';
import { LoadingButton } from '@mui/lab';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import { ruleService } from '../services/ruleService';

const RULE_TYPES = {
  BOOST: 'boost',
  BURY: 'bury',
};

const CONDITION_TYPES = {
  CATEGORY: 'category',
  BRAND: 'brand',
  PRODUCT_ID: 'product_id',
};

export default function RuleManager() {
  const [rules, setRules] = useState([]);
  const [loading, setLoading] = useState(false);
  const [openDialog, setOpenDialog] = useState(false);
  const [editingRule, setEditingRule] = useState(null);
  const [error, setError] = useState(null);
  const [apiHealth, setApiHealth] = useState(null);

  // Admin token stored ONLY in sessionStorage (never localStorage, never logged)
  const [adminToken, setAdminToken] = useState(() => {
    try {
      return sessionStorage.getItem('psearch_admin_token') || '';
    } catch {
      return '';
    }
  });

  const handleAdminTokenChange = (e) => {
    const val = e.target.value;
    setAdminToken(val);
    try {
      sessionStorage.setItem('psearch_admin_token', val);
    } catch {}
  };

  const [currentRule, setCurrentRule] = useState({
    name: '',
    type: RULE_TYPES.BOOST,
    conditionType: CONDITION_TYPES.CATEGORY,
    condition: '',
    score: 1.0,
    query_pattern: '',
    tenant_id: 'default',
    active: true,
  });

  const checkApiHealth = useCallback(async () => {
    try {
      const health = await ruleService.checkHealth();
      setApiHealth(health);
      setError(null);
    } catch {
      setApiHealth(null);
      setError('API Connection Error: Rule service unavailable');
    }
  }, []);

  const fetchRules = useCallback(async () => {
    setLoading(true);
    try {
      const rulesList = await ruleService.getRules();
      setRules(rulesList || []);
      setError(null);
    } catch (err) {
      if (err.response) {
        if (err.response.status === 403) {
          setError('403 Forbidden: Invalid or missing admin token.');
        } else if (err.response.status === 400) {
          const msg = err.response.data?.error || err.response.data?.message || 'Invalid request';
          setError(`400 Bad Request: ${msg}`);
        } else {
          setError(`Error (${err.response.status}): ${err.response.data?.error || err.message}`);
        }
      } else {
        setError('Failed to fetch rules. Please try again.');
      }
      setRules([]);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    checkApiHealth();
    fetchRules();
  }, [checkApiHealth, fetchRules]);

  const handleOpenDialog = (rule = null) => {
    if (rule) {
      setEditingRule(rule);
      setCurrentRule({
        ...rule,
        name: rule.name || (rule.condition ? `${rule.type} ${rule.condition}` : 'Rule'),
        score: rule.boost_multiplier !== undefined ? rule.boost_multiplier : (rule.score || 1.0),
        query_pattern: rule.query_pattern || '',
        tenant_id: rule.tenant_id || 'default',
        active: rule.active !== undefined ? rule.active : true,
      });
    } else {
      setEditingRule(null);
      setCurrentRule({
        name: '',
        type: RULE_TYPES.BOOST,
        conditionType: CONDITION_TYPES.CATEGORY,
        condition: '',
        score: 1.0,
        query_pattern: '',
        tenant_id: 'default',
        active: true,
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
    if (!rule.name || !rule.name.trim()) {
      throw new Error('Name cannot be empty');
    }
    if (!rule.condition || !rule.condition.trim()) {
      if (!rule.query_pattern && !rule.target_category && !rule.target_brand) {
        throw new Error('Condition cannot be empty');
      }
    }
    const score = parseFloat(rule.score);
    if (isNaN(score) || score < 0.1 || score > 10.0) {
      throw new Error('Score/multiplier must be between 0.1 and 10.0');
    }
  };

  const handleSaveRule = async () => {
    setLoading(true);
    setError(null);
    try {
      validateRule(currentRule);

      if (editingRule) {
        await ruleService.updateRule(editingRule.id, currentRule, adminToken);
      } else {
        await ruleService.createRule(currentRule, adminToken);
      }

      await fetchRules();
      handleCloseDialog();
    } catch (err) {
      if (err.response) {
        if (err.response.status === 403) {
          setError('403 Forbidden: Invalid or missing admin token.');
        } else if (err.response.status === 400) {
          const msg = err.response.data?.error || err.response.data?.message || 'Invalid rule parameters';
          setError(`400 Bad Request: ${msg}`);
        } else {
          setError(`Error (${err.response.status}): ${err.response.data?.error || err.message}`);
        }
      } else {
        setError(err.message || 'Failed to save rule. Please try again.');
      }
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteRule = async (ruleId) => {
    if (window.confirm('Are you sure you want to delete this rule?')) {
      setLoading(true);
      setError(null);
      try {
        await ruleService.deleteRule(ruleId, 'default', adminToken);
        await fetchRules();
      } catch (err) {
        if (err.response) {
          if (err.response.status === 403) {
            setError('403 Forbidden: Invalid or missing admin token.');
          } else if (err.response.status === 400) {
            const msg = err.response.data?.error || err.response.data?.message || 'Invalid request';
            setError(`400 Bad Request: ${msg}`);
          } else {
            setError(`Error (${err.response.status}): ${err.response.data?.error || err.message}`);
          }
        } else {
          setError(err.message || 'Failed to delete rule. Please try again.');
        }
      } finally {
        setLoading(false);
      }
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
      <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 3, flexWrap: 'wrap', gap: 2 }}>
        <Box>
          <Typography variant="h5" sx={{ mb: 1 }}>Search Rules Management</Typography>
          <Box sx={{ display: 'flex', gap: 1, alignItems: 'center' }}>
            {apiHealth && (
              <Chip
                label={`Status: ${apiHealth.status}`}
                color={apiHealth.status === 'healthy' ? 'success' : 'default'}
                size="small"
              />
            )}
            <Typography variant="caption" color="text.secondary">
              Merchandising ranking boosts, pins, and buries (Cloud Spanner)
            </Typography>
          </Box>
        </Box>
        <Box sx={{ display: 'flex', gap: 2, alignItems: 'center', flexWrap: 'wrap' }}>
          <TextField
            type="password"
            size="small"
            label="Admin token"
            id="admin-token-input"
            inputProps={{ 'aria-label': 'Admin token' }}
            value={adminToken}
            onChange={handleAdminTokenChange}
            placeholder="Enter X-Admin-Token"
            helperText="Stored only in session"
            sx={{ minWidth: 200 }}
          />
          <Button
            variant="contained"
            onClick={() => handleOpenDialog()}
            disabled={loading}
          >
            Add New Rule
          </Button>
        </Box>
      </Box>

      {error && (
        <Alert severity="error" sx={{ mb: 3 }} onClose={() => setError(null)}>
          {error}
        </Alert>
      )}

      <Grid container spacing={3}>
        {rules.map((rule) => {
          const isBoost = rule.type === RULE_TYPES.BOOST || (rule.boost_multiplier >= 1.0 && (!rule.bury_product_ids || rule.bury_product_ids.length === 0));
          return (
            <Grid item xs={12} md={6} key={rule.rule_id || rule.id}>
              <Card variant="outlined" sx={{ borderRadius: 2 }}>
                <CardContent>
                  <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', mb: 1.5 }}>
                    <Box>
                      <Typography variant="h6" sx={{ fontWeight: 600 }}>
                        {rule.name || rule.rule_id || rule.id}
                      </Typography>
                      <Box sx={{ display: 'flex', gap: 1, mt: 0.5 }}>
                        <Chip
                          label={isBoost ? 'BOOST' : 'BURY'}
                          color={isBoost ? 'success' : 'error'}
                          size="small"
                        />
                        <Chip
                          label={rule.active ? 'Active' : 'Inactive'}
                          variant="outlined"
                          size="small"
                          color={rule.active ? 'primary' : 'default'}
                        />
                      </Box>
                    </Box>
                    <Box>
                      <IconButton onClick={() => handleOpenDialog(rule)} size="small" aria-label="edit rule">
                        <EditIcon fontSize="small" />
                      </IconButton>
                      <IconButton onClick={() => handleDeleteRule(rule.rule_id || rule.id)} size="small" aria-label="delete rule">
                        <DeleteIcon fontSize="small" />
                      </IconButton>
                    </Box>
                  </Box>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Rule ID:</strong> {rule.rule_id || rule.id}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Condition Type:</strong> {rule.conditionType || 'N/A'}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Condition:</strong> {rule.condition || rule.target_category || rule.target_brand || 'N/A'}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Query Pattern:</strong> {rule.query_pattern ? `"${rule.query_pattern}"` : '(all queries)'}
                  </Typography>
                  <Typography variant="body2" color="text.secondary">
                    <strong>Boost Multiplier:</strong> {rule.boost_multiplier !== undefined ? rule.boost_multiplier : rule.score}
                  </Typography>
                </CardContent>
              </Card>
            </Grid>
          );
        })}
      </Grid>

      <Dialog open={openDialog} onClose={handleCloseDialog} maxWidth="sm" fullWidth>
        <DialogTitle>{editingRule ? 'Edit Rule' : 'Create New Rule'}</DialogTitle>
        <DialogContent>
          <Box sx={{ pt: 1 }}>
            <TextField
              fullWidth
              label="Rule Name"
              value={currentRule.name}
              onChange={(e) => setCurrentRule({ ...currentRule, name: e.target.value })}
              sx={{ mb: 2 }}
              required
            />

            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel id="rule-type-label">Rule Type</InputLabel>
              <Select
                labelId="rule-type-label"
                value={currentRule.type}
                label="Rule Type"
                onChange={(e) => setCurrentRule({ ...currentRule, type: e.target.value })}
              >
                <MenuItem value={RULE_TYPES.BOOST}>Boost</MenuItem>
                <MenuItem value={RULE_TYPES.BURY}>Bury</MenuItem>
              </Select>
            </FormControl>

            <FormControl fullWidth sx={{ mb: 2 }}>
              <InputLabel id="condition-type-label">Condition Type</InputLabel>
              <Select
                labelId="condition-type-label"
                value={currentRule.conditionType}
                label="Condition Type"
                onChange={(e) => setCurrentRule({ ...currentRule, conditionType: e.target.value })}
              >
                <MenuItem value={CONDITION_TYPES.CATEGORY}>Category</MenuItem>
                <MenuItem value={CONDITION_TYPES.BRAND}>Brand</MenuItem>
                <MenuItem value={CONDITION_TYPES.PRODUCT_ID}>Product ID (Pin/Bury)</MenuItem>
              </Select>
            </FormControl>

            <TextField
              fullWidth
              label="Condition"
              value={currentRule.condition}
              onChange={(e) => setCurrentRule({ ...currentRule, condition: e.target.value })}
              sx={{ mb: 2 }}
              helperText={
                currentRule.conditionType === CONDITION_TYPES.PRODUCT_ID
                  ? 'Comma-separated product IDs to pin or bury'
                  : 'Target category or brand name'
              }
              required
            />

            <TextField
              fullWidth
              label="Query Pattern"
              helperText="Optional query substring (leave empty or * to apply to all queries)"
              value={currentRule.query_pattern}
              onChange={(e) => setCurrentRule({ ...currentRule, query_pattern: e.target.value })}
              sx={{ mb: 2 }}
            />

            <TextField
              fullWidth
              type="number"
              label="Boost Multiplier (0.1 - 10)"
              value={currentRule.score}
              onChange={(e) => setCurrentRule({ ...currentRule, score: parseFloat(e.target.value) || 0 })}
              inputProps={{ step: 0.1, min: 0.1, max: 10.0 }}
              sx={{ mb: 2 }}
            />

            <FormControlLabel
              control={
                <Switch
                  checked={Boolean(currentRule.active)}
                  onChange={(e) => setCurrentRule({ ...currentRule, active: e.target.checked })}
                  color="primary"
                />
              }
              label="Active"
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
