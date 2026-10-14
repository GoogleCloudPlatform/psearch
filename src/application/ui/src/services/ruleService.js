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

const API_URL = config.apiUrl;

/**
 * Retrieve admin token strictly from session storage.
 * Never reads or touches localStorage.
 */
function getAdminToken(explicitToken) {
  if (explicitToken && typeof explicitToken === 'string') {
    return explicitToken.trim();
  }
  try {
    return (sessionStorage.getItem('psearch_admin_token') || '').trim();
  } catch {
    return '';
  }
}

function getAuthHeaders(token) {
  const adminToken = getAdminToken(token);
  const headers = {};
  if (adminToken) {
    headers['X-Admin-Token'] = adminToken;
  }
  return headers;
}

/**
 * Map an API rule object to the UI rule representation.
 */
export function fromApiRule(apiRule) {
  if (!apiRule) return null;
  const pinIds = Array.isArray(apiRule.pin_product_ids) ? apiRule.pin_product_ids : [];
  const buryIds = Array.isArray(apiRule.bury_product_ids) ? apiRule.bury_product_ids : [];
  const boostMult = typeof apiRule.boost_multiplier === 'number' ? apiRule.boost_multiplier : 1.0;

  let conditionType = 'category';
  let condition = '';
  let type = boostMult < 1.0 || buryIds.length > 0 ? 'bury' : 'boost';

  if (pinIds.length > 0) {
    conditionType = 'product_id';
    condition = pinIds.join(', ');
    type = 'boost';
  } else if (buryIds.length > 0) {
    conditionType = 'product_id';
    condition = buryIds.join(', ');
    type = 'bury';
  } else if (apiRule.target_category) {
    conditionType = 'category';
    condition = apiRule.target_category;
  } else if (apiRule.target_brand) {
    conditionType = 'brand';
    condition = apiRule.target_brand;
  } else if (apiRule.query_pattern) {
    conditionType = 'category';
    condition = apiRule.query_pattern;
  }

  return {
    id: apiRule.rule_id || apiRule.id,
    rule_id: apiRule.rule_id || apiRule.id,
    tenant_id: apiRule.tenant_id || 'default',
    name: apiRule.name || '',
    query_pattern: apiRule.query_pattern || '',
    target_brand: apiRule.target_brand || '',
    target_category: apiRule.target_category || '',
    boost_multiplier: boostMult,
    pin_product_ids: pinIds,
    bury_product_ids: buryIds,
    active: apiRule.active !== undefined ? apiRule.active : true,
    // UI legacy fields
    type,
    conditionType,
    condition,
    score: boostMult,
  };
}

/**
 * Map a UI rule object to the API rule representation.
 */
export function toApiRule(uiRule) {
  const tenant_id = uiRule.tenant_id || 'default';
  const rule_id = uiRule.rule_id || uiRule.id || '';

  let target_category = uiRule.target_category || '';
  let target_brand = uiRule.target_brand || '';
  let pin_product_ids = Array.isArray(uiRule.pin_product_ids) ? [...uiRule.pin_product_ids] : [];
  let bury_product_ids = Array.isArray(uiRule.bury_product_ids) ? [...uiRule.bury_product_ids] : [];
  let boost_multiplier = typeof uiRule.boost_multiplier === 'number'
    ? uiRule.boost_multiplier
    : (typeof uiRule.score === 'number' ? uiRule.score : parseFloat(uiRule.score) || 1.0);
  let query_pattern = uiRule.query_pattern !== undefined ? uiRule.query_pattern : '';

  if (uiRule.conditionType === 'category' && !target_category) {
    target_category = uiRule.condition || '';
  } else if (uiRule.conditionType === 'brand' && !target_brand) {
    target_brand = uiRule.condition || '';
  } else if (uiRule.conditionType === 'product_id' && pin_product_ids.length === 0 && bury_product_ids.length === 0) {
    const ids = (uiRule.condition || '').split(',').map(s => s.trim()).filter(Boolean);
    if (uiRule.type === 'bury') {
      bury_product_ids = ids;
    } else {
      pin_product_ids = ids;
    }
  }

  if (uiRule.type === 'bury' && boost_multiplier >= 1.0 && bury_product_ids.length === 0) {
    boost_multiplier = 0.5;
  }
  if (isNaN(boost_multiplier) || boost_multiplier <= 0) {
    boost_multiplier = 1.0;
  } else if (boost_multiplier < 0.1) {
    boost_multiplier = 0.1;
  } else if (boost_multiplier > 10.0) {
    boost_multiplier = 10.0;
  }

  let name = uiRule.name && uiRule.name.trim() ? uiRule.name.trim() : '';
  if (!name) {
    const desc = uiRule.condition || target_category || target_brand || query_pattern || 'Rule';
    name = `${(uiRule.type || 'boost').toUpperCase()} ${desc}`;
  }

  return {
    tenant_id,
    rule_id,
    name: name.slice(0, 256),
    query_pattern: query_pattern.slice(0, 1024),
    target_brand: target_brand.toLowerCase().slice(0, 256),
    target_category: target_category.toLowerCase().slice(0, 256),
    boost_multiplier,
    pin_product_ids,
    bury_product_ids,
    active: uiRule.active !== undefined ? Boolean(uiRule.active) : true,
  };
}

export const ruleService = {
  /**
   * Get all rules for tenant
   * @param {string} tenantId - Optional tenant ID (defaults to 'default')
   * @returns {Promise<Array>} Array of mapped rule objects
   */
  async getRules(tenantId = 'default') {
    const res = await axios.get(`${API_URL}/api/rules`, {
      params: { tenant_id: tenantId || 'default' },
    });
    const rulesList = (res.data && res.data.rules) || [];
    return rulesList.map(fromApiRule);
  },

  /**
   * Create a new rule via POST /api/rules
   * @param {Object} rule - Rule object
   * @param {string} token - Optional X-Admin-Token
   * @returns {Promise<Object>} Created rule with server-assigned ID
   */
  async createRule(rule, token) {
    const apiPayload = toApiRule(rule);
    const tenantId = apiPayload.tenant_id || 'default';
    const res = await axios.post(`${API_URL}/api/rules`, apiPayload, {
      params: { tenant_id: tenantId },
      headers: getAuthHeaders(token),
    });
    return fromApiRule(res.data);
  },

  /**
   * Update an existing rule via PUT /api/rules/:id
   * @param {string} ruleId - ID of rule
   * @param {Object} rule - Updated rule object
   * @param {string} token - Optional X-Admin-Token
   * @returns {Promise<Object>} Updated rule
   */
  async updateRule(ruleId, rule, token) {
    const apiPayload = toApiRule(rule);
    const tenantId = apiPayload.tenant_id || 'default';
    const res = await axios.put(`${API_URL}/api/rules/${encodeURIComponent(ruleId)}`, apiPayload, {
      params: { tenant_id: tenantId },
      headers: getAuthHeaders(token),
    });
    return fromApiRule(res.data);
  },

  /**
   * Delete a rule via DELETE /api/rules/:id
   * @param {string} ruleId - ID of rule to delete
   * @param {string} tenantId - Optional tenant ID
   * @param {string} token - Optional X-Admin-Token
   * @returns {Promise<void>}
   */
  async deleteRule(ruleId, tenantId = 'default', token) {
    let tid = tenantId;
    let tkn = token;
    // Handle overload if called as deleteRule(ruleId, token)
    if (typeof tenantId === 'string' && tenantId.length > 25 && !token) {
      tkn = tenantId;
      tid = 'default';
    }
    await axios.delete(`${API_URL}/api/rules/${encodeURIComponent(ruleId)}`, {
      params: { tenant_id: tid || 'default' },
      headers: getAuthHeaders(tkn),
    });
  },

  /**
   * Check health status of serving service
   * @returns {Promise<Object>}
   */
  async checkHealth() {
    try {
      const res = await axios.get(`${API_URL}/api/health`);
      return res.data || { status: 'healthy' };
    } catch {
      try {
        const res2 = await axios.get(`${API_URL}/health`);
        return res2.data || { status: 'healthy' };
      } catch {
        return { status: 'available' };
      }
    }
  },
};

export default ruleService;
