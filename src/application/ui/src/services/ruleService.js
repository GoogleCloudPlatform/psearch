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

// Mock data for rules
const mockRules = [
  {
    id: 'rule1',
    type: 'boost',
    conditionType: 'category',
    condition: 'Electronics',
    score: 1.5
  },
  {
    id: 'rule2',
    type: 'bury',
    conditionType: 'brand',
    condition: 'GenericBrand',
    score: 0.7
  },
  {
    id: 'rule3',
    type: 'boost',
    conditionType: 'price_range',
    condition: '100-200',
    score: 1.2
  }
];

// Mock implementation using local storage to persist changes
export const ruleService = {
  /**
   * Get all rules
   * @returns {Promise<Array>} Array of rule objects
   */
  async getRules() {
    // Check if we have rules in localStorage first
    const storedRules = localStorage.getItem('psearch_mock_rules');
    if (storedRules) {
      return JSON.parse(storedRules);
    }
    
    // Otherwise use default mock rules
    localStorage.setItem('psearch_mock_rules', JSON.stringify(mockRules));
    return mockRules;
  },

  /**
   * Create a new rule
   * @param {Object} rule - Rule object to create
   * @returns {Promise<Object>} Created rule with ID
   */
  async createRule(rule) {
    const rules = await this.getRules();
    
    // Generate a new ID
    const newRule = {
      ...rule,
      id: `rule${Date.now()}`
    };
    
    rules.push(newRule);
    localStorage.setItem('psearch_mock_rules', JSON.stringify(rules));
    
    return newRule;
  },

  /**
   * Update an existing rule
   * @param {string} ruleId - ID of the rule to update
   * @param {Object} rule - Updated rule object
   * @returns {Promise<Object>} Updated rule
   */
  async updateRule(ruleId, rule) {
    const rules = await this.getRules();
    
    const updatedRules = rules.map(r => 
      r.id === ruleId ? { ...rule, id: ruleId } : r
    );
    
    localStorage.setItem('psearch_mock_rules', JSON.stringify(updatedRules));
    
    return { ...rule, id: ruleId };
  },

  /**
   * Delete a rule
   * @param {string} ruleId - ID of the rule to delete
   * @returns {Promise<void>}
   */
  async deleteRule(ruleId) {
    const rules = await this.getRules();
    
    const filteredRules = rules.filter(r => r.id !== ruleId);
    
    localStorage.setItem('psearch_mock_rules', JSON.stringify(filteredRules));
  },

  /**
   * Check health status
   * @returns {Promise<Object>} Health status object
   */
  async checkHealth() {
    // Always return healthy since this is a mock implementation
    return {
      status: 'healthy',
      version: '1.0.0-mock',
      database: 'mock-data (will use Spanner in future)',
      collection: 'mock-rules'
    };
  }
};
