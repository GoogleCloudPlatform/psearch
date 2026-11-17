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

function isUnconfiguredNodeLocalStorage(storageName) {
  if (storageName !== 'localStorage') {
    return false;
  }
  if (typeof window !== 'undefined' || typeof document !== 'undefined') {
    return false;
  }
  if (
    typeof process === 'undefined' ||
    !process ||
    !process.versions ||
    !process.versions.node
  ) {
    return false;
  }
  const hasCliFlag =
    Array.isArray(process.execArgv) &&
    process.execArgv.some(
      (arg) => typeof arg === 'string' && arg.startsWith('--localstorage-file')
    );
  const hasEnvFlag = Boolean(
    process.env &&
      typeof process.env.NODE_OPTIONS === 'string' &&
      process.env.NODE_OPTIONS.includes('--localstorage-file')
  );
  if (hasCliFlag || hasEnvFlag) {
    return false;
  }
  try {
    const desc = Object.getOwnPropertyDescriptor(globalThis, 'localStorage');
    return Boolean(
      desc &&
        typeof desc.get === 'function' &&
        String(desc.get).includes('internal/webstorage')
    );
  } catch {
    return false;
  }
}

function readSingleStorage(storageName, key) {
  if (isUnconfiguredNodeLocalStorage(storageName)) {
    return '';
  }
  try {
    const root =
      typeof globalThis !== 'undefined'
        ? globalThis
        : typeof window !== 'undefined'
          ? window
          : undefined;
    if (!root) {
      return '';
    }
    const storage = root[storageName];
    if (!storage || typeof storage.getItem !== 'function') {
      return '';
    }
    const raw = storage.getItem(key);
    return typeof raw === 'string' ? raw.trim() : '';
  } catch {
    return '';
  }
}

function readStorage(key) {
  return (
    readSingleStorage('sessionStorage', key) ||
    readSingleStorage('localStorage', key)
  );
}

function readEnv(key) {
  try {
    if (typeof process !== 'undefined' && process && process.env) {
      const raw = process.env[key];
      return typeof raw === 'string' ? raw.trim() : '';
    }
  } catch {
    // Ignore restricted process.env access errors
  }
  return '';
}

function normalizeBearerToken(rawToken) {
  if (typeof rawToken !== 'string') {
    return '';
  }
  return rawToken.trim().replace(/^bearer(?:\s+|$)/i, '').trim();
}

function readBearerStorage(key) {
  return (
    normalizeBearerToken(readSingleStorage('sessionStorage', key)) ||
    normalizeBearerToken(readSingleStorage('localStorage', key))
  );
}

/**
 * Build request auth headers for the Gen AI API (`GEN_AI_URL`).
 * Sends at most one credential scheme (`Authorization: Bearer` preferred over
 * `X-Admin-Token`) to align with non-fallthrough backend validation.
 */
export function getGenAiAuthHeaders() {
  const bearerToken =
    readBearerStorage('genAiAuthToken') ||
    readBearerStorage('psearchAuthToken');
  const adminToken =
    readStorage('genAiAdminToken') || readEnv('REACT_APP_GEN_AI_ADMIN_TOKEN');

  if (bearerToken) {
    return { Authorization: `Bearer ${bearerToken}` };
  } else if (adminToken) {
    return { 'X-Admin-Token': adminToken };
  }
  return {};
}

/**
 * Build request auth headers for the PSearch Serving API (`API_URL`).
 * Sends at most one credential scheme (`Authorization: Bearer` preferred over
 * `X-API-Key`) to align with non-fallthrough `AuthorizeAPI` validation.
 */
export function getServingAuthHeaders() {
  const bearerToken = readBearerStorage('psearchAuthToken');
  const apiToken =
    readStorage('psearchApiToken') ||
    readStorage('psearch_api_token') ||
    readEnv('REACT_APP_API_TOKEN');

  if (bearerToken) {
    return { Authorization: `Bearer ${bearerToken}` };
  } else if (apiToken) {
    return { 'X-API-Key': apiToken };
  }
  return {};
}
