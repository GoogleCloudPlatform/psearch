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
const SESSION_STORAGE_KEY = 'psearch_session_id';

function generateRandomHex(len = 8) {
    if (typeof window !== 'undefined' && window.crypto && window.crypto.getRandomValues) {
        const arr = new Uint8Array(len);
        window.crypto.getRandomValues(arr);
        return Array.from(arr, b => b.toString(16).padStart(2, '0')).join('');
    }
    return Math.random().toString(16).substring(2, 2 + len * 2);
}

export function getSessionId() {
    if (typeof window === 'undefined') return 'sess-default';
    let sid = window.sessionStorage.getItem(SESSION_STORAGE_KEY);
    if (!sid || sid.length < 8) {
        sid = `sess-${generateRandomHex(8)}`;
        window.sessionStorage.setItem(SESSION_STORAGE_KEY, sid);
    }
    return sid;
}

export function setSessionId(newId) {
    if (typeof window !== 'undefined' && newId) {
        window.sessionStorage.setItem(SESSION_STORAGE_KEY, newId);
    }
    return newId;
}

export function resetSessionId() {
    const newId = `sess-${generateRandomHex(8)}`;
    return setSessionId(newId);
}

export async function recordSessionEvent(productId, eventType = 'click', tenantId = 'default') {
    const sid = getSessionId();
    try {
        const response = await axios.post(`${API_URL}/api/session/events`, {
            session_id: sid,
            product_id: productId,
            event_type: eventType,
            tenant_id: tenantId || 'default',
        });
        return response.data;
    } catch (err) {
        console.error('Failed to record session event:', err);
        return null;
    }
}

export async function fetchSessionEvents(tenantId = 'default') {
    const sid = getSessionId();
    try {
        const response = await axios.get(`${API_URL}/api/session/events`, {
            params: {
                session_id: sid,
                tenant_id: tenantId || 'default',
            },
        });
        return response.data;
    } catch (err) {
        console.error('Failed to fetch session events:', err);
        return { session_id: sid, events: [], count: 0 };
    }
}

export const DEMO_PERSONAS = [
    {
        id: 'runner',
        name: 'Corredor',
        icon: '🏃',
        description: 'Tênis de corrida e performance esportiva',
        productIds: ['prod-001', 'prod-003'],
    },
    {
        id: 'tech',
        name: 'Tech / Dev',
        icon: '💻',
        description: 'Laptops de alta performance e notebooks de desenvolvedor',
        productIds: ['prod-005', 'prod-007'],
    },
    {
        id: 'audio',
        name: 'Audiófilo',
        icon: '🎧',
        description: 'Fones de ouvido bluetooth com cancelamento de ruído',
        productIds: ['prod-008', 'prod-009'],
    },
];

export async function seedPersonaEvents(personaId, tenantId = 'default') {
    const persona = DEMO_PERSONAS.find(p => p.id === personaId);
    if (!persona) return null;

    // Reset session so persona starts fresh
    const newSessionId = resetSessionId();

    for (const pid of persona.productIds) {
        try {
            await axios.post(`${API_URL}/api/session/events`, {
                session_id: newSessionId,
                product_id: pid,
                event_type: 'click',
                tenant_id: tenantId || 'default',
            });
        } catch (e) {
            console.error(`Failed to seed event for ${pid}:`, e);
        }
    }
    return newSessionId;
}
