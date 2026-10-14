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
    Typography,
    Button,
    Chip,
    ButtonGroup,
    Card,
    CardContent,
    List,
    ListItem,
    ListItemIcon,
    ListItemText,
    Divider,
    Tooltip
} from '@mui/material';
import PersonIcon from '@mui/icons-material/Person';
import RestartAltIcon from '@mui/icons-material/RestartAlt';
import TouchAppIcon from '@mui/icons-material/TouchApp';
import ShoppingCartIcon from '@mui/icons-material/ShoppingCart';
import {
    getSessionId,
    resetSessionId,
    fetchSessionEvents,
    seedPersonaEvents,
    DEMO_PERSONAS
} from '../services/sessionService';

const SessionPanel = ({ onSessionUpdate, tenantId = 'default', refreshKey = 0 }) => {
    const [sessionId, setSessionIdState] = useState(getSessionId());
    const [events, setEvents] = useState([]);
    const [loading, setLoading] = useState(false);
    const [activePersona, setActivePersona] = useState(null);

    const loadEvents = useCallback(async () => {
        setLoading(true);
        try {
            const data = await fetchSessionEvents(tenantId);
            setEvents(data?.events || []);
        } finally {
            setLoading(false);
        }
    }, [tenantId]);

    useEffect(() => {
        setSessionIdState(getSessionId());
        loadEvents();
    }, [loadEvents, refreshKey]);

    const handleResetSession = () => {
        const newId = resetSessionId();
        setSessionIdState(newId);
        setActivePersona(null);
        setEvents([]);
        if (onSessionUpdate) {
            onSessionUpdate(newId);
        }
    };

    const handleSelectPersona = async (persona) => {
        setLoading(true);
        try {
            const newId = await seedPersonaEvents(persona.id, tenantId);
            if (newId) {
                setSessionIdState(newId);
                setActivePersona(persona.id);
                const data = await fetchSessionEvents(tenantId);
                setEvents(data?.events || []);
                if (onSessionUpdate) {
                    onSessionUpdate(newId);
                }
            }
        } finally {
            setLoading(false);
        }
    };

    const formatTime = (ts) => {
        if (!ts) return '';
        try {
            const d = new Date(ts);
            return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        } catch {
            return '';
        }
    };

    return (
        <Card sx={{
            mb: 2,
            boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
            borderRadius: '8px',
            border: '1px solid #e0e0e0',
        }}>
            <CardContent sx={{ p: 2, '&:last-child': { pb: 2 } }}>
                <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', mb: 1.5 }}>
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <PersonIcon color="primary" fontSize="small" />
                        <Typography variant="subtitle1" sx={{ fontWeight: 600, fontSize: '15px' }}>
                            Sessão Ativa
                        </Typography>
                    </Box>
                    <Tooltip title="Gerar nova sessão limpa">
                        <Button
                            size="small"
                            variant="outlined"
                            startIcon={<RestartAltIcon />}
                            onClick={handleResetSession}
                            sx={{ textTransform: 'none', py: 0.25, px: 1, fontSize: '0.75rem' }}
                        >
                            Nova sessão
                        </Button>
                    </Tooltip>
                </Box>

                <Box sx={{ mb: 1.5, display: 'flex', alignItems: 'center', gap: 0.5 }}>
                    <Typography variant="caption" color="text.secondary">
                        ID:
                    </Typography>
                    <Chip
                        label={sessionId}
                        size="small"
                        sx={{
                            fontFamily: 'monospace',
                            fontSize: '0.7rem',
                            height: 22,
                            maxWidth: '100%',
                            overflow: 'hidden',
                            textOverflow: 'ellipsis',
                        }}
                    />
                </Box>

                {/* Persona Switcher */}
                <Box sx={{ mb: 2 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, color: 'text.secondary', display: 'block', mb: 0.75 }}>
                        Personas Demo (Seed Events):
                    </Typography>
                    <ButtonGroup size="small" variant="outlined" fullWidth orientation="vertical">
                        {DEMO_PERSONAS.map((p) => {
                            const isSelected = activePersona === p.id;
                            return (
                                <Button
                                    key={p.id}
                                    variant={isSelected ? 'contained' : 'outlined'}
                                    onClick={() => handleSelectPersona(p)}
                                    sx={{
                                        textTransform: 'none',
                                        justifyContent: 'flex-start',
                                        fontSize: '0.78rem',
                                        py: 0.5,
                                    }}
                                >
                                    <span style={{ marginRight: 6 }}>{p.icon}</span>
                                    {p.name}
                                </Button>
                            );
                        })}
                    </ButtonGroup>
                </Box>

                <Divider sx={{ my: 1.5 }} />

                {/* Recent Events List */}
                <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 1 }}>
                    <Typography variant="caption" sx={{ fontWeight: 600, color: 'text.secondary' }}>
                        Eventos Recentes ({events.length}):
                    </Typography>
                    {loading && (
                        <Typography variant="caption" color="primary" sx={{ fontStyle: 'italic' }}>
                            atualizando...
                        </Typography>
                    )}
                </Box>

                {events.length === 0 ? (
                    <Typography variant="caption" color="text.secondary" sx={{ display: 'block', fontStyle: 'italic', py: 1 }}>
                        Nenhum evento registrado nesta sessão. Clique nos produtos ou escolha uma persona.
                    </Typography>
                ) : (
                    <List dense disablePadding sx={{ maxHeight: 180, overflowY: 'auto' }}>
                        {events.map((ev, idx) => {
                            const isPurchase = ev.event_type === 'purchase';
                            return (
                                <ListItem
                                    key={ev.event_id || idx}
                                    disableGutters
                                    sx={{
                                        py: 0.5,
                                        borderBottom: idx < events.length - 1 ? '1px solid #f0f0f0' : 'none',
                                    }}
                                >
                                    <ListItemIcon sx={{ minWidth: 26 }}>
                                        {isPurchase ? (
                                            <ShoppingCartIcon fontSize="inherit" color="success" />
                                        ) : (
                                            <TouchAppIcon fontSize="inherit" color="primary" />
                                        )}
                                    </ListItemIcon>
                                    <ListItemText
                                        primary={
                                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                                <Typography variant="caption" sx={{ fontWeight: 600 }}>
                                                    {ev.product_id}
                                                </Typography>
                                                <Chip
                                                    label={ev.event_type}
                                                    size="small"
                                                    color={isPurchase ? 'success' : 'default'}
                                                    sx={{ height: 16, fontSize: '0.65rem' }}
                                                />
                                            </Box>
                                        }
                                        secondary={
                                            <Typography variant="caption" color="text.secondary" sx={{ fontSize: '0.65rem' }}>
                                                {formatTime(ev.created_at)}
                                            </Typography>
                                        }
                                    />
                                </ListItem>
                            );
                        })}
                    </List>
                )}
            </CardContent>
        </Card>
    );
};

export default SessionPanel;
