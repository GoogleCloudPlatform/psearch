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

import React, { useState, useEffect, useRef, useCallback, memo } from 'react';
import axios from 'axios';
import config from '../config';
import {
    TextField,
    InputAdornment,
    Button,
    Slider,
    Box,
    Typography,
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    FormControlLabel,
    Switch,
    Paper
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';

// SearchInput component that isolates input handling from parent component
// and provides controls for alpha (hybrid weight), channel, and availability
const SearchInput = memo(({
    onSearch,
    query: controlledQuery,
    alpha: controlledAlpha,
    setAlpha: controlledSetAlpha,
    channel: controlledChannel,
    setChannel: controlledSetChannel,
    onlyInStock: controlledOnlyInStock,
    setOnlyInStock: controlledSetOnlyInStock,
    tenantId: controlledTenantId,
    locale: controlledLocale,
    cep: controlledCep,
    setCep: controlledSetCep,
    maxSla24h: controlledMaxSla24h,
    setMaxSla24h: controlledSetMaxSla24h,
    ltr: controlledLtr,
    setLtr: controlledSetLtr,
    parseIntent: controlledParseIntent,
    setParseIntent: controlledSetParseIntent,
    priceTableId: controlledPriceTableId,
    setPriceTableId: controlledSetPriceTableId,
    sortBy: controlledSortBy,
    setSortBy: controlledSetSortBy,
}) => {
    const [inputValue, setInputValue] = useState(controlledQuery || '');

    useEffect(() => {
        if (controlledQuery !== undefined) {
            setInputValue(controlledQuery);
        }
    }, [controlledQuery]);
    const [internalAlpha, setInternalAlpha] = useState(0.5);
    const [internalChannel, setInternalChannel] = useState('');
    const [internalOnlyInStock, setInternalOnlyInStock] = useState(false);
    const [internalTenantId] = useState('default');
    const [internalLocale] = useState('');
    const [internalCep, setInternalCep] = useState('');
    const [internalMaxSla24h, setInternalMaxSla24h] = useState(false);
    const [internalLtr, setInternalLtr] = useState(false);
    const [internalParseIntent, setInternalParseIntent] = useState(true);
    const [internalPriceTableId, setInternalPriceTableId] = useState('');
    const [internalSortBy, setInternalSortBy] = useState('');
    const [cepError, setCepError] = useState('');
    const [suggestions, setSuggestions] = useState([]);
    const [isOpen, setIsOpen] = useState(false);
    const [selectedIndex, setSelectedIndex] = useState(-1);
    const containerRef = useRef(null);
    const autocompleteTimerRef = useRef(null);
    const debounceTimerRef = useRef(null);

    const alpha = controlledAlpha !== undefined ? controlledAlpha : internalAlpha;
    const setAlpha = controlledSetAlpha || setInternalAlpha;
    const channel = controlledChannel !== undefined ? controlledChannel : internalChannel;
    const setChannel = controlledSetChannel || setInternalChannel;
    const onlyInStock = controlledOnlyInStock !== undefined ? controlledOnlyInStock : internalOnlyInStock;
    const setOnlyInStock = controlledSetOnlyInStock || setInternalOnlyInStock;
    const tenantId = controlledTenantId !== undefined ? controlledTenantId : internalTenantId;
    const locale = controlledLocale !== undefined ? controlledLocale : internalLocale;
    const cep = controlledCep !== undefined ? controlledCep : internalCep;
    const setCep = controlledSetCep || setInternalCep;
    const maxSla24h = controlledMaxSla24h !== undefined ? controlledMaxSla24h : internalMaxSla24h;
    const setMaxSla24h = controlledSetMaxSla24h || setInternalMaxSla24h;
    const ltr = controlledLtr !== undefined ? controlledLtr : internalLtr;
    const setLtr = controlledSetLtr || setInternalLtr;
    const parseIntent = controlledParseIntent !== undefined ? controlledParseIntent : internalParseIntent;
    const setParseIntent = controlledSetParseIntent || setInternalParseIntent;
    const priceTableId = controlledPriceTableId !== undefined ? controlledPriceTableId : internalPriceTableId;
    const setPriceTableId = controlledSetPriceTableId || setInternalPriceTableId;
    const sortBy = controlledSortBy !== undefined ? controlledSortBy : internalSortBy;
    const setSortBy = controlledSetSortBy || setInternalSortBy;

    // Local slider value for responsive dragging
    const [localAlpha, setLocalAlpha] = useState(alpha);

    useEffect(() => {
        setLocalAlpha(alpha);
    }, [alpha]);

    const fetchAutocomplete = useCallback(async (prefix) => {
        if (!prefix || prefix.trim().length < 2) {
            setSuggestions([]);
            setIsOpen(false);
            setSelectedIndex(-1);
            return;
        }
        try {
            const params = new URLSearchParams({
                q: prefix.trim(),
                limit: '8',
            });
            if (tenantId) params.append('tenant_id', tenantId);
            if (locale) params.append('locale', locale);
            const res = await axios.get(`${config.apiUrl}/api/autocomplete?${params.toString()}`);
            if (res.data && Array.isArray(res.data.suggestions)) {
                setSuggestions(res.data.suggestions);
                setIsOpen(res.data.suggestions.length > 0);
                setSelectedIndex(-1);
            } else {
                setSuggestions([]);
                setIsOpen(false);
            }
        } catch (err) {
            setSuggestions([]);
            setIsOpen(false);
        }
    }, [tenantId, locale]);

    // Handle input change with debounced (~150ms) typeahead
    const handleInputChange = (e) => {
        const val = e.target.value;
        setInputValue(val);
        if (autocompleteTimerRef.current) {
            clearTimeout(autocompleteTimerRef.current);
        }
        if (val.trim().length >= 2) {
            autocompleteTimerRef.current = setTimeout(() => {
                fetchAutocomplete(val);
            }, 150);
        } else {
            setSuggestions([]);
            setIsOpen(false);
            setSelectedIndex(-1);
        }
    };

    const handleSelectSuggestion = (suggestionText) => {
        setInputValue(suggestionText);
        setIsOpen(false);
        setSelectedIndex(-1);
        onSearch(suggestionText, {
            alpha: localAlpha,
            channel,
            onlyInStock,
            tenantId,
            locale,
            cep,
            maxSla24h,
            ltr,
            parseIntent,
            priceTableId,
            sortBy,
        });
    };

    const handleCepChange = (e) => {
        const raw = e.target.value.replace(/\D/g, '').slice(0, 8);
        let formatted = raw;
        if (raw.length > 5) {
            formatted = `${raw.slice(0, 5)}-${raw.slice(5)}`;
        }
        setCep(formatted);
        if (raw.length > 0 && raw.length < 8) {
            setCepError('CEP deve ter 8 dígitos');
        } else {
            setCepError('');
        }
    };

    // Submit search only when user explicitly requests it
    const handleSubmitSearch = () => {
        setIsOpen(false);
        setSelectedIndex(-1);
        const raw = (cep || '').replace(/\D/g, '');
        if (raw.length > 0 && raw.length < 8) {
            setCepError('CEP deve ter 8 dígitos');
        }
        onSearch(inputValue, {
            alpha: localAlpha,
            channel,
            onlyInStock,
            tenantId,
            locale,
            cep,
            maxSla24h,
            ltr,
            parseIntent,
            priceTableId,
            sortBy,
        });
    };

    // Handle keyboard navigation (up/down/enter/esc)
    const handleKeyDown = (event) => {
        if (event.key === 'ArrowDown') {
            if (isOpen && suggestions.length > 0) {
                event.preventDefault();
                setSelectedIndex(prev => (prev < suggestions.length - 1 ? prev + 1 : 0));
            }
        } else if (event.key === 'ArrowUp') {
            if (isOpen && suggestions.length > 0) {
                event.preventDefault();
                setSelectedIndex(prev => (prev > 0 ? prev - 1 : suggestions.length - 1));
            }
        } else if (event.key === 'Enter') {
            if (isOpen && selectedIndex >= 0 && selectedIndex < suggestions.length) {
                event.preventDefault();
                const sel = suggestions[selectedIndex];
                const text = typeof sel === 'object' ? sel.text : sel;
                handleSelectSuggestion(text);
            } else {
                handleSubmitSearch();
            }
        } else if (event.key === 'Escape') {
            setIsOpen(false);
            setSelectedIndex(-1);
        }
    };

    // Close suggestions dropdown on outside click
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (containerRef.current && !containerRef.current.contains(event.target)) {
                setIsOpen(false);
                setSelectedIndex(-1);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => {
            document.removeEventListener('mousedown', handleClickOutside);
        };
    }, []);

    // Clean up timers on unmount
    useEffect(() => {
        return () => {
            if (autocompleteTimerRef.current) {
                clearTimeout(autocompleteTimerRef.current);
            }
            if (debounceTimerRef.current) {
                clearTimeout(debounceTimerRef.current);
            }
        };
    }, []);

    return (
        <Paper variant="outlined" sx={{ p: 2.5, mb: 3, borderRadius: 2 }}>
            <Box sx={{ mb: 2, position: 'relative' }} ref={containerRef}>
                <TextField
                    fullWidth
                    label="Search products"
                    variant="outlined"
                    value={inputValue}
                    onChange={handleInputChange}
                    onKeyDown={handleKeyDown}
                    autoComplete="off"
                    InputProps={{
                        endAdornment: (
                            <InputAdornment position="end">
                                <Button
                                    variant="contained"
                                    color="primary"
                                    onClick={handleSubmitSearch}
                                    startIcon={<SearchIcon />}
                                >
                                    Search
                                </Button>
                            </InputAdornment>
                        )
                    }}
                />
                {isOpen && suggestions.length > 0 && (
                    <Paper
                        elevation={4}
                        sx={{
                            position: 'absolute',
                            top: '100%',
                            left: 0,
                            right: 0,
                            zIndex: 1300,
                            mt: 0.5,
                            maxHeight: 320,
                            overflowY: 'auto',
                            border: '1px solid',
                            borderColor: 'divider',
                            borderRadius: 1,
                        }}
                    >
                        {suggestions.map((suggestion, index) => {
                            const isSelected = selectedIndex === index;
                            const text = typeof suggestion === 'object' ? suggestion.text : suggestion;
                            return (
                                <Box
                                    key={index}
                                    onClick={() => handleSelectSuggestion(text)}
                                    sx={{
                                        px: 2,
                                        py: 1.25,
                                        cursor: 'pointer',
                                        backgroundColor: isSelected ? 'action.selected' : 'inherit',
                                        '&:hover': {
                                            backgroundColor: 'action.hover',
                                        },
                                        display: 'flex',
                                        justifyContent: 'space-between',
                                        alignItems: 'center',
                                        borderBottom: index < suggestions.length - 1 ? '1px solid' : 'none',
                                        borderColor: 'divider',
                                    }}
                                >
                                    <Typography variant="body2" sx={{ fontWeight: isSelected ? 600 : 400 }}>
                                        {text}
                                    </Typography>
                                    {typeof suggestion === 'object' && suggestion.locale && (
                                        <Typography variant="caption" color="text.secondary">
                                            {suggestion.locale}
                                        </Typography>
                                    )}
                                </Box>
                            );
                        })}
                    </Paper>
                )}
            </Box>

            <Box sx={{ display: 'flex', alignItems: 'center', gap: 3, flexWrap: 'wrap', justifyContent: 'space-between' }}>
                {/* Alpha Slider */}
                <Box sx={{ minWidth: 260, flex: '1 1 260px', maxWidth: 420 }}>
                    <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 0.5 }}>
                        <Typography variant="caption" color="text.secondary">0.0 (Textual)</Typography>
                        <Typography variant="caption" sx={{ fontWeight: 600 }}>α = {Number(localAlpha).toFixed(1)}</Typography>
                        <Typography variant="caption" color="text.secondary">1.0 (Vector)</Typography>
                    </Box>
                    <Slider
                        value={Number(localAlpha)}
                        min={0.0}
                        max={1.0}
                        step={0.1}
                        marks={[
                            { value: 0.0, label: '0.0' },
                            { value: 0.5, label: '0.5' },
                            { value: 1.0, label: '1.0' },
                        ]}
                        valueLabelDisplay="auto"
                        onChange={(_, val) => setLocalAlpha(val)}
                        onChangeCommitted={(_, val) => {
                            setLocalAlpha(val);
                            setAlpha(val);
                        }}
                    />
                </Box>

                {/* Channel Selector */}
                <FormControl size="small" sx={{ minWidth: 140 }}>
                    <InputLabel id="channel-select-label">Channel</InputLabel>
                    <Select
                        labelId="channel-select-label"
                        id="channel-select"
                        value={channel}
                        label="Channel"
                        onChange={(e) => setChannel(e.target.value)}
                    >
                        <MenuItem value="">All</MenuItem>
                        <MenuItem value="web">web</MenuItem>
                        <MenuItem value="mobile">mobile</MenuItem>
                        <MenuItem value="store">store</MenuItem>
                    </Select>
                </FormControl>

                {/* Tabela de Preço Selector */}
                <FormControl size="small" sx={{ minWidth: 160 }}>
                    <InputLabel id="price-table-select-label">Tabela de preço</InputLabel>
                    <Select
                        labelId="price-table-select-label"
                        id="price-table-select"
                        value={priceTableId}
                        label="Tabela de preço"
                        onChange={(e) => setPriceTableId(e.target.value)}
                    >
                        <MenuItem value="">Varejo Padrão</MenuItem>
                        <MenuItem value="b2b-corporate">B2B Corporate</MenuItem>
                        <MenuItem value="b2b-gold">B2B Gold</MenuItem>
                        <MenuItem value="vip-club">VIP Club</MenuItem>
                    </Select>
                </FormControl>

                {/* Ordenar por Selector */}
                <FormControl size="small" sx={{ minWidth: 150 }}>
                    <InputLabel id="sort-by-select-label">Ordenar por</InputLabel>
                    <Select
                        labelId="sort-by-select-label"
                        id="sort-by-select"
                        value={sortBy}
                        label="Ordenar por"
                        onChange={(e) => setSortBy(e.target.value)}
                    >
                        <MenuItem value="">Relevância</MenuItem>
                        <MenuItem value="price_asc">Menor preço</MenuItem>
                        <MenuItem value="price_desc">Maior preço</MenuItem>
                    </Select>
                </FormControl>

                {/* CEP Input */}
                <TextField
                    size="small"
                    label="Calcular Entrega"
                    placeholder="01310-100"
                    value={cep}
                    onChange={handleCepChange}
                    onKeyDown={handleKeyDown}
                    error={Boolean(cepError)}
                    helperText={cepError || undefined}
                    inputProps={{ maxLength: 9 }}
                    sx={{ minWidth: 150, maxWidth: 170 }}
                />

                {/* Full 24h Toggle */}
                <FormControlLabel
                    control={
                        <Switch
                            checked={Boolean(maxSla24h)}
                            onChange={(e) => setMaxSla24h(e.target.checked)}
                            color="primary"
                        />
                    }
                    label="Full 24h"
                />

                {/* Only in Stock Toggle */}
                <FormControlLabel
                    control={
                        <Switch
                            checked={Boolean(onlyInStock)}
                            onChange={(e) => setOnlyInStock(e.target.checked)}
                            color="primary"
                        />
                    }
                    label="Only in stock"
                />

                {/* Re-rank (LTR) Toggle */}
                <FormControlLabel
                    control={
                        <Switch
                            checked={Boolean(ltr)}
                            onChange={(e) => setLtr(e.target.checked)}
                            color="primary"
                        />
                    }
                    label="Re-rank (LTR)"
                />

                {/* Smart query Toggle */}
                <FormControlLabel
                    control={
                        <Switch
                            checked={Boolean(parseIntent)}
                            onChange={(e) => setParseIntent(e.target.checked)}
                            color="primary"
                        />
                    }
                    label="Smart query"
                />
            </Box>
        </Paper>
    );
});

export default SearchInput;
