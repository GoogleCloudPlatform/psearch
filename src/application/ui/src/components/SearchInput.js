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

import React, { useState, useEffect, useRef, memo } from 'react';
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
}) => {
    const [inputValue, setInputValue] = useState('');
    const [internalAlpha, setInternalAlpha] = useState(0.5);
    const [internalChannel, setInternalChannel] = useState('');
    const [internalOnlyInStock, setInternalOnlyInStock] = useState(false);
    const [internalTenantId] = useState('default');
    const [internalLocale] = useState('');
    const [internalCep, setInternalCep] = useState('');
    const [internalMaxSla24h, setInternalMaxSla24h] = useState(false);
    const [cepError, setCepError] = useState('');
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

    // Local slider value for responsive dragging
    const [localAlpha, setLocalAlpha] = useState(alpha);

    useEffect(() => {
        setLocalAlpha(alpha);
    }, [alpha]);

    // Handle input change without propagating all changes to parent
    const handleInputChange = (e) => {
        setInputValue(e.target.value);
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
            maxSla24h
        });
    };

    // Handle Enter key press
    const handleKeyDown = (event) => {
        if (event.key === 'Enter') {
            handleSubmitSearch();
        }
    };

    // Clean up timer on unmount
    useEffect(() => {
        return () => {
            if (debounceTimerRef.current) {
                clearTimeout(debounceTimerRef.current);
            }
        };
    }, []);

    return (
        <Paper variant="outlined" sx={{ p: 2.5, mb: 3, borderRadius: 2 }}>
            <Box sx={{ mb: 2 }}>
                <TextField
                    fullWidth
                    label="Search products"
                    variant="outlined"
                    value={inputValue}
                    onChange={handleInputChange}
                    onKeyDown={handleKeyDown}
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
            </Box>
        </Paper>
    );
});

export default SearchInput;
