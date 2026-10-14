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
}) => {
    const [inputValue, setInputValue] = useState('');
    const [internalAlpha, setInternalAlpha] = useState(0.5);
    const [internalChannel, setInternalChannel] = useState('');
    const [internalOnlyInStock, setInternalOnlyInStock] = useState(false);
    const debounceTimerRef = useRef(null);

    const alpha = controlledAlpha !== undefined ? controlledAlpha : internalAlpha;
    const setAlpha = controlledSetAlpha || setInternalAlpha;
    const channel = controlledChannel !== undefined ? controlledChannel : internalChannel;
    const setChannel = controlledSetChannel || setInternalChannel;
    const onlyInStock = controlledOnlyInStock !== undefined ? controlledOnlyInStock : internalOnlyInStock;
    const setOnlyInStock = controlledSetOnlyInStock || setInternalOnlyInStock;

    // Local slider value for responsive dragging
    const [localAlpha, setLocalAlpha] = useState(alpha);

    useEffect(() => {
        setLocalAlpha(alpha);
    }, [alpha]);

    // Handle input change without propagating all changes to parent
    const handleInputChange = (e) => {
        setInputValue(e.target.value);
    };

    // Submit search only when user explicitly requests it
    const handleSubmitSearch = () => {
        onSearch(inputValue, { alpha: localAlpha, channel, onlyInStock });
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
