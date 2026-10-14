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

import React, { useState, useMemo, useCallback, useEffect, memo } from 'react';
import { BrowserRouter as Router, Routes, Route, Link, useNavigate } from 'react-router-dom';
import {
    Container,
    Grid,
    TextField,
    Card,
    CardContent,
    Typography,
    Box,
    Chip,
    AppBar,
    Toolbar,
    Button,
    FormControl,
    InputLabel,
    Select,
    MenuItem,
    Alert,
    ThemeProvider,
    createTheme,
} from '@mui/material';
import SearchIcon from '@mui/icons-material/Search';
import SettingsIcon from '@mui/icons-material/Settings';
import FileUploadIcon from '@mui/icons-material/FileUpload';
import RuleManager from './components/RuleManager';
import ProductDetails from './components/ProductDetails';
import SourceIngestion from './components/SourceIngestion';
import Filters from './components/Filters'; // Import the custom filters component
import AIFilterSuggestion from './components/AIFilterSuggestion'; // Import the AI filter suggestion component
import ProductImage from './components/ProductImage'; // Import the optimized ProductImage component
import SearchInput from './components/SearchInput'; // Import the new isolated search component
import axios from 'axios';
import config from './config';
const API_URL = config.apiUrl;

function ProductSearch({
    tenantId: propTenantId,
    setTenantId: propSetTenantId,
    locale: propLocale,
    setLocale: propSetLocale,
}) {
    const navigate = useNavigate();
    // Simplified search state - only track the active search query
    const [activeSearchQuery, setActiveSearchQuery] = useState(''); // For actual API requests
    const [showAIFilterSuggestions, setShowAIFilterSuggestions] = useState(false); // Control visibility of AI filter suggestions
    const [products, setProducts] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [alpha, setAlpha] = useState(0.5);
    const [channel, setChannel] = useState('');
    const [onlyInStock, setOnlyInStock] = useState(false);
    const [cep, setCep] = useState('');
    const [maxSla24h, setMaxSla24h] = useState(false);
    const [ltr, setLtr] = useState(false);
    const [effectiveLtr, setEffectiveLtr] = useState(false);
    const [ltrModel, setLtrModel] = useState(null);
    const [ltrLatencyUs, setLtrLatencyUs] = useState(null);
    const [internalTenantId, setInternalTenantId] = useState('default');
    const [internalLocale, setInternalLocale] = useState('');
    const tenantId = propTenantId !== undefined ? propTenantId : internalTenantId;
    const setTenantId = propSetTenantId || setInternalTenantId;
    const locale = propLocale !== undefined ? propLocale : internalLocale;
    const setLocale = propSetLocale || setInternalLocale;
    const [effectiveAlpha, setEffectiveAlpha] = useState(null);
    const [retrievalMode, setRetrievalMode] = useState(null);
    const [effectiveTenantId, setEffectiveTenantId] = useState(null);
    const [effectiveLocale, setEffectiveLocale] = useState(null);
    const [expandedTerms, setExpandedTerms] = useState([]);
    const [appliedRules, setAppliedRules] = useState([]);
    const [serverFacets, setServerFacets] = useState(null);
    const [facetsLatencyMs, setFacetsLatencyMs] = useState(null);
    // State for the filters component - now a dynamic object
    const [selectedFilters, setSelectedFilters] = useState({});
    // Store filter configurations
    const [filterConfigs, setFilterConfigs] = useState([]);
    const [currentPage, setCurrentPage] = useState(1);
    const productsPerPage = 24; // Increased from 12 to show more products per page

    // Format a filter name for display (convert lens_color to Lens Color)
    const formatFilterName = (name) => {
        return name
            .split('_')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase())
            .join(' ');
    };

    // Generate filter configurations based on product data
    const generateFilterConfigs = useCallback((products) => {
        if (!products || products.length === 0) {
            return [];
        }

        const configs = [];
        // Track filter IDs to prevent duplicates
        const addedFilterIds = new Set();

        const filterAdders = [
            // Categories filter
            () => {
                const allCategories = new Set();
                products.forEach(product => {
                    if (product.categories) {
                        product.categories.forEach(category => allCategories.add(category));
                    }
                });

                const categories = Array.from(allCategories);
                if (categories.length > 0) {
                    configs.push({
                        id: 'categories',
                        title: 'Department',
                        priority: 100, // High priority
                        options: categories.map(cat => ({ label: cat, value: cat })),
                        type: 'checkbox'
                    });
                }
            },

            // Brands filter
            () => {
                const allBrands = new Set();
                products.forEach(product => {
                    if (product.brands) {
                        product.brands.forEach(brand => allBrands.add(brand));
                    }
                    // Also check attributes for brand if top-level is missing
                    if (!product.brands && product.attributes) {
                        const brandAttr = product.attributes.find(attr => attr.key === 'brand');
                        if (brandAttr && brandAttr.value?.text) {
                            brandAttr.value.text.forEach(b => allBrands.add(b));
                        }
                    }
                });

                const brands = Array.from(allBrands);
                if (brands.length > 0) {
                    configs.push({
                        id: 'brands',
                        title: 'Brands',
                        priority: 90, // High priority
                        options: brands.map(brand => ({ label: brand, value: brand })),
                        type: 'checkbox'
                    });
                }
            },

            // Price ranges filter
            () => {
                const validPrices = products
                    .map(p => p.priceInfo?.price)
                    .filter(price => price !== null && price !== undefined && !isNaN(parseFloat(price)))
                    .map(price => parseFloat(price));

                if (validPrices.length < 2) return; // Need at least two prices to make ranges

                const minPrice = Math.min(...validPrices);
                const maxPrice = Math.max(...validPrices);

                if (minPrice === maxPrice) {
                    configs.push({
                        id: 'prices',
                        title: 'Price',
                        priority: 80, // High priority
                        options: [{ label: `$${minPrice.toFixed(2)}`, value: `${minPrice}-${minPrice}` }],
                        type: 'checkbox'
                    });
                    return;
                }

                const numRanges = 4; // Define number of ranges
                const step = (maxPrice - minPrice) / numRanges;
                const ranges = [];

                for (let i = 0; i < numRanges; i++) {
                    const rangeMin = minPrice + i * step;
                    const rangeMax = minPrice + (i + 1) * step;
                    // Ensure the last range includes the max price
                    const finalMax = (i === numRanges - 1) ? maxPrice : rangeMax;

                    ranges.push({
                        // Use Math.floor/ceil to avoid overlapping ranges due to floating point issues
                        label: `$${Math.floor(rangeMin).toFixed(2)} - $${Math.ceil(finalMax).toFixed(2)}`,
                        value: `${Math.floor(rangeMin)}-${Math.ceil(finalMax)}`
                    });
                }

                configs.push({
                    id: 'prices',
                    title: 'Price',
                    options: ranges,
                    type: 'checkbox'
                });
            },

            // Colors filter
            () => {
                const allColors = new Set();
                products.forEach(product => {
                    if (product.colorInfo?.colors) {
                        product.colorInfo.colors.forEach(color => allColors.add(color));
                    }
                    if (product.colorInfo?.colorFamilies) {
                        product.colorInfo.colorFamilies.forEach(color => allColors.add(color));
                    }
                });

                const colors = Array.from(allColors);
                if (colors.length > 0) {
                    configs.push({
                        id: 'colors',
                        title: 'Colors',
                        priority: 70, // Medium-high priority
                        options: colors.map(color => ({ label: color, value: color })),
                        type: 'checkbox'
                    });
                }
            },

            // Sizes filter
            () => {
                const allSizes = new Set();
                products.forEach(product => {
                    if (product.sizes) {
                        product.sizes.forEach(size => allSizes.add(size));
                    }
                });

                const sizes = Array.from(allSizes);
                if (sizes.length > 0) {
                    configs.push({
                        id: 'sizes',
                        title: 'Sizes',
                        priority: 60, // Medium priority
                        options: sizes.map(size => ({ label: size, value: size })),
                        type: 'checkbox'
                    });
                }
            },

            // Availability filter
            () => {
                const availabilityOptions = [
                    { label: 'In Stock', value: 'IN_STOCK' },
                    { label: 'Out of Stock', value: 'OUT_OF_STOCK' }
                ];

                // Only add if both statuses are present
                const hasInStock = products.some(p => p.availability === 'IN_STOCK');
                const hasOutOfStock = products.some(p => p.availability === 'OUT_OF_STOCK');

                if (hasInStock && hasOutOfStock) {
                    configs.push({
                        id: 'availability',
                        title: 'Availability',
                        priority: 50, // Medium priority
                        options: availabilityOptions,
                        type: 'checkbox'
                    });
                }
            },

            // Dynamic attributes filter
            () => {
                // Find common attribute keys that appear in multiple products
                const attributeKeys = new Map();

                products.forEach(product => {
                    if (product.attributes) {
                        product.attributes.forEach(attr => {
                            if (attr.key && attr.value) {
                                const count = attributeKeys.get(attr.key) || 0;
                                attributeKeys.set(attr.key, count + 1);
                            }
                        });
                    }
                });

                // For each common attribute, create a filter
                attributeKeys.forEach((count, key) => {
                    // Skip if it only appears in one product or is already used (like brand)
                    if (count < 2 || key === 'brand') return;

                    // Get all values for this attribute
                    const values = new Set();
                    products.forEach(product => {
                        if (product.attributes) {
                            const attr = product.attributes.find(a => a.key === key);
                            if (attr && attr.value?.text) {
                                attr.value.text.forEach(val => values.add(val));
                            }
                        }
                    });

                    if (values.size > 0) {
                        configs.push({
                            id: `attr_${key}`,
                            title: formatFilterName(key),
                            priority: 40, // Lower priority for dynamic attributes
                            options: Array.from(values).map(value => ({ label: value, value })),
                            type: 'checkbox'
                        });
                    }
                });
            }
        ];

        // Apply all filter adders
        filterAdders.forEach(adder => adder());

        // Remove any duplicate filters by ID 
        let uniqueConfigs = configs.filter(config => {
            if (addedFilterIds.has(config.id)) {
                return false;
            }
            addedFilterIds.add(config.id);
            return true;
        });

        // Prepare the final filter list
        const finalFilters = [];

        // 1. Ensure Department filter is included
        // Department is typically derived from categories
        const categoryFilter = uniqueConfigs.find(config => config.id === 'categories');
        if (categoryFilter) {
            // Rename to Department for better user understanding
            categoryFilter.title = 'Department';
            finalFilters.push(categoryFilter);
            // Remove from uniqueConfigs to avoid duplication
            uniqueConfigs = uniqueConfigs.filter(config => config.id !== 'categories');
        }

        // 2. First ensure Price filter is included (if available)
        const priceFilter = uniqueConfigs.find(config => config.id === 'prices');
        if (priceFilter) {
            finalFilters.push(priceFilter);
            // Remove from uniqueConfigs to avoid duplication
            uniqueConfigs = uniqueConfigs.filter(config => config.id !== 'prices');
        }

        // 3. Sort remaining filters by priority and add up
        const remainingSlots = 10 - finalFilters.length;
        if (remainingSlots > 0 && uniqueConfigs.length > 0) {
            const remainingFilters = uniqueConfigs
                .sort((a, b) => (b.priority || 0) - (a.priority || 0))
                .slice(0, remainingSlots);

            finalFilters.push(...remainingFilters);
        }

        return finalFilters;
    }, []);

    // Generate filter configurations from server-side facets
    const generateFilterConfigsFromFacets = useCallback((facets) => {
        if (!facets || typeof facets !== 'object') return [];

        const configs = [];

        if (Array.isArray(facets.categories) && facets.categories.length > 0) {
            configs.push({
                id: 'categories',
                title: 'Department',
                priority: 100,
                type: 'checkbox',
                options: facets.categories.map(c => ({
                    label: typeof c === 'object' ? c.value : c,
                    value: typeof c === 'object' ? c.value : c,
                    count: typeof c === 'object' ? c.count : undefined,
                }))
            });
        }

        if (Array.isArray(facets.brands) && facets.brands.length > 0) {
            configs.push({
                id: 'brands',
                title: 'Brands',
                priority: 90,
                type: 'checkbox',
                options: facets.brands.map(b => ({
                    label: typeof b === 'object' ? b.value : b,
                    value: typeof b === 'object' ? b.value : b,
                    count: typeof b === 'object' ? b.count : undefined,
                }))
            });
        }

        if (Array.isArray(facets.price_ranges) && facets.price_ranges.length > 0) {
            configs.push({
                id: 'prices',
                title: 'Price',
                priority: 80,
                type: 'checkbox',
                options: facets.price_ranges.map(p => {
                    const val = typeof p === 'object' ? p.value : p;
                    const count = typeof p === 'object' ? p.count : undefined;
                    let label = val;
                    if (typeof val === 'string') {
                        if (val.endsWith('+')) {
                            label = `$${val.slice(0, -1)}+`;
                        } else if (val.includes('-')) {
                            const parts = val.split('-');
                            label = `$${parts[0]} - $${parts[1]}`;
                        }
                    }
                    return {
                        label,
                        value: val,
                        count,
                    };
                })
            });
        }

        if (Array.isArray(facets.availability) && facets.availability.length > 0) {
            configs.push({
                id: 'availability',
                title: 'Availability',
                priority: 70,
                type: 'checkbox',
                options: facets.availability.map(a => {
                    const val = typeof a === 'object' ? a.value : a;
                    const count = typeof a === 'object' ? a.count : undefined;
                    let label = val;
                    let optVal = val;
                    if (val === 'IN_STOCK' || val === true || val === 'true') {
                        label = 'In Stock';
                        optVal = 'IN_STOCK';
                    } else if (val === 'OUT_OF_STOCK' || val === false || val === 'false') {
                        label = 'Out of Stock';
                        optVal = 'OUT_OF_STOCK';
                    }
                    return {
                        label,
                        value: optVal,
                        count,
                    };
                })
            });
        }

        return configs;
    }, []);

    // Use memoized filter configurations: prioritize serverFacets, fall back to generateFilterConfigs
    useEffect(() => {
        if (serverFacets) {
            const facetConfigs = generateFilterConfigsFromFacets(serverFacets);
            if (facetConfigs.length > 0) {
                setFilterConfigs(facetConfigs);
                return;
            }
        }
        const configs = generateFilterConfigs(products);
        setFilterConfigs(configs);
    }, [products, serverFacets, generateFilterConfigs, generateFilterConfigsFromFacets]);

    // The ProductImage component has been moved to its own file for better performance

    const selectedCategory = (selectedFilters.categories && selectedFilters.categories.length > 0)
        ? selectedFilters.categories[0]
        : '';
    const selectedBrand = (selectedFilters.brands && selectedFilters.brands.length > 0)
        ? selectedFilters.brands[0]
        : '';

    const fetchProducts = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const payload = {
                query: activeSearchQuery,
                limit: 300,
                min_score: 0.01,
                alpha: typeof alpha === 'number' ? alpha : 0.5,
                only_in_stock: Boolean(onlyInStock),
                tenant_id: tenantId || 'default',
            };
            if (locale) {
                payload.locale = locale;
            }
            if (channel && channel !== 'All' && channel !== 'all') {
                payload.channel = channel;
            }
            if (selectedCategory) {
                payload.category = selectedCategory;
            }
            if (selectedBrand) {
                payload.brand = selectedBrand;
            }
            if (cep && cep.trim()) {
                payload.cep = cep.trim();
            }
            if (maxSla24h) {
                payload.max_sla_hours = 24;
            }
            if (ltr) {
                payload.ltr = true;
            }

            const response = await axios.post(`${API_URL}/search`, payload);

            console.log("API Response:", JSON.stringify(response.data, null, 2));

            if (response.data) {
                if (response.data.effective_alpha !== undefined) {
                    setEffectiveAlpha(response.data.effective_alpha);
                }
                if (response.data.retrieval_mode !== undefined) {
                    setRetrievalMode(response.data.retrieval_mode);
                }
                if (response.data.tenant_id !== undefined) {
                    setEffectiveTenantId(response.data.tenant_id);
                }
                if (response.data.locale !== undefined) {
                    setEffectiveLocale(response.data.locale);
                }
                if (response.data.expanded_terms && Array.isArray(response.data.expanded_terms)) {
                    setExpandedTerms(response.data.expanded_terms);
                } else {
                    setExpandedTerms([]);
                }
                if (response.data.applied_rules && Array.isArray(response.data.applied_rules)) {
                    setAppliedRules(response.data.applied_rules);
                } else {
                    setAppliedRules([]);
                }
                if (response.data.facets && Object.keys(response.data.facets).length > 0) {
                    setServerFacets(response.data.facets);
                } else {
                    setServerFacets(null);
                }
                const fLat = response.data.facets_latency_ms !== undefined
                    ? response.data.facets_latency_ms
                    : (response.data.facets && response.data.facets.latency_ms);
                if (fLat !== undefined && fLat !== null) {
                    setFacetsLatencyMs(fLat);
                } else {
                    setFacetsLatencyMs(null);
                }
                if (response.data.ltr !== undefined) {
                    setEffectiveLtr(Boolean(response.data.ltr));
                } else {
                    setEffectiveLtr(false);
                }
                if (response.data.ltr_model !== undefined) {
                    setLtrModel(response.data.ltr_model);
                } else {
                    setLtrModel(null);
                }
                if (response.data.ltr_latency_us !== undefined) {
                    setLtrLatencyUs(response.data.ltr_latency_us);
                } else {
                    setLtrLatencyUs(null);
                }
            }

            // Simple processing - just map products without modifying anything
            const processedResults = response.data.results || [];

            // Debug: Print the first product's image URL to console
            if (processedResults.length > 0 && processedResults[0].images && processedResults[0].images.length > 0) {
                console.log("First product image URL:", processedResults[0].images[0].uri);
            }

            console.log("Products to display:", processedResults.length);

            // Store only necessary product data (without images) in localStorage for details page
            const productsForStorage = processedResults.map(product => {
                // Create a copy of the product without the images array to reduce storage size
                const { images, ...productWithoutImages } = product;
                return {
                    ...productWithoutImages,
                    // Store just a reference to the first image URL if available
                    primaryImageUrl: images && images.length > 0 ? images[0].uri : null
                };
            });
            localStorage.setItem('searchResults', JSON.stringify(productsForStorage));

            // Set products and reset to first page
            setProducts(processedResults);
            setCurrentPage(1);

            // Show AI filter suggestions if we have products and a search query
            setShowAIFilterSuggestions(processedResults.length > 0 && activeSearchQuery.trim() !== '');
        } catch (error) {
            const serverErrMsg = error.response?.data?.error || error.response?.data?.message;
            const displayError = serverErrMsg || error.message || 'An error occurred while searching';
            setError(displayError);
            setProducts([]);
            console.error("Search error:", error);
        } finally {
            setLoading(false);
        }
    }, [activeSearchQuery, alpha, channel, onlyInStock, selectedCategory, selectedBrand, tenantId, locale, cep, maxSla24h, ltr]);

    // Handler for filter changes from the Filters component - now works with dynamic filter IDs
    const handleFilterChange = useCallback((filterId, value, isChecked) => {
        setSelectedFilters(prevFilters => {
            const currentSelection = prevFilters[filterId] || [];
            let newSelection;
            if (isChecked) {
                newSelection = [...currentSelection, value];
            } else {
                newSelection = currentSelection.filter(item => item !== value);
            }
            return {
                ...prevFilters,
                [filterId]: newSelection,
            };
        });
        setCurrentPage(1); // Reset to first page when filters change
    }, []);

    // Dynamic filter logic with optimized memory usage
    const filteredProducts = useMemo(() => {
        if (!products.length || !Object.keys(selectedFilters).length) {
            return products;
        }

        // First identify which filter types are actually being used to avoid unnecessary processing
        const activeFilters = Object.entries(selectedFilters).filter(
            ([_, values]) => values && values.length > 0
        );

        // If no active filters, return all products
        if (activeFilters.length === 0) {
            return products;
        }

        // Create filter functions for each active filter type
        // This avoids recreating them for each product
        const filterFunctions = activeFilters.map(([filterId, selectedValues]) => {
            switch (filterId) {
                case 'categories':
                    return product =>
                        product.categories &&
                        product.categories.some(cat => selectedValues.includes(cat));

                case 'brands':
                    return product => {
                        // Check direct brands first
                        if (product.brands && product.brands.some(brand => selectedValues.includes(brand))) {
                            return true;
                        }
                        // Check attribute brands as fallback
                        if (product.attributes) {
                            const brandAttr = product.attributes.find(attr => attr.key === 'brand');
                            return brandAttr && brandAttr.value?.text &&
                                brandAttr.value.text.some(b => selectedValues.includes(b));
                        }
                        return false;
                    };

                case 'prices':
                    // Pre-parse the price ranges to avoid repeated parsing
                    const priceRanges = selectedValues.map(range => {
                        if (typeof range === 'string' && range.endsWith('+')) {
                            const min = parseFloat(range.slice(0, -1));
                            return { min, max: Infinity };
                        }
                        const [min, max] = String(range).split('-').map(parseFloat);
                        return { min: isNaN(min) ? 0 : min, max: isNaN(max) ? Infinity : max };
                    });

                    return product => {
                        const price = product.priceInfo?.price ? parseFloat(product.priceInfo.price) : null;
                        if (price === null) return false;
                        return priceRanges.some(range => price >= range.min && price <= range.max);
                    };

                case 'colors':
                    return product => {
                        const colorInfo = product.colorInfo;
                        if (!colorInfo) return false;

                        // Check direct colors
                        if (colorInfo.colors && colorInfo.colors.some(color => selectedValues.includes(color))) {
                            return true;
                        }
                        // Check color families
                        return colorInfo.colorFamilies &&
                            colorInfo.colorFamilies.some(color => selectedValues.includes(color));
                    };

                case 'sizes':
                    return product =>
                        product.sizes &&
                        product.sizes.some(size => selectedValues.includes(size));

                case 'availability':
                    return product => selectedValues.includes(product.availability);

                default:
                    // Handle dynamic attribute filters (attr_*)
                    if (filterId.startsWith('attr_')) {
                        const attrKey = filterId.substring(5); // Remove 'attr_' prefix
                        return product => {
                            if (!product.attributes) return false;
                            const attr = product.attributes.find(a => a.key === attrKey);
                            return attr && attr.value?.text &&
                                attr.value.text.some(val => selectedValues.includes(val));
                        };
                    }

                    // Default behavior for unknown filter types
                    return () => true;
            }
        });

        // Apply all filter functions to each product
        return products.filter(product =>
            filterFunctions.every(filterFn => filterFn(product))
        );
    }, [products, selectedFilters]);

    // Handle search submission from SearchInput component - now resets all filters
    const handleSearch = useCallback((searchText, options = {}) => {
        // Reset all filters when a new search is performed
        setSelectedFilters({});
        setServerFacets(null);
        setFacetsLatencyMs(null);
        setExpandedTerms([]);
        setAppliedRules([]);
        if (options.alpha !== undefined) {
            setAlpha(options.alpha);
        }
        if (options.channel !== undefined) {
            setChannel(options.channel);
        }
        if (options.onlyInStock !== undefined) {
            setOnlyInStock(options.onlyInStock);
        }
        if (options.tenantId !== undefined) {
            setTenantId(options.tenantId);
        }
        if (options.locale !== undefined) {
            setLocale(options.locale);
        }
        if (options.cep !== undefined) {
            setCep(options.cep);
        }
        if (options.maxSla24h !== undefined) {
            setMaxSla24h(options.maxSla24h);
        }
        if (options.ltr !== undefined) {
            setLtr(options.ltr);
        }
        // Update active search query which triggers API call
        setActiveSearchQuery(typeof searchText === 'string' ? searchText : searchText?.query || '');
        // Reset AI filter suggestions when a new search is performed
        setShowAIFilterSuggestions(true);
    }, [setTenantId, setLocale]);

    useEffect(() => {
        if (activeSearchQuery.trim()) {
            fetchProducts();
        }

        // Cleanup function to prevent memory leaks
        return () => {
            // Clear any references to large data structures
            setProducts([]);
            setFilterConfigs([]);
            setExpandedTerms([]);
            setAppliedRules([]);
        };
    }, [fetchProducts, activeSearchQuery]);

    const indexOfLastProduct = currentPage * productsPerPage;
    const indexOfFirstProduct = indexOfLastProduct - productsPerPage;
    const currentProducts = useMemo(() => {
        return filteredProducts.slice(indexOfFirstProduct, indexOfLastProduct);
    }, [filteredProducts, indexOfFirstProduct, indexOfLastProduct]);

    const totalPages = Math.ceil(filteredProducts.length / productsPerPage);

    const handlePageChange = (newPage) => {
        setCurrentPage(newPage);
    };


    // Prevent search input changes from causing product grid re-renders
    // Only fetch products when activeSearchQuery changes (not during typing)

    // Stable handler for product clicks
    const handleProductClick = useCallback((productId) => {
        navigate(`/product/${productId}`);
    }, [navigate]);

    // Optimized product grid with exactly 6 products per row
    const ProductGrid = memo(({ products, onProductClick }) => {
        return (
            <Grid 
                container 
                sx={{ 
                    width: '100%', 
                    margin: 0,
                    display: 'flex',
                    flexWrap: 'wrap',
                    marginLeft: '-8px',
                    marginRight: '-8px'
                }}
            >
                {products.map((product) => {
                    // Extract just the minimal image data needed for this specific product
                    // This is done inline instead of processing all products at once
                    const imageUrl = product.images &&
                        Array.isArray(product.images) &&
                        product.images.length > 0 &&
                        product.images[0].uri ?
                        product.images[0].uri :
                        null;

                    return (
                        <Grid 
                            item 
                            key={product.id}
                            sx={{ 
                                width: 'calc(16.666% - 16px)',
                                padding: '8px',
                                boxSizing: 'border-box'
                            }}
                        >
                            <Card
                                sx={{
                                    height: '300px', // Further reduced height for 6-per-row layout
                                    display: 'flex',
                                    flexDirection: 'column',
                                    cursor: 'pointer',
                                    transition: 'box-shadow 0.3s ease',
                                    '&:hover': {
                                        boxShadow: 6,
                                    }
                                }}
                                onClick={() => onProductClick(product.id)}
                            >
                                {/* Each product processes its own image data */}
                                <ProductImage
                                    imageUrl={imageUrl}
                                    productName={product.name}
                                />
                                <CardContent sx={{ 
                                    flexGrow: 1,
                                    display: 'flex',
                                    flexDirection: 'column',
                                    justifyContent: 'space-between',
                                    height: '100px', // Further reduced content height for smaller cards
                                    padding: '8px',
                                    overflow: 'hidden' // Prevent content overflow
                                }}>
                                    <Box>
                                        <Typography 
                                            gutterBottom 
                                            variant="subtitle2" 
                                            component="div"
                                            sx={{
                                                height: '38px', // Reduced height for title
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                display: '-webkit-box',
                                                WebkitLineClamp: 2,
                                                WebkitBoxOrient: 'vertical'
                                            }}
                                        >
                                            {product.name}
                                        </Typography>
                                        <Typography 
                                            variant="body2" 
                                            color="text.secondary" 
                                            gutterBottom
                                            sx={{
                                                height: '40px', // Fixed height for description
                                                overflow: 'hidden',
                                                textOverflow: 'ellipsis',
                                                display: '-webkit-box',
                                                WebkitLineClamp: 2,
                                                WebkitBoxOrient: 'vertical'
                                            }}
                                        >
                                            {product.title}
                                        </Typography>
                                        
                                        {/* Categories with a fixed height container */}
                                        <Box sx={{ 
                                            height: '36px', 
                                            overflowY: 'hidden',
                                            display: 'flex',
                                            flexWrap: 'wrap',
                                            alignItems: 'flex-start'
                                        }}>
                                            {product.categories && product.categories.slice(0, 2).map(category => (
                                                <Chip
                                                    key={category}
                                                    label={category}
                                                    size="small"
                                                    sx={{ mr: 0.5, mb: 0.5 }}
                                                />
                                            ))}
                                        </Box>
                                    </Box>

                                    {/* Price and availability at the bottom */}
                                    <Box sx={{ mt: 'auto' }}>
                                        <Typography variant="h6" color="primary">
                                            ${product.priceInfo?.price}
                                        </Typography>
                                        {product.priceInfo?.originalPrice &&
                                            product.priceInfo.originalPrice !== "0" &&
                                            parseFloat(product.priceInfo.originalPrice) > parseFloat(product.priceInfo.price) && (
                                                <Typography
                                                    variant="body2"
                                                    color="text.secondary"
                                                    sx={{ textDecoration: 'line-through' }}
                                                >
                                                    ${product.priceInfo.originalPrice}
                                                </Typography>
                                            )}
                                        {(() => {
                                            const isStockIn = product.availability === 'IN_STOCK';
                                            const stockBadge = isStockIn
                                                ? (product.availableQuantity !== undefined && product.availableQuantity !== null
                                                    ? `IN_STOCK (${product.availableQuantity} unid.)`
                                                    : 'IN_STOCK')
                                                : 'OUT_OF_STOCK';
                                            const prodLocale = product.locale || product.Locale;
                                            const dp = product.deliveryPromise;
                                            return (
                                                <Box sx={{ display: 'flex', gap: 0.5, alignItems: 'center', flexWrap: 'wrap', mt: 1 }}>
                                                    <Chip
                                                        label={stockBadge}
                                                        color={isStockIn ? 'success' : 'error'}
                                                        size="small"
                                                    />
                                                    {prodLocale && (
                                                        <Chip
                                                            label={prodLocale}
                                                            size="small"
                                                            variant="outlined"
                                                            color="info"
                                                            sx={{ fontWeight: 600, height: 24, fontSize: '0.75rem' }}
                                                        />
                                                    )}
                                                    {dp && dp.label && (
                                                        <Chip
                                                            label={dp.label}
                                                            size="small"
                                                            color="success"
                                                            sx={{ fontWeight: 600, height: 24, fontSize: '0.75rem' }}
                                                        />
                                                    )}
                                                    {dp && dp.free_shipping && (
                                                        <Chip
                                                            label="Frete grátis"
                                                            size="small"
                                                            variant="outlined"
                                                            color="success"
                                                            sx={{ fontWeight: 600, height: 24, fontSize: '0.75rem' }}
                                                        />
                                                    )}
                                                    {(() => {
                                                        const s = product.score || {};
                                                        if (s.stage1_rank !== undefined && s.stage2_rank !== undefined) {
                                                            const diff = s.stage1_rank - s.stage2_rank;
                                                            let rankLabel = `${s.stage1_rank} → ${s.stage2_rank}`;
                                                            if (diff > 0) rankLabel += ` (↑${diff})`;
                                                            else if (diff < 0) rankLabel += ` (↓${Math.abs(diff)})`;
                                                            const color = diff > 0 ? 'success' : diff < 0 ? 'warning' : 'default';
                                                            return (
                                                                <Chip
                                                                    label={rankLabel}
                                                                    size="small"
                                                                    color={color}
                                                                    variant="outlined"
                                                                    sx={{ fontWeight: 600, height: 24, fontSize: '0.75rem' }}
                                                                />
                                                            );
                                                        }
                                                        return null;
                                                    })()}
                                                </Box>
                                            );
                                        })()}
                                    </Box>
                                </CardContent>
                            </Card>
                        </Grid>
                    );
                })}
            </Grid>
        );
    });

    return (
        <Container maxWidth="lg" sx={{ mt: 4 }}>
            <Grid container spacing={3} sx={{ 
                display: 'flex', 
                flexWrap: { xs: 'wrap', sm: 'nowrap' }
            }}>
                {/* Filters Section */}
                <Grid item xs={12} sm={4} md={3} sx={{ 
                    flex: '0 0 auto',
                    maxWidth: { xs: '100%', sm: '33.333%', md: '25%' },
                    display: { xs: 'none', sm: 'block' } 
                }}>
                    <Card sx={{
                        p: 2,
                        boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
                        borderRadius: '8px',
                        position: 'sticky',
                        top: '16px',
                    }}>
                        <Typography
                            variant="h6"
                            component="div"
                            sx={{
                                mb: 3,
                                fontWeight: 600,
                                fontSize: '20px',
                                color: '#212121'
                            }}
                        >
                            Filters
                        </Typography>

                        {/* Render the dynamic Filters component */}
                        <Filters
                            filterConfigs={filterConfigs}
                            selectedFilters={selectedFilters}
                            onFilterChange={handleFilterChange}
                            facetsLatencyMs={facetsLatencyMs}
                        />
                    </Card>
                </Grid>

                {/* Filters Section - Mobile view */}
                <Grid item sx={{ 
                    display: { xs: 'block', sm: 'none' },
                    width: '100%',
                    mb: 2
                }}>
                    <Card sx={{
                        p: 2,
                        boxShadow: '0 1px 3px rgba(0,0,0,0.08)',
                        borderRadius: '8px',
                    }}>
                        <Typography
                            variant="h6"
                            component="div"
                            sx={{
                                mb: 3,
                                fontWeight: 600,
                                fontSize: '20px',
                                color: '#212121'
                            }}
                        >
                            Filters
                        </Typography>

                        {/* Render the dynamic Filters component */}
                        <Filters
                            filterConfigs={filterConfigs}
                            selectedFilters={selectedFilters}
                            onFilterChange={handleFilterChange}
                            facetsLatencyMs={facetsLatencyMs}
                        />
                    </Card>
                </Grid>

                {/* Search and Products Section */}
                <Grid item xs={12} sm={8} md={9} sx={{ 
                    flex: '1 1 auto',
                    maxWidth: { xs: '100%', sm: '66.666%', md: '75%' } 
                }}>
                    {/* Use the isolated SearchInput component */}
                    <SearchInput
                        onSearch={handleSearch}
                        alpha={alpha}
                        setAlpha={setAlpha}
                        channel={channel}
                        setChannel={setChannel}
                        onlyInStock={onlyInStock}
                        setOnlyInStock={setOnlyInStock}
                        tenantId={tenantId}
                        setTenantId={setTenantId}
                        locale={locale}
                        setLocale={setLocale}
                        cep={cep}
                        setCep={setCep}
                        maxSla24h={maxSla24h}
                        setMaxSla24h={setMaxSla24h}
                        ltr={ltr}
                        setLtr={setLtr}
                    />

                    {/* Display search result count and search metadata */}
                    {/* Results container with min-height to prevent layout shifts */}
                    <Box sx={{
                        minHeight: '50vh',
                        transition: 'all 0.3s ease',  /* Add transition for smooth changes */
                    }}>
                        {/* AI Filter Suggestions Component - updated to use filterConfigs */}
                        <AIFilterSuggestion
                            filterConfigs={filterConfigs}
                            selectedFilters={selectedFilters}
                            onFilterChange={handleFilterChange}
                            isVisible={showAIFilterSuggestions}
                            searchQuery={activeSearchQuery}
                            allProducts={products} // Pass all products for smart filtering
                        />

                        <Box sx={{ mb: 2, display: 'flex', alignItems: 'center', justifyContent: 'space-between', flexWrap: 'wrap', gap: 1 }}>
                            <Typography variant="body2" color="text.secondary">
                                {products.length ? `Found ${products.length} products` : 'No products found. Try a search!'}
                                {effectiveAlpha !== null && ` • effective_alpha: ${effectiveAlpha}`}
                                {retrievalMode && ` • retrieval_mode: ${retrievalMode}`}
                                {effectiveTenantId && ` • tenant: ${effectiveTenantId}`}
                                {effectiveLocale && ` • locale: ${effectiveLocale}`}
                                {facetsLatencyMs !== null && ` • facets: ${Number(facetsLatencyMs).toFixed(1)}ms`}
                                {appliedRules && appliedRules.length > 0 && ` • applied_rules: ${appliedRules.join(', ')}`}
                                {effectiveLtr && ltrModel && ` • ltr: ${ltrModel} (${ltrLatencyUs || 0}µs)`}
                            </Typography>
                            <Box sx={{ display: 'flex', gap: 1, alignItems: 'center', flexWrap: 'wrap' }}>
                                {effectiveAlpha !== null && (
                                    <Chip size="small" variant="outlined" label={`effective_alpha: ${effectiveAlpha}`} />
                                )}
                                {retrievalMode && (
                                    <Chip size="small" color="primary" variant="outlined" label={`retrieval_mode: ${retrievalMode}`} />
                                )}
                                {effectiveTenantId && (
                                    <Chip size="small" variant="outlined" label={`tenant: ${effectiveTenantId}`} />
                                )}
                                {effectiveLocale && (
                                    <Chip size="small" color="info" variant="outlined" label={`locale: ${effectiveLocale}`} />
                                )}
                                {facetsLatencyMs !== null && (
                                    <Chip size="small" variant="outlined" label={`facets: ${Number(facetsLatencyMs).toFixed(1)}ms`} />
                                )}
                                {appliedRules && appliedRules.length > 0 && (
                                    <Chip size="small" color="secondary" variant="outlined" label={`applied_rules: ${appliedRules.join(', ')}`} />
                                )}
                                {effectiveLtr && ltrModel && (
                                    <Chip size="small" color="secondary" variant="outlined" label={`ltr_model: ${ltrModel}`} />
                                )}
                                {effectiveLtr && ltrLatencyUs !== null && ltrLatencyUs !== undefined && (
                                    <Chip size="small" variant="outlined" label={`ltr_latency: ${ltrLatencyUs}µs`} />
                                )}
                            </Box>
                        </Box>

                        {expandedTerms && expandedTerms.length > 0 && (
                            <Box sx={{ mb: 1.5, display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
                                <Typography variant="body2" color="text.secondary" sx={{ fontStyle: 'italic' }}>
                                    Also matching: {expandedTerms.join(', ')}
                                </Typography>
                            </Box>
                        )}

                        {appliedRules && appliedRules.length > 0 && (
                            <Box sx={{ mb: 1.5, display: 'flex', alignItems: 'center', gap: 1, flexWrap: 'wrap' }}>
                                <Typography variant="body2" color="text.secondary">
                                    Applied rules: {appliedRules.join(', ')}
                                </Typography>
                            </Box>
                        )}

                        {loading && <Typography>Loading products...</Typography>}
                        {error && (
                            <Alert severity="error" sx={{ mb: 2 }}>
                                {typeof error === 'string' ? error : (error.message || 'An error occurred')}
                            </Alert>
                        )}

                        {/* Use memoized product grid component with stable click handler */}
                        <ProductGrid
                            products={currentProducts}
                            onProductClick={handleProductClick}
                        />
                    </Box>

                    {/* Pagination Controls */}
                    <Box sx={{ display: 'flex', justifyContent: 'center', alignItems: 'center', mt: 2 }}>
                        <Button
                            disabled={currentPage === 1}
                            onClick={() => handlePageChange(currentPage - 1)}
                        >
                            Previous
                        </Button>

                        {/* Advanced pagination with ellipsis */}
                        {(() => {
                            // Pages to always show - first page, last page, and pages around current
                            const pageButtons = [];
                            const showEllipsisStart = currentPage > 4;
                            const showEllipsisEnd = currentPage < totalPages - 3;

                            // Logic for which page numbers to show
                            for (let i = 1; i <= totalPages; i++) {
                                // Always show first and last page
                                if (i === 1 || i === totalPages) {
                                    pageButtons.push(
                                        <Button
                                            key={i}
                                            onClick={() => handlePageChange(i)}
                                            color={currentPage === i ? 'primary' : 'inherit'}
                                            sx={{ minWidth: '36px' }}
                                        >
                                            {i}
                                        </Button>
                                    );
                                    continue;
                                }

                                // Show pages around current page (current-1, current, current+1)
                                if (i >= currentPage - 1 && i <= currentPage + 1) {
                                    pageButtons.push(
                                        <Button
                                            key={i}
                                            onClick={() => handlePageChange(i)}
                                            color={currentPage === i ? 'primary' : 'inherit'}
                                            sx={{ minWidth: '36px' }}
                                        >
                                            {i}
                                        </Button>
                                    );
                                    continue;
                                }

                                // Show early pages if we're not too far
                                if (i < 5 && currentPage < 6) {
                                    pageButtons.push(
                                        <Button
                                            key={i}
                                            onClick={() => handlePageChange(i)}
                                            color={currentPage === i ? 'primary' : 'inherit'}
                                            sx={{ minWidth: '36px' }}
                                        >
                                            {i}
                                        </Button>
                                    );
                                    continue;
                                }

                                // Show late pages if we're close to the end
                                if (i > totalPages - 4 && currentPage > totalPages - 5) {
                                    pageButtons.push(
                                        <Button
                                            key={i}
                                            onClick={() => handlePageChange(i)}
                                            color={currentPage === i ? 'primary' : 'inherit'}
                                            sx={{ minWidth: '36px' }}
                                        >
                                            {i}
                                        </Button>
                                    );
                                    continue;
                                }

                                // Add ellipsis at the start if needed
                                if (i === 2 && showEllipsisStart) {
                                    pageButtons.push(
                                        <Box key="ellipsis-start" sx={{ mx: 1 }}>
                                            ...
                                        </Box>
                                    );
                                }

                                // Add ellipsis at the end if needed
                                if (i === totalPages - 1 && showEllipsisEnd) {
                                    pageButtons.push(
                                        <Box key="ellipsis-end" sx={{ mx: 1 }}>
                                            ...
                                        </Box>
                                    );
                                }
                            }

                            return pageButtons;
                        })()}

                        <Button
                            disabled={currentPage === totalPages || totalPages === 0}
                            onClick={() => handlePageChange(currentPage + 1)}
                        >
                            Next
                        </Button>
                    </Box>
                </Grid>
            </Grid>
        </Container>
    );
}

// Create a theme with Pic2Catalog colors
const theme = createTheme({
    palette: {
        primary: {
            main: '#4285F4', // Blue color from Pic2Catalog
            contrastText: '#fff',
        },
        secondary: {
            main: '#f5f5f5', // Light gray for backgrounds
        },
        background: {
            default: '#f5f5f5',
            paper: '#ffffff',
        },
        text: {
            primary: '#333333',
            secondary: '#666666',
        },
    },
    components: {
        MuiButton: {
            styleOverrides: {
                contained: {
                    backgroundColor: '#4285F4',
                    '&:hover': {
                        backgroundColor: '#3367d6',
                    },
                },
            },
        },
        MuiCard: {
            styleOverrides: {
                root: {
                    boxShadow: '0 1px 3px rgba(0,0,0,0.1)',
                    borderRadius: '8px',
                },
            },
        },
    },
});

function App() {
    const [tenantId, setTenantId] = useState('default');
    const [locale, setLocale] = useState('');

    return (
        <ThemeProvider theme={theme}>
            <Router>
                <Box sx={{ flexGrow: 1, bgcolor: 'background.default', minHeight: '100vh' }}>
                    <AppBar position="static" sx={{ backgroundColor: '#4285F4' }}>
                        <Toolbar sx={{ display: 'flex', flexWrap: 'wrap', gap: 2 }}>
                            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
                                PSearch
                            </Typography>
                            <TextField
                                id="header-tenant-input"
                                label="Tenant"
                                size="small"
                                value={tenantId}
                                onChange={(e) => setTenantId(e.target.value)}
                                inputProps={{ 'aria-label': 'Tenant' }}
                                sx={{
                                    width: 120,
                                    '& .MuiInputBase-root': { color: 'white', bgcolor: 'rgba(255,255,255,0.15)' },
                                    '& .MuiInputLabel-root': { color: 'rgba(255,255,255,0.85)' },
                                    '& .MuiOutlinedInput-notchedOutline': { borderColor: 'rgba(255,255,255,0.4)' },
                                    '&:hover .MuiOutlinedInput-notchedOutline': { borderColor: 'white' },
                                }}
                            />
                            <FormControl size="small" sx={{
                                minWidth: 120,
                                '& .MuiInputBase-root': { color: 'white', bgcolor: 'rgba(255,255,255,0.15)' },
                                '& .MuiInputLabel-root': { color: 'rgba(255,255,255,0.85)' },
                                '& .MuiOutlinedInput-notchedOutline': { borderColor: 'rgba(255,255,255,0.4)' },
                                '&:hover .MuiOutlinedInput-notchedOutline': { borderColor: 'white' },
                                '& .MuiSvgIcon-root': { color: 'white' },
                            }}>
                                <InputLabel id="header-locale-select-label" sx={{ color: 'rgba(255,255,255,0.85)' }}>Locale</InputLabel>
                                <Select
                                    labelId="header-locale-select-label"
                                    id="header-locale-select"
                                    value={locale}
                                    label="Locale"
                                    onChange={(e) => setLocale(e.target.value)}
                                    inputProps={{ 'aria-label': 'Locale' }}
                                >
                                    <MenuItem value="">All</MenuItem>
                                    <MenuItem value="pt-BR">pt-BR</MenuItem>
                                    <MenuItem value="en-US">en-US</MenuItem>
                                    <MenuItem value="es-MX">es-MX</MenuItem>
                                </Select>
                            </FormControl>
                            <Button
                                component={Link}
                                to="/"
                                color="inherit"
                                startIcon={<SearchIcon />}
                            >
                                Search
                            </Button>
                            <Button
                                component={Link}
                                to="/source-ingestion"
                                color="inherit"
                                startIcon={<FileUploadIcon />}
                            >
                                Source Ingestion
                            </Button>
                            <Button
                                component={Link}
                                to="/manage"
                                color="inherit"
                                startIcon={<SettingsIcon />}
                            >
                                Manage Rules
                            </Button>
                        </Toolbar>
                    </AppBar>

                    <Routes>
                        <Route path="/" element={<ProductSearch tenantId={tenantId} setTenantId={setTenantId} locale={locale} setLocale={setLocale} />} />
                        <Route path="/source-ingestion" element={<SourceIngestion />} />
                        <Route path="/manage" element={<RuleManager />} />
                        <Route path="/product/:productId" element={<ProductDetails />} />
                    </Routes>
                </Box>
            </Router>
        </ThemeProvider>
    );
}

export default App;
