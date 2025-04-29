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

import React, { useState, useMemo, useCallback, useEffect, useRef, memo } from 'react';
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
    InputAdornment,
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

function ProductSearch() {
    const navigate = useNavigate();
    // Simplified search state - only track the active search query
    const [activeSearchQuery, setActiveSearchQuery] = useState(''); // For actual API requests
    const [showAIFilterSuggestions, setShowAIFilterSuggestions] = useState(false); // Control visibility of AI filter suggestions
    const [products, setProducts] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
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

    // Use memoized filter configurations
    useEffect(() => {
        const configs = generateFilterConfigs(products);
        setFilterConfigs(configs);
    }, [products, generateFilterConfigs]);

    // The ProductImage component has been moved to its own file for better performance

    const fetchProducts = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const response = await axios.post(`${API_URL}/search`, {
                query: activeSearchQuery, // Use activeSearchQuery instead of searchQuery
                limit: 300,
                min_score: 0.01,
                alpha: 0.0,
            });

            console.log("API Response:", JSON.stringify(response.data, null, 2));

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
            setError(error);
            console.error("Search error:", error);
        } finally {
            setLoading(false);
        }
    }, [activeSearchQuery]); // Changed dependency from searchQuery to activeSearchQuery

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
                        const [min, max] = range.split('-').map(parseFloat);
                        return { min, max };
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
    const handleSearch = useCallback((searchText) => {
        // Reset all filters when a new search is performed
        setSelectedFilters({});
        // Update active search query which triggers API call
        setActiveSearchQuery(searchText);
        // Reset AI filter suggestions when a new search is performed
        setShowAIFilterSuggestions(true);
    }, []);

    useEffect(() => {
        if (activeSearchQuery.trim()) {
            fetchProducts();
        }

        // Cleanup function to prevent memory leaks
        return () => {
            // Clear any references to large data structures
            setProducts([]);
            setFilterConfigs([]);
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
                                        <Chip
                                            label={product.availability}
                                            color={product.availability === 'IN_STOCK' ? 'success' : 'error'}
                                            size="small"
                                            sx={{ mt: 1 }}
                                        />
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
                        />
                    </Card>
                </Grid>

                {/* Search and Products Section */}
                <Grid item xs={12} sm={8} md={9} sx={{ 
                    flex: '1 1 auto',
                    maxWidth: { xs: '100%', sm: '66.666%', md: '75%' } 
                }}>
                    {/* Use the isolated SearchInput component */}
                    <SearchInput onSearch={handleSearch} />

                    {/* Display search result count */}
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

                        <Typography variant="body2" color="text.secondary" sx={{ mb: 2 }}>
                            {products.length ? `Found ${products.length} products` : 'No products found. Try a search!'}
                        </Typography>

                        {loading && <Typography>Loading products...</Typography>}
                        {error && <Typography color="error">Error: {error.message}</Typography>}

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
    return (
        <ThemeProvider theme={theme}>
            <Router>
                <Box sx={{ flexGrow: 1, bgcolor: 'background.default', minHeight: '100vh' }}>
                    <AppBar position="static" sx={{ backgroundColor: '#4285F4' }}>
                        <Toolbar>
                            <Typography variant="h6" component="div" sx={{ flexGrow: 1 }}>
                                PSearch
                            </Typography>
            <Button
                component={Link}
                to="/"
                color="inherit"
                startIcon={<SearchIcon />}
                sx={{ mr: 2 }}
            >
                Search
            </Button>
            <Button
                component={Link}
                to="/source-ingestion"
                color="inherit"
                startIcon={<FileUploadIcon />}
                sx={{ mr: 2 }}
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
                        <Route path="/" element={<ProductSearch />} />
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
