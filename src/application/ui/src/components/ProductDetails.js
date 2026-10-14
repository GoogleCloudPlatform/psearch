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

import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
    Container, Box, Typography, Tabs, Tab, Card, CardContent,
    Grid, Button, Chip, Rating, Divider, List, ListItem, ListItemIcon,
    Paper, Table, TableBody, TableCell, TableContainer, TableHead, TableRow,
    Snackbar, Alert, Fab, Zoom
} from '@mui/material';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import StarIcon from '@mui/icons-material/Star';
import CircleIcon from '@mui/icons-material/Circle';
import AutoFixHighIcon from '@mui/icons-material/AutoFixHigh';
import LocalShippingIcon from '@mui/icons-material/LocalShipping';
import AssignmentReturnIcon from '@mui/icons-material/AssignmentReturn';
import SecurityIcon from '@mui/icons-material/Security';
import CampaignIcon from '@mui/icons-material/Campaign';
import axios from 'axios';
import config from '../config';
import EnhanceableContent, { GeminiIcon } from './EnhanceableContent';
import ContentDiff from './ContentDiff';
import ConfirmationDialog from './ConfirmationDialog';
import MarketingCampaignDialog from './MarketingCampaignDialog';
import ProductImageEnhancerDialog from './ProductImageEnhancerDialog';
import {
    getProductEnrichment,
    generateMarketingContent,
    generateEnhancedImage
} from '../services/genAiService';
const API_URL = config.apiUrl;

// Helper functions to extract attribute values
const getAttributeValues = (product, key) => {
    if (!product.attributes) return [];

    return product.attributes
        .filter(attr => attr.key === key)
        .flatMap(attr => attr.value.text || []);
};

const getAllTags = (product) => {
    return getAttributeValues(product, 'tag');
};

// Function to format price
const formatPrice = (price, currencyCode = 'USD') => {
    if (!price || price === 'None') return '';

    // Format based on currencyCode (BRL for Brazilian Real)
    if (currencyCode === 'USD') {
        return `$${parseFloat(price).toFixed(2)}`;
    } else if (currencyCode === 'BRL') {
        return `R$${parseFloat(price).toFixed(2)}`;
    }

    return `${parseFloat(price).toFixed(2)}`;
};

function ProductDetails() {
    const { productId } = useParams();
    const navigate = useNavigate();
    const [product, setProduct] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [tabValue, setTabValue] = useState(0);

    // State for AI enhancement features
    const [enhancementStates, setEnhancementStates] = useState({
        image: 'idle',
        description: 'idle',
        specifications: 'idle'
    });

    // State for enhanced content
    const [enhancedContent, setEnhancedContent] = useState({
        image: null,
        description: null,
        specifications: null
    });

    // State for confirmation dialog
    const [confirmDialog, setConfirmDialog] = useState({
        open: false,
        contentType: null
    });

    // State for success notification
    const [notification, setNotification] = useState({
        open: false,
        message: '',
        severity: 'success'
    });

    // State for marketing campaign dialog
    const [marketingDialogOpen, setMarketingDialogOpen] = useState(false);

    // State for image enhancer dialog
    const [imageEnhancerOpen, setImageEnhancerOpen] = useState(false);

    useEffect(() => {
        setLoading(true);
        try {
            // Get product data from localStorage
            const searchResults = localStorage.getItem('searchResults');
            if (searchResults) {
                const products = JSON.parse(searchResults);
                const foundProduct = products.find(p => p.id === productId);

                if (foundProduct) {
                    setProduct(foundProduct);
                } else {
                    setError('Product not found');
                }
            } else {
                // If no search results in localStorage, try to fetch from API
                console.warn('No search results found in localStorage, product details may be incomplete');

                // Create a mock product if needed for demonstration
                const mockProduct = {
                    id: productId,
                    name: "San Pellegrino Aranciata",
                    title: "Premium Italian Sparkling Orange Beverage",
                    brands: ["San Pellegrino"],
                    categories: ["Beverages", "Sparkling Water"],
                    priceInfo: {
                        price: "4.00",
                        originalPrice: "7.00",
                        currencyCode: "USD"
                    },
                    availability: "IN_STOCK",
                    images: [
                        {
                            uri: "https://m.media-amazon.com/images/I/71FSUOFvAQL._SL1500_.jpg"
                        }
                    ],
                    attributes: [
                        {
                            key: "material",
                            value: { text: ["Aluminum Can"] }
                        },
                        {
                            key: "features",
                            value: { text: ["Made with Italian Oranges", "Refreshing Citrus Flavor", "Elegant Packaging"] }
                        },
                        {
                            key: "gender",
                            value: { text: ["Adults"] }
                        },
                        {
                            key: "activity",
                            value: { text: ["Dining", "Refreshment"] }
                        }
                    ]
                };

                setProduct(mockProduct);
            }
        } catch (err) {
            console.error('Error loading product:', err);
            setError('Failed to load product details');
        } finally {
            setLoading(false);
        }
    }, [productId]);

    const handleTabChange = (event, newValue) => {
        setTabValue(newValue);
    };

    // Function to get formatted image URL - now supports primaryImageUrl
    const getImageUrl = (product) => {
        // First check if we have the primaryImageUrl (optimized storage format)
        if (product.primaryImageUrl) {
            const imageUrl = product.primaryImageUrl;

            // Convert gs:// URLs to https://storage.googleapis.com/
            if (imageUrl.startsWith('gs://')) {
                const gcsPath = imageUrl.replace('gs://', '');
                const slashIndex = gcsPath.indexOf('/');
                if (slashIndex !== -1) {
                    const bucket = gcsPath.substring(0, slashIndex);
                    const objectPath = gcsPath.substring(slashIndex + 1);
                    return `https://storage.googleapis.com/${bucket}/${objectPath}`;
                }
            }

            return imageUrl;
        }

        // Fall back to images array if available
        if (product.images && product.images.length && product.images[0].uri) {
            const imageUrl = product.images[0].uri;

            // Convert gs:// URLs to https://storage.googleapis.com/
            if (imageUrl.startsWith('gs://')) {
                const gcsPath = imageUrl.replace('gs://', '');
                const slashIndex = gcsPath.indexOf('/');
                if (slashIndex !== -1) {
                    const bucket = gcsPath.substring(0, slashIndex);
                    const objectPath = gcsPath.substring(slashIndex + 1);
                    return `https://storage.googleapis.com/${bucket}/${objectPath}`;
                }
            }

            return imageUrl;
        }

        // If no image is available, return placeholder
        return 'https://via.placeholder.com/400x400?text=No+Image';
    };

    // Handler for initiating content enhancement
    const handleEnhance = async (contentType) => {
        // Check if this is a cancel action
        if (contentType === 'cancel') {
            // Reset enhancement state back to idle
            setEnhancementStates(prev => ({
                ...prev,
                [contentType]: 'idle'
            }));
            return;
        }

        // Set the state to "enhancing"
        setEnhancementStates(prev => ({
            ...prev,
            [contentType]: 'enhancing'
        }));

        // Setup a timeout to prevent UI from being stuck in loading state
        const timeoutId = setTimeout(() => {
            // Check if we're still in enhancing state after timeout
            setEnhancementStates(prev => {
                if (prev[contentType] === 'enhancing') {
                    // Show error notification for timeout
                    setNotification({
                        open: true,
                        message: `The AI enhancement is taking longer than expected. Please try again.`,
                        severity: 'warning'
                    });
                    // Reset to idle state
                    return {
                        ...prev,
                        [contentType]: 'idle'
                    };
                }
                return prev;
            });
        }, 30000); // 30 second timeout

        try {
            let enhancedContent;

            switch (contentType) {
                // Image enhancement now handled by dialog
                case 'description':
                    const descriptionResponse = await getProductEnrichment(
                        product.id,
                        product,
                        ['description']
                    );
                    enhancedContent = descriptionResponse.enriched_fields.description;
                    break;

                case 'specifications':
                    // Log the product data being sent for enrichment
                    console.log('Product data sent for specifications enrichment:', {
                        id: product.id,
                        data: product
                    });

                    const specsResponse = await getProductEnrichment(
                        product.id,
                        product,
                        ['technical_specs']
                    );

                    console.log('Raw specifications response received:', specsResponse);

                    // Handle different potential response formats gracefully
                    if (specsResponse.enriched_fields && specsResponse.enriched_fields.technical_specs) {
                        const techSpecs = specsResponse.enriched_fields.technical_specs;
                        console.log('Technical specs data:', techSpecs);

                        // Handle both object format and array format
                        if (typeof techSpecs === 'object' && !Array.isArray(techSpecs)) {
                            // Compare with current product data to determine which specs are new/changed
                            enhancedContent = Object.entries(techSpecs).map(([name, value]) => {
                                // Check if this spec already exists with same value
                                let isNew = true;
                                const nameLower = name.toLowerCase();

                                // Check for brand
                                if (nameLower === 'brand' && product.brands && product.brands.length > 0) {
                                    const currentBrand = product.brands[0];
                                    isNew = currentBrand !== value;
                                }
                                // Check for category
                                else if (nameLower === 'category' && product.categories && product.categories.length > 0) {
                                    const currentCategory = product.categories[0];
                                    isNew = currentCategory !== value;
                                }
                                // Check in attributes
                                else {
                                    const attrValues = getAttributeValues(product, nameLower);
                                    if (attrValues.length > 0) {
                                        // Consider unchanged if any attribute value matches
                                        isNew = !attrValues.some(attr => attr === value);
                                    }
                                }

                                return { name, value, isNew };
                            });
                            console.log('Transformed specs data (from object) with isNew flags:', enhancedContent);
                        } else if (Array.isArray(techSpecs)) {
                            enhancedContent = techSpecs.map(spec => {
                                const name = spec.name || spec.key || 'Specification';
                                const value = spec.value || spec.text || '';
                                const nameLower = name.toLowerCase();

                                // Check if this spec already exists with same value
                                let isNew = true;

                                // Check for brand
                                if (nameLower === 'brand' && product.brands && product.brands.length > 0) {
                                    const currentBrand = product.brands[0];
                                    isNew = currentBrand !== value;
                                }
                                // Check for category
                                else if (nameLower === 'category' && product.categories && product.categories.length > 0) {
                                    const currentCategory = product.categories[0];
                                    isNew = currentCategory !== value;
                                }
                                // Check in attributes
                                else {
                                    const attrValues = getAttributeValues(product, nameLower);
                                    if (attrValues.length > 0) {
                                        // Consider unchanged if any attribute value matches
                                        isNew = !attrValues.some(attr => attr === value);
                                    }
                                }

                                return { name, value, isNew };
                            });
                            console.log('Transformed specs data (from array) with isNew flags:', enhancedContent);
                        } else {
                            // Fallback for unexpected format
                            console.error('Unexpected format for technical_specs:', techSpecs);
                            throw new Error('Unexpected data format received for specifications');
                        }
                    } else {
                        console.error('Missing technical_specs in response:', specsResponse);
                        throw new Error('Technical specifications data missing from AI response');
                    }
                    break;

                default:
                    enhancedContent = null;
            }

            // Clear the timeout since we got a response
            clearTimeout(timeoutId);

            // Set the enhanced content
            setEnhancedContent(prev => ({
                ...prev,
                [contentType]: enhancedContent
            }));

            // Update state to show diff
            setEnhancementStates(prev => {
                // Only update if we're still in enhancing state (in case timeout fired)
                if (prev[contentType] === 'enhancing') {
                    return {
                        ...prev,
                        [contentType]: 'showingDiff'
                    };
                }
                return prev;
            });
        } catch (error) {
            // Clear the timeout since we got a response (error)
            clearTimeout(timeoutId);

            console.error(`Error enhancing ${contentType}:`, error);

            // Show error notification
            setNotification({
                open: true,
                message: `Error enhancing ${contentType}: ${error.message || 'Unknown error occurred'}`,
                severity: 'error'
            });

            // Reset enhancement state
            setEnhancementStates(prev => ({
                ...prev,
                [contentType]: 'idle'
            }));
        }
    };

    // Handler for confirming updates
    const handleUpdate = (contentType) => {
        setConfirmDialog({
            open: true,
            contentType
        });
    };

    // Handler for closing the confirmation dialog
    const handleCloseConfirmation = () => {
        setConfirmDialog({
            ...confirmDialog,
            open: false
        });
    };

    // Handler for confirming the update
    const handleConfirmUpdate = () => {
        const contentType = confirmDialog.contentType;

        // Close the dialog
        setConfirmDialog({
            ...confirmDialog,
            open: false
        });

        // Update product with enhanced content
        if (contentType === 'image') {
            setProduct({
                ...product,
                images: [{ uri: enhancedContent.image }]
            });
        } else if (contentType === 'description') {
            setProduct({
                ...product,
                description: enhancedContent.description
            });
        } else if (contentType === 'specifications') {
            // For technical specifications, we need to update the product's attributes
            console.log('Updating product with new specifications:', enhancedContent.specifications);

            // Create a copy of product data to update
            const updatedProduct = { ...product };

            // Extract specs data
            const specsData = enhancedContent.specifications;

            // Create a helper to update attributes
            const updateAttributeValue = (key, value) => {
                // First check if this attribute already exists
                const existingAttrIndex = updatedProduct.attributes?.findIndex(attr =>
                    attr.key?.toLowerCase() === key.toLowerCase());

                if (existingAttrIndex >= 0 && updatedProduct.attributes) {
                    // Update existing attribute
                    updatedProduct.attributes[existingAttrIndex] = {
                        ...updatedProduct.attributes[existingAttrIndex],
                        value: { text: [value] }
                    };
                } else if (updatedProduct.attributes) {
                    // Add new attribute
                    updatedProduct.attributes.push({
                        key: key.toLowerCase(),
                        value: { text: [value] }
                    });
                } else {
                    // Initialize attributes array if it doesn't exist
                    updatedProduct.attributes = [{
                        key: key.toLowerCase(),
                        value: { text: [value] }
                    }];
                }
            };

            // Update attributes based on specs
            specsData.forEach(spec => {
                // Skip brand and category as they are stored elsewhere
                if (spec.name.toLowerCase() === 'brand') {
                    if (spec.value && spec.value.trim() !== '') {
                        updatedProduct.brands = [spec.value];
                    }
                } else if (spec.name.toLowerCase() === 'category') {
                    if (spec.value && spec.value.trim() !== '') {
                        updatedProduct.categories = [spec.value];
                    }
                } else {
                    // Update other attributes
                    updateAttributeValue(spec.name, spec.value);
                }
            });

            // Update the product state
            setProduct(updatedProduct);
        }

        // Reset enhancement state
        setEnhancementStates(prev => ({
            ...prev,
            [contentType]: 'idle'
        }));

        // Show success notification
        setNotification({
            open: true,
            message: `The product ${contentType} has been successfully updated!`,
            severity: 'success'
        });
    };

    // Handler for closing the notification
    const handleCloseNotification = () => {
        setNotification({
            ...notification,
            open: false
        });
    };

    // Render loading state
    if (loading) {
        return (
            <Container>
                <Typography variant="h5" sx={{ my: 4 }}>Loading product details...</Typography>
            </Container>
        );
    }

    // Render error state
    if (error || !product) {
        return (
            <Container>
                <Typography variant="h5" color="error" sx={{ my: 4 }}>
                    {error || 'Product not found'}
                </Typography>
                <Button startIcon={<ArrowBackIcon />} onClick={() => navigate('/')}>
                    Return to Search
                </Button>
            </Container>
        );
    }

    // Extract product attributes
    const gender = getAttributeValues(product, 'gender');
    const colors = getAttributeValues(product, 'color');
    const style = getAttributeValues(product, 'style');
    const material = getAttributeValues(product, 'material');
    const features = getAttributeValues(product, 'features');
    const activity = getAttributeValues(product, 'activity');
    const tags = getAllTags(product);

    // Extract price info
    const price = product.priceInfo?.price || '';
    const originalPrice = product.priceInfo?.originalPrice || '';
    const currencyCode = product.priceInfo?.currencyCode || 'USD';

    // Format price for display
    const formattedPrice = formatPrice(price, 'BRL'); // Display in Brazilian Real
    const formattedOriginalPrice = originalPrice && originalPrice !== 'None'
        ? formatPrice(originalPrice, 'BRL') : '';

    return (
        <Box>
            {/* Header section */}
            <Box sx={{ backgroundColor: '#4285f4', color: 'white', p: 3 }}>
                <Container>
                    <Typography variant="h3" component="h1">{product.name}</Typography>
                    <Typography variant="h6">{product.brands?.[0] || ''}</Typography>
                </Container>
            </Box>

            {/* Main content */}
            <Container sx={{ mt: 4, mb: 4 }}>
                <Button
                    startIcon={<ArrowBackIcon />}
                    onClick={() => navigate('/')}
                    sx={{ mb: 2 }}
                >
                    Return to Search
                </Button>

                <Grid container spacing={4}>
                    {/* Product image section */}
                    <Grid item xs={12} md={5}>
                        <Card sx={{ position: 'relative' }}>
                            {/* NOVO badge */}
                            <Box
                                sx={{
                                    position: 'absolute',
                                    top: 20,
                                    left: 20,
                                    bgcolor: '#e74c3c',
                                    color: 'white',
                                    px: 2,
                                    py: 0.5,
                                    zIndex: 1,
                                }}
                            >
                                <Typography variant="subtitle2">NEW</Typography>
                            </Box>

                            {/* Product image */}
                            <img
                                src={getImageUrl(product)}
                                alt={product.name}
                                style={{ width: '100%', height: 'auto', display: 'block' }}
                            />
                            <Fab
                                size="small"
                                color="secondary"
                                aria-label="enhance image"
                                sx={{
                                    position: 'absolute',
                                    bottom: 16,
                                    right: 16,
                                    zIndex: 2,
                                }}
                                onClick={() => setImageEnhancerOpen(true)}
                            >
                                <AutoFixHighIcon />
                            </Fab>
                        </Card>

                        {/* Price section */}
                        <Card sx={{ mt: 3 }}>
                            <CardContent>
                                <Typography variant="subtitle1" color="text.secondary">
                                    Suggested Price:
                                </Typography>
                                <Typography variant="h4" color="#4285f4" sx={{ mb: 2 }}>
                                    {formattedPrice ? formattedPrice : 'R$4,00 - R$7,00'}
                                </Typography>

                                {formattedOriginalPrice && (
                                    <Typography
                                        variant="body2"
                                        color="text.secondary"
                                        sx={{ textDecoration: 'line-through', mb: 2 }}
                                    >
                                        {formattedOriginalPrice}
                                    </Typography>
                                )}

                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <LocalShippingIcon color="primary" sx={{ mr: 1 }} />
                                    <Typography>Free Shipping Nationwide</Typography>
                                </Box>

                                <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                    <AssignmentReturnIcon color="primary" sx={{ mr: 1 }} />
                                    <Typography>Free Returns Within 30 Days</Typography>
                                </Box>

                                <Box sx={{ display: 'flex', alignItems: 'center' }}>
                                    <SecurityIcon color="primary" sx={{ mr: 1 }} />
                                    <Typography>12-Month Warranty</Typography>
                                </Box>
                            </CardContent>
                        </Card>
                    </Grid>

                    {/* Product information section */}
                    <Grid item xs={12} md={7}>
                        <Box sx={{ width: '100%', mb: 3 }}>
                            <Tabs
                                value={tabValue}
                                onChange={handleTabChange}
                                aria-label="product information tabs"
                                sx={{ borderBottom: 1, borderColor: 'divider' }}
                            >
                                <Tab label="Description" />
                                <Tab label="Specifications" />
                            </Tabs>
                        </Box>

                        {/* Description Tab */}
                        <Box hidden={tabValue !== 0}>
                            <Typography variant="h5" component="h2" gutterBottom>
                                About this Product
                            </Typography>

                            <EnhanceableContent
                                contentType="description"
                                enhancementState={enhancementStates.description}
                                onEnhance={handleEnhance}
                                onUpdate={handleUpdate}
                            >
                                <Typography variant="body1" paragraph>
                                    {product.description ||
                                        `Enjoy this high-quality product from ${product.brands?.[0] || 'premium'} brand. 
                                        ${product.title || product.name} is carefully designed to provide 
                                        maximum comfort and durability. Made with ${material.length ? material.join(', ') : 'high-quality materials'}
                                        to ensure an exceptional experience.`
                                    }
                                </Typography>
                            </EnhanceableContent>

                            {/* Description Diff */}
                            {enhancementStates.description === 'showingDiff' && (
                                <ContentDiff
                                    contentType="description"
                                    originalContent={product.description ||
                                        `Enjoy this high-quality product from ${product.brands?.[0] || 'premium'} brand. 
                                        ${product.title || product.name} is carefully designed to provide 
                                        maximum comfort and durability. Made with ${material.length ? material.join(', ') : 'high-quality materials'}
                                        to ensure an exceptional experience.`
                                    }
                                    enhancedContent={enhancedContent.description}
                                />
                            )}

                            {features.length > 0 && (
                                <>
                                    <Typography variant="h5" component="h2" sx={{ display: 'flex', alignItems: 'center', mt: 4, mb: 2 }}>
                                        ✨ Key Features
                                    </Typography>
                                    <List>
                                        {features.map((feature, index) => (
                                            <ListItem key={index}>
                                                <ListItemIcon><CircleIcon sx={{ fontSize: 10 }} /></ListItemIcon>
                                                <Typography>{feature}</Typography>
                                            </ListItem>
                                        ))}
                                    </List>
                                </>
                            )}

                            <Typography variant="h5" component="h2" sx={{ display: 'flex', alignItems: 'center', mt: 4, mb: 2 }}>
                                🎯 Target Audience
                            </Typography>
                            <Typography variant="body1" paragraph>
                                {gender.length ? `${gender.join(', ')}` : 'Adults'},
                                fans of {product.categories?.[0]?.toLowerCase() || 'quality products'},
                                consumers looking for {activity.length ? activity.join(', ').toLowerCase() : 'style and comfort'}.
                            </Typography>
                        </Box>

                        {/* Specifications Tab */}
                        <Box hidden={tabValue !== 1}>
                            <Typography variant="h5" component="h2" gutterBottom>
                                Specifications
                            </Typography>

                            <EnhanceableContent
                                contentType="specifications"
                                enhancementState={enhancementStates.specifications}
                                onEnhance={handleEnhance}
                                onUpdate={handleUpdate}
                            >
                                <TableContainer component={Paper} sx={{ mb: 4 }}>
                                    <Table>
                                        <TableBody>
                                            {product.brands?.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Brand
                                                    </TableCell>
                                                    <TableCell>{product.brands.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            {product.categories?.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Category
                                                    </TableCell>
                                                    <TableCell>{product.categories.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            {gender.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Gender
                                                    </TableCell>
                                                    <TableCell>{gender.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            {material.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Material
                                                    </TableCell>
                                                    <TableCell>{material.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            {colors.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Color
                                                    </TableCell>
                                                    <TableCell>{colors.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            {style.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Style
                                                    </TableCell>
                                                    <TableCell>{style.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            {product.sizes?.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Sizes
                                                    </TableCell>
                                                    <TableCell>{product.sizes.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            {activity.length > 0 && (
                                                <TableRow>
                                                    <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                        Activities
                                                    </TableCell>
                                                    <TableCell>{activity.join(', ')}</TableCell>
                                                </TableRow>
                                            )}

                                            <TableRow>
                                                <TableCell component="th" scope="row" sx={{ fontWeight: 'bold' }}>
                                                    Availability
                                                </TableCell>
                                                <TableCell>
                                                    {(() => {
                                                        const isStockIn = product.availability === 'IN_STOCK';
                                                        const stockBadge = isStockIn
                                                            ? (product.availableQuantity !== undefined && product.availableQuantity !== null
                                                                ? `IN_STOCK (${product.availableQuantity} unid.)`
                                                                : 'IN_STOCK')
                                                            : 'OUT_OF_STOCK';
                                                        return (
                                                            <Chip
                                                                label={stockBadge}
                                                                color={isStockIn ? 'success' : 'error'}
                                                                size="small"
                                                            />
                                                        );
                                                    })()}
                                                </TableCell>
                                            </TableRow>
                                        </TableBody>
                                    </Table>
                                </TableContainer>

                                {tags.length > 0 && (
                                    <>
                                        <Typography variant="h6" gutterBottom>Tags</Typography>
                                        <Box sx={{ mb: 3 }}>
                                            {tags.map((tag, index) => (
                                                <Chip
                                                    key={index}
                                                    label={tag}
                                                    size="small"
                                                    sx={{ mr: 0.5, mb: 0.5 }}
                                                />
                                            ))}
                                        </Box>
                                    </>
                                )}
                            </EnhanceableContent>

                            {/* Specifications Diff */}
                            {enhancementStates.specifications === 'showingDiff' && (
                                <ContentDiff
                                    contentType="specifications"
                                    originalContent={[
                                        { name: "Brand", value: product.brands?.join(', ') || '' },
                                        { name: "Category", value: product.categories?.join(', ') || '' },
                                        { name: "Material", value: material.join(', ') }
                                    ]}
                                    enhancedContent={enhancedContent.specifications}
                                />
                            )}
                        </Box>
                    </Grid>
                </Grid>

                {/* Confirmation Dialog */}
                <ConfirmationDialog
                    open={confirmDialog.open}
                    contentType={confirmDialog.contentType}
                    onClose={handleCloseConfirmation}
                    onConfirm={handleConfirmUpdate}
                />

                {/* Success Notification */}
                <Snackbar
                    open={notification.open}
                    autoHideDuration={5000}
                    onClose={handleCloseNotification}
                    anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
                >
                    <Alert onClose={handleCloseNotification} severity={notification.severity} sx={{ width: '100%' }}>
                        {notification.message}
                    </Alert>
                </Snackbar>

                {/* Marketing Campaign Dialog */}
                <MarketingCampaignDialog
                    open={marketingDialogOpen}
                    onClose={() => setMarketingDialogOpen(false)}
                    product={product}
                />

                {/* Marketing Campaign Fab */}
                <Fab
                    color="primary"
                    aria-label="generate marketing campaign"
                    sx={{
                        position: 'fixed',
                        bottom: 16,
                        right: 16,
                        bgcolor: 'rgba(66, 133, 244, 0.9)',
                        '&:hover': {
                            bgcolor: 'rgba(66, 133, 244, 1)',
                        },
                        transform: 'translateZ(0)',
                        transition: 'transform 0.2s ease-in-out',
                        '&:active': {
                            transform: 'scale(0.95)'
                        }
                    }}
                    onClick={() => {
                        setMarketingDialogOpen(true);
                    }}
                >
                    <CampaignIcon />
                </Fab>

                {/* Image Enhancer Dialog */}
                <ProductImageEnhancerDialog
                    open={imageEnhancerOpen}
                    onClose={() => setImageEnhancerOpen(false)}
                    productId={product.id}
                    productData={product}
                    baseImageUrl={getImageUrl(product)}
                />
            </Container>
        </Box>
    );
}

export default ProductDetails;
