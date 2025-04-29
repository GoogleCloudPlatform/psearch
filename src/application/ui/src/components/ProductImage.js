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

import React, { useState, useEffect, useRef, memo, useCallback } from 'react';

/**
 * Enhanced LRU Cache implementation for image URLs with a max size of 50 entries
 * When the cache reaches the maximum size, it will remove the least recently used entries
 * with preference to keep successfully loaded images
 */
class EnhancedLRUImageCache {
    constructor(maxSize = 50) {
        this.cache = new Map(); // The actual cache storage
        this.maxSize = maxSize; // Maximum number of entries in the cache
        this.accessOrder = []; // Array to track the order of access (most recent at the end)
        this.loadedImages = new Set(); // Track successfully loaded images
        this.errorImages = new Set(); // Track images that failed to load
        this.visibleImages = new Set(); // Track images currently visible on screen
    }

    /**
     * Get a value from the cache and mark it as recently used
     */
    get(key) {
        if (!this.cache.has(key)) return null;

        // Mark this key as recently accessed by moving it to the end of the access order
        this.markAsRecentlyUsed(key);

        return this.cache.get(key);
    }

    /**
     * Check if the cache has a key
     */
    has(key) {
        return this.cache.has(key);
    }

    /**
     * Add a new key-value pair to the cache
     */
    set(key, value) {
        // If the key already exists, just update its value and mark as recently used
        if (this.cache.has(key)) {
            this.cache.set(key, value);
            this.markAsRecentlyUsed(key);

            // If it's an error image, add to error set
            if (value === ERROR_IMAGE) {
                this.errorImages.add(key);
                this.loadedImages.delete(key); // Remove from loaded if it was there
            }
            return;
        }

        // If we're at capacity, remove the least recently used item
        if (this.accessOrder.length >= this.maxSize) {
            this.evictLeastUsedItem();
        }

        // Add the new item
        this.cache.set(key, value);
        this.accessOrder.push(key);

        // If it's an error image, add to error set
        if (value === ERROR_IMAGE) {
            this.errorImages.add(key);
        }
    }

    /**
     * Mark an image as successfully loaded
     */
    markAsLoaded(key) {
        if (this.cache.has(key) && !this.errorImages.has(key)) {
            this.loadedImages.add(key);
        }
    }

    /**
     * Mark an image as visible in the viewport
     */
    markAsVisible(key) {
        this.visibleImages.add(key);
    }

    /**
     * Mark an image as no longer visible
     */
    markAsInvisible(key) {
        this.visibleImages.delete(key);
    }

    /**
     * Evict the least used item with preference to keep loaded and visible images
     */
    evictLeastUsedItem() {
        // Keep trying items from the start of the array until we find one to evict
        for (let i = 0; i < this.accessOrder.length; i++) {
            const key = this.accessOrder[i];

            // Don't evict visible images
            if (this.visibleImages.has(key)) {
                continue;
            }

            // Don't evict loaded images if there are error or unloaded images available
            if (this.loadedImages.has(key) &&
                (this.errorImages.size > 0 ||
                    this.accessOrder.length - this.loadedImages.size > 0)) {
                continue;
            }

            // Found an item to evict
            this.accessOrder.splice(i, 1);
            this.cache.delete(key);
            this.loadedImages.delete(key);
            this.errorImages.delete(key);
            this.visibleImages.delete(key);

            // Log detailed eviction information when debug mode is enabled
            if (DEBUG_MODE) {
                const status = this.errorImages.has(key) ? "error" :
                    this.loadedImages.has(key) ? "loaded" : "unloaded";
                console.log(`LRU Cache: Evicted image ${key} (${status}, position ${i}/${this.accessOrder.length})`);
            }
            return;
        }

        // If we got here, all images are visible or loaded and we couldn't find one to evict
        // Just evict the oldest
        const key = this.accessOrder.shift();
        this.cache.delete(key);
        this.loadedImages.delete(key);
        this.errorImages.delete(key);
        this.visibleImages.delete(key);

        if (DEBUG_MODE) {
            console.log(`LRU Cache: Forced eviction of image ${key} (oldest)`);
        }
    }

    /**
     * Move a key to the end of the access order (mark as most recently used)
     */
    markAsRecentlyUsed(key) {
        // Remove the key from its current position
        const index = this.accessOrder.indexOf(key);
        if (index !== -1) {
            this.accessOrder.splice(index, 1);
        }

        // Add it to the end (most recently used position)
        this.accessOrder.push(key);
    }

    /**
     * Get the current size of the cache
     */
    size() {
        return this.cache.size;
    }

    /**
     * Get statistics about the cache
     */
    getStats() {
        return {
            total: this.cache.size,
            loaded: this.loadedImages.size,
            errors: this.errorImages.size,
            visible: this.visibleImages.size
        };
    }

    /**
     * Clear the cache
     */
    clear() {
        this.cache.clear();
        this.accessOrder = [];
        this.loadedImages.clear();
        this.errorImages.clear();
        this.visibleImages.clear();
    }
}

// Constants for configuration
const MAX_CACHE_SIZE = 50; // Maximum number of images to cache
const DEBUG_MODE = false; // Toggle console logs for debugging

// Create a single instance of the enhanced LRU cache for the entire application
const imageUrlCache = new EnhancedLRUImageCache(MAX_CACHE_SIZE);

// Constants for fallback images
const PLACEHOLDER_IMAGE = "https://via.placeholder.com/400x200?text=No+Image";
const ERROR_IMAGE = "https://via.placeholder.com/400x200?text=Image+Error";

/**
 * Get the processed URL from the cache or process it
 * This function is extracted outside the component to avoid 
 * recreating it on each render
 */
function getProcessedUrl(imageUrl) {
    if (!imageUrl) return PLACEHOLDER_IMAGE;

    // Return from cache if available
    if (imageUrlCache.has(imageUrl)) {
        return imageUrlCache.get(imageUrl);
    }

    // Process the URL
    let finalUrl;
    if (imageUrl.startsWith('gs://')) {
        const gcsPath = imageUrl.replace('gs://', '');
        const slashIndex = gcsPath.indexOf('/');
        if (slashIndex !== -1) {
            const bucket = gcsPath.substring(0, slashIndex);
            const objectPath = gcsPath.substring(slashIndex + 1);
            finalUrl = `https://storage.googleapis.com/${bucket}/${objectPath}`;
        } else {
            finalUrl = PLACEHOLDER_IMAGE;
        }
    } else {
        finalUrl = imageUrl;
    }

    // Cache the result and log the current cache size (in debug mode only)
    imageUrlCache.set(imageUrl, finalUrl);
    if (DEBUG_MODE) {
        console.log(`Image cache size: ${imageUrlCache.size()}`);
    }
    return finalUrl;
}

/**
 * Log cache statistics for debugging
 */
function logCacheStats() {
    // Only log stats when in debug mode
    if (!DEBUG_MODE) return;

    const stats = imageUrlCache.getStats();
    console.log(`Image cache stats: ${stats.total} total, ${stats.loaded} loaded, ${stats.errors} errors, ${stats.visible} visible`);
}

// Memoized ProductImage component with proper caching
const ProductImage = memo(({ imageUrl, productName }) => {
    // Get the processed URL on first render, but don't update it on re-renders
    // This uses a ref to persist the value without causing re-renders
    const processedUrlRef = useRef(getProcessedUrl(imageUrl));
    const [error, setError] = useState(false);
    const imgRef = useRef(null);
    const containerRef = useRef(null);

    // Stable error handler to prevent recreation on renders
    const handleError = useCallback(() => {
        setError(true);
        // Cache the error state
        imageUrlCache.set(imageUrl, ERROR_IMAGE);
        if (DEBUG_MODE) {
            console.log(`Image error for ${imageUrl}`);
            logCacheStats();
        }
    }, [imageUrl]);

    // Handler for successful image loading
    const handleLoad = useCallback(() => {
        if (imageUrl) {
            // Mark the image as loaded in our cache
            imageUrlCache.markAsLoaded(imageUrl);
            if (DEBUG_MODE) {
                console.log(`Image loaded: ${imageUrl}`);
                logCacheStats();
            }
        }
    }, [imageUrl]);

    // Set up intersection observer to track when image is visible in viewport
    useEffect(() => {
        if (!imageUrl || !containerRef.current) return;

        // Configure the intersection observer
        const options = {
            root: null, // use the viewport
            rootMargin: '0px',
            threshold: 0.1 // consider visible when 10% is in view
        };

        const observer = new IntersectionObserver((entries) => {
            entries.forEach(entry => {
                if (entry.isIntersecting) {
                    // Image has entered viewport
                    imageUrlCache.markAsVisible(imageUrl);
                    imageUrlCache.markAsRecentlyUsed(imageUrl);
                    if (DEBUG_MODE) {
                        console.log(`Image visible: ${imageUrl}`);
                        logCacheStats();
                    }
                } else {
                    // Image has left viewport
                    imageUrlCache.markAsInvisible(imageUrl);
                    if (DEBUG_MODE) {
                        console.log(`Image no longer visible: ${imageUrl}`);
                        logCacheStats();
                    }
                }
            });
        }, options);

        // Start observing
        observer.observe(containerRef.current);

        // Cleanup function to stop observing when component unmounts
        return () => {
            observer.disconnect();
            if (imageUrl) {
                imageUrlCache.markAsInvisible(imageUrl);
            }
        };
    }, [imageUrl]); // Only re-run if imageUrl changes

    // This effect runs when component mounts and when imageUrl changes
    useEffect(() => {
        // Handle case where this URL already errored in another component
        if (imageUrlCache.has(imageUrl) && imageUrlCache.get(imageUrl) === ERROR_IMAGE) {
            setError(true);
        }

        // Mark this image as recently used whenever the component is mounted with a new URL
        if (imageUrl && imageUrlCache.has(imageUrl)) {
            imageUrlCache.markAsRecentlyUsed(imageUrl);
        }

        // Cleanup when component unmounts or imageUrl changes
        return () => {
            if (imageUrl) {
                imageUrlCache.markAsInvisible(imageUrl);
            }
        };
    }, [imageUrl]);

    // Use logical condition instead of state to determine the src
    // This prevents unnecessary re-renders for the same value
    const src = error ? ERROR_IMAGE : processedUrlRef.current;

    return (
        <div
            ref={containerRef}
            style={{ 
                position: 'relative', 
                height: '180px', // Reduced height to match smaller cards
                width: '100%',
                overflow: 'hidden',
                backgroundColor: '#f5f5f5', // Light gray background for empty spaces
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center'
            }}
        >
            <img
                ref={imgRef}
                src={src}
                alt={productName}
                style={{
                    maxWidth: '100%',
                    maxHeight: '170px',
                    objectFit: 'contain', // Changed to contain to maintain aspect ratio
                    objectPosition: 'center',
                    display: 'block'
                }}
                loading="lazy" // Use browser's lazy loading for off-screen images
                onError={handleError}
                onLoad={handleLoad}
            />
        </div>
    );
}, (prevProps, nextProps) => {
    // Custom equality check for memo - only re-render if the URL actually changed
    // This helps prevent re-renders when parent components re-render
    return prevProps.imageUrl === nextProps.imageUrl;
});

export default ProductImage;
