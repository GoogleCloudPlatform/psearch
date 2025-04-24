# PSearch Usage Guide

This guide provides instructions on how to use the deployed PSearch application.

## Accessing the Application

1.  **Find the Frontend UI URL:** After successfully deploying the platform using Terraform, the URL for the frontend UI should be available.
    *   Check the Terraform output variables (you might need to add an output for the UI service URL in `src/iac/main.tf` or the UI module).
    *   Alternatively, navigate to Cloud Run in your Google Cloud Console and find the service corresponding to the frontend UI (likely named similar to `ps-ui-service` or based on the configuration in `src/iac/modules/ui/main.tf`). The URL will be listed there.

2.  **Open the URL:** Paste the obtained URL into your web browser.

## Searching for Products

1.  **Search Bar:** Locate the search bar, typically displayed prominently at the top of the page.
2.  **Enter Query:** Type your desired search term (e.g., "red running shoes", "organic cotton t-shirt") into the search bar.
3.  **Execute Search:** Press Enter or click the search icon/button.
4.  **View Results:** The search results page will display products matching your query, ranked by relevance based on the hybrid (vector + keyword) search algorithm.

## Filtering Results

The search results page usually offers various filtering options to refine your search:

*   **Categories:** Select specific product categories (e.g., "Apparel", "Electronics", "Shoes").
*   **Brands:** Filter by specific brands.
*   **Price Range:** Adjust sliders or enter minimum/maximum prices.
*   **Other Attributes:** Depending on the indexed data, you might find filters for size, color, ratings, etc.
*   **AI Filter Suggestions:** The platform may suggest relevant filters based on your search query and the available results.

Select the desired filters to narrow down the product list. The results should update automatically or upon clicking an "Apply Filters" button.

## Viewing Product Details

Click on any product card or title in the search results to navigate to its dedicated detail page (if implemented). This page typically shows more information, including:

*   Multiple product images.
*   Detailed description.
*   Price information.
*   Available sizes/colors.
*   Specifications or attributes.
*   Customer reviews (if available).

## Managing Rules (Admin Feature)

*Note: The Rules Management feature is currently in transition from Firestore to Spanner.*

PSearch includes a Rules Engine that allows administrators to configure search behavior:

1.  **Access Rule Management:** 
    * From the main UI, click on the "Manage Rules" button in the navigation bar
    * This will take you to the rules management interface

2.  **Create/Modify Rules:** Use the interface to:
    * **Boost Products:** Increase the ranking of specific products based on categories, brands, price ranges, or product IDs
    * **Bury Products:** Decrease the ranking of specific products that match certain criteria
    * Define conditions under which rules apply (e.g., specific categories, brands)
    * Set score values to control the magnitude of boosting or burying

3.  **Implementation Note:** 
    * The current implementation uses client-side storage (localStorage) for persistence while in development
    * Rules are stored locally in your browser
    * A future update will migrate rule storage to Spanner for production use

4.  **Rule Application:**
    * Once created, rules will automatically influence search results according to their defined conditions
    * Higher boost scores increase a product's visibility in search results
    * Higher bury scores decrease a product's visibility
