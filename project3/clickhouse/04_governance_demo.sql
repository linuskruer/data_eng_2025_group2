-- Project 3: Governance demonstration queries
-- Shows the difference between full and masked views

-- Full view: shows actual prices, seller locations, exact feedback
SELECT 
    item_id,
    seller_location,
    price,
    seller_feedback_percentage,
    zip_prefix
FROM serving_views_full.vw_listing_weather_full
LIMIT 5;

-- Masked view: shows hashed IDs, bucketed prices, feedback bands, masked locations
SELECT 
    listing_surrogate,  -- hashed item_id
    seller_location_masked,  -- hashed location
    price_bucket,  -- rounded to nearest 10
    seller_feedback_band,  -- LOW_CONFIDENCE/MODERATE/HIGH/EXCELLENT
    zip_prefix_masked  -- prefixed with 'ZIP_'
FROM serving_views_masked.vw_listing_weather_masked
LIMIT 5;

