


SELECT DISTINCT
    buying_options_cleaned AS buying_option
FROM default.silver_ebay_listings
WHERE buying_options_cleaned IS NOT NULL
  AND buying_options_cleaned != 'Unknown'
