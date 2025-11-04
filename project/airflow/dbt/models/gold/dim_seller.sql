{{ config(enabled=var('enable_ebay_silver', false)) }}

{% if var('enable_ebay_silver', false) %}
SELECT DISTINCT
    -- Bin seller feedback score
    CASE 
        WHEN seller_feedback_score_cleaned = 0 THEN 'No_Feedback'
        WHEN seller_feedback_score_cleaned < 10 THEN 'Low_0-9'
        WHEN seller_feedback_score_cleaned < 100 THEN 'Medium_10-99'
        WHEN seller_feedback_score_cleaned < 1000 THEN 'High_100-999'
        WHEN seller_feedback_score_cleaned < 10000 THEN 'Very_High_1000-9999'
        ELSE 'Excellent_10000+'
    END AS feedback_score_bin,
    
    -- Bin seller feedback percentage
    CASE 
        WHEN seller_feedback_percentage_cleaned < 90 THEN 'Poor_0-89'
        WHEN seller_feedback_percentage_cleaned < 95 THEN 'Good_90-94'
        WHEN seller_feedback_percentage_cleaned < 98 THEN 'Very_Good_95-97'
        ELSE 'Excellent_98-100'
    END AS feedback_percentage_bin,
    
    -- Use actual values for detailed analysis
    seller_feedback_score_cleaned AS feedback_score,
    seller_feedback_percentage_cleaned AS feedback_percentage
    
FROM {{ ref('silver_ebay_listings') }}
WHERE seller_feedback_score_cleaned IS NOT NULL
  AND seller_feedback_percentage_cleaned IS NOT NULL
{% else %}
SELECT '' AS feedback_score_bin, '' AS feedback_percentage_bin, 0 AS feedback_score, 0.0 AS feedback_percentage WHERE 1=0
{% endif %}

