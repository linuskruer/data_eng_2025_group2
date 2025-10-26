
  
    
    
    
        
         


        insert into `default`.`dim_seller`
        ("seller_key", "seller_feedback_score", "seller_feedback_percentage", "feedback_score_tier", "feedback_percentage_tier", "seller_tier", "status", "created_at", "updated_at")-- Gold Layer: Dimension Table - Sellers
-- Seller dimension with performance tiers


WITH seller_data AS (
    SELECT DISTINCT
        seller_feedback_score,
        seller_feedback_percentage,
        CASE
            WHEN seller_feedback_score < 100 THEN 'Low (0-99)'
            WHEN seller_feedback_score < 1000 THEN 'Medium (100-999)'
            WHEN seller_feedback_score < 5000 THEN 'High (1000-4999)'
            ELSE 'Very High (5000+)'
        END AS feedback_score_tier,
        CASE
            WHEN seller_feedback_percentage < 95 THEN 'Poor (<95%)'
            WHEN seller_feedback_percentage < 97 THEN 'Fair (95-97%)'
            WHEN seller_feedback_percentage < 99 THEN 'Good (97-99%)'
            ELSE 'Excellent (99%+)'
        END AS feedback_percentage_tier
    FROM `default`.`silver_ebay_data`
    WHERE seller_feedback_score IS NOT NULL
      AND seller_feedback_percentage IS NOT NULL
)

SELECT 
    -- Surrogate Key
    cityHash64(seller_feedback_score, seller_feedback_percentage) as seller_key,
    
    -- Natural Keys
    seller_feedback_score,
    seller_feedback_percentage,
    
    -- Attributes
    feedback_score_tier,
    feedback_percentage_tier,
    
    -- Seller classification
    CASE 
        WHEN feedback_score_tier = 'Very High (5000+)' AND feedback_percentage_tier = 'Excellent (99%+)' THEN 'Premium'
        WHEN feedback_score_tier IN ('High (1000-4999)', 'Very High (5000+)') AND feedback_percentage_tier IN ('Good (97-99%)', 'Excellent (99%+)') THEN 'High Quality'
        WHEN feedback_score_tier IN ('Medium (100-999)', 'High (1000-4999)') AND feedback_percentage_tier IN ('Fair (95-97%)', 'Good (97-99%)') THEN 'Standard'
        ELSE 'Basic'
    END AS seller_tier,
    
    -- Metadata
    'active' as status,
    now() as created_at,
    now() as updated_at
    
FROM seller_data
  