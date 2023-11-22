WITH 

reports AS (
    SELECT 
        id AS ra_id,
        user_id,
        ticket_creation_date AS created_at,
        public_treatment_time AS time_to_first_contact,
        first_contact_time AS cse_interaction_time,
        DATE_DIFF(first_contact_time, ticket_creation_date, SECOND) AS time_to_cse_interaction,
        rating,
        resolved_issue, --bool
        back_doing_business --bool

    FROM 
        infinitepay-production.maindb.ra_reports
    WHERE user_id IS NOT NULL
),

merchants AS (
    SELECT
        user_id,
        created_at AS merchant_created_at
    FROM 
        infinitepay-production.maindb.merchants
),

merchant_agg AS (
    SELECT
        merchants.user_id AS merchant_id,
        COALESCE(
            TIMESTAMP(DATE_ADD(CAST(reports.created_at AS DATE), INTERVAL 1 DAY)),
            TIMESTAMP(DATE_ADD(CAST(merchants.merchant_created_at AS DATE), INTERVAL 1 DAY))
        ) AS event_timestamp,

        --Count of RA reports by merchant
        COUNT(reports.ra_id) OVER Wunb AS count_ra_from_merchant_unbounded,
        COUNT(reports.ra_id) OVER W90d AS count_ra_from_merchant_last_90d,
        COUNT(reports.ra_id) OVER W60d AS count_ra_from_merchant_last_60d,
        COUNT(reports.ra_id) OVER W30d AS count_ra_from_merchant_last_30d,
        COUNT(reports.ra_id) OVER W15d AS count_ra_from_merchant_last_15d,
        COUNT(reports.ra_id) OVER W7d AS count_ra_from_merchant_last_7d,
        --Count of reports with status resolved by merchant 
        COUNT(CASE WHEN resolved_issue THEN 1 END) OVER Wunb AS count_ra_resolved_unbounded,
        COUNT(CASE WHEN resolved_issue THEN 1 END) OVER W90d AS count_ra_resolved_last_90d,
        COUNT(CASE WHEN resolved_issue THEN 1 END) OVER W60d AS count_ra_resolved_last_60d,
        COUNT(CASE WHEN resolved_issue THEN 1 END) OVER W30d AS count_ra_resolved_last_30d,
        COUNT(CASE WHEN resolved_issue THEN 1 END) OVER W15d AS count_ra_resolved_last_15d,
        COUNT(CASE WHEN resolved_issue THEN 1 END) OVER W7d AS count_ra_resolved_last_7d,
        --Count of reports with status NOT resolved by merchant
        COUNT(CASE WHEN NOT resolved_issue THEN 1 END) OVER Wunb AS count_ra_not_resolved_unbounded,
        COUNT(CASE WHEN NOT resolved_issue THEN 1 END) OVER W90d AS count_ra_not_resolved_last_90d,
        COUNT(CASE WHEN NOT resolved_issue THEN 1 END) OVER W60d AS count_ra_not_resolved_last_60d,
        COUNT(CASE WHEN NOT resolved_issue THEN 1 END) OVER W30d AS count_ra_not_resolved_last_30d,
        COUNT(CASE WHEN NOT resolved_issue THEN 1 END) OVER W15d AS count_ra_not_resolved_last_15d,
        COUNT(CASE WHEN NOT resolved_issue THEN 1 END) OVER W7d AS count_ra_not_resolved_last_7d,
        --Count of reports where merchant checked the back_doing_business willingness 
        COUNT(CASE WHEN back_doing_business THEN 1 END) OVER Wunb AS count_ra_back_business_unbounded,
        COUNT(CASE WHEN back_doing_business THEN 1 END) OVER W90d AS count_ra_rback_business_last_90d,
        COUNT(CASE WHEN back_doing_business THEN 1 END) OVER W60d AS count_ra_back_business_last_60d,
        COUNT(CASE WHEN back_doing_business THEN 1 END) OVER W30d AS count_ra_back_business_last_30d,
        COUNT(CASE WHEN back_doing_business THEN 1 END) OVER W15d AS count_ra_back_business_last_15d,
        COUNT(CASE WHEN back_doing_business THEN 1 END) OVER W7d AS count_ra_back_business_last_7d,
        --Count of reports where merchant did NOT check the back_doing_business willingness 
        COUNT(CASE WHEN NOT back_doing_business THEN 1 END) OVER Wunb AS count_ra_not_back_business_unbounded,
        COUNT(CASE WHEN NOT back_doing_business THEN 1 END) OVER W90d AS count_ra_not_back_business_last_90d,
        COUNT(CASE WHEN NOT back_doing_business THEN 1 END) OVER W60d AS count_ra_not_back_business_last_60d,
        COUNT(CASE WHEN NOT back_doing_business THEN 1 END) OVER W30d AS count_ra_not_back_business_last_30d,
        COUNT(CASE WHEN NOT back_doing_business THEN 1 END) OVER W15d AS count_ra_not_back_business_last_15d,
        COUNT(CASE WHEN NOT back_doing_business THEN 1 END) OVER W7d AS count_ra_back_business_last_7d,
        --Count how many times user choose not to give a rating (or forget)
        COUNT(CASE WHEN rating IS NULL THEN 1 END) OVER Wunb AS count_ra_not_rated_unbounded,
        COUNT(CASE WHEN rating IS NULL THEN 1 END) OVER W90d AS count_ra_not_rated_last_90d,
        COUNT(CASE WHEN rating IS NULL THEN 1 END) OVER W60d AS count_ra_not_rated_last_60d,
        COUNT(CASE WHEN rating IS NULL THEN 1 END) OVER W30d AS count_ra_not_rated_last_30d,
        COUNT(CASE WHEN rating IS NULL THEN 1 END) OVER W15d AS count_ra_not_rated_last_15d,
        COUNT(CASE WHEN rating IS NULL THEN 1 END) OVER W7d AS count_ra_not_rated_last_7d,
        --AVG rating of ra_report excluding null cases 
        AVG(CASE WHEN rating IS NOT NULL THEN rating END) OVER Wunb AS avg_ra_rating_unbounded,
        AVG(CASE WHEN rating IS NOT NULL THEN rating END) OVER W90d AS avg_ra_rating_last_90d,
        AVG(CASE WHEN rating IS NOT NULL THEN rating END) OVER W60d AS avg_ra_rating_last_60d,
        AVG(CASE WHEN rating IS NOT NULL THEN rating END) OVER W30d AS avg_ra_rating_last_30d,
        AVG(CASE WHEN rating IS NOT NULL THEN rating END) OVER W15d AS avg_ra_rating_last_15d,
        AVG(CASE WHEN rating IS NOT NULL THEN rating END) OVER W7d AS avg_ra_rating_last_7d,
        --AVG time to first contact in seconds by merchant
        AVG(reports.time_to_first_contact) OVER Wunb AS avg_ra_time_first_reply_seconds_by_merchant_unbounded,
        AVG(reports.time_to_first_contact) OVER W90d AS avg_ra_time_first_reply_seconds_by_merchant_last_90d,
        AVG(reports.time_to_first_contact) OVER W60d AS avg_ra_time_first_reply_seconds_by_merchant_last_60d,
        AVG(reports.time_to_first_contact) OVER W30d AS avg_ra_time_first_reply_seconds_by_merchant_last_30d,
        AVG(reports.time_to_first_contact) OVER W15d AS avg_ra_time_first_reply_seconds_by_merchant_last_15d,
        AVG(reports.time_to_first_contact) OVER W7d AS avg_ra_time_first_reply_seconds_by_merchant_last_7d,  
        --Max time to first contact  in seconds    
        MAX(reports.time_to_first_contact) OVER Wunb AS max_ra_time_first_reply_seconds_by_merchant_unbounded,
        MAX(reports.time_to_first_contact) OVER W90d AS max_ra_time_first_reply_seconds_by_merchant_last_90d,
        MAX(reports.time_to_first_contact) OVER W60d AS max_ra_time_first_reply_seconds_by_merchant_last_60d,
        MAX(reports.time_to_first_contact) OVER W30d AS max_ra_time_first_reply_seconds_by_merchant_last_30d,
        MAX(reports.time_to_first_contact) OVER W15d AS max_ra_time_first_reply_seconds_by_merchant_last_15d,
        MAX(reports.time_to_first_contact) OVER W7d AS max_ra_time_first_reply_seconds_by_merchant_last_7d,  
        --Min time to first contact  in seconds    
        MIN(reports.time_to_first_contact) OVER Wunb AS min_ra_time_first_reply_seconds_by_merchant_unbounded,
        MIN(reports.time_to_first_contact) OVER W90d AS min_ra_time_first_reply_seconds_by_merchant_last_90d,
        MIN(reports.time_to_first_contact) OVER W60d AS min_ra_time_first_reply_seconds_by_merchant_last_60d,
        MIN(reports.time_to_first_contact) OVER W30d AS min_ra_time_first_reply_seconds_by_merchant_last_30d,
        MIN(reports.time_to_first_contact) OVER W15d AS min_ra_time_first_reply_seconds_by_merchant_last_15d,
        MIN(reports.time_to_first_contact) OVER W7d AS min_ra_time_first_reply_seconds_by_merchant_last_7d, 
        --AVG time to first CSE interaction in seconds
        AVG(reports.time_to_cse_interaction) OVER Wunb AS avg_ra_time_first_interaction_seconds_from_cse_unbounded,
        AVG(reports.time_to_cse_interaction) OVER W90d AS avg_ra_time_first_interaction_seconds_from_cse_last_90d,
        AVG(reports.time_to_cse_interaction) OVER W60d AS avg_ra_time_first_interaction_seconds_from_cse_last_60d,
        AVG(reports.time_to_cse_interaction) OVER W30d AS avg_ra_time_first_interaction_seconds_from_cse_last_30d,
        AVG(reports.time_to_cse_interaction) OVER W15d AS avg_ra_time_first_interaction_seconds_from_cse_last_15d,
        AVG(reports.time_to_cse_interaction) OVER W7d AS avg_ra_time_first_interaction_seconds_from_cse_last_7d,
        --Max time to first CSE interaction in seconds    
        MAX(reports.time_to_cse_interaction) OVER Wunb AS max_ra_time_first_interaction_seconds_from_cse_unbounded,
        MAX(reports.time_to_cse_interaction) OVER W90d AS max_ra_time_first_interaction_seconds_from_cse_last_90d,
        MAX(reports.time_to_cse_interaction) OVER W60d AS max_ra_time_first_interaction_seconds_from_cse_last_60d,
        MAX(reports.time_to_cse_interaction) OVER W30d AS max_ra_time_first_interaction_seconds_from_cse_last_30d,
        MAX(reports.time_to_cse_interaction) OVER W15d AS max_ra_time_first_interaction_seconds_from_cse_last_15d,
        MAX(reports.time_to_cse_interaction) OVER W7d AS max_ra_time_first_interaction_seconds_from_cse_last_7d,
        --Min time to first CSE interaction in seconds    
        MIN(reports.time_to_cse_interaction) OVER Wunb AS min_ra_time_first_interaction_seconds_from_cse_unbounded,
        MIN(reports.time_to_cse_interaction) OVER W90d AS min_ra_time_first_interaction_seconds_from_cse_last_90d,
        MIN(reports.time_to_cse_interaction) OVER W60d AS min_ra_time_first_interaction_seconds_from_cse_last_60d,
        MIN(reports.time_to_cse_interaction) OVER W30d AS min_ra_time_first_interaction_seconds_from_cse_last_30d,
        MIN(reports.time_to_cse_interaction) OVER W15d AS min_ra_time_first_interaction_seconds_from_cse_last_15d,
        MIN(reports.time_to_cse_interaction) OVER W7d AS min_ra_time_first_interaction_seconds_from_cse_last_7d,

    FROM 
        merchants
    LEFT JOIN 
        reports
    ON  
        merchants.user_id = reports.user_id

    WINDOW
        Wunb AS (
        PARTITION BY
        merchants.user_id
        ORDER BY
        UNIX_SECONDS(TIMESTAMP(reports.created_at)) RANGE BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
        ),
        W90d AS (
        PARTITION BY
        merchants.user_id
        ORDER BY
        UNIX_SECONDS(TIMESTAMP(reports.created_at)) RANGE BETWEEN 7776000 PRECEDING
        AND CURRENT ROW
        ),
        W60d AS (
        PARTITION BY
        merchants.user_id
        ORDER BY
        UNIX_SECONDS(TIMESTAMP(reports.created_at)) RANGE BETWEEN 5184000 PRECEDING
        AND CURRENT ROW
        ),
        W30d AS (
        PARTITION BY
        merchants.user_id
        ORDER BY
        UNIX_SECONDS(TIMESTAMP(reports.created_at)) RANGE BETWEEN 2592000 PRECEDING
        AND CURRENT ROW
        ),
        W15d AS (
        PARTITION BY
        merchants.user_id
        ORDER BY
        UNIX_SECONDS(TIMESTAMP(reports.created_at)) RANGE BETWEEN 1296000 PRECEDING
        AND CURRENT ROW
        ),
        W7d AS (
        PARTITION BY
        merchants.user_id
        ORDER BY
        UNIX_SECONDS(TIMESTAMP(reports.created_at)) RANGE BETWEEN 604800 PRECEDING
        AND CURRENT ROW
        )
)

SELECT * 
FROM 
    merchant_agg