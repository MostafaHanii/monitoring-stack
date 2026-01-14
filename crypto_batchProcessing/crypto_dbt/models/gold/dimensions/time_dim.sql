{{
    config(
        materialized='table',
        schema='gold'
    )
}}
WITH seconds_cte AS (
    -- Generate all seconds in a day (0 to 86399)
    SELECT 
        SEQ4() AS seconds_since_midnight
    FROM TABLE(GENERATOR(ROWCOUNT => 86400))
),

point_in_time_cte AS (
    SELECT
        seconds_since_midnight,
        DATEADD(SECOND, seconds_since_midnight, '00:00:00'::TIME) AS point_in_time
    FROM seconds_cte
)
SELECT
    -- Primary Key
    seconds_since_midnight AS time_sk,
    
    -- Time of day
    point_in_time::TIME AS time_of_day,
    
    -- 12-hour format description
    TO_CHAR(point_in_time, 'HH12:MI:SS AM') AS time_desc_12,
    
    -- 24-hour format description
    TO_CHAR(point_in_time, 'HH24:MI:SS') AS time_desc_24,
    
    -- AM or PM
    CASE 
        WHEN DATE_PART('HOUR', point_in_time) < 12 THEN 'AM'
        ELSE 'PM'
    END AS am_or_pm,
    
    -- Time division (Day Part)
    CASE 
        WHEN DATE_PART('HOUR', point_in_time) < 6  THEN 'Night'
        WHEN DATE_PART('HOUR', point_in_time) < 12 THEN 'Morning'
        WHEN DATE_PART('HOUR', point_in_time) < 17 THEN 'Afternoon'
        WHEN DATE_PART('HOUR', point_in_time) < 20 THEN 'Evening'
        ELSE 'Night'
    END AS time_division,
    
    -- Military time (HHMM format)
    LPAD(DATE_PART('HOUR', point_in_time)::VARCHAR, 2, '0') ||
    LPAD(DATE_PART('MINUTE', point_in_time)::VARCHAR, 2, '0') AS military_time,
    
    -- Hour of day (0-23)
    DATE_PART('HOUR', point_in_time)::INT AS hour_of_day,
    
    -- Minute of day (0-1439)
    (DATE_PART('HOUR', point_in_time) * 60 + DATE_PART('MINUTE', point_in_time))::INT AS minute_of_day,
    
    -- Minute of hour (0-59)
    DATE_PART('MINUTE', point_in_time)::INT AS minute_of_hour,
    
    -- Second of day (0-86399)
    seconds_since_midnight AS second_of_day,
    
    -- Second of hour (0-3599)
    (DATE_PART('MINUTE', point_in_time) * 60 + DATE_PART('SECOND', point_in_time))::INT AS second_of_hour,
    
    -- Second of minute (0-59)
    DATE_PART('SECOND', point_in_time)::INT AS second_of_minute,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at

FROM point_in_time_cte