-- ============================================================================
-- E-Commerce Streaming Pipeline - PostgreSQL Schema
-- ============================================================================

-- Main events table with enrichment columns
-- Note: user_id can be NULL for anonymous view/search events (guest browsing)
CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id VARCHAR(36) PRIMARY KEY,  -- PRIMARY KEY already ensures uniqueness (prevents duplicates)
    user_id INT,  -- NULL allowed for anonymous/guest users (view, search events)
    session_id VARCHAR(50),
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'purchase', 'add_to_cart', 'remove_from_cart', 'wishlist', 'search')),
    product_id INT NOT NULL,
    category VARCHAR(50) DEFAULT 'unknown',
    price NUMERIC(10,2) NOT NULL CHECK (price >= 0),
    quantity INT DEFAULT 0,
    total_amount NUMERIC(12,2) DEFAULT 0,
    user_segment VARCHAR(20) DEFAULT 'unknown',
    search_query VARCHAR(255) DEFAULT '',
    event_time TIMESTAMP NOT NULL,
    
    -- Time-based analytics columns
    event_year INT,
    event_month INT,
    event_day INT,
    event_hour INT,
    event_dayofweek INT,
    
    -- Data quality tracking
    is_late_arrival BOOLEAN DEFAULT FALSE,
    
    -- Data lineage
    source_file VARCHAR(500),
    source_system VARCHAR(50) DEFAULT 'unknown',
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Business logic constraint: purchase/cart/wishlist events MUST have user_id
    CONSTRAINT chk_user_required_for_actions CHECK (
        (event_type IN ('view', 'search')) OR (user_id IS NOT NULL)
    )
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_events_event_time ON ecommerce_events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON ecommerce_events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON ecommerce_events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_category ON ecommerce_events (category);
CREATE INDEX IF NOT EXISTS idx_events_session_id ON ecommerce_events (session_id);
CREATE INDEX IF NOT EXISTS idx_events_user_segment ON ecommerce_events (user_segment);

-- Composite index for common queries
CREATE INDEX IF NOT EXISTS idx_events_time_type ON ecommerce_events (event_time, event_type);
CREATE INDEX IF NOT EXISTS idx_events_user_time ON ecommerce_events (user_id, event_time);


-- ============================================================================
-- Dead Letter Table (for invalid/rejected records)
-- ============================================================================

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(36),
    user_id INT,
    event_type VARCHAR(50),
    product_id INT,
    price NUMERIC(10,2),
    event_time TIMESTAMP,
    validation_errors TEXT NOT NULL,
    source_file VARCHAR(500),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reprocessed BOOLEAN DEFAULT FALSE,
    reprocessed_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_errors ON dead_letter_events (validation_errors);
CREATE INDEX IF NOT EXISTS idx_dead_letter_processed ON dead_letter_events (processed_at);


-- ============================================================================
-- Data Quality Metrics Table (for monitoring dashboards)
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    batch_id INT NOT NULL,
    total_rows INT NOT NULL,
    valid_rows INT NOT NULL,
    invalid_rows INT NOT NULL,
    validity_rate NUMERIC(5,4),
    late_arrival_count INT DEFAULT 0,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_quality_recorded ON data_quality_metrics (recorded_at);


-- ============================================================================
-- Useful Views for Analytics
-- ============================================================================

-- Hourly event summary
CREATE OR REPLACE VIEW v_hourly_event_summary AS
SELECT 
    DATE_TRUNC('hour', event_time) AS event_hour,
    event_type,
    COUNT(*) AS event_count,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(total_amount) AS total_revenue
FROM ecommerce_events
GROUP BY DATE_TRUNC('hour', event_time), event_type
ORDER BY event_hour DESC, event_type;

-- User session summary
CREATE OR REPLACE VIEW v_user_sessions AS
SELECT 
    user_id,
    session_id,
    user_segment,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS views,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
    SUM(total_amount) AS session_revenue,
    MIN(event_time) AS session_start,
    MAX(event_time) AS session_end
FROM ecommerce_events
GROUP BY user_id, session_id, user_segment;

-- Category performance
CREATE OR REPLACE VIEW v_category_performance AS
SELECT 
    category,
    COUNT(*) AS total_events,
    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
    SUM(total_amount) AS total_revenue,
    AVG(price) AS avg_price
FROM ecommerce_events
GROUP BY category
ORDER BY total_revenue DESC;

-- Data quality summary (last 24 hours)
CREATE OR REPLACE VIEW v_data_quality_summary AS
SELECT 
    DATE_TRUNC('hour', recorded_at) AS hour,
    SUM(total_rows) AS total_rows,
    SUM(valid_rows) AS valid_rows,
    SUM(invalid_rows) AS invalid_rows,
    AVG(validity_rate) AS avg_validity_rate,
    SUM(late_arrival_count) AS late_arrivals
FROM data_quality_metrics
WHERE recorded_at >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', recorded_at)
ORDER BY hour DESC;
