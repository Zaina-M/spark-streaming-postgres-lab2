CREATE TABLE IF NOT EXISTS ecommerce_events (
    event_id VARCHAR(36) PRIMARY KEY,
    user_id INT NOT NULL,
    event_type VARCHAR(20) NOT NULL CHECK (event_type IN ('view', 'purchase')),
    product_id INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    event_time TIMESTAMP NOT NULL
);
CREATE INDEX idx_ecommerce_events_event_time
ON ecommerce_events (event_time);

CREATE INDEX idx_ecommerce_events_user_id
ON ecommerce_events (user_id);
