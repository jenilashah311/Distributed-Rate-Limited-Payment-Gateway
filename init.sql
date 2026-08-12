CREATE TABLE IF NOT EXISTS idempotency_keys (
    key_uuid UUID PRIMARY KEY,
    response_code INTEGER NOT NULL,
    response_body TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
