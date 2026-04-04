-- Create schema
CREATE SCHEMA IF NOT EXISTS murex;

-- Create sequence for ID generation
CREATE SEQUENCE IF NOT EXISTS murex.id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    CACHE 1;

-- Create sequence for GlobalID (version_id) generation
CREATE SEQUENCE IF NOT EXISTS murex.version_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MAXVALUE
    CACHE 1;

-- Function to get next batch of IDs
CREATE OR REPLACE FUNCTION murex.get_next_batch_id(batch_size INTEGER)
RETURNS BIGINT AS $$
DECLARE
    start_id BIGINT;
BEGIN
    -- Get the current value and increment by batch_size
    SELECT nextval('murex.id_seq') INTO start_id;

    -- Set the sequence to skip ahead
    IF batch_size > 1 THEN
        PERFORM setval('murex.id_seq', start_id + batch_size - 1, true);
    END IF;

    RETURN start_id;
END;
$$ LANGUAGE plpgsql;

-- Function to get next batch of GlobalIDs (version_ids)
CREATE OR REPLACE FUNCTION murex.get_next_batch_version_id(batch_size INTEGER)
RETURNS BIGINT AS $$
DECLARE
    start_id BIGINT;
BEGIN
    -- Get the current value and increment by batch_size
    SELECT nextval('murex.version_id_seq') INTO start_id;

    -- Set the sequence to skip ahead
    IF batch_size > 1 THEN
        PERFORM setval('murex.version_id_seq', start_id + batch_size - 1, true);
    END IF;

    RETURN start_id;
END;
$$ LANGUAGE plpgsql;

-- Create object_lock table for distributed locking
CREATE TABLE IF NOT EXISTS murex.object_lock (
    global_id BIGINT PRIMARY KEY,
    token VARCHAR(36) NOT NULL,
    expire TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Create index for cleanup job (expired locks)
CREATE INDEX IF NOT EXISTS idx_object_lock_expire ON murex.object_lock(expire);

-- Create index for token lookups
CREATE INDEX IF NOT EXISTS idx_object_lock_token ON murex.object_lock(global_id, token);

-- Grant permissions
GRANT USAGE ON SCHEMA murex TO postgres;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA murex TO postgres;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA murex TO postgres;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA murex TO postgres;
