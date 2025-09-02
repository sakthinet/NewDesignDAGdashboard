-- =============================================
-- Complete ia_users Setup Script - pgAdmin4 Compatible
-- Combined table creation and data insertion
-- =============================================
-- 
-- This script creates the ia_users table and inserts existing user data
-- Optimized for pgAdmin4 Query Editor
-- =============================================

-- Start transaction
BEGIN;

-- Drop existing table if it exists (uncomment if needed)
-- DROP TABLE IF EXISTS ia_users CASCADE;

-- Create ia_users table
CREATE TABLE IF NOT EXISTS ia_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user' NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    created_by INTEGER
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_ia_users_username ON ia_users(username);
CREATE INDEX IF NOT EXISTS idx_ia_users_email ON ia_users(email);
CREATE INDEX IF NOT EXISTS idx_ia_users_role ON ia_users(role);
CREATE INDEX IF NOT EXISTS idx_ia_users_active ON ia_users(is_active);
CREATE INDEX IF NOT EXISTS idx_ia_users_created_by ON ia_users(created_by);

-- Create update trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger
DROP TRIGGER IF EXISTS update_ia_users_updated_at ON ia_users;
CREATE TRIGGER update_ia_users_updated_at 
    BEFORE UPDATE ON ia_users 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert initial system user (id=1) first
INSERT INTO ia_users (
    id, username, email, password, role, is_active, 
    created_at, updated_at, last_login, created_by
) VALUES 
(
    1,
    'system',
    'system@datatransformer.local',
    '$2b$12$knwb5NPr3wmI.oo2umRLoeIH7nFjDflrJNfMSLOlXg060Ghp1YtNC',
    'admin',
    true,
    '2025-08-20 13:58:00.000000',
    '2025-08-20 13:58:00.000000',
    NULL,
    NULL
)
ON CONFLICT (username) DO UPDATE SET
    email = EXCLUDED.email,
    password = EXCLUDED.password,
    role = EXCLUDED.role,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at,
    last_login = EXCLUDED.last_login,
    created_by = EXCLUDED.created_by;

-- Insert other users
INSERT INTO ia_users (
    id, username, email, password, role, is_active, 
    created_at, updated_at, last_login, created_by
) VALUES 
(
    2,
    'admin',
    'admin@datatransformer.local',
    '$2b$12$knwb5NPr3wmI.oo2umRLoeIH7nFjDflrJNfMSLOlXg060Ghp1YtNC',
    'admin',
    true,
    '2025-08-20 13:59:09.835014',
    '2025-09-02 12:35:32.955909',
    '2025-09-02 12:35:32.953',
    1
),
(
    3,
    'sakthi',
    '233264@tcs.com',
    '$2b$12$iP0VEEvQL9ax0rYtUR3HOe3WDetS/ImrpZsLGZb4cmTWJDdfiYPma',
    'user',
    true,
    '2025-08-21 06:25:40.184223',
    '2025-08-22 09:27:33.793775',
    '2025-08-22 09:27:33.792',
    2
)
ON CONFLICT (username) DO UPDATE SET
    email = EXCLUDED.email,
    password = EXCLUDED.password,
    role = EXCLUDED.role,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at,
    last_login = EXCLUDED.last_login,
    created_by = EXCLUDED.created_by;

-- Add constraints (simplified approach for pgAdmin4)
-- Drop constraints if they exist, then add them
ALTER TABLE ia_users DROP CONSTRAINT IF EXISTS chk_role;
ALTER TABLE ia_users ADD CONSTRAINT chk_role CHECK (role IN ('admin', 'user'));

ALTER TABLE ia_users DROP CONSTRAINT IF EXISTS chk_username_length;
ALTER TABLE ia_users ADD CONSTRAINT chk_username_length CHECK (LENGTH(username) >= 3);

ALTER TABLE ia_users DROP CONSTRAINT IF EXISTS chk_email_format;
ALTER TABLE ia_users ADD CONSTRAINT chk_email_format CHECK (email LIKE '%@%.%');

-- Add foreign key constraint after data insertion
ALTER TABLE ia_users DROP CONSTRAINT IF EXISTS fk_ia_users_created_by;
ALTER TABLE ia_users ADD CONSTRAINT fk_ia_users_created_by 
    FOREIGN KEY (created_by) REFERENCES ia_users(id);

-- Update sequence to correct value
SELECT setval('ia_users_id_seq', (SELECT MAX(id) FROM ia_users));

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON ia_users TO your_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE ia_users_id_seq TO your_app_user;

-- Commit transaction
COMMIT;

-- Verification query
SELECT 
    id,
    username,
    email,
    role,
    is_active,
    created_at,
    updated_at,
    last_login,
    created_by
FROM ia_users 
ORDER BY id;