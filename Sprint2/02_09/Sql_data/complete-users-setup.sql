-- =============================================
-- Complete Users Setup Script
-- Combined table creation and data insertion
-- =============================================
-- 
-- This script creates the users table and inserts existing user data
-- Use this for a complete setup in one go
--
-- Usage:
-- psql -h localhost -p 5432 -U postgres -d your_database -f complete-users-setup.sql
-- =============================================

-- Start transaction
BEGIN;

-- Drop existing table if it exists (uncomment if needed)
-- DROP TABLE IF EXISTS users CASCADE;

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user' NOT NULL,
    is_active BOOLEAN DEFAULT true NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    created_by INTEGER REFERENCES users(id)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_created_by ON users(created_by);

-- Create update trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger
DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at 
    BEFORE UPDATE ON users 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Add constraints
ALTER TABLE users ADD CONSTRAINT IF NOT EXISTS chk_role CHECK (role IN ('admin', 'user'));
ALTER TABLE users ADD CONSTRAINT IF NOT EXISTS chk_username_length CHECK (LENGTH(username) >= 3);
ALTER TABLE users ADD CONSTRAINT IF NOT EXISTS chk_email_format CHECK (email LIKE '%@%.%');

-- Insert users data
INSERT INTO users (
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

-- Update sequence
SELECT setval('users_id_seq', (SELECT MAX(id) FROM users));

-- Grant permissions (adjust as needed)
-- GRANT SELECT, INSERT, UPDATE, DELETE ON users TO your_app_user;
-- GRANT USAGE, SELECT ON SEQUENCE users_id_seq TO your_app_user;

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
FROM users 
ORDER BY id;

-- Success message
\echo '=============================================';
\echo 'Users table and data setup completed!';
\echo '=============================================';
\echo 'Table: users created with 2 users';
\echo 'Users: admin (admin), sakthi (user)';
\echo 'Features: auto-timestamps, role validation, indexes';
\echo 'Ready for authentication system integration';
\echo '=============================================';
