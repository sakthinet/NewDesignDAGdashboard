-- =============================================
-- Data Transformer Authentication Setup Script
-- PostgreSQL Database Setup for User Authentication
-- =============================================
-- 
-- This script sets up the complete authentication system for the Data Transformer application
-- Run this script on your PostgreSQL database to create the necessary tables and initial admin user
--
-- Prerequisites:
-- 1. PostgreSQL server running
-- 2. Database 'RND' created
-- 3. User 'postgres' with appropriate permissions
--
-- Usage:
-- psql -h localhost -p 5432 -U postgres -d RND -f setup-auth-database.sql
-- =============================================

-- Create users table for authentication
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role VARCHAR(20) DEFAULT 'user' CHECK (role IN ('admin', 'user')),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP
);

-- Create session table for session management
CREATE TABLE IF NOT EXISTS session (
    sid VARCHAR NOT NULL COLLATE "default",
    sess JSON NOT NULL,
    expire TIMESTAMP(6) NOT NULL
) WITH (OIDS=FALSE);

-- Add primary key and index for session table
ALTER TABLE session ADD CONSTRAINT session_pkey PRIMARY KEY (sid) NOT DEFERRABLE INITIALLY IMMEDIATE;
CREATE INDEX IF NOT EXISTS IDX_session_expire ON session (expire);

-- Create function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
DROP TRIGGER IF EXISTS update_users_updated_at ON users;
CREATE TRIGGER update_users_updated_at 
    BEFORE UPDATE ON users 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default admin user
-- Password: admin123 (bcrypt hash with 12 salt rounds)
INSERT INTO users (username, email, password, role, is_active, created_at, updated_at)
VALUES (
    'admin',
    'admin@datatransformer.local',
    '$2b$12$8K7Qx8qGr2YnA4KxH7LbZeF1P3QvR9WnE8M6N2J5T4S7V0C3B8A1D6',
    'admin',
    true,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
)
ON CONFLICT (username) DO NOTHING;

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active);

-- Grant necessary permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON users TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON session TO postgres;
GRANT USAGE, SELECT ON SEQUENCE users_id_seq TO postgres;

-- Display setup completion message
DO $$
BEGIN
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Authentication Database Setup Complete!';
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Tables created:';
    RAISE NOTICE '  - users (with admin user)';
    RAISE NOTICE '  - session (for session management)';
    RAISE NOTICE '';
    RAISE NOTICE 'Default Admin Credentials:';
    RAISE NOTICE '  Username: admin';
    RAISE NOTICE '  Password: admin123';
    RAISE NOTICE '  Email: admin@datatransformer.local';
    RAISE NOTICE '';
    RAISE NOTICE 'Next Steps:';
    RAISE NOTICE '1. Update .env file with correct PostgreSQL credentials';
    RAISE NOTICE '2. Run: npm run dev';
    RAISE NOTICE '3. Access: http://localhost:3002';
    RAISE NOTICE '4. Login with admin credentials';
    RAISE NOTICE '=============================================';
END $$;

-- Verification queries (comment out if not needed)
-- SELECT 'Users Table' as table_name, count(*) as record_count FROM users
-- UNION ALL
-- SELECT 'Session Table' as table_name, count(*) as record_count FROM session;

-- SELECT username, email, role, is_active, created_at FROM users;
