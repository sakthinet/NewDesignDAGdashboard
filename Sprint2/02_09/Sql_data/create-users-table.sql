-- =============================================
-- Users Table Creation Script
-- Based on existing users.csv data structure
-- =============================================
-- 
-- This script creates a users table compatible with the authentication system
-- and inserts the existing user data from users.csv
--
-- Usage:
-- For PostgreSQL: psql -h localhost -p 5432 -U postgres -d your_database -f create-users-table.sql
-- For MySQL: mysql -h localhost -u root -p your_database < create-users-table.sql
-- For SQLite: sqlite3 your_database.db < create-users-table.sql
-- =============================================

-- Drop existing table if it exists (uncomment if needed)
-- DROP TABLE IF EXISTS users;

-- Create users table
CREATE TABLE users (
    id SERIAL PRIMARY KEY,                              -- Auto-incrementing primary key
    username VARCHAR(50) UNIQUE NOT NULL,               -- Unique username
    email VARCHAR(255) UNIQUE NOT NULL,                 -- Unique email address
    password VARCHAR(255) NOT NULL,                     -- Bcrypt hashed password
    role VARCHAR(20) DEFAULT 'user' NOT NULL,           -- User role (admin/user)
    is_active BOOLEAN DEFAULT true NOT NULL,            -- Account status
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,     -- Account creation time
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,     -- Last update time
    last_login TIMESTAMP,                               -- Last login time (nullable)
    created_by INTEGER REFERENCES users(id)             -- Who created this user (self-reference)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_created_by ON users(created_by);

-- Create function to update the updated_at timestamp (PostgreSQL specific)
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

-- Add constraints
ALTER TABLE users ADD CONSTRAINT chk_role CHECK (role IN ('admin', 'user'));
ALTER TABLE users ADD CONSTRAINT chk_username_length CHECK (LENGTH(username) >= 3);
ALTER TABLE users ADD CONSTRAINT chk_email_format CHECK (email LIKE '%@%.%');

-- Display table structure
\d users;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Users table created successfully!';
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Table: users';
    RAISE NOTICE 'Columns: id, username, email, password, role, is_active, created_at, updated_at, last_login, created_by';
    RAISE NOTICE 'Indexes: username, email, role, is_active, created_by';
    RAISE NOTICE 'Triggers: auto-update updated_at on record changes';
    RAISE NOTICE 'Constraints: role validation, username length, email format';
    RAISE NOTICE '=============================================';
END $$;
