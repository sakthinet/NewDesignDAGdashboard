-- =============================================
-- SQLite Compatible Users Setup Script
-- For SQLite databases
-- =============================================

-- Create users table (SQLite syntax)
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT DEFAULT 'user' NOT NULL CHECK (role IN ('admin', 'user')),
    is_active BOOLEAN DEFAULT 1 NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    last_login DATETIME,
    created_by INTEGER,
    
    FOREIGN KEY (created_by) REFERENCES users(id),
    CHECK (LENGTH(username) >= 3),
    CHECK (email LIKE '%@%.%')
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_created_by ON users(created_by);

-- Create trigger for auto-updating updated_at
CREATE TRIGGER IF NOT EXISTS update_users_updated_at 
    AFTER UPDATE ON users
    FOR EACH ROW
BEGIN
    UPDATE users SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
END;

-- Insert users data
INSERT OR REPLACE INTO users (
    id, username, email, password, role, is_active, 
    created_at, updated_at, last_login, created_by
) VALUES 
(
    2,
    'admin',
    'admin@datatransformer.local',
    '$2b$12$knwb5NPr3wmI.oo2umRLoeIH7nFjDflrJNfMSLOlXg060Ghp1YtNC',
    'admin',
    1,
    '2025-08-20 13:59:09',
    '2025-09-02 12:35:32',
    '2025-09-02 12:35:32',
    1
),
(
    3,
    'sakthi',
    '233264@tcs.com',
    '$2b$12$iP0VEEvQL9ax0rYtUR3HOe3WDetS/ImrpZsLGZb4cmTWJDdfiYPma',
    'user',
    1,
    '2025-08-21 06:25:40',
    '2025-08-22 09:27:33',
    '2025-08-22 09:27:33',
    2
);

-- Verification
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
