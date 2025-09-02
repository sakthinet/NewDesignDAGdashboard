-- =============================================
-- MySQL Compatible Users Setup Script
-- For MySQL/MariaDB databases
-- =============================================

-- Create users table (MySQL syntax)
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role ENUM('admin', 'user') DEFAULT 'user' NOT NULL,
    is_active BOOLEAN DEFAULT TRUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    last_login TIMESTAMP NULL,
    created_by INT,
    
    INDEX idx_users_username (username),
    INDEX idx_users_email (email),
    INDEX idx_users_role (role),
    INDEX idx_users_active (is_active),
    INDEX idx_users_created_by (created_by),
    
    FOREIGN KEY (created_by) REFERENCES users(id),
    CONSTRAINT chk_username_length CHECK (LENGTH(username) >= 3),
    CONSTRAINT chk_email_format CHECK (email LIKE '%@%.%')
);

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
    TRUE,
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
    TRUE,
    '2025-08-21 06:25:40',
    '2025-08-22 09:27:33',
    '2025-08-22 09:27:33',
    2
)
ON DUPLICATE KEY UPDATE
    email = VALUES(email),
    password = VALUES(password),
    role = VALUES(role),
    is_active = VALUES(is_active),
    updated_at = VALUES(updated_at),
    last_login = VALUES(last_login),
    created_by = VALUES(created_by);

-- Reset auto increment
ALTER TABLE users AUTO_INCREMENT = 4;

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
