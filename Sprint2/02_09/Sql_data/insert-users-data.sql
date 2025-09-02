-- =============================================
-- Users Data Insert Script
-- Inserts existing user data from users.csv
-- =============================================
-- 
-- This script inserts the existing users data into the users table
-- Data includes admin and sakthi users with their encrypted passwords
--
-- Prerequisites:
-- 1. Users table must exist (run create-users-table.sql first)
-- 2. Database connection established
--
-- Usage:
-- psql -h localhost -p 5432 -U postgres -d your_database -f insert-users-data.sql
-- =============================================

-- Insert admin user (id: 2)
INSERT INTO users (
    id,
    username,
    email,
    password,
    role,
    is_active,
    created_at,
    updated_at,
    last_login,
    created_by
) VALUES (
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
) ON CONFLICT (username) DO UPDATE SET
    email = EXCLUDED.email,
    password = EXCLUDED.password,
    role = EXCLUDED.role,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at,
    last_login = EXCLUDED.last_login;

-- Insert sakthi user (id: 3)
INSERT INTO users (
    id,
    username,
    email,
    password,
    role,
    is_active,
    created_at,
    updated_at,
    last_login,
    created_by
) VALUES (
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
) ON CONFLICT (username) DO UPDATE SET
    email = EXCLUDED.email,
    password = EXCLUDED.password,
    role = EXCLUDED.role,
    is_active = EXCLUDED.is_active,
    updated_at = EXCLUDED.updated_at,
    last_login = EXCLUDED.last_login,
    created_by = EXCLUDED.created_by;

-- Update sequence to start from next available ID
SELECT setval('users_id_seq', (SELECT MAX(id) FROM users));

-- Verify the inserted data
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

-- Display summary
DO $$
DECLARE
    user_count INTEGER;
    admin_count INTEGER;
    active_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO user_count FROM users;
    SELECT COUNT(*) INTO admin_count FROM users WHERE role = 'admin';
    SELECT COUNT(*) INTO active_count FROM users WHERE is_active = true;
    
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Users data inserted successfully!';
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Total users: %', user_count;
    RAISE NOTICE 'Admin users: %', admin_count;
    RAISE NOTICE 'Active users: %', active_count;
    RAISE NOTICE '';
    RAISE NOTICE 'User Details:';
    RAISE NOTICE '1. admin - admin@datatransformer.local (Admin)';
    RAISE NOTICE '2. sakthi - 233264@tcs.com (User)';
    RAISE NOTICE '';
    RAISE NOTICE 'Note: Passwords are bcrypt hashed with 12 salt rounds';
    RAISE NOTICE 'Admin password: admin123';
    RAISE NOTICE 'Sakthi password: [original password from system]';
    RAISE NOTICE '=============================================';
END $$;
