# Users Table Setup Scripts

This `Sql_data` directory contains SQL scripts to create a users table and insert data based on the existing users.csv file.

## 📁 Directory Contents

- `users.csv` - Original user data export
- `setup-auth-database.sql` - Original authentication database setup
- User table creation and data scripts (see below)

## 📋 Available Scripts

### 1. **PostgreSQL Scripts** (Recommended)
- `create-users-table.sql` - Creates users table with full PostgreSQL features
- `insert-users-data.sql` - Inserts user data from CSV
- `complete-users-setup.sql` - Combined creation and data insertion

### 2. **MySQL/MariaDB Script**
- `mysql-users-setup.sql` - Complete setup for MySQL databases

### 3. **SQLite Script**
- `sqlite-users-setup.sql` - Complete setup for SQLite databases

## 🚀 Usage Instructions

### PostgreSQL
```bash
# Navigate to Sql_data directory first
cd Sql_data

# Option 1: Separate scripts
psql -h localhost -p 5432 -U postgres -d your_database -f create-users-table.sql
psql -h localhost -p 5432 -U postgres -d your_database -f insert-users-data.sql

# Option 2: Combined script (recommended)
psql -h localhost -p 5432 -U postgres -d your_database -f complete-users-setup.sql

# Option 3: Original auth setup
psql -h localhost -p 5432 -U postgres -d your_database -f setup-auth-database.sql
```

### MySQL/MariaDB
```bash
cd Sql_data
mysql -h localhost -u root -p your_database < mysql-users-setup.sql
```

### SQLite
```bash
cd Sql_data
sqlite3 your_database.db < sqlite-users-setup.sql
```

## 📊 Table Structure

```sql
CREATE TABLE users (
    id              SERIAL PRIMARY KEY,
    username        VARCHAR(50) UNIQUE NOT NULL,
    email           VARCHAR(255) UNIQUE NOT NULL,
    password        VARCHAR(255) NOT NULL,        -- bcrypt hashed
    role            VARCHAR(20) DEFAULT 'user',   -- 'admin' or 'user'
    is_active       BOOLEAN DEFAULT true,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login      TIMESTAMP,
    created_by      INTEGER REFERENCES users(id)
);
```

## 👥 Existing Users Data

The scripts will insert these users from your CSV:

| ID | Username | Email | Role | Status |
|----|----------|-------|------|--------|
| 2 | admin | admin@datatransformer.local | admin | Active |
| 3 | sakthi | 233264@tcs.com | user | Active |

## 🔒 Security Features

- **Password Hashing**: All passwords are bcrypt hashed with 12 salt rounds
- **Unique Constraints**: Username and email must be unique
- **Role Validation**: Only 'admin' and 'user' roles allowed
- **Email Validation**: Basic email format checking
- **Username Length**: Minimum 3 characters required

## ⚡ Performance Features

- **Indexes**: Created on username, email, role, is_active, created_by
- **Auto-timestamps**: updated_at automatically updates on record changes
- **Foreign Keys**: Self-referencing for created_by field

## 🔧 Authentication Integration

These scripts are compatible with:
- ✅ Passport.js with bcrypt
- ✅ Express.js authentication middleware
- ✅ Drizzle ORM
- ✅ Node.js authentication systems

## 🎯 Login Credentials

After running the scripts, you can login with:

**Admin User:**
- Username: `admin`
- Password: `admin123`
- Email: `admin@datatransformer.local`

**Regular User:**
- Username: `sakthi`
- Password: `[original password from system]`
- Email: `233264@tcs.com`

## 🛠️ Customization

To modify for your system:

1. **Change database name** in connection strings
2. **Update user credentials** if needed
3. **Add/remove columns** as required
4. **Modify constraints** based on your requirements
5. **Adjust indexes** for your query patterns

## 📝 Notes

- The admin password hash corresponds to `admin123`
- All timestamps are preserved from the original CSV data
- Self-referencing foreign key allows tracking who created each user
- Scripts include conflict resolution (ON CONFLICT/ON DUPLICATE KEY)
- Auto-increment sequences are properly reset after data insertion

## 🆘 Support

If you encounter issues:
1. Check database permissions
2. Ensure the target database exists
3. Verify the database user has CREATE TABLE privileges
4. Check for existing tables with the same name
