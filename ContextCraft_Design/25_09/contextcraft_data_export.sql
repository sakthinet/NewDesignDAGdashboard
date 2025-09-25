-- ContextCraft Database Data Export
-- Generated on: 2025-09-25T15:47:13.606Z
-- This file contains only data, not schema
-- Apply your schema migrations first, then run this script

BEGIN;

-- Table api_endpoints is empty

-- Table audit_logs is empty

-- Data for table: connectors (2 rows)
DELETE FROM "connectors"; -- Clear existing data
INSERT INTO "connectors" ("id", "name", "description", "type", "config", "status", "last_sync", "last_sync_status", "documents_count", "total_size_bytes", "error_message", "created_by", "created_at", "updated_at") VALUES ('a0129008-32a2-450d-b2d7-19380668bf72', 'Test S3 Connector', NULL, 's3', '{"bucket":"test-bucket","region":"us-east-1","accessKeyId":"test-key","secretAccessKey":"test-secret"}', 'inactive', NULL, NULL, 0, '0', NULL, NULL, '2025-09-24T12:43:30.897Z', '2025-09-24T12:43:30.897Z');
INSERT INTO "connectors" ("id", "name", "description", "type", "config", "status", "last_sync", "last_sync_status", "documents_count", "total_size_bytes", "error_message", "created_by", "created_at", "updated_at") VALUES ('bad7d20c-2e3d-4f18-84bd-7e0d5704a102', 'MyGCPConnector', NULL, 'gcs', '{"projectId":"123456","bucketName":"GCP_Bucket","pathPrefix":"","filePattern":"","serviceAccountJson":"\"type\": \"service_account\",\n  \"project_id\": \"my-gcp-project-id\",\n  \"private_key_id\": \"0123456789abcdef0123456789abcdef01234567\",\n  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKg...\\n-----END PRIVATE KEY-----\\n\",\n  \"client_email\": \"my-service-account@my-gcp-project-id.iam.gserviceaccount.com\",\n  \"client_id\": \"123456789012345678901\",\n  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/my-service-account%40my-gcp-project-id.iam.gserviceaccount.com\",\n  \"universe_domain\": \"googleapis.com\""}', 'inactive', NULL, NULL, 0, '0', NULL, NULL, '2025-09-24T13:54:28.383Z', '2025-09-24T13:54:28.381Z');

-- Table documents is empty

-- Table parser_profiles is empty

-- Table pipeline_executions is empty

-- Table pipelines is empty

-- Data for table: system_settings (3 rows)
DELETE FROM "system_settings"; -- Clear existing data
INSERT INTO "system_settings" ("id", "category", "key", "value", "description", "is_encrypted", "is_public", "created_by", "created_at", "updated_at") VALUES ('3165d60e-83c7-4bb5-9231-01695eef86a1', 'embedding', 'default_model', 'text-embedding-3-small', 'Default embedding model for text processing', false, true, NULL, '2025-09-24T11:56:24.107Z', '2025-09-24T11:56:24.107Z');
INSERT INTO "system_settings" ("id", "category", "key", "value", "description", "is_encrypted", "is_public", "created_by", "created_at", "updated_at") VALUES ('f088b541-5c71-4544-bebc-59d676c4289f', 'system', 'max_file_size_mb', 100, 'Maximum file size allowed for upload in MB', false, true, NULL, '2025-09-24T11:56:24.107Z', '2025-09-24T11:56:24.107Z');
INSERT INTO "system_settings" ("id", "category", "key", "value", "description", "is_encrypted", "is_public", "created_by", "created_at", "updated_at") VALUES ('4bcc5ce9-24de-44e2-a671-6b4e654aa9d9', 'notifications', 'smtp_settings', '{"host":"","port":587,"password":"","username":""}', 'SMTP configuration for email notifications', false, false, NULL, '2025-09-24T11:56:24.107Z', '2025-09-24T11:56:24.107Z');

-- Data for table: users (1 rows)
DELETE FROM "users"; -- Clear existing data
INSERT INTO "users" ("id", "username", "password", "email", "full_name", "role", "is_active", "created_at", "updated_at", "last_login") VALUES ('d5cecb04-18b5-4f5d-9f65-b1e37535143e', 'admin', '$2b$10$example_hashed_password', 'admin@contextcraft.com', 'System Administrator', 'admin', true, '2025-09-24T11:56:24.104Z', '2025-09-24T11:56:24.104Z', NULL);

COMMIT;

-- Data export completed successfully