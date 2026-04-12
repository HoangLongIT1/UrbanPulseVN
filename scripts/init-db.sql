-- =============================================
-- UrbanPulse VN — PostgreSQL Initialization
-- =============================================
-- This script runs automatically on first container start
-- via docker-entrypoint-initdb.d/
--
-- Creates Medallion Architecture schemas for data pipeline.

-- Schemas for Medallion Architecture
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS sandbox;

-- Confirm schemas created
DO $$
BEGIN
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'UrbanPulse VN — Database initialized';
    RAISE NOTICE '  Schemas: bronze, silver, gold, sandbox';
    RAISE NOTICE '=============================================';
END $$;
