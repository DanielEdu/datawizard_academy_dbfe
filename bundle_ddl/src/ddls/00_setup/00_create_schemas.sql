-- Schemas Bronze, uno por fuente/job de ingesta
-- lending_gzl   → Azure SQL (base transaccional Wizard Bank) · ingesta desde base de datos
-- cobranzas_gzl → sistema legado on-premise de cobranzas    · ingesta batch vía ADF
CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema_lending)   COMMENT 'Bronze de la base transaccional Lending de Wizard Bank (Azure SQL). Job de ingesta desde base de datos.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.' || :schema_cobranzas) COMMENT 'Bronze del sistema legado on-premise de cobranzas. Job de ingesta batch independiente (ADF).';
