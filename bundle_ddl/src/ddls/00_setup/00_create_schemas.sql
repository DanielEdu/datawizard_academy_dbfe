-- Schemas Bronze, uno por fuente/job de ingesta
-- lending    → Azure SQL (base transaccional Wizard Bank)  · ingesta desde base de datos
-- cobranzas  → sistema legado on-premise de cobranzas      · ingesta batch vía ADF
-- _richard   → reto: mismas tablas de cobranzas, cargadas por el bundle bronze/cobranzas_richard
-- app        → telemetría de la app (topic Kafka wizard.lending.eventos-app)
-- cobranzas_jhersson → reto: mismas tablas de cobranzas, cargadas por el bundle bronze/cobranzas_jhersson
-- cobranzas_acarrasco → reto: mismas tablas de cobranzas (_brz), cargadas por el bundle bronze/cobranzas_acarrasco
CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.lending')   COMMENT 'Bronze de la base transaccional Lending de Wizard Bank (Azure SQL). Job de ingesta desde base de datos.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.cobranzas') COMMENT 'Bronze del sistema legado on-premise de cobranzas. Job de ingesta batch independiente (ADF).';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '._richard')   COMMENT 'Reto: Bronze de cobranzas (cuotas, pagos, gestiones_cobranza) cargado con Auto Loader por el bundle cobranzas_richard.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.cobranzas_jhersson') COMMENT 'Reto: Bronze de cobranzas (cuotas, pagos, gestiones_cobranza) cargado con Auto Loader por el bundle cobranzas_jhersson.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.app')       COMMENT 'Bronze de la telemetría de la app (topic Kafka wizard.lending.eventos-app). Job de ingesta bronze/app.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.cobranzas_acarrasco') COMMENT 'Bronze de cobranzas cargado con Auto Loader por el bundle cobranzas_acarrasco. Tablas con sufijo _brz.';
