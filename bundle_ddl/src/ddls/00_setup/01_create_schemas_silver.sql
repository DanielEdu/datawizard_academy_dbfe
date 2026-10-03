-- Schemas Silver, uno por dominio de origen (los mismos que Bronze + app)
-- lending    → estado vigente, maestras SCD2 e historiales de la base transaccional Lending
-- cobranzas  → estado vigente y hechos del sistema legado de cobranzas
-- app        → eventos de la app (Kafka) y sesiones derivadas
-- Cada job de Silver es dueño de un schema; la integración entre dominios es trabajo de Gold.
CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog);
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.lending')   COMMENT 'Silver de Lending: tablas tipadas, deduplicadas y con estado resuelto desde bronze.lending. Incluye dim_clientes y dim_productos (SCD2) y los historiales de estados.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.cobranzas') COMMENT 'Silver del sistema legado de cobranzas: cuotas con estado vigente, pagos y gestiones como hechos inmutables.';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:catalog || '.app')       COMMENT 'Silver de la telemetría de la app: eventos deduplicados con watermark y sesiones construidas con session_window.';
