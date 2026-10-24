-- Gold paso a paso · 06b · App: Materialized View de CTR por ubicación (Sesión 20)
--
-- EJECUTAR EN EL SQL EDITOR con un SQL warehouse (no en un notebook: las Materialized Views
-- se crean y refrescan sobre un SQL warehouse serverless).
-- Requiere: haber corrido 06_app_metricas.py (crea gold.app_lab.metricas_funnel_diario).
--
-- Una Materialized View es una tabla cuyo contenido es el resultado de una consulta y que Databricks
-- sabe mantener al día. Se escribe solo el SELECT; el refresco es REFRESH (incremental cuando puede).

-- 1. Crear la MV
CREATE OR REPLACE MATERIALIZED VIEW gold.app_lab.mv_ctr_ubicacion
COMMENT 'CTR y funnel por ubicación en pantalla (se refresca con REFRESH MATERIALIZED VIEW)'
AS
SELECT
  ubicacion_pantalla,
  sum(vistas)                                                   AS vistas,
  sum(clicks)                                                   AS clicks,
  sum(simulaciones)                                             AS simulaciones,
  sum(solicitudes_iniciadas)                                    AS solicitudes_iniciadas,
  round(try_divide(sum(clicks), sum(vistas)), 4)                AS ctr,
  round(try_divide(sum(solicitudes_iniciadas), sum(vistas)), 4) AS conversion_vista_a_solicitud
FROM gold.app_lab.metricas_funnel_diario
GROUP BY ubicacion_pantalla;

-- 2. Consultarla: se lee como una tabla, el cómputo ya está hecho
SELECT * FROM gold.app_lab.mv_ctr_ubicacion ORDER BY ctr DESC;

-- 3. Ponerla al día cuando la tabla fuente cambie (en producción: un job, o SCHEDULE en la definición)
REFRESH MATERIALIZED VIEW gold.app_lab.mv_ctr_ubicacion;

-- 4. Ver cómo se refrescó: sección 'Refresh Information' (Last Refreshed, Last Refresh Type, Refresh Schedule).
--    Last Refresh Type = NO OPERATION si la fuente no cambió; INCREMENTAL o FULL_RECOMPUTE si cambió.
DESCRIBE EXTENDED gold.app_lab.mv_ctr_ubicacion;
