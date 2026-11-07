-- Sesión 20 · Paso 5 · Metric View sobre una métrica materializada: el funnel de la app.
-- metricas_funnel_app_diario_gld ya guarda las BASES por día (vistas, clicks…). La metric view las suma al corte que
-- se pida y recién ahí divide: el CTR de un mes sale de sum(clicks)/sum(vistas), no del promedio de los CTR diarios.
CREATE OR REPLACE VIEW datawizard_dev.lending.kpi_funnel_app
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "KPIs del funnel de la app de Wizard Bank (datos sintéticos): de ver una oferta a iniciar una solicitud, por día, canal y ubicación en pantalla."
source: datawizard_dev.lending.metricas_funnel_app_diario_gld
dimensions:
  - name: fecha
    expr: source.fecha
    comment: "Día del evento"
  - name: mes
    expr: "trunc(source.fecha, 'MM')"
    comment: "Mes del evento, como su día 1"
  - name: canal
    expr: source.canal
    comment: "APP_ANDROID, APP_IOS o WEB"
  - name: ubicacion_pantalla
    expr: source.ubicacion_pantalla
    comment: "Dónde se mostró la oferta: home_banner, home_card, push o detalle_oferta"
measures:
  - name: vistas
    expr: SUM(source.vistas)
    comment: "Ofertas vistas"
  - name: clicks
    expr: SUM(source.clicks)
    comment: "Clicks en ofertas"
  - name: simulaciones
    expr: SUM(source.simulaciones)
    comment: "Simulaciones de crédito"
  - name: solicitudes_iniciadas
    expr: SUM(source.solicitudes_iniciadas)
    comment: "Solicitudes iniciadas desde la app"
  - name: ctr
    expr: MEASURE(clicks) / MEASURE(vistas)
    comment: "Click-through rate: clicks / vistas (0 a 1)"
  - name: tasa_click_a_simulacion
    expr: MEASURE(simulaciones) / MEASURE(clicks)
    comment: "Simulaciones por click (0 a 1)"
  - name: conversion_vista_a_solicitud
    expr: MEASURE(solicitudes_iniciadas) / MEASURE(vistas)
    comment: "Solicitudes iniciadas por oferta vista (0 a 1)"
$$
