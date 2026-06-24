# GDELT Analytics Pipeline

Pipeline de análisis de eventos mundiales usando datos de [GDELT Project](https://www.gdeltproject.org/).

> **Documentación completa:** ver [DOCUMENTACION.md](./DOCUMENTACION.md) — arquitectura, flujos, diseño de bases de datos, tecnologías y uso de IA.

## Descripción breve

Pipeline de análisis de eventos mundiales usando datos de [GDELT Project](https://www.gdeltproject.org/).
Arquitectura basada en contenedores Docker.

```
GDELT API → Loader (Parquet) → Spark (análisis) → MongoDB → Dashboard Web
                    ↑
              Airflow (cada 15 min)
```

## Arquitectura de Contenedores

| Contenedor | Función | Puerto |
|---|---|---|
| `gdelt-loader` | Descarga Events, Mentions, GKG cada 15 min | — |
| `gdelt-spark-master` | Master de Apache Spark | 8080 |
| `gdelt-spark-worker-1/2` | Workers de Spark (×2) | — |
| `gdelt-mongo` | MongoDB (resultados) | 27017 |
| `gdelt-dashboard` | Dashboard web Flask | 5000 |
| `gdelt-airflow-web` | Apache Airflow UI | 8081 |
| `gdelt-airflow-scheduler` | Scheduler Airflow | — |
| `gdelt-postgres` | BD metadata Airflow | — |

## Requisitos

- Docker Desktop 4.x+
- Docker Compose v2
- 8 GB RAM mínimo recomendado
- Conexión a internet (descarga GDELT)

## Inicio Rápido

```bash
# 1. Levantar todos los servicios
docker compose up -d --build

# 2. Esperar ~2 min a que MongoDB y Spark estén listos

# 3. El loader descarga datos automáticamente cada 15 min
#    Para forzar una descarga inmediata:
docker compose exec loader python loader.py --once

# 4. Ejecutar análisis Spark (después de que haya datos)
docker compose --profile manual run --rm spark-analysis

# 5. Abrir dashboard
# http://localhost:5000

# 6. Airflow UI (opcional)
# http://localhost:8081  (admin / admin)
```

## Almacenamiento de Datos

- **RAW/Parquet**: Volumen Docker `parquet_data` montado en `/data/parquet`
- **Retención RAW**: 1 hora (configurable via `RAW_RETENTION_HOURS`)
- **Resultados**: MongoDB colección `gdelt_analytics`

### Estructura Parquet

```
/data/parquet/
├── events/ts=YYYYMMDDHHMMSS/data.parquet
├── mentions/ts=YYYYMMDDHHMMSS/data.parquet
└── gkg/ts=YYYYMMDDHHMMSS/data.parquet
```

## Análisis Implementados (19 + 2 extras)

| # | Colección MongoDB | Descripción |
|---|---|---|
| 1 | `conflict_heatmap` | Mapa de calor intensidad conflictos (Goldstein) por país/día |
| 2 | `top_countries_events` | Top 10 países con más eventos por día |
| 3 | `tone_sources_correlation` | Correlación AvgTone vs NumSources |
| 4 | `cameo_by_region` | Distribución tipos CAMEO por región |
| 5 | `actor_interaction_matrix` | Matriz gobierno vs militar vs rebeldes |
| 6 | `media_coverage` | Cobertura mediática (menciones/evento) por país |
| 7 | `sentiment_trend` | Tendencia sentimiento con promedio móvil 7d |
| 8 | `conflict_country_pairs` | Pares de países en conflicto frecuente |
| 9 | `escalation_events` | Escalada de menciones en 24h |
| 10 | `religion_conflict_clusters` | Conflictos por religión y región |
| 11 | `gkg_themes_by_continent` | Temas GKG por continente/año |
| 12 | `top_organizations` | Organizaciones más mencionadas por día |
| 13 | `tone_lag_analysis` | Rezago: tono hoy → conflictos mañana |
| 14 | `diplomatic_conflict_graph` | Grafo diplomático vs conflicto |
| 15 | `source_diversity_index` | Diversidad de fuentes por país |
| 16 | `ethnic_conflict_frequency` | Conflictos por etnia de actores |
| 17 | `breaking_news` | 0 → 100+ menciones en <1 hora |
| 18 | `quadclass_timeline` | **[Extra]** Evolución QuadClass por región |
| 19 | `hourly_event_density` | **[Extra]** Densidad eventos por hora UTC |

## Conclusiones del Análisis

1. **Concentración geográfica**: Los conflictos de mayor intensidad (Goldstein negativo) se concentran en Medio Oriente, África Subsahariana y Asia, con patrones bilaterales persistentes entre mismos pares de países.

2. **Sesgo mediático global**: La correlación entre tono y número de fuentes es débil; países occidentales tienen mayor diversidad de fuentes, revelando un sesgo en la cobertura global de eventos.

3. **Detección temprana viable**: Los patrones de escalada de menciones y breaking news permiten identificar crisis emergentes horas antes de que se materialicen como conflictos materiales.

## Credenciales

| Servicio | Usuario | Contraseña |
|---|---|---|
| MongoDB | admin | gdelt2026 |
| Airflow | admin | admin |

## Detener

```bash
docker compose down
# Para eliminar volúmenes también:
docker compose down -v
```
