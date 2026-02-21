# flashscore-legacy

Script en Python para generar un calendario `.ics` de eventos deportivos a partir de:

- Flashscore (`flashscore.es`)
- Futboleras (`futboleras.es`)
- SofaScore (APIs/páginas de apoyo para enriquecer datos)

El proceso consolida eventos, marca cancelaciones cuando corresponde, añade información de clasificación y guarda resultados persistidos en disco.

## Requisitos

- Python 3.10+
- Dependencias de Python:
  - `requests`
  - `beautifulsoup4`
  - `pytz`
  - `ics`
  - `curl-cffi` (opcional, recomendado para integraciones con SofaScore)

Instalación rápida:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install requests beautifulsoup4 pytz ics curl-cffi
```

## Ejecución

```bash
python3 flashscore.py
```

## Salida

Por defecto los archivos se escriben en `/var/www/html`:

- `/var/www/html/SportsCalendar.ics`
- `/var/www/html/SportsCalendar.pkl`
- `/var/www/html/obsolete.pkl`
- `/var/www/html/classification_cache.pkl`

## Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `FLASHSCORE_OUTPUT_DIR` | `/var/www/html` | Directorio de salida. |
| `FUTBOLERAS_TEAM_URLS` | 2 URLs por defecto | Lista CSV de equipos de Futboleras a scrapear. |
| `FLASHSCORE_SCRAPE_MAX_WORKERS` | `12` | Hilos para scraping de Flashscore. |
| `FLASHSCORE_CLASSIFICATION_MAX_WORKERS` | `8` | Hilos para enriquecimiento de clasificaciones. |
| `FLASHSCORE_PAST_RESULTS_DAYS` | `30` | Días de lookback para conservar eventos recientes. |
| `FLASHSCORE_LEAGUE_FALLBACK` | `""` | Texto de liga por defecto si no se puede inferir. |
| `FLASHSCORE_CLASSIFICATION_CACHE_TTL_DAYS` | `14` | TTL (días) de caché de clasificaciones. |
| `FLASHSCORE_CLASSIFICATION_REFRESH_EMPTY_CACHE` | `1` | Refresca caché vacía de clasificación. |
| `FLASHSCORE_CLASSIFICATION_SKIP_FETCH_WHEN_PRESENT` | `1` | Evita refetch cuando ya hay dato en evento. |
| `FLASHSCORE_INCLUDE_MOTORSPORT_SESSIONS` | `1` | Incluye sesiones de motorsport desde feeds de torneo. |
| `FLASHSCORE_INDIVIDUAL_MERGE_SPORTS` | `AUTOMOVILISMO,MOTOCICLISMO,CICLISMO` | Deportes individuales a fusionar por competición/fecha. |
| `FLASHSCORE_CLASSIFICATION_SPORTS` | `TENIS,TENIS DE MESA,BÁDMINTON,BADMINTON` | Deportes con clasificación individual en descripción. |
| `FLASHSCORE_TEAM_CLASSIFICATION_SPORTS` | `FÚTBOL,FUTBOL,FÚTBOL SALA,FUTBOL SALA` | Deportes con clasificación por equipos. |
| `FLASHSCORE_OVERRUN_EXTENSION_MINUTES` | `30` | Minutos de extensión que se aplican en cada actualización si un evento sigue en curso más allá de la duración prevista. |
| `FLASHSCORE_OVERRUN_MAX_HOURS` | `12` | Tope de horas para seguir extendiendo automáticamente un evento pasado de duración. |
| `FLASHSCORE_OVERRUN_EXTENSION_SPORTS` | `AUTOMOVILISMO,MOTOCICLISMO,CICLISMO` | Deportes donde se activa la extensión automática por sobretiempo (orientado a carreras). |
| `SOFASCORE_API_BASE_URLS` | `https://www.sofascore.com/api/v1,https://api.sofascore.com/api/v1` | Base URLs para API SofaScore. |
| `SOFASCORE_FETCH_PAGES` | `3` | Número de páginas de eventos SofaScore a consultar. |
| `SOFASCORE_DEFAULT_TIMEZONE` | `Europe/Madrid` | Zona horaria por defecto para SofaScore. |
| `SOFASCORE_USE_CURL_CFFI` | `1` | Usa `curl-cffi` para mejorar compatibilidad de requests. |
| `SOFASCORE_CURL_CFFI_IMPERSONATE` | `chrome124` | Perfil de impersonación en `curl-cffi`. |
| `FUTBOLERAS_DEFAULT_TIMEZONE` | `Europe/Madrid` | Zona horaria por defecto para Futboleras. |

## Flujo resumido

1. Scraping concurrente de Flashscore y Futboleras.
2. Merge de eventos por `gameid` y consolidación de payload.
3. Fusión de eventos especiales (golf e individuales).
4. Marcado de cancelaciones cuando el scraping fue completo.
5. Enriquecimiento con clasificaciones.
6. Persistencia (`.pkl`) y generación del calendario `.ics`.

## Notas de calidad de datos

- La fusión de eventos de golf consolida jugadores por competición y elimina duplicados de nombres (normalizando mayúsculas, acentos y espacios).
- En competiciones individuales se añade `participant_rankings` cuando hay datos de ranking.
- En deportes con dos equipos (p. ej. fútbol sala, baloncesto, balonmano y similares), el título del evento muestra el marcador cuando está disponible; además se conserva la mejor versión del resultado al fusionar actualizaciones.
- En partidos de fútbol se añaden tarjetas al evento: expulsiones desde `AJ/AK` y amarillas desde el feed de estadísticas (`df_st_*`), mostrando iconos junto al nombre del equipo en el título (`🟨`, `🟥`) y resumen en la descripción.
