# SIBUAC Tarifas – Guía de instalación y uso

Esta guía permite clonar el proyecto, preparar el entorno, **crear la base SQLite**, **cargar datos** e iniciar la **API Flask** para consultar tarifas (vigentes, histórico y snapshots).

> Probado principalmente en **Windows (PowerShell)**. Se incluyen equivalentes para **Linux/macOS (bash)**.

## 1) Requisitos

- **Python 3.10+** (con `pip`)
- **Git**
- **SQLite** (opcional si usas el método con Python para aplicar `.sql`)
- Windows PowerShell o terminal bash

> Verifica Python:
```powershell
python --version
pip --version
```

Opcionalmente, instala la CLI de SQLite (Windows):
- Descarga *sqlite-tools-win-x64*.zip desde https://www.sqlite.org/download.html
- Extrae en `C:\sqlite\` y agrega al PATH (abrir nueva consola después):
```powershell
setx PATH "$env:PATH;C:\sqlite"
```

## 2) Clonar el repositorio

```powershell
git clone <URL_DE_TU_REPO>.git
cd peajes_SIBUAC
```

*(Sustituye `<URL_DE_TU_REPO>` por el URL real de tu Git.)*

## 3) Crear y activar entorno virtual

### Windows (PowerShell)
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install --upgrade pip
pip install -r requirements.txt
```

### Linux/macOS (bash)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 4) Inicializar la base de datos

> La BD por defecto es `scrapers/sibuac_tarifas.sqlite`.

### Opción A: con SQLite CLI
```powershell
sqlite3 .\scrapers\sibuac_tarifas.sqlite ".read .\scrapers\schema_norm.sql"
sqlite3 .\scrapers\sibuac_tarifas.sqlite ".read .\db\migrations\003_anti_duplicados.sql"
```

### Opción B (recomendada si no tienes la CLI): con Python (una sola línea)
```powershell
python - << 'PY'
import sqlite3, pathlib
db = sqlite3.connect(r'scrapers/sibuac_tarifas.sqlite')
for sql_path in (pathlib.Path('scrapers/schema_norm.sql'), pathlib.Path('db/migrations/003_anti_duplicados.sql')):
    with open(sql_path, 'r', encoding='utf-8') as f:
        db.executescript(f.read())
db.commit(); db.close()
print('OK: esquema y migraciones aplicadas')
PY
```

## 5) Cargar datos (scraper)

El scraper **normaliza** y crea/actualiza registros de forma **idempotente** (anti-duplicados).

Si ya cuentas con datos de staging en `tarifa_snapshot_raw`, puedes procesarlos con:

```powershell
python .\scrapers\sibuac_tarifas_full.py --db .\scrapers\sibuac_tarifas.sqlite --min-vias 120
#  --from-raw
```

- `--min-vias 120` filtra por casetas con `long_km >= 120` (ajústalo o quítalo).
- Agrega `--no-snapshot` para evitar escribir en `tarifa_snapshot` y solo actualizar el histórico vigente.

## 6) Iniciar la API Flask

```powershell
python .\app.py
```

La app normalmente levanta en **http://127.0.0.1:5001/** (si tu `app.py` especifica 5001). Si no, Flask usa 5000 por defecto. Observa el puerto en la terminal.

### Endpoints principales
- `GET /vigente` → usa la vista `vw_tarifa_vigente`
- `GET /hist` → usa la vista `vw_tarifa_hist`
- `GET /snapshot` → usa la vista `vw_tarifa_snapshot`

Ejemplos de prueba (PowerShell):
```powershell
curl http://127.0.0.1:5001/vigente
curl http://127.0.0.1:5001/hist
curl http://127.0.0.1:5001/snapshot
```

> **UTF-8 sin escapes**: el proyecto está configurado para devolver JSON con `ensure_ascii=False` (por ejemplo “**Armería - Manzanillo**” y no `Armer\u00eda`).

## 7) Estructura relevante

```
peajes_SIBUAC/
├─ app.py                         # API Flask
├─ scrapers/
│  ├─ schema_norm.sql            # Esquema base
│  ├─ sibuac_tarifas.sqlite      # (Se crea al inicializar)
│  ├─ sibuac_tarifas_full.py     # Scraper/normalizador (idempotente)
│  └─ ...                        # Otros scripts o recursos
├─ db/
│  └─ migrations/
│     └─ 003_anti_duplicados.sql # Índices únicos y triggers
├─ requirements.txt
└─ README.md (este archivo)
```

## 8) Notas técnicas (anti-duplicados)

- **tarifa_definicion**: `UNIQUE(via_id, clase_id, ejes)` + índice parcial cuando `ejes IS NULL`.
- **tarifa_historial**:
  - `ux_hist_vigente`: solo 1 fila vigente por definición.
  - `ux_hist_intervalo`: evita duplicar el mismo intervalo exacto.
  - Trigger `trg_hist_close_previous_vigente`: al insertar nueva vigente, cierra la anterior.
- **tarifa_snapshot**: `ux_snap_def_corte_fuente` evita duplicar por (definición, fecha_corte, fuente).
- Scraper usa **UPSERT** para mantener idempotencia en histórico y snapshots.

## 9) Errores comunes y soluciones

- **`sqlite3: command not found`** (Windows):
  - Usa la opción B (Python) para aplicar SQL, o instala la CLI y **abre una nueva consola** tras `setx PATH`.
- **`no such table: ...`**:
  - Asegúrate de haber ejecutado **primero** `schema_norm.sql` y **después** `003_anti_duplicados.sql`.
- **Caracteres escapados `\u00xx` en JSON**:
  - El `app.py` ya desactiva `ensure_ascii`. Si no ves cambios, reinicia el servidor Flask y prueba de nuevo.

## 10) Scripts útiles (opcional)

### `init_db.ps1`
Crea un archivo `init_db.ps1` con:
```powershell
$ErrorActionPreference = 'Stop'
python - << 'PY'
import sqlite3, pathlib
db = sqlite3.connect(r'scrapers/sibuac_tarifas.sqlite')
for sql_path in (pathlib.Path('scrapers/schema_norm.sql'), pathlib.Path('db/migrations/003_anti_duplicados.sql')):
    with open(sql_path, 'r', encoding='utf-8') as f:
        db.executescript(f.read())
db.commit(); db.close()
print('OK: esquema y migraciones aplicadas')
PY
```

Ejecuta:
```powershell
./init_db.ps1
```

## 11) Licencia / Contribuciones

- Crea ramas por feature y abre PRs con descripción clara.
- Usa mensajes de commit útiles (ej.: `feat(scraper): upsert de snapshot e histórico`).

¡Listo! Con esto cualquier persona podrá clonar, instalar, inicializar la BD, cargar datos y **levantar la API**.
