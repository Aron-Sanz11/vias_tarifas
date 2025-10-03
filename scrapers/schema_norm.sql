PRAGMA foreign_keys=ON;
PRAGMA journal_mode=WAL;

-- =============== Catálogos ===============
CREATE TABLE IF NOT EXISTS via (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  via       TEXT NOT NULL,
  long_km   INTEGER,
  UNIQUE(via, long_km)
);
-- Evita duplicados con long_km NULL (SQLite trata NULL como distinto)
CREATE UNIQUE INDEX IF NOT EXISTS uq_via_nullkm ON via(via) WHERE long_km IS NULL;

CREATE TABLE IF NOT EXISTS vehiculo_clase (
  id      INTEGER PRIMARY KEY AUTOINCREMENT,
  nombre  TEXT NOT NULL,
  UNIQUE(nombre)
);

CREATE TABLE IF NOT EXISTS tarifa_definicion (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  via_id    INTEGER NOT NULL REFERENCES via(id),
  clase_id  INTEGER NOT NULL REFERENCES vehiculo_clase(id),
  ejes      INTEGER,                     -- << NUMÉRICO
  CONSTRAINT uq_def UNIQUE (via_id, clase_id, ejes)
);
-- Evita duplicados cuando ejes sea NULL
CREATE UNIQUE INDEX IF NOT EXISTS uq_def_null ON tarifa_definicion(via_id, clase_id) WHERE ejes IS NULL;

-- =============== Histórico (SCD2) ===============
CREATE TABLE IF NOT EXISTS tarifa_historial (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  definicion_id  INTEGER NOT NULL REFERENCES tarifa_definicion(id),
  tarifa         REAL    NOT NULL,
  vigente_desde  TEXT,              -- ISO (YYYY-MM-DD[..])
  vigente_hasta  TEXT,              -- NULL => vigente
  fuente         TEXT DEFAULT 'SIBUAC'
);
CREATE INDEX IF NOT EXISTS ix_hist_def ON tarifa_historial(definicion_id);
CREATE INDEX IF NOT EXISTS ix_hist_vig ON tarifa_historial(vigente_desde, vigente_hasta);

-- =============== Trazabilidad consulta ===============
CREATE TABLE IF NOT EXISTS consulta (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  executed_at  TEXT NOT NULL,
  params_json  TEXT,
  status       TEXT
);
CREATE TABLE IF NOT EXISTS consulta_item (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  consulta_id   INTEGER NOT NULL REFERENCES consulta(id),
  historial_id  INTEGER NOT NULL REFERENCES tarifa_historial(id)
);
CREATE INDEX IF NOT EXISTS ix_citem_consulta ON consulta_item(consulta_id);

-- =============== Crudo opcional (auditoría) ===============
CREATE TABLE IF NOT EXISTS tarifa_snapshot_raw (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  via            TEXT,
  long_km        TEXT,
  vigente_desde  TEXT,
  clase          TEXT,
  ejes           TEXT,
  tarifa         TEXT,
  captured_at    TEXT DEFAULT (datetime('now'))
);

-- =============== Snapshot por definición ===============
CREATE TABLE IF NOT EXISTS tarifa_snapshot (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  definicion_id  INTEGER NOT NULL REFERENCES tarifa_definicion(id),
  consulta_id    INTEGER REFERENCES consulta(id),
  fecha_corte    TEXT NOT NULL,
  vigente_desde  TEXT,
  tarifa         REAL NOT NULL,
  fuente         TEXT DEFAULT 'SIBUAC'
);
CREATE INDEX IF NOT EXISTS ix_snap_def ON tarifa_snapshot(definicion_id);
CREATE INDEX IF NOT EXISTS ix_snap_fecha ON tarifa_snapshot(fecha_corte);

-- =============== Vistas (compatibles con app__.py) ===============
DROP VIEW IF EXISTS vw_tarifa_vigente;
CREATE VIEW vw_tarifa_vigente AS
SELECT
  h.vigente_desde              AS fecha,
  v.via                        AS caseta,
  vc.nombre                    AS categoria,
  COALESCE(CAST(d.ejes AS TEXT), '') AS Ejes,
  h.tarifa                     AS tarifa,
  v.long_km                    AS long_km,
  h.fuente                     AS fuente
FROM tarifa_historial h
JOIN tarifa_definicion d ON d.id = h.definicion_id
JOIN via v              ON v.id = d.via_id
JOIN vehiculo_clase vc  ON vc.id = d.clase_id
WHERE h.vigente_hasta IS NULL;

DROP VIEW IF EXISTS vw_tarifa_hist;
CREATE VIEW vw_tarifa_hist AS
SELECT
  h.vigente_desde              AS fecha,
  v.via                        AS caseta,
  vc.nombre                    AS categoria,
  COALESCE(CAST(d.ejes AS TEXT), '') AS Ejes,
  h.tarifa                     AS tarifa,
  h.vigente_hasta              AS vigente_hasta,
  v.long_km                    AS long_km,
  h.fuente                     AS fuente
FROM tarifa_historial h
JOIN tarifa_definicion d ON d.id = h.definicion_id
JOIN via v              ON v.id = d.via_id
JOIN vehiculo_clase vc  ON vc.id = d.clase_id;

DROP VIEW IF EXISTS vw_tarifa_snapshot;
CREATE VIEW vw_tarifa_snapshot AS
SELECT
  ts.fecha_corte               AS fecha,
  v.via                        AS caseta,
  vc.nombre                    AS categoria,
  COALESCE(CAST(d.ejes AS TEXT), '') AS Ejes,
  ts.tarifa                    AS tarifa,
  v.long_km                    AS long_km,
  ts.vigente_desde             AS vigente_desde,
  ts.fuente                    AS fuente
FROM tarifa_snapshot ts
JOIN tarifa_definicion d ON d.id = ts.definicion_id
JOIN via v              ON v.id = d.via_id
JOIN vehiculo_clase vc  ON vc.id = d.clase_id;

-- (Opcional) Cambios recientes (vigente vs anterior)
DROP VIEW IF EXISTS vw_cambios_recientes;
CREATE VIEW vw_cambios_recientes AS
WITH hist AS (
  SELECT
    d.via_id, d.clase_id, IFNULL(d.ejes, -1) AS ejes_key,
    h.id, h.tarifa, h.vigente_desde, h.vigente_hasta,
    ROW_NUMBER() OVER (
      PARTITION BY d.via_id, d.clase_id, IFNULL(d.ejes, -1)
      ORDER BY h.vigente_desde DESC, h.id DESC
    ) AS rn
  FROM tarifa_historial h
  JOIN tarifa_definicion d ON d.id = h.definicion_id
)
SELECT
  v.via             AS caseta,
  vc.nombre         AS categoria,
  NULLIF(x.ejes_key, -1) AS Ejes,
  x.tarifa          AS tarifa_vigente,
  x.vigente_desde   AS vigente_desde,
  y.tarifa          AS tarifa_anterior,
  (x.tarifa - y.tarifa) AS delta
FROM hist x
LEFT JOIN hist y
  ON y.via_id=x.via_id AND y.clase_id=x.clase_id AND y.ejes_key=x.ejes_key AND y.rn=x.rn+1
JOIN via v ON v.id=x.via_id
JOIN vehiculo_clase vc ON vc.id=x.clase_id
WHERE x.rn=1;