PRAGMA foreign_keys=ON;

-- ========= Anti-duplicados / Reglas de unicidad adicionales =========

-- A) tarifa_definicion ya tiene:
--    CONSTRAINT uq_def UNIQUE (via_id, clase_id, ejes)
--    y el índice parcial uq_def_null (via_id, clase_id) WHERE ejes IS NULL
--    => No añadimos nada aquí.

-- B) tarifa_historial
--    1) Solo una fila vigente por definicion (vigente_hasta IS NULL)
CREATE UNIQUE INDEX IF NOT EXISTS ux_hist_vigente
ON tarifa_historial(definicion_id)
WHERE vigente_hasta IS NULL;

--    2) Evitar duplicar exactamente el mismo intervalo (desde/hasta)
CREATE UNIQUE INDEX IF NOT EXISTS ux_hist_intervalo
ON tarifa_historial(definicion_id, vigente_desde, COALESCE(vigente_hasta, '9999-12-31'));

-- C) tarifa_snapshot
--    Evitar duplicar snapshots por (definición, fecha_corte, fuente)
CREATE UNIQUE INDEX IF NOT EXISTS ux_snap_def_corte_fuente
ON tarifa_snapshot(definicion_id, fecha_corte, fuente);

-- ========= Triggers de integridad temporal =========

-- Cerrar automáticamente la vigente anterior cuando entra una nueva vigente
DROP TRIGGER IF EXISTS trg_hist_close_previous_vigente;
CREATE TRIGGER trg_hist_close_previous_vigente
BEFORE INSERT ON tarifa_historial
FOR EACH ROW
WHEN NEW.vigente_hasta IS NULL
BEGIN
  UPDATE tarifa_historial
  SET vigente_hasta = DATETIME(NEW.vigente_desde, '-1 second')
  WHERE definicion_id = NEW.definicion_id
    AND vigente_hasta IS NULL;
END;

-- Evitar intervalos inválidos (hasta < desde)
DROP TRIGGER IF EXISTS trg_hist_guard_intervalo;
CREATE TRIGGER trg_hist_guard_intervalo
BEFORE INSERT ON tarifa_historial
FOR EACH ROW
WHEN NEW.vigente_hasta IS NOT NULL AND NEW.vigente_hasta < NEW.vigente_desde
BEGIN
  SELECT RAISE(ABORT, 'Intervalo inválido: vigente_hasta < vigente_desde');
END;
