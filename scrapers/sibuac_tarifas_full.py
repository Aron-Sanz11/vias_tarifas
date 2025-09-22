# -*- coding: utf-8 -*-
"""
SIBUAC - Scraper + Normalización + Histórico
--------------------------------------------
- Flujo: GET CmdSelTarifaRep1Data → POST con TODAS las vías (varias acciones).
- Parseo de tabla con encabezado multinivel (clase arriba, ejes abajo).
- Normaliza filas → via, long_km, vigente_desde, clase, ejes(INT), tarifa(TEXT).
- Persiste en SQLite con schema_norm.sql:
    via / vehiculo_clase / tarifa_definicion(ejes INTEGER) / tarifa_historial(SCD2)
    consulta / consulta_item / tarifa_snapshot_raw / tarifa_snapshot(definicion_id)
    + vistas vw_tarifa_vigente, vw_tarifa_hist, vw_tarifa_snapshot.
"""

import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import time
from typing import Dict, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE = "https://app.sct.gob.mx/sibuac_internet"
URL_FORM = f"{BASE}/ControllerUI?action=CmdSelTarifaRep1Data"


# ------------------- util debug -------------------

def dump_html(prefix: str, html: str):
    ts = time.strftime("%Y%m%d-%H%M%S")
    fname = f"{prefix}_{ts}.html"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[DEBUG] Guardado {fname}")


# ------------------- HTTP + parsing de form -------------------

def get_form_and_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    r = s.get(URL_FORM, timeout=30)
    r.raise_for_status()
    dump_html("debug_GET_CmdSelTarifaRep1Data", r.text)
    soup = BeautifulSoup(r.text, "html.parser")
    return s, soup, r.url, r.text


def pick_form(soup: BeautifulSoup):
    forms = soup.find_all("form")
    for f in forms:
        if f.find_all("input", {"type": "radio"}) and f.find("select", {"name": "selectVia"}):
            return f
    if forms:
        return forms[0]
    raise RuntimeError("No se encontró ningún <form> en la página.")


def extract_form_data(form, page_url: str):
    action = form.get("action") or page_url
    action_url = urljoin(page_url, action)

    data = {}
    radios = []  # (name, value, checked, label)
    consultar_submit = None

    id2label = {}
    for label in form.find_all("label"):
        fo = label.get("for")
        if fo:
            id2label[fo] = label.get_text(strip=True)

    for inp in form.find_all("input"):
        itype = (inp.get("type") or "").lower()
        name = inp.get("name")
        value = inp.get("value", "")
        if not name:
            continue
        if itype in ("hidden", "text"):
            data[name] = value
        elif itype == "radio":
            rid = inp.get("id") or ""
            lbl = id2label.get(rid, "")
            checked = bool(inp.get("checked"))
            radios.append((name, value, checked, lbl))
        elif itype == "submit":
            label = (value or "").lower()
            if "consultar" in label:
                consultar_submit = (name, value)

    for btn in form.find_all("button"):
        if (btn.get("type") or "").lower() == "submit":
            label = (btn.get("value","") + btn.get_text("")).lower()
            if "consultar" in label:
                name = btn.get("name") or "consultar"
                consultar_submit = (name, btn.get("value","consultar"))

    return action_url, data, radios, consultar_submit


def extract_all_select_via_values(form) -> List[str]:
    vals = []
    sel = form.find("select", {"name": "selectVia"})
    if sel:
        for opt in sel.find_all("option"):
            v = opt.get("value")
            if v:
                vals.append(v)
    return vals


def extract_select_via_labels(form) -> set:
    labels = set()
    sel = form.find("select", {"name": "selectVia"})
    if sel:
        for opt in sel.find_all("option"):
            txt = (opt.get_text() or "").strip()
            if txt:
                labels.add(txt)
    return labels


def choose_second_radio_payload(radios):
    """
    radios: [(name, value, checked, label)]
    Preferimos el que tenga label 'Todas las vías...' ; si no, la 2a opción.
    """
    if not radios:
        return {}
    groups = {}
    order = []
    for name, value, checked, label in radios:
        if name not in groups:
            groups[name] = []
            order.append(name)
        groups[name].append((value, bool(checked), label or ""))

    for gname in order:
        for value, _, label in groups[gname]:
            if "todas las vías" in label.lower():
                return {gname: value}
    for gname in order:
        if len(groups[gname]) >= 2:
            return {gname: groups[gname][1][0]}
    first = order[0]
    return {first: groups[first][-1][0]}


# ------------------- POST con variantes de action -------------------

def looks_like_tarifas_table(html: str) -> bool:
    if "Disculpe usted, pero por el momento no podemos atenderlo" in html:
        return False
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return False
    table = max(tables, key=lambda t: len(t.find_all("tr")))
    trs = [tr for tr in table.find_all("tr") if tr.find_all(["th","td"])]
    if len(trs) < 2:
        return False
    row0 = [c.get_text(strip=True).lower() for c in trs[0].find_all(["th","td"])]
    text_all = " ".join(row0)
    has_via = ("vía" in text_all) or (" via " in f" {text_all} ")
    has_long = ("long" in text_all) or ("km" in text_all)
    return has_via and has_long


def post_consultar(session, action_url, base_data, radio_choice, consultar_submit, page_html, all_vias):
    # base payload sin 'action'
    payload = {k: v for k, v in base_data.items() if k.lower() != "action"}
    payload.update(radio_choice or {})
    payload["radioSel"] = payload.get("radioSel", "1")
    if all_vias:
        payload["countVias"] = str(len(all_vias))

    # lista de pares para repetir selectVia
    data_items = []
    for k, v in payload.items():
        if isinstance(v, list):
            for vi in v:
                data_items.append((k, str(vi)))
        else:
            data_items.append((k, str(v)))
    for v in (all_vias or []):
        data_items.append(("selectVia", str(v)))

    headers = {"Referer": URL_FORM, "User-Agent": "Mozilla/5.0"}

    base = action_url.split("?")[0]
    variants = [
        "CmdTarifaRep1Data",
        "cmdTarifaRep1Data",
        "CmdTarifaRep1",
        "cmdTarifaRep1",
        "CmdImpTarifasRep1Data",
        "cmdImpTarifasRep1Data",
    ]

    print("[DEBUG] payload keys =", sorted(set(k for k, _ in data_items)))
    last_html = None
    for i, v in enumerate(variants, 1):
        url_try = base + ("&" if "?" in action_url else "?") + f"action={v}"
        print(f"[DEBUG] POST try#{i} => {url_try}")
        r = session.post(url_try, data=data_items, timeout=60, headers=headers)
        r.raise_for_status()
        last_html = r.text
        dump_html(f"debug_POST_try{i}", last_html)
        if looks_like_tarifas_table(last_html):
            return last_html

    return last_html


# ------------------- Parseo con encabezado multinivel -------------------

def _expand_row_cells_for_header(tr):
    cells = []
    colspans = []
    rowspans = []
    total_cols = 0
    for cell in tr.find_all(["th","td"], recursive=False):
        txt = cell.get_text(strip=True)
        cspan = int(cell.get("colspan", 1) or 1)
        rspan = int(cell.get("rowspan", 1) or 1)
        cells.append(txt)
        colspans.append(cspan)
        rowspans.append(rspan)
        total_cols += cspan
    return cells, colspans, rowspans, total_cols


def build_header_grid(table):
    """Construye dos filas de header expandidas a un mismo ancho, manejando colspan/rowspan."""
    trs = [tr for tr in table.find_all("tr") if tr.find_all(["th","td"])]
    if len(trs) < 1:
        raise RuntimeError("Tabla no tiene filas suficientes.")

    # Primera fila (nivel 1)
    h1_cells, h1_csp, h1_rsp, total_cols = _expand_row_cells_for_header(trs[0])
    row0 = [""] * total_cols
    row1 = [""] * total_cols
    row1_blocked = [False] * total_cols

    # Llenar row0 y bloquear row1 donde rowspan>=2
    col = 0
    for txt, csp, rsp in zip(h1_cells, h1_csp, h1_rsp):
        for _ in range(csp):
            row0[col] = txt
            if rsp >= 2:
                row1_blocked[col] = True
            col += 1

    # Segunda fila (nivel 2) si existe
    if len(trs) >= 2:
        h2_cells, h2_csp, h2_rsp, _ = _expand_row_cells_for_header(trs[1])
        def next_free(c):
            while c < total_cols and (row1_blocked[c] or row1[c]):
                c += 1
            return c
        col = next_free(0)
        for txt, csp, _ in zip(h2_cells, h2_csp, h2_rsp):
            filled = 0
            while col < total_cols and filled < csp:
                if not row1_blocked[col] and not row1[col]:
                    row1[col] = txt
                    filled += 1
                col += 1
            col = next_free(col)

    return row0, row1, row1_blocked, total_cols, trs


def parse_ejes_int(texto: str):
    if not texto:
        return None
    m = re.search(r"\d+", str(texto))
    return int(m.group(0)) if m else None


def parse_long_km(raw):
    if raw is None:
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    s = (s.replace("kms","").replace("km","")
           .replace("kilómetros","").replace("kilometros","")
           .replace(" ", "").replace(",", "."))
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    try:
      return int(round(float(s), 0))
    except:
      return None


def parse_table_with_multilevel_headers(html: str):
    if "Disculpe usted, pero por el momento no podemos atenderlo" in html:
        dump_html("debug_ERROR_like", html)
        raise RuntimeError("Respuesta del servidor: ‘Disculpe usted…’. No se insertó nada.")

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        raise RuntimeError("No se encontraron tablas en la página.")

    # Tomamos la tabla más grande
    table = max(tables, key=lambda t: len(t.find_all("tr")))
    row0, row1, row1_blocked, total_cols, trs = build_header_grid(table)

    # Validación básica
    low = " ".join([c.lower() for c in row0])
    if ("vía" not in low) and (" via " not in f" {low} "):
        dump_html("debug_ERROR_like", html)
        raise RuntimeError("No parece la tabla esperada (faltó 'Vía').")

    # Data rows: a partir de la tercera fila real (si hubo segunda de header)
    data_start_idx = 2 if any(row1) else 1

    def expand_data_row(tr):
        # Expandir por colspan si existiera
        cells = []
        for cell in tr.find_all(["td","th"], recursive=False):
            txt = cell.get_text(strip=True)
            cspan = int(cell.get("colspan", 1) or 1)
            for _ in range(cspan):
                cells.append(txt)
        # iguala al ancho total_cols
        if len(cells) < total_cols:
            cells += [""] * (total_cols - len(cells))
        elif len(cells) > total_cols:
            cells = cells[:total_cols]
        return cells

    data_rows = [expand_data_row(tr) for tr in trs[data_start_idx:]]
    return row0, row1, row1_blocked, data_rows


def normalize_multilevel(row0, row1, row1_blocked, data_rows):
    """
    Columnas base: posiciones con row1_blocked=True (suelen ser Vía, Long, Vigente).
    Columnas de tarifas: el resto; clase=row0[col], ejes_int=parse(row1[col] o row0[col]).
    """
    row0_lower = [h.strip().lower() for h in row0]
    def find_col(possible):
        for name in possible:
            if name in row0_lower:
                return row0_lower.index(name)
        return -1

    idx_via = find_col(["vía", "via"])
    idx_long = find_col(["long km", "long. km", "long (km)", "long(km)", "longitud", "long"])
    idx_vig  = find_col(["vigente desde", "vigencia", "fecha vigencia", "vigente"])
    base_idxs = {i for i in [idx_via, idx_long, idx_vig] if i >= 0}

    results = []
    last_km = None
    for row in data_rows:
        via = row[idx_via] if idx_via >= 0 else ""
        if not via:
            continue
        raw_km = row[idx_long] if idx_long >= 0 else ""
        km = parse_long_km(raw_km)
        if km is None and last_km is not None and via:
            km = last_km
        if km is not None:
            last_km = km
        vigente = row[idx_vig] if idx_vig >= 0 else ""

        for ci, (top, bot, blocked) in enumerate(zip(row0, row1, row1_blocked)):
            if ci in base_idxs:
                continue
            clase = (top or "").strip()
            ejes_txt = (bot or "").strip() or (top or "")
            tarifa = (row[ci] or "").strip()
            if not clase or not tarifa:
                continue
            ejes = parse_ejes_int(ejes_txt)
            results.append({
                "via": via,
                "long_km": km,
                "vigente_desde": vigente,
                "clase": clase,
                "ejes": ejes,      # INTEGER o None
                "tarifa": tarifa,
            })
    return results


# ------------------- SQLite (esquema normalizado) -------------------

SCHEMA_NORM_FALLBACK = r"""
PRAGMA foreign_keys=ON;
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS via (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  via       TEXT NOT NULL,
  long_km   INTEGER,
  UNIQUE(via, long_km)
);
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
  ejes      INTEGER,
  CONSTRAINT uq_def UNIQUE (via_id, clase_id, ejes)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_def_null ON tarifa_definicion(via_id, clase_id) WHERE ejes IS NULL;

CREATE TABLE IF NOT EXISTS tarifa_historial (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  definicion_id  INTEGER NOT NULL REFERENCES tarifa_definicion(id),
  tarifa         REAL    NOT NULL,
  vigente_desde  TEXT,
  vigente_hasta  TEXT,
  fuente         TEXT DEFAULT 'SIBUAC'
);
CREATE INDEX IF NOT EXISTS ix_hist_def ON tarifa_historial(definicion_id);
CREATE INDEX IF NOT EXISTS ix_hist_vig ON tarifa_historial(vigente_desde, vigente_hasta);

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
"""


def ensure_db_norm(db_path: str):
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA journal_mode=WAL;")
    schema_path = os.path.join(os.path.dirname(__file__), "schema_norm.sql")
    if os.path.exists(schema_path):
        with open(schema_path, "r", encoding="utf-8") as f:
            con.executescript(f.read())
    else:
        con.executescript(SCHEMA_NORM_FALLBACK)
    con.commit()
    return con


def _parse_decimal(txt: str):
    if txt is None:
        return None
    s = str(txt).strip()
    s = s.replace("$", "").replace(" ", "")

    # Quitar separador de miles (coma en casos como 1,340.0)
    if "," in s and "." in s:
        s = s.replace(",", "")

    # Caso: formato europeo con coma decimal (ej. 228,50)
    elif s.count(",") == 1 and s.count(".") == 0:
        s = s.replace(",", ".")

    # Caso raro: más de un punto => quita todos menos el último
    if s.count(".") > 1:
        parts = s.split(".")
        s = "".join(parts[:-1]) + "." + parts[-1]

    try:
        return float(s)
    except ValueError:
        return None

def _upsert_via(con, via, long_km):
    cur = con.cursor()
    def to_int(x):
        if x is None: return None
        if isinstance(x, int): return x
        return parse_long_km(x)
    km = to_int(long_km)

    if km is not None:
        row = cur.execute("SELECT id FROM via WHERE via=? AND long_km=?", (via, km)).fetchone()
        if row: return row[0]
        # ¿existe con NULL? promuévelo
        row = cur.execute("SELECT id FROM via WHERE via=? AND long_km IS NULL", (via,)).fetchone()
        if row:
            cur.execute("UPDATE via SET long_km=? WHERE id=?", (km, row[0])); con.commit(); return row[0]
        cur.execute("INSERT INTO via(via,long_km) VALUES(?,?)", (via, km)); con.commit(); return cur.lastrowid
    else:
        row = cur.execute("SELECT id FROM via WHERE via=? AND long_km IS NULL", (via,)).fetchone()
        if row: return row[0]
        cur.execute("INSERT OR IGNORE INTO via(via,long_km) VALUES(?,NULL)", (via,)); con.commit()
        return cur.execute("SELECT id FROM via WHERE via=? AND long_km IS NULL", (via,)).fetchone()[0]


def _upsert_clase(con, nombre):
    cur = con.cursor()
    nombre = (nombre or "SIN CLASE").strip()
    cur.execute("INSERT OR IGNORE INTO vehiculo_clase(nombre) VALUES(?)", (nombre,)); con.commit()
    return cur.execute("SELECT id FROM vehiculo_clase WHERE nombre=?", (nombre,)).fetchone()[0]


def _upsert_def(con, via_id, clase_id, ejes_int):
    cur = con.cursor()
    row = cur.execute("""
        SELECT id FROM tarifa_definicion
        WHERE via_id=? AND clase_id=?
          AND ((ejes IS NULL AND ? IS NULL) OR ejes=?)
        LIMIT 1
    """, (via_id, clase_id, ejes_int, ejes_int)).fetchone()
    if row: return row[0]
    cur.execute("INSERT INTO tarifa_definicion(via_id,clase_id,ejes) VALUES(?,?,?)",
                (via_id, clase_id, ejes_int)); con.commit()
    return cur.lastrowid


def _hist_vigente(con, def_id):
    cur = con.cursor()
    cur.execute("""SELECT id, tarifa FROM tarifa_historial 
                   WHERE definicion_id=? AND vigente_hasta IS NULL
                   ORDER BY id DESC LIMIT 1""", (def_id,))
    return cur.fetchone()


def _close_hist(con, hist_id, hasta_iso):
    con.execute("UPDATE tarifa_historial SET vigente_hasta=? WHERE id=?", (hasta_iso, hist_id)); con.commit()


def _insert_hist(con, def_id, tarifa, desde_iso, fuente="SIBUAC"):
    cur = con.cursor()
    cur.execute("""INSERT INTO tarifa_historial(definicion_id, tarifa, vigente_desde, fuente)
                   VALUES(?,?,?,?)""", (def_id, float(tarifa), desde_iso, fuente)); con.commit()
    return cur.lastrowid


def _begin_consulta(con, params: dict):
    cur = con.cursor()
    cur.execute("""INSERT INTO consulta(executed_at, params_json, status)
                   VALUES(?,?,?)""", (dt.datetime.now().isoformat(timespec="seconds"),
                                      json.dumps(params, ensure_ascii=False), "RUNNING")); con.commit()
    return cur.lastrowid


def _end_consulta(con, cid, status):
    con.execute("UPDATE consulta SET status=? WHERE id=?", (status, cid)); con.commit()


def _append_citem(con, cid, hid):
    con.execute("INSERT INTO consulta_item(consulta_id, historial_id) VALUES(?,?)", (cid, hid)); con.commit()


def _insert_snapshot_def(con, def_id, consulta_id, fecha_corte, vigente_desde, tarifa, fuente="SIBUAC"):
    con.execute("""
        INSERT INTO tarifa_snapshot(definicion_id, consulta_id, fecha_corte, vigente_desde, tarifa, fuente)
        VALUES (?,?,?,?,?,?)
    """, (def_id, consulta_id, fecha_corte, vigente_desde, float(tarifa), fuente)); con.commit()


def persist_items_normalizados(con, items, fecha_corte, save_raw=True):
    cid = _begin_consulta(con, {"fecha_corte": fecha_corte, "fuente":"SIBUAC"})
    nuevos = 0
    try:
        for it in items:
            if save_raw:
                con.execute("""INSERT INTO tarifa_snapshot_raw(via,long_km,vigente_desde,clase,ejes,tarifa)
                               VALUES(?,?,?,?,?,?)""",
                            (it.get("via"),
                             str(it.get("long_km") if it.get("long_km") is not None else ""),
                             it.get("vigente_desde"),
                             it.get("clase"),
                             str(it.get("ejes") if it.get("ejes") is not None else ""),
                             it.get("tarifa")))

            via = (it.get("via") or "").strip()
            clase = (it.get("clase") or "").strip()
            ejes_int  = it.get("ejes") if isinstance(it.get("ejes"), int) else parse_ejes_int(it.get("ejes"))
            tarifa_val = _parse_decimal(it.get("tarifa"))
            if not via or tarifa_val is None:
                continue

            via_id   = _upsert_via(con, via, it.get("long_km"))
            clase_id = _upsert_clase(con, clase)
            def_id   = _upsert_def(con, via_id, clase_id, ejes_int)

            desde = (it.get("vigente_desde") or fecha_corte)

            # Snapshot (siempre, por definición)
            _insert_snapshot_def(con, def_id, cid, fecha_corte, desde, tarifa_val, "SIBUAC")

            # Histórico SCD2 (sólo si cambia)
            h = _hist_vigente(con, def_id)
            if h is None:
                hid = _insert_hist(con, def_id, tarifa_val, desde); _append_citem(con, cid, hid); nuevos += 1
            else:
                if float(h[1]) != float(tarifa_val):
                    _close_hist(con, h[0], desde)
                    hid = _insert_hist(con, def_id, tarifa_val, desde); _append_citem(con, cid, hid); nuevos += 1

        _end_consulta(con, cid, "OK")
        return nuevos
    except Exception as ex:
        _end_consulta(con, cid, f"ERROR: {ex}")
        raise


# ------------------- main/CLI -------------------

def main():
    parser = argparse.ArgumentParser(description="Extractor SIBUAC tarifas (Tarifas Vigentes → Consultar)")
    parser.add_argument("--db", default="sibuac_tarifas.sqlite", help="Ruta BD SQLite")
    parser.add_argument("--dump-csv", help="Opcional: exportar CSV normalizado")
    parser.add_argument("--min-vias", type=int, default=120, help="Abortar si vías únicas < min (sanity check)")
    args = parser.parse_args()

    # 1) GET + form
    s, soup, page_url, page_html = get_form_and_session()
    form = pick_form(soup)

    # 2) vias ids + labels (diagnóstico)
    all_vias = extract_all_select_via_values(form)
    print(f"[DEBUG] selectVia options encontrados: {len(all_vias)}")
    via_labels = extract_select_via_labels(form)
    print(f"[DEBUG] labels válidos de vía: {len(via_labels)}")

    # 3) payload y radio
    action_url, base_data, radios, consultar_submit = extract_form_data(form, page_url)
    print("[DEBUG] action_url =", action_url)
    print("[DEBUG] radios =", [(n, v, c, l) for (n, v, c, l) in radios][:5])
    print("[DEBUG] submit consultar =", consultar_submit)
    radio_choice = choose_second_radio_payload(radios)
    print("[DEBUG] elegido segundo radio =", radio_choice)

    # 4) POST (varias actions)
    html = post_consultar(s, action_url, base_data, radio_choice, consultar_submit, page_html, all_vias)
    dump_html("debug_POST_consultar_final", html)

    # 5) Parseo + normalización (SIN filtrar por labels del <select>)
    row0, row1, row1_blocked, data_rows = parse_table_with_multilevel_headers(html)
    items = normalize_multilevel(row0, row1, row1_blocked, data_rows)

    # 6) Sanity check
    vias_unicas = {it["via"] for it in items}
    print(f"[DEBUG] vías únicas detectadas en tabla: {len(vias_unicas)}")
    if len(vias_unicas) < args.min_vias:
        dump_html("debug_ERROR_like_few_vias", html)
        raise RuntimeError(f"Demasiado pocas vías ({len(vias_unicas)}<{args.min_vias}). Aborto para evitar basura.")

    # 7) Persistencia normalizada (hist + snapshot(definición) + raw)
    con = ensure_db_norm(args.db)
    fecha_corte = dt.date.today().isoformat()
    new_hist = persist_items_normalizados(con, items, fecha_corte, save_raw=True)
    print(f"[HIST] Nuevos cambios en histórico: {new_hist}")
    con.close()

    # 8) CSV opcional
    if args.dump_csv:
        import csv
        with open(args.dump_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["via","long_km","vigente_desde","clase","ejes","tarifa"])
            w.writeheader()
            w.writerows(items)

    print(f"OK: {len(items)} filas normalizadas. Vías únicas: {len(vias_unicas)}. Snapshot/Hist guardados en {args.db}.")
    if args.dump_csv:
        print(f"CSV: {args.dump_csv}")


if __name__ == "__main__":
    main()
