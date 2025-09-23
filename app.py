from flask import Flask, request, render_template_string, send_file, Blueprint, jsonify
import os, sqlite3, csv, io
from flask_cors import CORS

APP_TITLE = "SIBUAC Tarifas"

# --- Ruta robusta a la BD (env > ./scrapers/ > junto a app.py) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = (
    os.environ.get("DB_PATH")
    or os.path.join(BASE_DIR, "scrapers", "sibuac_tarifas.sqlite")
    or os.path.join(BASE_DIR, "sibuac_tarifas.sqlite")
)

# --- Tablas/vistas permitidas (todas existen en schema_norm.sql) ---
DEFAULT_TABLE = "vw_tarifa_vigente"
ALLOWED_TABLES = {
    "vw_tarifa_snapshot",
    "vw_tarifa_vigente",
    "vw_tarifa_hist",
    # "vw_tarifa_sibuac",
}


# --- Columnas “amigables” que intentaremos mostrar si existen ---
PREFERRED_ORDER = ["fecha", "caseta", "Caseta", "categoria", "Clase", "Ejes", "tarifa", "vigente_desde", "long_km", "fuente"]

app = Flask(__name__)

BASE_TMPL = """
<!doctype html>
<html lang="es">
    <head>
        <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{{title}}</title>
        <style>
            body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Ubuntu,Calibri,Arial,sans-serif;margin:24px}
            header{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
            form input, form select{padding:8px;border:1px solid #ddd;border-radius:8px}
            form button, a.btn{padding:8px 12px;border:1px solid #ccc;border-radius:8px;background:#f6f6f6;cursor:pointer;text-decoration:none;color:#111}
            table{border-collapse:collapse;width:100%;margin-top:16px}
            th,td{border:1px solid #eee;padding:8px;text-align:left}
            th{background:#fafafa}
            .muted{color:#666}
            code{background:#f6f6f6;padding:2px 6px;border-radius:6px}
        </style>
    </head>
    <body>
        <header>
            <h1 style="margin:0">{{title}}</h1>
            <!-- <a class="btn" href="/">Inicio</a>
            <a class="btn" href="/introspect">Introspect</a> -->
        </header>

        <form method="get" action="/">
            <!-- <input type="text" name="q" placeholder="Buscar Caseta (LIKE)" value="{{q or ''}}">
            <input type="text" name="fecha" placeholder="Fecha exacta (YYYY-MM-DD)" value="{{fecha or ''}}">-->
            <select name="table">
                {% for t in allowed %}
                <option value="{{t}}" {% if t==table %}selected{% endif %}>{{t}}</option>
                {% endfor %}
            </select>
            <select name="limit">
                {% for n in ["50","100","200","500","Todos"] %}
                <option value="{{n}}" {% if limit_arg==n %}selected{% endif %}>{{n}}</option>
                {% endfor %}
            </select>
            <button type="submit">Buscar</button>
            <a class="btn" href="/export{{'?' + request.query_string.decode() if request.query_string else ''}}">Exportar CSV</a>
        </form>

        <!-- <p class="muted">
            Base: <code>{{db_path}}</code> |
            Vista/tabla: <code>{{table}}</code> |
            Total registros: {{total}} |
            Última fecha: {{ultima or "—"}}
        </p> -->

        {% if error %}<p style="color:#b00"><strong>{{error}}</strong></p>{% endif %}

        <table>
            <thead>
                <tr>
                    {% for h in headers %}
                    <th>
                        {{h}}
                    </th>
                    {% endfor %}
                </tr>
            </thead>
            <tbody>
                {% for r in rows %}
                <tr>
                    {% for k in headers %}
                    <td>
                        {{r[k]}}
                    </td>
                    {% endfor %}</tr>
                {% endfor %}
            </tbody>
        </table>
    </body>
</html>
"""

# --- Utilidades SQLite ---
def connect():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def list_all(cur):
    cur.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view') ORDER BY type, name")
    return [(r["name"], r["type"]) for r in cur.fetchall()]

def cols_for(cur, table):
    cur.execute(f"PRAGMA table_info({table})")
    return [r["name"] for r in cur.fetchall()]

def choose_headers(cur, table):
    cols = cols_for(cur, table)
    # mantén el orden preferido, pero sólo las que existan
    headers = [c for c in PREFERRED_ORDER if c in cols]
    # si la vista tiene otras columnas, añádelas al final
    extras = [c for c in cols if c not in headers]
    return headers + extras

# --- Rutas ---
@app.route("/introspect")
def introspect():
    con = connect(); cur = con.cursor()
    all_objs = list_all(cur)
    out = ["<h1>Introspect</h1>", f"<p>DB: <code>{DB_PATH}</code></p>", "<ul>"]
    for name, typ in all_objs:
        out.append(f"<li><strong>{name}</strong> <em>({typ})</em><br>")
        cur.execute(f"PRAGMA table_info({name})")
        rows = cur.fetchall()
        out.append("<table border=1 cellpadding=4><tr><th>#</th><th>columna</th><th>tipo</th></tr>")
        for r in rows:
            out.append(f"<tr><td>{r['cid']}</td><td>{r['name']}</td><td>{r['type']}</td></tr>")
        out.append("</table></li>")
    out.append("</ul>")
    return "\n".join(out)

@app.route("/")
def index():
    q = request.args.get("q")
    fecha = request.args.get("fecha")
    table = request.args.get("table") or DEFAULT_TABLE
    if table not in ALLOWED_TABLES:
        table = DEFAULT_TABLE

    limit_arg = (request.args.get("limit") or "100").strip().lower()
    unlimited = limit_arg in ("all","todo","todos","infinity","inf","0","Todos")
    if unlimited:
        limit_clause = ""
    else:
        try:
            limit_clause = f"LIMIT {int(limit_arg)}"
        except ValueError:
            limit_clause = "LIMIT 100"; limit_arg = "100"

    con = connect(); cur = con.cursor()

    # columnas a mostrar (dinámicas pero simples)
    headers = choose_headers(cur, table)

    # filtros (sólo los que están presentes)
    filters, params = [], []
    if q and "via" in cols_for(cur, table):
        filters.append("via LIKE ?"); params.append(f"%{q}%")
    if fecha and "fecha" in cols_for(cur, table):
        filters.append("fecha = ?"); params.append(fecha)

    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""
    order_sql = "ORDER BY fecha DESC" if "fecha" in headers else ""

    sel = ", ".join(headers)
    sql = f"SELECT {sel} FROM {table} {where_sql} {order_sql} {limit_clause}"
    cur.execute(sql, params); rows = [dict(r) for r in cur.fetchall()]

    # total y última fecha
    cur.execute(f"SELECT COUNT(*) AS c FROM {table}")
    total = cur.fetchone()["c"]
    ultima = None
    if "fecha" in headers:
        cur.execute(f"SELECT MAX(fecha) AS f FROM {table}")
        ultima = cur.fetchone()["f"]

    return render_template_string(
        BASE_TMPL,
        title=APP_TITLE, db_path=DB_PATH,
        table=table, allowed=sorted(ALLOWED_TABLES),
        headers=headers, rows=rows, total=total, ultima=ultima,
        q=q, fecha=fecha, limit_arg=limit_arg, error=None
    )

@app.route("/export")
def export_csv():
    q = request.args.get("q")
    fecha = request.args.get("fecha")
    table = request.args.get("table") or DEFAULT_TABLE
    if table not in ALLOWED_TABLES:
        table = DEFAULT_TABLE

    con = connect(); cur = con.cursor()
    headers = choose_headers(cur, table)

    filters, params = [], []
    if q and "via" in cols_for(cur, table):
        filters.append("via LIKE ?"); params.append(f"%{q}%")
    if fecha and "fecha" in cols_for(cur, table):
        filters.append("fecha = ?"); params.append(fecha)

    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""
    order_sql = "ORDER BY fecha DESC" if "fecha" in headers else ""
    sel = ", ".join(headers)
    sql = f"SELECT {sel} FROM {table} {where_sql} {order_sql} LIMIT 100000"
    cur.execute(sql, params); rows = [dict(r) for r in cur.fetchall()]

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=headers)
    writer.writeheader()
    for r in rows: writer.writerow(r)

    mem = io.BytesIO(output.getvalue().encode("utf-8"))
    mem.seek(0)
    return send_file(mem, mimetype="text/csv", as_attachment=True, download_name=f"{table}.csv")

# --- API ---
API_KEY = os.environ.get("API_KEY", "admin")
CORS(app, resources={r"/api/*":{"origins": "*"}})
api = Blueprint("api", __name__, url_prefix="/api/v1")

@api.route("/vigente")
def api_vigente():
    return query_view("vw_tarifa_vigente")

@api.route("/hist")
def api_hist():
    return query_view("vw_tarifa_hist")

@api.route("/snapshot")
def api_snapshot():
    return query_view("vw_tarifa_snapshot")

@api.route("/cambios")
def api_cambios():
    # vista compara vigente vs anterior y muestra delta (ya definida en tu schema)
    return query_view("vw_cambios_recientes")

# Registra el blueprint
app.register_blueprint(api)


def require_api_key():
    key = request.headers.get("X-API-Key") or request.args.get("api_key")
    if not key or key != API_KEY:
        return False
    return True

def parse_pagination():
    limit = (request.args.get("limit") or "100").strip().lower()
    offset = int(request.args.get("offset", 0))
    if limit in ("all","todo","0","inf","infinity"):
        return None, offset
    try:
        return max(int(limit), 1), max(offset, 0)
    except:
        return 100, max(offset, 0)
    
def query_view(table, allowed_filters=("q","fecha","from","to")):
    if not require_api_key():
        return jsonify({"error":"unauthorized"}), 401

    con = connect(); cur = con.cursor()
    cols = cols_for(cur, table:=table) if 'cols_for' in globals() else [c for c in choose_headers(cur, table)]
    headers = choose_headers(cur, table)

    q = request.args.get("q")           # LIKE sobre caseta
    fecha = request.args.get("fecha")   # igualdad
    ffrom = request.args.get("from")    # rango desde (YYYY-MM-DD)
    tto = request.args.get("to")        # rango hasta (YYYY-MM-DD)
    lim, off = parse_pagination()

    filters, params = [], []
    if q and "caseta" in cols:
        filters.append("caseta LIKE ?"); params.append(f"%{q}%")
    if fecha and "fecha" in cols:
        filters.append("fecha = ?"); params.append(fecha)
    if ffrom and "fecha" in cols:
        filters.append("fecha >= ?"); params.append(ffrom)
    if tto and "fecha" in cols:
        filters.append("fecha <= ?"); params.append(tto)
    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""

    # total
    cur.execute(f"SELECT COUNT(*) AS c FROM {table} {where_sql}", params)
    total = cur.fetchone()["c"]

    order_sql = "ORDER BY fecha DESC" if "fecha" in cols else ""
    sel = ", ".join(headers)
    limit_sql = "" if lim is None else f"LIMIT {lim} OFFSET {off}"
    sql = f"SELECT {sel} FROM {table} {where_sql} {order_sql} {limit_sql}"
    cur.execute(sql, params)
    items = [dict(r) for r in cur.fetchall()]

    return jsonify({
        "table": table,
        "total": total,
        "count": len(items),
        "limit": lim,
        "offset": off,
        "items": items
    })


if __name__ == "__main__":
    app.run(debug=True, port=5001)