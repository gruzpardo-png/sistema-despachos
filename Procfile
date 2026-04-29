
import os
import shutil
import json
import sqlite3
from io import BytesIO
from datetime import datetime, date
from functools import wraps

from flask import (
    Flask, request, redirect, url_for, session, flash,
    send_file, render_template_string
)
from markupsafe import Markup
from werkzeug.security import generate_password_hash, check_password_hash
from openpyxl import Workbook


APP_NAME = "Ferretería Cloud Tool"
APP_VERSION = "v4.2 Data Safe"
DB_PATH = os.environ.get("DATABASE_PATH", "ferreteria_cloud_tool.db")
SECRET_KEY = os.environ.get("SECRET_KEY", "cambiar-esta-clave-en-render")


app = Flask(__name__)
app.secret_key = SECRET_KEY


# ============================================================
# UTILIDADES BASE
# ============================================================

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def today_str():
    return date.today().isoformat()


def database_directory():
    folder = os.path.dirname(os.path.abspath(DB_PATH))
    return folder if folder else "."


def backup_database_if_exists(label="manual"):
    """
    Crea un respaldo físico de la base SQLite si existe.
    No borra ni modifica la base principal.
    Mantiene los últimos 20 respaldos para no llenar el disco.
    """
    try:
        if not os.path.exists(DB_PATH):
            return None
        if os.path.getsize(DB_PATH) == 0:
            return None

        backup_dir = os.path.join(database_directory(), "backups")
        os.makedirs(backup_dir, exist_ok=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"ferreteria_cloud_tool_{label}_{stamp}.db"
        destination = os.path.join(backup_dir, filename)

        shutil.copy2(DB_PATH, destination)

        backups = sorted(
            [
                os.path.join(backup_dir, f)
                for f in os.listdir(backup_dir)
                if f.endswith(".db")
            ],
            key=os.path.getmtime,
            reverse=True
        )

        for old_backup in backups[20:]:
            try:
                os.remove(old_backup)
            except Exception:
                pass

        return destination
    except Exception:
        return None


def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def execute(sql, params=()):
    with db() as conn:
        conn.execute(sql, params)
        conn.commit()


def insert_and_get_id(sql, params=()):
    with db() as conn:
        cur = conn.execute(sql, params)
        conn.commit()
        return cur.lastrowid


def query_all(sql, params=()):
    with db() as conn:
        return conn.execute(sql, params).fetchall()


def query_one(sql, params=()):
    with db() as conn:
        return conn.execute(sql, params).fetchone()


def money_to_float(value):
    if value is None:
        return 0.0
    text = str(value).replace("$", "").replace(" ", "").strip()
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text)
    except Exception:
        return 0.0


@app.template_filter("money")
def money(value):
    try:
        number = float(value or 0)
    except Exception:
        number = 0
    return "$" + f"{number:,.0f}".replace(",", ".")


@app.template_filter("date_short")
def date_short(value):
    if not value:
        return ""
    return str(value)[:10]


def table_columns(table_name):
    try:
        rows = query_all(f"PRAGMA table_info({table_name})")
        return {r["name"] for r in rows}
    except Exception:
        return set()


def add_column_if_missing(table_name, column_definition):
    column_name = column_definition.split()[0]
    if column_name not in table_columns(table_name):
        execute(f"ALTER TABLE {table_name} ADD COLUMN {column_definition}")


def get_config_list(clave, default=None):
    default = default or []
    row = query_one("SELECT valor FROM system_config WHERE clave = ?", (clave,))
    if not row:
        return default
    try:
        data = json.loads(row["valor"])
        return data if isinstance(data, list) else default
    except Exception:
        return default


def set_config_list(clave, values):
    clean = []
    for v in values:
        v = str(v).strip()
        if v and v not in clean:
            clean.append(v)
    execute("""
        INSERT INTO system_config (clave, valor, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(clave) DO UPDATE SET valor = excluded.valor, updated_at = excluded.updated_at
    """, (clave, json.dumps(clean, ensure_ascii=False), now_str()))


# ============================================================
# INICIALIZACIÓN / MIGRACIONES
# ============================================================

def init_db():
    # Respaldo automático antes de cualquier migración. No elimina datos.
    backup_database_if_exists('pre_migracion')
    execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        full_name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'operador',
        is_active INTEGER NOT NULL DEFAULT 1,
        permissions TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """)

    execute("""
    CREATE TABLE IF NOT EXISTS despachos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero_documento TEXT NOT NULL,
        tipo_documento TEXT NOT NULL,
        estado TEXT NOT NULL,
        cliente TEXT NOT NULL,
        telefono TEXT,
        destino TEXT,
        patente TEXT,
        conductor TEXT,
        pioneta TEXT,
        monto REAL DEFAULT 0,
        usuario_id INTEGER,
        usuario_nombre TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """)

    execute("""
    CREATE TABLE IF NOT EXISTS maquinarias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT,
        nombre TEXT NOT NULL,
        tipo TEXT,
        marca TEXT,
        modelo TEXT,
        anio TEXT,
        patente TEXT,
        estado TEXT NOT NULL DEFAULT 'Activa',
        observaciones TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """)

    execute("""
    CREATE TABLE IF NOT EXISTS mantenciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        maquinaria_id INTEGER,
        maquinaria_nombre TEXT,
        fecha TEXT NOT NULL,
        tipo_mantencion TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Pendiente',
        responsable TEXT,
        costo REAL DEFAULT 0,
        observaciones TEXT,
        usuario_id INTEGER,
        usuario_nombre TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """)

    execute("""
    CREATE TABLE IF NOT EXISTS vehiculos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        patente TEXT UNIQUE NOT NULL,
        tipo TEXT,
        marca TEXT,
        modelo TEXT,
        anio TEXT,
        estado TEXT NOT NULL DEFAULT 'Activo',
        permiso_circulacion_vencimiento TEXT,
        revision_tecnica_vencimiento TEXT,
        seguro_obligatorio_vencimiento TEXT,
        observaciones TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """)

    execute("""
    CREATE TABLE IF NOT EXISTS personas_logistica (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        tipo TEXT NOT NULL,
        estado TEXT NOT NULL DEFAULT 'Activo',
        telefono TEXT,
        observaciones TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT
    )
    """)

    execute("""
    CREATE TABLE IF NOT EXISTS audit_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        modulo TEXT NOT NULL,
        accion TEXT NOT NULL,
        registro_id INTEGER,
        campo TEXT,
        valor_anterior TEXT,
        valor_nuevo TEXT,
        usuario_id INTEGER,
        usuario_nombre TEXT,
        created_at TEXT NOT NULL
    )
    """)

    execute("""
    CREATE TABLE IF NOT EXISTS system_config (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        clave TEXT UNIQUE NOT NULL,
        valor TEXT NOT NULL,
        updated_at TEXT
    )
    """)

    # Migraciones tolerantes para bases anteriores.
    migrations = {
        "users": [
            "full_name TEXT DEFAULT ''",
            "password_hash TEXT DEFAULT ''",
            "role TEXT DEFAULT 'operador'",
            "is_active INTEGER DEFAULT 1",
            "permissions TEXT DEFAULT '{}'",
            "created_at TEXT DEFAULT ''",
            "updated_at TEXT",
        ],
        "despachos": [
            "numero_documento TEXT DEFAULT ''",
            "tipo_documento TEXT DEFAULT 'Factura'",
            "estado TEXT DEFAULT 'Pendiente'",
            "cliente TEXT DEFAULT ''",
            "telefono TEXT",
            "destino TEXT",
            "patente TEXT",
            "conductor TEXT",
            "pioneta TEXT",
            "monto REAL DEFAULT 0",
            "usuario_id INTEGER",
            "usuario_nombre TEXT",
            "created_at TEXT DEFAULT ''",
            "updated_at TEXT",
        ],
        "maquinarias": [
            "codigo TEXT",
            "nombre TEXT DEFAULT ''",
            "tipo TEXT",
            "marca TEXT",
            "modelo TEXT",
            "anio TEXT",
            "patente TEXT",
            "estado TEXT DEFAULT 'Activa'",
            "observaciones TEXT",
            "created_at TEXT DEFAULT ''",
            "updated_at TEXT",
        ],
        "mantenciones": [
            "maquinaria_id INTEGER",
            "maquinaria_nombre TEXT",
            "fecha TEXT DEFAULT ''",
            "tipo_mantencion TEXT DEFAULT 'Preventiva'",
            "estado TEXT DEFAULT 'Pendiente'",
            "responsable TEXT",
            "costo REAL DEFAULT 0",
            "observaciones TEXT",
            "usuario_id INTEGER",
            "usuario_nombre TEXT",
            "created_at TEXT DEFAULT ''",
            "updated_at TEXT",
        ],
    }

    for table, cols in migrations.items():
        for col in cols:
            try:
                add_column_if_missing(table, col)
            except Exception:
                pass

    # Configuración inicial.
    defaults = {
        "estados_despacho": ["Entregado", "Pendiente"],
        "tipos_documento": ["Factura", "Boleta", "Guía de despacho", "Otro"],
        "sucursales": ["Sucursal 1", "Sucursal 2", "La Americana"],
        "estados_maquinaria": ["Activa", "En mantención", "Fuera de servicio", "Vendida / dada de baja"],
        "estados_mantencion": ["Pendiente", "En proceso", "Realizada", "Anulada"],
        "tipos_mantencion": ["Preventiva", "Correctiva", "Emergencia", "Revisión", "Otro"],
    }
    for clave, valor in defaults.items():
        row = query_one("SELECT id FROM system_config WHERE clave = ?", (clave,))
        if not row:
            set_config_list(clave, valor)

    # No se sobreescribe configuración existente para evitar pérdida de ajustes.
    # El formulario rápido de despacho usa solo Entregado/Pendiente en la interfaz.

    # Usuario admin inicial.
    admin = query_one("SELECT id FROM users WHERE username = ?", ("admin",))
    if not admin:
        execute("""
            INSERT INTO users (
                username, full_name, password_hash, role, is_active,
                permissions, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "admin",
            "Administrador",
            generate_password_hash("admin123"),
            "admin",
            1,
            json.dumps({"all": True}),
            now_str(),
            now_str(),
        ))

    # Usuario operativo inicial de referencia.
    operador = query_one("SELECT id FROM users WHERE username = ?", ("operador",))
    if not operador:
        execute("""
            INSERT INTO users (
                username, full_name, password_hash, role, is_active,
                permissions, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            "operador",
            "Usuario Operador",
            generate_password_hash("operador123"),
            "operador",
            1,
            json.dumps({
                "despachos": True,
                "mantenciones": True,
                "consulta": True,
                "exportar": False,
                "dashboard": False,
                "auditoria": False,
                "administracion": False,
            }),
            now_str(),
            now_str(),
        ))


# ============================================================
# SEGURIDAD / SESIÓN
# ============================================================

PERMISSION_LABELS = {
    "despachos": "Despachos",
    "mantenciones": "Mantenciones",
    "consulta": "Consulta",
    "dashboard": "Dashboard admin",
    "auditoria": "Auditoría",
    "administracion": "Administración / Configuración",
    "usuarios": "Usuarios",
    "maquinarias": "Maquinarias",
    "vehiculos": "Vehículos / Patentes",
    "logistica": "Conductores / Pionetas",
    "exportar": "Exportar Excel",
    "integraciones": "Integraciones",
}


def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return query_one("SELECT * FROM users WHERE id = ? AND is_active = 1", (uid,))


def user_permissions(user=None):
    user = user or current_user()
    if not user:
        return {}
    if user["role"] == "admin":
        return {"all": True}
    try:
        return json.loads(user["permissions"] or "{}")
    except Exception:
        return {}


def is_admin(user=None):
    user = user or current_user()
    return bool(user and user["role"] == "admin")


def has_perm(module_name):
    user = current_user()
    if not user:
        return False
    if is_admin(user):
        return True
    perms = user_permissions(user)
    return bool(perms.get(module_name))


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not current_user():
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def permission_required(module_name):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            if not current_user():
                return redirect(url_for("login"))
            if not has_perm(module_name):
                flash("No tienes permiso para acceder a esa sección.", "error")
                return redirect(url_for("index"))
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def write_audit(modulo, accion, registro_id=None, campo=None, valor_anterior=None, valor_nuevo=None):
    user = current_user()
    execute("""
        INSERT INTO audit_log (
            modulo, accion, registro_id, campo, valor_anterior, valor_nuevo,
            usuario_id, usuario_nombre, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        modulo,
        accion,
        registro_id,
        campo,
        "" if valor_anterior is None else str(valor_anterior),
        "" if valor_nuevo is None else str(valor_nuevo),
        user["id"] if user else None,
        user["full_name"] if user else "Sistema",
        now_str(),
    ))


# ============================================================
# HTML BASE
# ============================================================

BASE_TEMPLATE = r"""
<!doctype html>
<html lang="es">
<head>
    <meta charset="utf-8">
    <title>{{ page_title }} · {{ app_name }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        :root{
            --bg:#f5f7fb;
            --panel:#ffffff;
            --panel-2:#f9fafb;
            --text:#111827;
            --muted:#6b7280;
            --border:#e5e7eb;
            --primary:#0f766e;
            --primary-dark:#115e59;
            --danger:#dc2626;
            --warning:#d97706;
            --ok:#059669;
            --shadow:0 10px 30px rgba(15,23,42,.08);
            --radius:18px;
        }
        *{box-sizing:border-box}
        body{
            margin:0;
            font-family:Inter,Segoe UI,Roboto,Arial,sans-serif;
            background:var(--bg);
            color:var(--text);
        }
        a{color:inherit;text-decoration:none}
        .login-bg{
            min-height:100vh;
            display:flex;
            align-items:center;
            justify-content:center;
            padding:24px;
            background:linear-gradient(135deg,#0f766e,#172554);
        }
        .login-card{
            width:100%;
            max-width:430px;
            background:white;
            border-radius:24px;
            padding:30px;
            box-shadow:var(--shadow);
        }
        .topbar{
            background:#0f172a;
            color:white;
            padding:14px 20px;
            display:flex;
            align-items:center;
            justify-content:space-between;
            gap:16px;
            position:sticky;
            top:0;
            z-index:20;
            box-shadow:0 4px 18px rgba(0,0,0,.15);
        }
        .brand{
            display:flex;
            flex-direction:column;
            gap:2px;
            min-width:230px;
        }
        .brand strong{font-size:16px}
        .brand small{font-size:12px;color:#cbd5e1}
        .nav-wrap{
            display:flex;
            flex-wrap:wrap;
            justify-content:center;
            gap:10px;
            flex:1;
        }
        .nav-section{
            display:flex;
            align-items:center;
            gap:6px;
            padding:6px;
            border-radius:14px;
            border:1px solid rgba(255,255,255,.14);
            background:rgba(255,255,255,.07);
        }
        .nav-section.admin{background:rgba(20,184,166,.18);border-color:rgba(45,212,191,.35)}
        .nav-section.control{background:rgba(251,191,36,.15);border-color:rgba(251,191,36,.35)}
        .nav-section.integrations{background:rgba(129,140,248,.17);border-color:rgba(129,140,248,.35)}
        .nav-label{
            color:#cbd5e1;
            font-size:11px;
            font-weight:700;
            text-transform:uppercase;
            letter-spacing:.06em;
            padding:0 4px;
        }
        .nav-link{
            font-size:13px;
            padding:8px 10px;
            border-radius:10px;
            color:#f8fafc;
        }
        .nav-link:hover{background:rgba(255,255,255,.14)}
        .userbox{
            display:flex;
            align-items:center;
            gap:8px;
            font-size:13px;
            color:#e5e7eb;
        }
        .logout{
            background:#334155;
            color:white;
            padding:8px 10px;
            border-radius:10px;
        }
        .layout{max-width:1450px;margin:0 auto;padding:22px}
        .page-head{
            display:flex;
            align-items:flex-start;
            justify-content:space-between;
            gap:16px;
            margin-bottom:18px;
        }
        .page-head h1{margin:0;font-size:28px}
        .page-head p{margin:6px 0 0;color:var(--muted)}
        .card{
            background:var(--panel);
            border:1px solid var(--border);
            border-radius:var(--radius);
            box-shadow:var(--shadow);
            padding:20px;
            margin-bottom:18px;
        }
        .card h2,.card h3{margin-top:0}
        .grid{display:grid;gap:16px}
        .grid-2{grid-template-columns:repeat(2,minmax(0,1fr))}
        .grid-3{grid-template-columns:repeat(3,minmax(0,1fr))}
        .grid-4{grid-template-columns:repeat(4,minmax(0,1fr))}
        .stat{
            background:white;
            border:1px solid var(--border);
            border-radius:18px;
            padding:18px;
            box-shadow:0 6px 18px rgba(15,23,42,.05);
        }
        .stat span{display:block;color:var(--muted);font-size:13px}
        .stat strong{display:block;font-size:30px;margin-top:8px}
        label{
            display:block;
            font-size:13px;
            font-weight:700;
            margin-bottom:6px;
            color:#374151;
        }
        input,select,textarea{
            width:100%;
            border:1px solid #d1d5db;
            border-radius:12px;
            padding:11px 12px;
            font-size:14px;
            background:white;
            color:#111827;
        }
        textarea{min-height:96px;resize:vertical}
        .form-row{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:14px}
        .form-row-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px}
        .form-row-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px}
        .actions{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-top:16px}
        .btn{
            border:0;
            border-radius:12px;
            padding:11px 14px;
            font-weight:800;
            cursor:pointer;
            display:inline-flex;
            align-items:center;
            gap:8px;
            font-size:14px;
        }
        .btn-primary{background:var(--primary);color:white}
        .btn-primary:hover{background:var(--primary-dark)}
        .btn-secondary{background:#e5e7eb;color:#111827}
        .btn-danger{background:var(--danger);color:white}
        .btn-warning{background:var(--warning);color:white}
        .btn-small{padding:7px 9px;border-radius:9px;font-size:12px}
        table{
            width:100%;
            border-collapse:separate;
            border-spacing:0;
            overflow:hidden;
            border:1px solid var(--border);
            border-radius:16px;
            background:white;
        }
        th,td{
            padding:11px 12px;
            border-bottom:1px solid var(--border);
            text-align:left;
            font-size:13px;
            vertical-align:top;
        }
        th{
            background:#f8fafc;
            color:#475569;
            font-size:12px;
            text-transform:uppercase;
            letter-spacing:.04em;
        }
        tr:last-child td{border-bottom:0}
        .table-wrap{overflow:auto}
        .badge{
            display:inline-flex;
            align-items:center;
            border-radius:999px;
            padding:4px 9px;
            font-weight:800;
            font-size:12px;
            background:#e5e7eb;
            color:#374151;
            white-space:nowrap;
        }
        .badge.ok{background:#d1fae5;color:#065f46}
        .badge.warn{background:#fef3c7;color:#92400e}
        .badge.bad{background:#fee2e2;color:#991b1b}
        .badge.info{background:#dbeafe;color:#1e40af}
        .flash{
            padding:12px 14px;
            border-radius:14px;
            margin-bottom:12px;
            font-weight:700;
        }
        .flash.success{background:#d1fae5;color:#065f46}
        .flash.error{background:#fee2e2;color:#991b1b}
        .flash.info{background:#dbeafe;color:#1e40af}
        .muted{color:var(--muted)}
        .section-title{
            display:flex;
            align-items:center;
            justify-content:space-between;
            gap:12px;
            margin-bottom:12px;
        }
        .danger-zone{
            border:1px solid #fecaca;
            background:#fff7f7;
        }
        .checkbox-grid{
            display:grid;
            grid-template-columns:repeat(3,minmax(0,1fr));
            gap:10px;
        }
        .check-item{
            display:flex;
            align-items:center;
            gap:8px;
            border:1px solid var(--border);
            border-radius:12px;
            padding:10px;
            background:#fff;
        }
        .check-item input{width:auto}
        .placeholder{
            border:2px dashed #cbd5e1;
            background:#f8fafc;
            border-radius:18px;
            padding:20px;
            color:#475569;
        }
        @media (max-width:1000px){
            .form-row,.form-row-3,.form-row-2,.grid-2,.grid-3,.grid-4{grid-template-columns:1fr}
            .topbar{align-items:flex-start;flex-direction:column}
            .nav-wrap{justify-content:flex-start}
            .brand{min-width:auto}
            .checkbox-grid{grid-template-columns:1fr}
        }
    </style>
</head>
<body>
{% if login_screen %}
    {{ body|safe }}
{% else %}
    <div class="topbar">
        <div class="brand">
            <strong>{{ app_name }}</strong>
            <small>{{ app_version }}</small>
        </div>

        <div class="nav-wrap">
            <div class="nav-section">
                <span class="nav-label">Operación</span>
                {% if has_perm("despachos") %}<a class="nav-link" href="{{ url_for('despachos') }}">Despachos</a>{% endif %}
                {% if has_perm("mantenciones") %}<a class="nav-link" href="{{ url_for('mantenciones') }}">Mantenciones</a>{% endif %}
                {% if has_perm("consulta") %}<a class="nav-link" href="{{ url_for('consulta') }}">Consulta</a>{% endif %}
            </div>

            {% if is_admin %}
            <div class="nav-section admin">
                <span class="nav-label">Administración</span>
                <a class="nav-link" href="{{ url_for('administracion') }}">Administración</a>
                <a class="nav-link" href="{{ url_for('usuarios') }}">Usuarios</a>
                <a class="nav-link" href="{{ url_for('maquinarias') }}">Maquinarias</a>
                <a class="nav-link" href="{{ url_for('vehiculos') }}">Vehículos</a>
                <a class="nav-link" href="{{ url_for('logistica') }}">Conductores</a>
            </div>

            <div class="nav-section control">
                <span class="nav-label">Control</span>
                <a class="nav-link" href="{{ url_for('dashboard') }}">Dashboard</a>
                <a class="nav-link" href="{{ url_for('auditoria') }}">Auditoría</a>
            </div>

            <div class="nav-section integrations">
                <span class="nav-label">Integraciones</span>
                <a class="nav-link" href="{{ url_for('facturacion') }}">Facturación.cl</a>
                <a class="nav-link" href="{{ url_for('exportar') }}">Exportar Excel</a>
            </div>
            {% endif %}
        </div>

        <div class="userbox">
            <span>{{ user.full_name }} · {{ user.role }}</span>
            <a class="logout" href="{{ url_for('logout') }}">Salir</a>
        </div>
    </div>

    <main class="layout">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="flash {{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
        {{ body|safe }}
    </main>
{% endif %}
</body>
</html>
"""


def render_page(title, body_template, login_screen=False, **context):
    body = render_template_string(body_template, **context)
    return render_template_string(
        BASE_TEMPLATE,
        page_title=title,
        app_name=APP_NAME,
        app_version=APP_VERSION,
        body=Markup(body),
        login_screen=login_screen,
        user=current_user(),
        is_admin=is_admin(),
        has_perm=has_perm,
    )


# ============================================================
# RUTAS PRINCIPALES
# ============================================================

@app.route("/health")
def health():
    return {"status": "ok", "app": APP_NAME, "version": APP_VERSION}


@app.route("/")
@login_required
def index():
    if is_admin():
        return redirect(url_for("dashboard"))
    if has_perm("despachos"):
        return redirect(url_for("despachos"))
    if has_perm("consulta"):
        return redirect(url_for("consulta"))
    if has_perm("mantenciones"):
        return redirect(url_for("mantenciones"))
    flash("Tu usuario no tiene módulos asignados. Solicita acceso a un administrador.", "error")
    return redirect(url_for("logout"))


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = query_one("SELECT * FROM users WHERE username = ? AND is_active = 1", (username,))
        if user and check_password_hash(user["password_hash"], password):
            session.clear()
            session["user_id"] = user["id"]
            write_audit("login", "ingreso", user["id"], "usuario", "", username)
            return redirect(url_for("index"))

        flash("Usuario o contraseña incorrectos.", "error")

    return render_page("Ingreso", r"""
    <div class="login-bg">
        <div class="login-card">
            <h1 style="margin:0 0 6px;">Ferretería Cloud Tool</h1>
            <p class="muted" style="margin-top:0;">Sistema interno de despachos, mantenciones, auditoría y administración.</p>

            {% with messages = get_flashed_messages(with_categories=true) %}
                {% if messages %}
                    {% for category, message in messages %}
                        <div class="flash {{ category }}">{{ message }}</div>
                    {% endfor %}
                {% endif %}
            {% endwith %}

            <form method="post">
                <div style="margin-bottom:12px;">
                    <label>Usuario</label>
                    <input name="username" autocomplete="username" required>
                </div>
                <div style="margin-bottom:12px;">
                    <label>Contraseña</label>
                    <input type="password" name="password" autocomplete="current-password" required>
                </div>
                <button class="btn btn-primary" style="width:100%;justify-content:center;">Ingresar</button>
            </form>

            <div class="placeholder" style="margin-top:16px;">
                <strong>Primer ingreso</strong><br>
                Admin: <b>admin</b> / <b>admin123</b><br>
                Operador: <b>operador</b> / <b>operador123</b><br>
                Cambia estas claves desde Administración &gt; Usuarios.
            </div>
        </div>
    </div>
    """, login_screen=True)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ============================================================
# DASHBOARD ADMIN
# ============================================================

@app.route("/dashboard")
@login_required
@permission_required("dashboard")
def dashboard():
    stats = {
        "total_despachos": query_one("SELECT COUNT(*) c FROM despachos")["c"],
        "pendientes": query_one("SELECT COUNT(*) c FROM despachos WHERE estado = 'Pendiente'")["c"],
        "entregados": query_one("SELECT COUNT(*) c FROM despachos WHERE estado = 'Entregado'")["c"],
        "monto_total": query_one("SELECT COALESCE(SUM(monto),0) total FROM despachos")["total"],
        "mant_pendientes": query_one("SELECT COUNT(*) c FROM mantenciones WHERE estado IN ('Pendiente','En proceso')")["c"],
        "maq_fuera": query_one("SELECT COUNT(*) c FROM maquinarias WHERE estado IN ('En mantención','Fuera de servicio')")["c"],
        "usuarios": query_one("SELECT COUNT(*) c FROM users WHERE is_active = 1")["c"],
    }

    por_usuario = query_all("""
        SELECT COALESCE(usuario_nombre, 'Sin usuario') usuario, COUNT(*) total, COALESCE(SUM(monto),0) monto
        FROM despachos
        GROUP BY COALESCE(usuario_nombre, 'Sin usuario')
        ORDER BY total DESC
        LIMIT 10
    """)

    por_estado = query_all("""
        SELECT COALESCE(NULLIF(estado,''), 'Sin estado') estado, COUNT(*) total, COALESCE(SUM(monto),0) monto
        FROM despachos
        GROUP BY COALESCE(NULLIF(estado,''), 'Sin estado')
        ORDER BY total DESC
        LIMIT 10
    """)

    mantenciones_pendientes = query_all("""
        SELECT * FROM mantenciones
        WHERE estado IN ('Pendiente','En proceso')
        ORDER BY fecha ASC, id DESC
        LIMIT 10
    """)

    recientes = query_all("""
        SELECT * FROM despachos
        ORDER BY id DESC
        LIMIT 10
    """)

    return render_page("Dashboard", r"""
    <div class="page-head">
        <div>
            <h1>Dashboard Admin</h1>
            <p>Resumen ejecutivo de despachos rápidos, mantenciones, usuarios y alertas operacionales.</p>
        </div>
    </div>

    <div class="grid grid-4">
        <div class="stat"><span>Total despachos</span><strong>{{ stats.total_despachos }}</strong></div>
        <div class="stat"><span>Entregados</span><strong>{{ stats.entregados }}</strong></div>
        <div class="stat"><span>Pendientes</span><strong>{{ stats.pendientes }}</strong></div>
        <div class="stat"><span>Monto documentado</span><strong>{{ stats.monto_total|money }}</strong></div>
        <div class="stat"><span>Mantenciones pendientes</span><strong>{{ stats.mant_pendientes }}</strong></div>
        <div class="stat"><span>Maquinarias con alerta</span><strong>{{ stats.maq_fuera }}</strong></div>
        <div class="stat"><span>Usuarios activos</span><strong>{{ stats.usuarios }}</strong></div>
    </div>

    <div class="grid grid-2" style="margin-top:18px;">
        <div class="card">
            <h3>Despachos por usuario</h3>
            <div class="table-wrap">
                <table>
                    <tr><th>Usuario</th><th>Total</th><th>Monto</th></tr>
                    {% for r in por_usuario %}
                    <tr>
                        <td>{{ r.usuario }}</td>
                        <td>{{ r.total }}</td>
                        <td>{{ r.monto|money }}</td>
                    </tr>
                    {% else %}
                    <tr><td colspan="3" class="muted">Sin datos.</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>

        <div class="card">
            <h3>Despachos por estado</h3>
            <div class="table-wrap">
                <table>
                    <tr><th>Estado</th><th>Total</th><th>Monto</th></tr>
                    {% for r in por_estado %}
                    <tr><td>{{ r.estado }}</td><td>{{ r.total }}</td><td>{{ r.monto|money }}</td></tr>
                    {% else %}
                    <tr><td colspan="3" class="muted">Sin datos.</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>
    </div>

    <div class="grid grid-2">
        <div class="card">
            <h3>Mantenciones pendientes</h3>
            <div class="table-wrap">
                <table>
                    <tr><th>Fecha</th><th>Maquinaria</th><th>Tipo</th><th>Estado</th></tr>
                    {% for m in mantenciones_pendientes %}
                    <tr>
                        <td>{{ m.fecha }}</td>
                        <td>{{ m.maquinaria_nombre }}</td>
                        <td>{{ m.tipo_mantencion }}</td>
                        <td><span class="badge warn">{{ m.estado }}</span></td>
                    </tr>
                    {% else %}
                    <tr><td colspan="4" class="muted">Sin mantenciones pendientes.</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>

        <div class="card">
            <h3>Últimos despachos</h3>
            <div class="table-wrap">
                <table>
                    <tr><th>Documento</th><th>Estado</th><th>Monto</th><th>Usuario</th><th>Fecha</th></tr>
                    {% for d in recientes %}
                    <tr>
                        <td>{{ d.tipo_documento }} {{ d.numero_documento }}</td>
                        <td><span class="badge {% if d.estado == 'Entregado' %}ok{% elif d.estado == 'Pendiente' %}warn{% else %}info{% endif %}">{{ d.estado }}</span></td>
                        <td>{{ d.monto|money }}</td>
                        <td>{{ d.usuario_nombre }}</td>
                        <td>{{ d.created_at }}</td>
                    </tr>
                    {% else %}
                    <tr><td colspan="5" class="muted">Sin registros.</td></tr>
                    {% endfor %}
                </table>
            </div>
        </div>
    </div>
    """, stats=stats, por_usuario=por_usuario, por_estado=por_estado,
       mantenciones_pendientes=mantenciones_pendientes, recientes=recientes)

# ============================================================
# DESPACHOS
# ============================================================

@app.route("/despachos", methods=["GET", "POST"])
@login_required
@permission_required("despachos")
def despachos():
    estados = ["Entregado", "Pendiente"]
    tipos = get_config_list("tipos_documento")

    if request.method == "POST":
        user = current_user()
        numero = request.form.get("numero_documento", "").strip()
        tipo = request.form.get("tipo_documento", "").strip()
        estado = request.form.get("estado", "Entregado").strip() or "Entregado"
        if estado not in estados:
            estado = "Entregado"

        if not numero or not tipo:
            flash("Número de documento y tipo de documento son obligatorios.", "error")
        else:
            new_id = insert_and_get_id("""
                INSERT INTO despachos (
                    numero_documento, tipo_documento, estado, cliente, telefono, destino,
                    patente, conductor, pioneta, monto, usuario_id, usuario_nombre,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                numero,
                tipo,
                estado,
                "",
                "",
                "",
                "",
                "",
                "",
                money_to_float(request.form.get("monto")),
                user["id"],
                user["full_name"],
                now_str(),
                now_str(),
            ))
            write_audit("despachos", "crear", new_id, "registro", "", f"{tipo} {numero} / {estado}")
            flash("Despacho registrado correctamente.", "success")
            return redirect(url_for("despachos"))

    recientes = query_all("SELECT * FROM despachos ORDER BY id DESC LIMIT 25")
    audit = query_all("SELECT * FROM audit_log WHERE modulo='despachos' ORDER BY id DESC LIMIT 8") if is_admin() else []

    return render_page("Despachos", r"""
    <div class="page-head">
        <div>
            <h1>Módulo Despachos</h1>
            <p>Registro rápido para operadores: documento, tipo, estado y monto. El usuario queda registrado automáticamente.</p>
        </div>
    </div>

    <div class="card">
        <h2>Nuevo registro rápido</h2>
        <form method="post">
            <div class="form-row">
                <div>
                    <label>Número documento *</label>
                    <input name="numero_documento" required autofocus>
                </div>
                <div>
                    <label>Tipo documento *</label>
                    <select name="tipo_documento" required>
                        {% for t in tipos %}<option>{{ t }}</option>{% endfor %}
                    </select>
                </div>
                <div>
                    <label>Estado *</label>
                    <select name="estado" required>
                        <option value="Entregado" selected>Entregado</option>
                        <option value="Pendiente">Pendiente</option>
                    </select>
                </div>
                <div>
                    <label>Monto documento</label>
                    <input name="monto" inputmode="decimal" placeholder="Ej: 120000">
                </div>
            </div>

            <div class="actions">
                <button class="btn btn-primary">Guardar despacho</button>
                <a class="btn btn-secondary" href="{{ url_for('consulta') }}">Ir a consulta</a>
            </div>
        </form>
    </div>

    <div class="card">
        <div class="section-title">
            <h2>Registros recientes</h2>
            {% if is_admin %}<a class="btn btn-secondary btn-small" href="{{ url_for('export_despachos') }}">Exportar Excel</a>{% endif %}
        </div>
        <div class="table-wrap">
            <table>
                <tr>
                    <th>ID</th><th>Documento</th><th>Estado</th><th>Monto</th><th>Usuario</th><th>Fecha</th>{% if is_admin %}<th>Acción</th>{% endif %}
                </tr>
                {% for d in recientes %}
                <tr>
                    <td>{{ d.id }}</td>
                    <td>{{ d.tipo_documento }} {{ d.numero_documento }}</td>
                    <td><span class="badge {% if d.estado == 'Entregado' %}ok{% elif d.estado == 'Pendiente' %}warn{% else %}info{% endif %}">{{ d.estado }}</span></td>
                    <td>{{ d.monto|money }}</td>
                    <td>{{ d.usuario_nombre }}</td>
                    <td>{{ d.created_at }}</td>
                    {% if is_admin %}
                    <td><a class="btn btn-secondary btn-small" href="{{ url_for('edit_despacho', despacho_id=d.id) }}">Editar</a></td>
                    {% endif %}
                </tr>
                {% else %}
                <tr><td colspan="7" class="muted">Sin registros.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>

    {% if is_admin %}
    <div class="card">
        <h3>Auditoría reciente de despachos</h3>
        <div class="table-wrap">
            <table>
                <tr><th>Fecha</th><th>Usuario</th><th>Acción</th><th>Campo</th><th>Anterior</th><th>Nuevo</th></tr>
                {% for a in audit %}
                <tr>
                    <td>{{ a.created_at }}</td><td>{{ a.usuario_nombre }}</td><td>{{ a.accion }}</td>
                    <td>{{ a.campo }}</td><td>{{ a.valor_anterior }}</td><td>{{ a.valor_nuevo }}</td>
                </tr>
                {% else %}
                <tr><td colspan="6" class="muted">Sin auditoría.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
    {% endif %}
    """, estados=estados, tipos=tipos, recientes=recientes, audit=audit, is_admin=is_admin())

@app.route("/despachos/<int:despacho_id>/editar", methods=["GET", "POST"])
@login_required
@permission_required("dashboard")
def edit_despacho(despacho_id):
    despacho = query_one("SELECT * FROM despachos WHERE id = ?", (despacho_id,))
    if not despacho:
        flash("Despacho no encontrado.", "error")
        return redirect(url_for("despachos"))

    estados = ["Entregado", "Pendiente"]
    tipos = get_config_list("tipos_documento")
    fields = ["numero_documento", "tipo_documento", "estado", "monto"]

    if request.method == "POST":
        estado = request.form.get("estado", "Entregado").strip() or "Entregado"
        if estado not in estados:
            estado = "Entregado"

        new_data = {
            "numero_documento": request.form.get("numero_documento", "").strip(),
            "tipo_documento": request.form.get("tipo_documento", "").strip(),
            "estado": estado,
            "monto": money_to_float(request.form.get("monto")),
        }

        for field in fields:
            old = despacho[field]
            new = new_data[field]
            if str(old or "") != str(new or ""):
                write_audit("despachos", "editar", despacho_id, field, old, new)

        execute("""
            UPDATE despachos
            SET numero_documento=?, tipo_documento=?, estado=?, monto=?, updated_at=?
            WHERE id=?
        """, (
            new_data["numero_documento"], new_data["tipo_documento"], new_data["estado"],
            new_data["monto"], now_str(), despacho_id
        ))
        flash("Despacho actualizado correctamente.", "success")
        return redirect(url_for("despachos"))

    return render_page("Editar despacho", r"""
    <div class="page-head">
        <div>
            <h1>Editar despacho #{{ despacho.id }}</h1>
            <p>Edición restringida a administradores. Cada cambio queda registrado en auditoría.</p>
        </div>
    </div>

    <div class="card">
        <form method="post">
            <div class="form-row">
                <div><label>Número documento</label><input name="numero_documento" value="{{ despacho.numero_documento }}" required></div>
                <div>
                    <label>Tipo documento</label>
                    <select name="tipo_documento">
                        {% for t in tipos %}<option {% if despacho.tipo_documento==t %}selected{% endif %}>{{ t }}</option>{% endfor %}
                    </select>
                </div>
                <div>
                    <label>Estado</label>
                    <select name="estado">
                        {% for e in estados %}<option {% if despacho.estado==e %}selected{% endif %}>{{ e }}</option>{% endfor %}
                    </select>
                </div>
                <div><label>Monto</label><input name="monto" value="{{ despacho.monto }}"></div>
            </div>
            <div class="actions">
                <button class="btn btn-primary">Guardar cambios</button>
                <a class="btn btn-secondary" href="{{ url_for('despachos') }}">Volver</a>
            </div>
        </form>
    </div>
    """, despacho=despacho, estados=estados, tipos=tipos)

# ============================================================
# CONSULTA
# ============================================================

@app.route("/consulta")
@login_required
@permission_required("consulta")
def consulta():
    q = request.args.get("q", "").strip()
    estado = request.args.get("estado", "").strip()
    desde = request.args.get("desde", "").strip()
    hasta = request.args.get("hasta", "").strip()

    wheres = []
    params = []
    if q:
        wheres.append("(numero_documento LIKE ? OR tipo_documento LIKE ? OR usuario_nombre LIKE ?)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if estado:
        wheres.append("estado = ?")
        params.append(estado)
    if desde:
        wheres.append("date(created_at) >= date(?)")
        params.append(desde)
    if hasta:
        wheres.append("date(created_at) <= date(?)")
        params.append(hasta)

    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""
    rows = query_all(f"""
        SELECT * FROM despachos
        {where_sql}
        ORDER BY id DESC
        LIMIT 500
    """, params)
    estados = ["Entregado", "Pendiente"]

    return render_page("Consulta", r"""
    <div class="page-head">
        <div>
            <h1>Consulta</h1>
            <p>Búsqueda de registros rápidos de despacho. Vista simple para operación.</p>
        </div>
    </div>

    <div class="card">
        <form method="get">
            <div class="form-row">
                <div>
                    <label>Buscar</label>
                    <input name="q" value="{{ request.args.get('q','') }}" placeholder="Documento, tipo o usuario">
                </div>
                <div>
                    <label>Estado</label>
                    <select name="estado">
                        <option value="">Todos</option>
                        {% for e in estados %}
                        <option value="{{ e }}" {% if request.args.get('estado')==e %}selected{% endif %}>{{ e }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div>
                    <label>Fecha desde</label>
                    <input type="date" name="desde" value="{{ request.args.get('desde','') }}">
                </div>
                <div>
                    <label>Fecha hasta</label>
                    <input type="date" name="hasta" value="{{ request.args.get('hasta','') }}">
                </div>
            </div>
            <div class="actions">
                <button class="btn btn-primary">Filtrar</button>
                <a class="btn btn-secondary" href="{{ url_for('consulta') }}">Limpiar</a>
                {% if is_admin %}
                <a class="btn btn-secondary" href="{{ url_for('export_despachos', q=request.args.get('q',''), estado=request.args.get('estado',''), desde=request.args.get('desde',''), hasta=request.args.get('hasta','')) }}">Exportar resultado</a>
                {% endif %}
            </div>
        </form>
    </div>

    <div class="card">
        <h2>Resultado: {{ rows|length }} registros</h2>
        <div class="table-wrap">
            <table>
                <tr>
                    <th>ID</th><th>Documento</th><th>Estado</th><th>Monto</th><th>Usuario</th><th>Fecha</th>
                </tr>
                {% for d in rows %}
                <tr>
                    <td>{{ d.id }}</td>
                    <td>{{ d.tipo_documento }} {{ d.numero_documento }}</td>
                    <td><span class="badge {% if d.estado == 'Entregado' %}ok{% elif d.estado == 'Pendiente' %}warn{% else %}info{% endif %}">{{ d.estado }}</span></td>
                    <td>{{ d.monto|money }}</td>
                    <td>{{ d.usuario_nombre }}</td>
                    <td>{{ d.created_at }}</td>
                </tr>
                {% else %}
                <tr><td colspan="6" class="muted">Sin resultados.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """, rows=rows, estados=estados, request=request, is_admin=is_admin())

# ============================================================
# MANTENCIONES
# ============================================================

@app.route("/mantenciones", methods=["GET", "POST"])
@login_required
@permission_required("mantenciones")
def mantenciones():
    maqs = query_all("SELECT * FROM maquinarias WHERE estado != 'Vendida / dada de baja' ORDER BY nombre")
    estados = get_config_list("estados_mantencion")
    tipos = get_config_list("tipos_mantencion")

    if request.method == "POST":
        user = current_user()
        maquinaria_id = request.form.get("maquinaria_id") or None
        maquinaria_nombre = request.form.get("maquinaria_nombre_manual", "").strip()

        if maquinaria_id:
            maq = query_one("SELECT * FROM maquinarias WHERE id = ?", (maquinaria_id,))
            maquinaria_nombre = maq["nombre"] if maq else maquinaria_nombre

        fecha = request.form.get("fecha", today_str())
        tipo_mantencion = request.form.get("tipo_mantencion", "").strip()
        estado = request.form.get("estado", "Pendiente").strip()

        if not maquinaria_nombre or not fecha or not tipo_mantencion:
            flash("Maquinaria, fecha y tipo de mantención son obligatorios.", "error")
        else:
            new_id = insert_and_get_id("""
                INSERT INTO mantenciones (
                    maquinaria_id, maquinaria_nombre, fecha, tipo_mantencion, estado,
                    responsable, costo, observaciones, usuario_id, usuario_nombre, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                maquinaria_id,
                maquinaria_nombre,
                fecha,
                tipo_mantencion,
                estado,
                request.form.get("responsable", "").strip(),
                money_to_float(request.form.get("costo")),
                request.form.get("observaciones", "").strip(),
                user["id"],
                user["full_name"],
                now_str(),
                now_str(),
            ))
            write_audit("mantenciones", "crear", new_id, "registro", "", maquinaria_nombre)
            flash("Mantención registrada correctamente.", "success")
            return redirect(url_for("mantenciones"))

    rows = query_all("SELECT * FROM mantenciones ORDER BY fecha DESC, id DESC LIMIT 100")

    return render_page("Mantenciones", r"""
    <div class="page-head">
        <div>
            <h1>Módulo Mantenciones</h1>
            <p>Control de mantenciones, maquinaria y alertas operativas.</p>
        </div>
    </div>

    <div class="card">
        <div class="section-title">
            <h2>Nueva mantención</h2>
            <a class="btn btn-secondary btn-small" href="{{ url_for('export_mantenciones') }}">Exportar Excel</a>
        </div>
        <form method="post">
            <div class="form-row">
                <div>
                    <label>Maquinaria existente</label>
                    <select name="maquinaria_id">
                        <option value="">Seleccionar o escribir manual</option>
                        {% for m in maqs %}
                        <option value="{{ m.id }}">{{ m.nombre }}{% if m.patente %} · {{ m.patente }}{% endif %}</option>
                        {% endfor %}
                    </select>
                </div>
                <div>
                    <label>Maquinaria manual</label>
                    <input name="maquinaria_nombre_manual" placeholder="Solo si no existe en listado">
                </div>
                <div>
                    <label>Fecha</label>
                    <input type="date" name="fecha" value="{{ today }}" required>
                </div>
                <div>
                    <label>Tipo</label>
                    <select name="tipo_mantencion" required>
                        {% for t in tipos %}<option>{{ t }}</option>{% endfor %}
                    </select>
                </div>
            </div>

            <div class="form-row" style="margin-top:14px;">
                <div>
                    <label>Estado</label>
                    <select name="estado">
                        {% for e in estados %}<option>{{ e }}</option>{% endfor %}
                    </select>
                </div>
                <div>
                    <label>Responsable</label>
                    <input name="responsable">
                </div>
                <div>
                    <label>Costo</label>
                    <input name="costo" inputmode="decimal">
                </div>
                <div>
                    <label>Usuario</label>
                    <input value="{{ user.full_name }}" disabled>
                </div>
            </div>

            <div style="margin-top:14px;">
                <label>Observaciones</label>
                <textarea name="observaciones"></textarea>
            </div>

            <div class="actions">
                <button class="btn btn-primary">Guardar mantención</button>
            </div>
        </form>
    </div>

    <div class="card">
        <h2>Historial de mantenciones</h2>
        <div class="table-wrap">
            <table>
                <tr>
                    <th>ID</th><th>Fecha</th><th>Maquinaria</th><th>Tipo</th><th>Estado</th>
                    <th>Responsable</th><th>Costo</th><th>Usuario</th><th>Obs.</th>{% if is_admin %}<th>Acción</th>{% endif %}
                </tr>
                {% for m in rows %}
                <tr>
                    <td>{{ m.id }}</td>
                    <td>{{ m.fecha }}</td>
                    <td>{{ m.maquinaria_nombre }}</td>
                    <td>{{ m.tipo_mantencion }}</td>
                    <td><span class="badge {% if m.estado == 'Realizada' %}ok{% elif m.estado in ['Pendiente','En proceso'] %}warn{% else %}info{% endif %}">{{ m.estado }}</span></td>
                    <td>{{ m.responsable }}</td>
                    <td>{{ m.costo|money }}</td>
                    <td>{{ m.usuario_nombre }}</td>
                    <td>{{ m.observaciones }}</td>
                    {% if is_admin %}<td><a class="btn btn-secondary btn-small" href="{{ url_for('edit_mantencion', mantencion_id=m.id) }}">Editar</a></td>{% endif %}
                </tr>
                {% else %}
                <tr><td colspan="10" class="muted">Sin mantenciones.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """, maqs=maqs, estados=estados, tipos=tipos, rows=rows, today=today_str(), user=current_user(), is_admin=is_admin())


@app.route("/mantenciones/<int:mantencion_id>/editar", methods=["GET", "POST"])
@login_required
@permission_required("dashboard")
def edit_mantencion(mantencion_id):
    m = query_one("SELECT * FROM mantenciones WHERE id = ?", (mantencion_id,))
    if not m:
        flash("Mantención no encontrada.", "error")
        return redirect(url_for("mantenciones"))

    estados = get_config_list("estados_mantencion")
    tipos = get_config_list("tipos_mantencion")

    if request.method == "POST":
        data = {
            "maquinaria_nombre": request.form.get("maquinaria_nombre", "").strip(),
            "fecha": request.form.get("fecha", "").strip(),
            "tipo_mantencion": request.form.get("tipo_mantencion", "").strip(),
            "estado": request.form.get("estado", "").strip(),
            "responsable": request.form.get("responsable", "").strip(),
            "costo": money_to_float(request.form.get("costo")),
            "observaciones": request.form.get("observaciones", "").strip(),
        }
        for k, v in data.items():
            if str(m[k] or "") != str(v or ""):
                write_audit("mantenciones", "editar", mantencion_id, k, m[k], v)

        execute("""
            UPDATE mantenciones
            SET maquinaria_nombre=?, fecha=?, tipo_mantencion=?, estado=?, responsable=?, costo=?, observaciones=?, updated_at=?
            WHERE id=?
        """, (
            data["maquinaria_nombre"], data["fecha"], data["tipo_mantencion"], data["estado"],
            data["responsable"], data["costo"], data["observaciones"], now_str(), mantencion_id
        ))
        flash("Mantención actualizada.", "success")
        return redirect(url_for("mantenciones"))

    return render_page("Editar mantención", r"""
    <div class="page-head">
        <div>
            <h1>Editar mantención #{{ m.id }}</h1>
            <p>Edición restringida a administradores.</p>
        </div>
    </div>
    <div class="card">
        <form method="post">
            <div class="form-row">
                <div><label>Maquinaria</label><input name="maquinaria_nombre" value="{{ m.maquinaria_nombre }}" required></div>
                <div><label>Fecha</label><input type="date" name="fecha" value="{{ m.fecha }}" required></div>
                <div>
                    <label>Tipo</label>
                    <select name="tipo_mantencion">
                        {% for t in tipos %}<option {% if m.tipo_mantencion==t %}selected{% endif %}>{{ t }}</option>{% endfor %}
                    </select>
                </div>
                <div>
                    <label>Estado</label>
                    <select name="estado">
                        {% for e in estados %}<option {% if m.estado==e %}selected{% endif %}>{{ e }}</option>{% endfor %}
                    </select>
                </div>
            </div>
            <div class="form-row-2" style="margin-top:14px;">
                <div><label>Responsable</label><input name="responsable" value="{{ m.responsable }}"></div>
                <div><label>Costo</label><input name="costo" value="{{ m.costo }}"></div>
            </div>
            <div style="margin-top:14px;">
                <label>Observaciones</label>
                <textarea name="observaciones">{{ m.observaciones }}</textarea>
            </div>
            <div class="actions">
                <button class="btn btn-primary">Guardar cambios</button>
                <a class="btn btn-secondary" href="{{ url_for('mantenciones') }}">Volver</a>
            </div>
        </form>
    </div>
    """, m=m, estados=estados, tipos=tipos)


# ============================================================
# ADMINISTRACIÓN / CONFIGURACIÓN
# ============================================================

@app.route("/administracion", methods=["GET", "POST"])
@login_required
@permission_required("administracion")
def administracion():
    keys = [
        ("estados_despacho", "Estados de despacho"),
        ("tipos_documento", "Tipos de documento"),
        ("sucursales", "Sucursales"),
        ("estados_maquinaria", "Estados de maquinaria"),
        ("estados_mantencion", "Estados de mantención"),
        ("tipos_mantencion", "Tipos de mantención"),
    ]

    if request.method == "POST":
        for clave, _label in keys:
            raw = request.form.get(clave, "")
            values = [line.strip() for line in raw.splitlines() if line.strip()]
            old = get_config_list(clave)
            set_config_list(clave, values)
            write_audit("administracion", "configurar", None, clave, ", ".join(old), ", ".join(values))
        flash("Configuración actualizada.", "success")
        return redirect(url_for("administracion"))

    config = {clave: "\n".join(get_config_list(clave)) for clave, _ in keys}

    return render_page("Administración", r"""
    <div class="page-head">
        <div>
            <h1>Módulo Administración / Configuración</h1>
            <p>Centro de control del sistema. Visible solo para administradores.</p>
        </div>
    </div>

    <div class="grid grid-3">
        <div class="stat"><span>Usuarios y permisos</span><strong>{{ users_count }}</strong></div>
        <div class="stat"><span>Maquinarias</span><strong>{{ maqs_count }}</strong></div>
        <div class="stat"><span>Vehículos</span><strong>{{ vehs_count }}</strong></div>
    </div>

    <div class="card" style="margin-top:18px;">
        <h2>Configuración general</h2>
        <p class="muted">Escribe un valor por línea. Estos listados alimentan los formularios del sistema.</p>
        <form method="post">
            <div class="grid grid-2">
                {% for clave, label in keys %}
                <div>
                    <label>{{ label }}</label>
                    <textarea name="{{ clave }}">{{ config[clave] }}</textarea>
                </div>
                {% endfor %}
            </div>
            <div class="actions">
                <button class="btn btn-primary">Guardar configuración</button>
            </div>
        </form>
    </div>

    <div class="grid grid-3">
        <a class="card" href="{{ url_for('usuarios') }}">
            <h3>Usuarios y permisos</h3>
            <p class="muted">Crear usuarios, asignar roles y ocultar Dashboard a quienes no sean admin.</p>
        </a>
        <a class="card" href="{{ url_for('maquinarias') }}">
            <h3>Maquinarias</h3>
            <p class="muted">Crear, editar y desactivar maquinaria. Modificación solo admin.</p>
        </a>
        <a class="card" href="{{ url_for('vehiculos') }}">
            <h3>Vehículos / Patentes</h3>
            <p class="muted">Controlar patentes, vencimientos, revisión técnica y seguro.</p>
        </a>
    </div>
    """, keys=keys, config=config,
       users_count=query_one("SELECT COUNT(*) c FROM users")["c"],
       maqs_count=query_one("SELECT COUNT(*) c FROM maquinarias")["c"],
       vehs_count=query_one("SELECT COUNT(*) c FROM vehiculos")["c"])


# ============================================================
# USUARIOS
# ============================================================

@app.route("/usuarios", methods=["GET", "POST"])
@login_required
@permission_required("usuarios")
def usuarios():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        full_name = request.form.get("full_name", "").strip()
        password = request.form.get("password", "").strip()
        role = request.form.get("role", "operador")
        perms = request.form.getlist("permissions")

        if not username or not full_name or not password:
            flash("Usuario, nombre y contraseña son obligatorios.", "error")
        elif query_one("SELECT id FROM users WHERE username = ?", (username,)):
            flash("Ese usuario ya existe.", "error")
        else:
            permissions = {"all": True} if role == "admin" else {p: True for p in perms}
            new_id = insert_and_get_id("""
                INSERT INTO users (username, full_name, password_hash, role, is_active, permissions, created_at, updated_at)
                VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            """, (
                username, full_name, generate_password_hash(password), role,
                json.dumps(permissions), now_str(), now_str()
            ))
            write_audit("usuarios", "crear", new_id, "usuario", "", username)
            flash("Usuario creado.", "success")
            return redirect(url_for("usuarios"))

    rows = query_all("SELECT * FROM users ORDER BY id")
    permission_labels = PERMISSION_LABELS

    return render_page("Usuarios", r"""
    <div class="page-head">
        <div>
            <h1>Usuarios y permisos</h1>
            <p>Dashboard, Auditoría y Administración quedan ocultos para usuarios no autorizados.</p>
        </div>
    </div>

    <div class="card">
        <h2>Crear usuario</h2>
        <form method="post">
            <div class="form-row">
                <div><label>Usuario</label><input name="username" required></div>
                <div><label>Nombre completo</label><input name="full_name" required></div>
                <div><label>Contraseña</label><input name="password" required></div>
                <div>
                    <label>Rol</label>
                    <select name="role">
                        <option value="operador">Operador</option>
                        <option value="admin">Administrador</option>
                    </select>
                </div>
            </div>

            <h3>Permisos del usuario operativo</h3>
            <div class="checkbox-grid">
                {% for key, label in permission_labels.items() %}
                    {% if key != "dashboard" and key != "auditoria" and key != "administracion" %}
                    <label class="check-item"><input type="checkbox" name="permissions" value="{{ key }}"> {{ label }}</label>
                    {% else %}
                    <label class="check-item"><input type="checkbox" name="permissions" value="{{ key }}"> {{ label }} <span class="muted">(admin recomendado)</span></label>
                    {% endif %}
                {% endfor %}
            </div>

            <div class="actions">
                <button class="btn btn-primary">Crear usuario</button>
            </div>
        </form>
    </div>

    <div class="card">
        <h2>Usuarios existentes</h2>
        <div class="table-wrap">
            <table>
                <tr><th>ID</th><th>Usuario</th><th>Nombre</th><th>Rol</th><th>Estado</th><th>Permisos</th><th>Acción</th></tr>
                {% for u in rows %}
                <tr>
                    <td>{{ u.id }}</td>
                    <td>{{ u.username }}</td>
                    <td>{{ u.full_name }}</td>
                    <td><span class="badge {% if u.role == 'admin' %}ok{% else %}info{% endif %}">{{ u.role }}</span></td>
                    <td>{% if u.is_active %}<span class="badge ok">Activo</span>{% else %}<span class="badge bad">Inactivo</span>{% endif %}</td>
                    <td class="muted">{{ u.permissions }}</td>
                    <td><a class="btn btn-secondary btn-small" href="{{ url_for('edit_usuario', user_id=u.id) }}">Editar</a></td>
                </tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """, rows=rows, permission_labels=permission_labels)


@app.route("/usuarios/<int:user_id>/editar", methods=["GET", "POST"])
@login_required
@permission_required("usuarios")
def edit_usuario(user_id):
    u = query_one("SELECT * FROM users WHERE id = ?", (user_id,))
    if not u:
        flash("Usuario no encontrado.", "error")
        return redirect(url_for("usuarios"))

    current_perms = user_permissions(u)

    if request.method == "POST":
        full_name = request.form.get("full_name", "").strip()
        role = request.form.get("role", "operador")
        is_active = 1 if request.form.get("is_active") == "1" else 0
        password = request.form.get("password", "").strip()
        perms = request.form.getlist("permissions")
        permissions = {"all": True} if role == "admin" else {p: True for p in perms}

        # Evita que el usuario actual se desactive a sí mismo por accidente.
        if u["id"] == current_user()["id"] and not is_active:
            flash("No puedes desactivar tu propio usuario desde esta pantalla.", "error")
            return redirect(url_for("edit_usuario", user_id=user_id))

        if password:
            execute("""
                UPDATE users
                SET full_name=?, role=?, is_active=?, permissions=?, password_hash=?, updated_at=?
                WHERE id=?
            """, (
                full_name, role, is_active, json.dumps(permissions),
                generate_password_hash(password), now_str(), user_id
            ))
            write_audit("usuarios", "editar", user_id, "password", "anterior", "actualizada")
        else:
            execute("""
                UPDATE users
                SET full_name=?, role=?, is_active=?, permissions=?, updated_at=?
                WHERE id=?
            """, (
                full_name, role, is_active, json.dumps(permissions), now_str(), user_id
            ))

        write_audit("usuarios", "editar", user_id, "usuario", u["username"], f"{full_name} / {role}")
        flash("Usuario actualizado.", "success")
        return redirect(url_for("usuarios"))

    return render_page("Editar usuario", r"""
    <div class="page-head">
        <div>
            <h1>Editar usuario: {{ u.username }}</h1>
            <p>Los permisos controlan qué botones y secciones ve cada persona.</p>
        </div>
    </div>

    <div class="card">
        <form method="post">
            <div class="form-row">
                <div><label>Usuario</label><input value="{{ u.username }}" disabled></div>
                <div><label>Nombre completo</label><input name="full_name" value="{{ u.full_name }}" required></div>
                <div><label>Nueva contraseña</label><input name="password" placeholder="Dejar vacío para mantener"></div>
                <div>
                    <label>Rol</label>
                    <select name="role">
                        <option value="operador" {% if u.role=='operador' %}selected{% endif %}>Operador</option>
                        <option value="admin" {% if u.role=='admin' %}selected{% endif %}>Administrador</option>
                    </select>
                </div>
            </div>

            <div style="margin-top:14px;">
                <label>Estado</label>
                <select name="is_active">
                    <option value="1" {% if u.is_active %}selected{% endif %}>Activo</option>
                    <option value="0" {% if not u.is_active %}selected{% endif %}>Inactivo</option>
                </select>
            </div>

            <h3>Permisos</h3>
            <div class="checkbox-grid">
                {% for key, label in permission_labels.items() %}
                <label class="check-item">
                    <input type="checkbox" name="permissions" value="{{ key }}" {% if current_perms.get(key) or current_perms.get('all') %}checked{% endif %}>
                    {{ label }}
                </label>
                {% endfor %}
            </div>

            <div class="actions">
                <button class="btn btn-primary">Guardar usuario</button>
                <a class="btn btn-secondary" href="{{ url_for('usuarios') }}">Volver</a>
            </div>
        </form>
    </div>
    """, u=u, current_perms=current_perms, permission_labels=PERMISSION_LABELS)


# ============================================================
# MAQUINARIAS
# ============================================================

@app.route("/maquinarias", methods=["GET", "POST"])
@login_required
@permission_required("maquinarias")
def maquinarias():
    estados = get_config_list("estados_maquinaria")

    if request.method == "POST":
        new_id = insert_and_get_id("""
            INSERT INTO maquinarias (codigo, nombre, tipo, marca, modelo, anio, patente, estado, observaciones, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form.get("codigo", "").strip(),
            request.form.get("nombre", "").strip(),
            request.form.get("tipo", "").strip(),
            request.form.get("marca", "").strip(),
            request.form.get("modelo", "").strip(),
            request.form.get("anio", "").strip(),
            request.form.get("patente", "").strip(),
            request.form.get("estado", "Activa").strip(),
            request.form.get("observaciones", "").strip(),
            now_str(),
            now_str(),
        ))
        write_audit("maquinarias", "crear", new_id, "maquinaria", "", request.form.get("nombre", ""))
        flash("Maquinaria creada.", "success")
        return redirect(url_for("maquinarias"))

    rows = query_all("SELECT * FROM maquinarias ORDER BY estado, nombre")

    return render_page("Maquinarias", r"""
    <div class="page-head">
        <div>
            <h1>Maquinarias</h1>
            <p>Crear, editar o desactivar maquinaria. Esta sección es solo para administradores.</p>
        </div>
    </div>

    <div class="card">
        <h2>Nueva maquinaria</h2>
        <form method="post">
            <div class="form-row">
                <div><label>Código interno</label><input name="codigo"></div>
                <div><label>Nombre *</label><input name="nombre" required></div>
                <div><label>Tipo</label><input name="tipo" placeholder="Grúa, camión, elevador, etc."></div>
                <div><label>Estado</label>
                    <select name="estado">{% for e in estados %}<option>{{ e }}</option>{% endfor %}</select>
                </div>
            </div>
            <div class="form-row" style="margin-top:14px;">
                <div><label>Marca</label><input name="marca"></div>
                <div><label>Modelo</label><input name="modelo"></div>
                <div><label>Año</label><input name="anio"></div>
                <div><label>Patente</label><input name="patente"></div>
            </div>
            <div style="margin-top:14px;">
                <label>Observaciones</label>
                <textarea name="observaciones"></textarea>
            </div>
            <div class="actions"><button class="btn btn-primary">Guardar maquinaria</button></div>
        </form>
    </div>

    <div class="card">
        <div class="section-title">
            <h2>Listado de maquinarias</h2>
            <a class="btn btn-secondary btn-small" href="{{ url_for('export_maquinarias') }}">Exportar Excel</a>
        </div>
        <div class="table-wrap">
            <table>
                <tr><th>Código</th><th>Nombre</th><th>Tipo</th><th>Marca/Modelo</th><th>Patente</th><th>Estado</th><th>Obs.</th><th>Acción</th></tr>
                {% for m in rows %}
                <tr>
                    <td>{{ m.codigo }}</td>
                    <td>{{ m.nombre }}</td>
                    <td>{{ m.tipo }}</td>
                    <td>{{ m.marca }} {{ m.modelo }} {{ m.anio }}</td>
                    <td>{{ m.patente }}</td>
                    <td><span class="badge {% if m.estado == 'Activa' %}ok{% elif m.estado == 'Fuera de servicio' %}bad{% else %}warn{% endif %}">{{ m.estado }}</span></td>
                    <td>{{ m.observaciones }}</td>
                    <td><a class="btn btn-secondary btn-small" href="{{ url_for('edit_maquinaria', maquinaria_id=m.id) }}">Editar</a></td>
                </tr>
                {% else %}
                <tr><td colspan="8" class="muted">Sin maquinarias.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """, rows=rows, estados=estados)


@app.route("/maquinarias/<int:maquinaria_id>/editar", methods=["GET", "POST"])
@login_required
@permission_required("maquinarias")
def edit_maquinaria(maquinaria_id):
    m = query_one("SELECT * FROM maquinarias WHERE id = ?", (maquinaria_id,))
    if not m:
        flash("Maquinaria no encontrada.", "error")
        return redirect(url_for("maquinarias"))
    estados = get_config_list("estados_maquinaria")

    if request.method == "POST":
        data = {
            "codigo": request.form.get("codigo", "").strip(),
            "nombre": request.form.get("nombre", "").strip(),
            "tipo": request.form.get("tipo", "").strip(),
            "marca": request.form.get("marca", "").strip(),
            "modelo": request.form.get("modelo", "").strip(),
            "anio": request.form.get("anio", "").strip(),
            "patente": request.form.get("patente", "").strip(),
            "estado": request.form.get("estado", "").strip(),
            "observaciones": request.form.get("observaciones", "").strip(),
        }
        for k, v in data.items():
            if str(m[k] or "") != str(v or ""):
                write_audit("maquinarias", "editar", maquinaria_id, k, m[k], v)
        execute("""
            UPDATE maquinarias
            SET codigo=?, nombre=?, tipo=?, marca=?, modelo=?, anio=?, patente=?, estado=?, observaciones=?, updated_at=?
            WHERE id=?
        """, (
            data["codigo"], data["nombre"], data["tipo"], data["marca"], data["modelo"], data["anio"],
            data["patente"], data["estado"], data["observaciones"], now_str(), maquinaria_id
        ))
        flash("Maquinaria actualizada.", "success")
        return redirect(url_for("maquinarias"))

    return render_page("Editar maquinaria", r"""
    <div class="page-head"><div><h1>Editar maquinaria</h1><p>Modificación solo admin.</p></div></div>
    <div class="card">
        <form method="post">
            <div class="form-row">
                <div><label>Código</label><input name="codigo" value="{{ m.codigo }}"></div>
                <div><label>Nombre</label><input name="nombre" value="{{ m.nombre }}" required></div>
                <div><label>Tipo</label><input name="tipo" value="{{ m.tipo }}"></div>
                <div><label>Estado</label><select name="estado">{% for e in estados %}<option {% if m.estado==e %}selected{% endif %}>{{ e }}</option>{% endfor %}</select></div>
            </div>
            <div class="form-row" style="margin-top:14px;">
                <div><label>Marca</label><input name="marca" value="{{ m.marca }}"></div>
                <div><label>Modelo</label><input name="modelo" value="{{ m.modelo }}"></div>
                <div><label>Año</label><input name="anio" value="{{ m.anio }}"></div>
                <div><label>Patente</label><input name="patente" value="{{ m.patente }}"></div>
            </div>
            <div style="margin-top:14px;"><label>Observaciones</label><textarea name="observaciones">{{ m.observaciones }}</textarea></div>
            <div class="actions">
                <button class="btn btn-primary">Guardar cambios</button>
                <a class="btn btn-secondary" href="{{ url_for('maquinarias') }}">Volver</a>
            </div>
        </form>
    </div>
    """, m=m, estados=estados)


# ============================================================
# VEHÍCULOS / PATENTES
# ============================================================

@app.route("/vehiculos", methods=["GET", "POST"])
@login_required
@permission_required("vehiculos")
def vehiculos():
    if request.method == "POST":
        patente = request.form.get("patente", "").strip().upper()
        if not patente:
            flash("La patente es obligatoria.", "error")
        elif query_one("SELECT id FROM vehiculos WHERE patente = ?", (patente,)):
            flash("Esa patente ya existe.", "error")
        else:
            new_id = insert_and_get_id("""
                INSERT INTO vehiculos (
                    patente, tipo, marca, modelo, anio, estado,
                    permiso_circulacion_vencimiento, revision_tecnica_vencimiento,
                    seguro_obligatorio_vencimiento, observaciones, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                patente,
                request.form.get("tipo", "").strip(),
                request.form.get("marca", "").strip(),
                request.form.get("modelo", "").strip(),
                request.form.get("anio", "").strip(),
                request.form.get("estado", "Activo").strip(),
                request.form.get("permiso_circulacion_vencimiento", "").strip(),
                request.form.get("revision_tecnica_vencimiento", "").strip(),
                request.form.get("seguro_obligatorio_vencimiento", "").strip(),
                request.form.get("observaciones", "").strip(),
                now_str(),
                now_str(),
            ))
            write_audit("vehiculos", "crear", new_id, "patente", "", patente)
            flash("Vehículo creado.", "success")
            return redirect(url_for("vehiculos"))

    rows = query_all("SELECT * FROM vehiculos ORDER BY patente")

    return render_page("Vehículos", r"""
    <div class="page-head"><div><h1>Vehículos / Patentes</h1><p>Control de vehículos, documentos y vencimientos.</p></div></div>

    <div class="card">
        <h2>Nuevo vehículo</h2>
        <form method="post">
            <div class="form-row">
                <div><label>Patente *</label><input name="patente" required></div>
                <div><label>Tipo</label><input name="tipo" placeholder="Camión, camioneta, auto"></div>
                <div><label>Marca</label><input name="marca"></div>
                <div><label>Modelo</label><input name="modelo"></div>
            </div>
            <div class="form-row" style="margin-top:14px;">
                <div><label>Año</label><input name="anio"></div>
                <div><label>Estado</label><select name="estado"><option>Activo</option><option>En mantención</option><option>Fuera de servicio</option><option>Vendido</option></select></div>
                <div><label>Permiso circulación</label><input type="date" name="permiso_circulacion_vencimiento"></div>
                <div><label>Revisión técnica</label><input type="date" name="revision_tecnica_vencimiento"></div>
            </div>
            <div class="form-row-2" style="margin-top:14px;">
                <div><label>Seguro obligatorio</label><input type="date" name="seguro_obligatorio_vencimiento"></div>
                <div><label>Observaciones</label><input name="observaciones"></div>
            </div>
            <div class="actions"><button class="btn btn-primary">Guardar vehículo</button></div>
        </form>
    </div>

    <div class="card">
        <div class="section-title">
            <h2>Listado de vehículos</h2>
            <a class="btn btn-secondary btn-small" href="{{ url_for('export_vehiculos') }}">Exportar Excel</a>
        </div>
        <div class="table-wrap">
            <table>
                <tr><th>Patente</th><th>Tipo</th><th>Marca/Modelo</th><th>Estado</th><th>Permiso</th><th>Rev. técnica</th><th>SOAP</th><th>Acción</th></tr>
                {% for v in rows %}
                <tr>
                    <td>{{ v.patente }}</td>
                    <td>{{ v.tipo }}</td>
                    <td>{{ v.marca }} {{ v.modelo }} {{ v.anio }}</td>
                    <td><span class="badge {% if v.estado == 'Activo' %}ok{% elif v.estado == 'Fuera de servicio' %}bad{% else %}warn{% endif %}">{{ v.estado }}</span></td>
                    <td>{{ v.permiso_circulacion_vencimiento }}</td>
                    <td>{{ v.revision_tecnica_vencimiento }}</td>
                    <td>{{ v.seguro_obligatorio_vencimiento }}</td>
                    <td><a class="btn btn-secondary btn-small" href="{{ url_for('edit_vehiculo', vehiculo_id=v.id) }}">Editar</a></td>
                </tr>
                {% else %}
                <tr><td colspan="8" class="muted">Sin vehículos.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """, rows=rows)


@app.route("/vehiculos/<int:vehiculo_id>/editar", methods=["GET", "POST"])
@login_required
@permission_required("vehiculos")
def edit_vehiculo(vehiculo_id):
    v = query_one("SELECT * FROM vehiculos WHERE id = ?", (vehiculo_id,))
    if not v:
        flash("Vehículo no encontrado.", "error")
        return redirect(url_for("vehiculos"))

    if request.method == "POST":
        data = {
            "patente": request.form.get("patente", "").strip().upper(),
            "tipo": request.form.get("tipo", "").strip(),
            "marca": request.form.get("marca", "").strip(),
            "modelo": request.form.get("modelo", "").strip(),
            "anio": request.form.get("anio", "").strip(),
            "estado": request.form.get("estado", "").strip(),
            "permiso_circulacion_vencimiento": request.form.get("permiso_circulacion_vencimiento", "").strip(),
            "revision_tecnica_vencimiento": request.form.get("revision_tecnica_vencimiento", "").strip(),
            "seguro_obligatorio_vencimiento": request.form.get("seguro_obligatorio_vencimiento", "").strip(),
            "observaciones": request.form.get("observaciones", "").strip(),
        }
        for k, value in data.items():
            if str(v[k] or "") != str(value or ""):
                write_audit("vehiculos", "editar", vehiculo_id, k, v[k], value)
        execute("""
            UPDATE vehiculos
            SET patente=?, tipo=?, marca=?, modelo=?, anio=?, estado=?,
                permiso_circulacion_vencimiento=?, revision_tecnica_vencimiento=?,
                seguro_obligatorio_vencimiento=?, observaciones=?, updated_at=?
            WHERE id=?
        """, (
            data["patente"], data["tipo"], data["marca"], data["modelo"], data["anio"], data["estado"],
            data["permiso_circulacion_vencimiento"], data["revision_tecnica_vencimiento"],
            data["seguro_obligatorio_vencimiento"], data["observaciones"], now_str(), vehiculo_id
        ))
        flash("Vehículo actualizado.", "success")
        return redirect(url_for("vehiculos"))

    return render_page("Editar vehículo", r"""
    <div class="page-head"><div><h1>Editar vehículo</h1><p>Control administrativo de patente y vencimientos.</p></div></div>
    <div class="card">
        <form method="post">
            <div class="form-row">
                <div><label>Patente</label><input name="patente" value="{{ v.patente }}" required></div>
                <div><label>Tipo</label><input name="tipo" value="{{ v.tipo }}"></div>
                <div><label>Marca</label><input name="marca" value="{{ v.marca }}"></div>
                <div><label>Modelo</label><input name="modelo" value="{{ v.modelo }}"></div>
            </div>
            <div class="form-row" style="margin-top:14px;">
                <div><label>Año</label><input name="anio" value="{{ v.anio }}"></div>
                <div><label>Estado</label><select name="estado">
                    {% for e in ['Activo','En mantención','Fuera de servicio','Vendido'] %}
                    <option {% if v.estado==e %}selected{% endif %}>{{ e }}</option>
                    {% endfor %}
                </select></div>
                <div><label>Permiso circulación</label><input type="date" name="permiso_circulacion_vencimiento" value="{{ v.permiso_circulacion_vencimiento }}"></div>
                <div><label>Revisión técnica</label><input type="date" name="revision_tecnica_vencimiento" value="{{ v.revision_tecnica_vencimiento }}"></div>
            </div>
            <div class="form-row-2" style="margin-top:14px;">
                <div><label>Seguro obligatorio</label><input type="date" name="seguro_obligatorio_vencimiento" value="{{ v.seguro_obligatorio_vencimiento }}"></div>
                <div><label>Observaciones</label><input name="observaciones" value="{{ v.observaciones }}"></div>
            </div>
            <div class="actions">
                <button class="btn btn-primary">Guardar cambios</button>
                <a class="btn btn-secondary" href="{{ url_for('vehiculos') }}">Volver</a>
            </div>
        </form>
    </div>
    """, v=v)


# ============================================================
# CONDUCTORES / PIONETAS
# ============================================================

@app.route("/logistica", methods=["GET", "POST"])
@login_required
@permission_required("logistica")
def logistica():
    if request.method == "POST":
        new_id = insert_and_get_id("""
            INSERT INTO personas_logistica (nombre, tipo, estado, telefono, observaciones, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form.get("nombre", "").strip(),
            request.form.get("tipo", "Conductor").strip(),
            request.form.get("estado", "Activo").strip(),
            request.form.get("telefono", "").strip(),
            request.form.get("observaciones", "").strip(),
            now_str(),
            now_str(),
        ))
        write_audit("logistica", "crear", new_id, "persona", "", request.form.get("nombre", ""))
        flash("Persona logística creada.", "success")
        return redirect(url_for("logistica"))

    rows = query_all("SELECT * FROM personas_logistica ORDER BY tipo, nombre")
    return render_page("Conductores y pionetas", r"""
    <div class="page-head"><div><h1>Conductores y pionetas</h1><p>Mantiene limpio el listado de personas para despachos internos.</p></div></div>

    <div class="card">
        <h2>Nueva persona</h2>
        <form method="post">
            <div class="form-row">
                <div><label>Nombre</label><input name="nombre" required></div>
                <div><label>Tipo</label><select name="tipo"><option>Conductor</option><option>Pioneta</option></select></div>
                <div><label>Estado</label><select name="estado"><option>Activo</option><option>Inactivo</option></select></div>
                <div><label>Teléfono</label><input name="telefono"></div>
            </div>
            <div style="margin-top:14px;"><label>Observaciones</label><input name="observaciones"></div>
            <div class="actions"><button class="btn btn-primary">Guardar</button></div>
        </form>
    </div>

    <div class="card">
        <h2>Listado</h2>
        <div class="table-wrap">
            <table>
                <tr><th>Nombre</th><th>Tipo</th><th>Estado</th><th>Teléfono</th><th>Obs.</th><th>Acción</th></tr>
                {% for p in rows %}
                <tr>
                    <td>{{ p.nombre }}</td>
                    <td>{{ p.tipo }}</td>
                    <td><span class="badge {% if p.estado == 'Activo' %}ok{% else %}bad{% endif %}">{{ p.estado }}</span></td>
                    <td>{{ p.telefono }}</td>
                    <td>{{ p.observaciones }}</td>
                    <td><a class="btn btn-secondary btn-small" href="{{ url_for('edit_logistica', persona_id=p.id) }}">Editar</a></td>
                </tr>
                {% else %}
                <tr><td colspan="6" class="muted">Sin personas.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """, rows=rows)


@app.route("/logistica/<int:persona_id>/editar", methods=["GET", "POST"])
@login_required
@permission_required("logistica")
def edit_logistica(persona_id):
    p = query_one("SELECT * FROM personas_logistica WHERE id = ?", (persona_id,))
    if not p:
        flash("Persona no encontrada.", "error")
        return redirect(url_for("logistica"))

    if request.method == "POST":
        data = {
            "nombre": request.form.get("nombre", "").strip(),
            "tipo": request.form.get("tipo", "").strip(),
            "estado": request.form.get("estado", "").strip(),
            "telefono": request.form.get("telefono", "").strip(),
            "observaciones": request.form.get("observaciones", "").strip(),
        }
        for k, value in data.items():
            if str(p[k] or "") != str(value or ""):
                write_audit("logistica", "editar", persona_id, k, p[k], value)
        execute("""
            UPDATE personas_logistica
            SET nombre=?, tipo=?, estado=?, telefono=?, observaciones=?, updated_at=?
            WHERE id=?
        """, (data["nombre"], data["tipo"], data["estado"], data["telefono"], data["observaciones"], now_str(), persona_id))
        flash("Persona actualizada.", "success")
        return redirect(url_for("logistica"))

    return render_page("Editar persona logística", r"""
    <div class="page-head"><div><h1>Editar conductor / pioneta</h1></div></div>
    <div class="card">
        <form method="post">
            <div class="form-row">
                <div><label>Nombre</label><input name="nombre" value="{{ p.nombre }}" required></div>
                <div><label>Tipo</label><select name="tipo"><option {% if p.tipo=='Conductor' %}selected{% endif %}>Conductor</option><option {% if p.tipo=='Pioneta' %}selected{% endif %}>Pioneta</option></select></div>
                <div><label>Estado</label><select name="estado"><option {% if p.estado=='Activo' %}selected{% endif %}>Activo</option><option {% if p.estado=='Inactivo' %}selected{% endif %}>Inactivo</option></select></div>
                <div><label>Teléfono</label><input name="telefono" value="{{ p.telefono }}"></div>
            </div>
            <div style="margin-top:14px;"><label>Observaciones</label><input name="observaciones" value="{{ p.observaciones }}"></div>
            <div class="actions">
                <button class="btn btn-primary">Guardar cambios</button>
                <a class="btn btn-secondary" href="{{ url_for('logistica') }}">Volver</a>
            </div>
        </form>
    </div>
    """, p=p)


# ============================================================
# AUDITORÍA
# ============================================================

@app.route("/auditoria")
@login_required
@permission_required("auditoria")
def auditoria():
    modulo = request.args.get("modulo", "").strip()
    usuario = request.args.get("usuario", "").strip()

    wheres = []
    params = []
    if modulo:
        wheres.append("modulo = ?")
        params.append(modulo)
    if usuario:
        wheres.append("usuario_nombre LIKE ?")
        params.append(f"%{usuario}%")

    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""
    rows = query_all(f"""
        SELECT * FROM audit_log
        {where_sql}
        ORDER BY id DESC
        LIMIT 1000
    """, params)
    modulos = query_all("SELECT DISTINCT modulo FROM audit_log ORDER BY modulo")

    return render_page("Auditoría", r"""
    <div class="page-head">
        <div>
            <h1>Auditoría</h1>
            <p>Registro de creación, edición, configuración y accesos relevantes del sistema.</p>
        </div>
    </div>

    <div class="card">
        <form method="get">
            <div class="form-row-3">
                <div>
                    <label>Módulo</label>
                    <select name="modulo">
                        <option value="">Todos</option>
                        {% for m in modulos %}
                        <option value="{{ m.modulo }}" {% if request.args.get('modulo')==m.modulo %}selected{% endif %}>{{ m.modulo }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div><label>Usuario</label><input name="usuario" value="{{ request.args.get('usuario','') }}"></div>
                <div class="actions" style="margin-top:24px;">
                    <button class="btn btn-primary">Filtrar</button>
                    <a class="btn btn-secondary" href="{{ url_for('auditoria') }}">Limpiar</a>
                    <a class="btn btn-secondary" href="{{ url_for('export_auditoria') }}">Exportar Excel</a>
                </div>
            </div>
        </form>
    </div>

    <div class="card">
        <h2>Eventos: {{ rows|length }}</h2>
        <div class="table-wrap">
            <table>
                <tr><th>ID</th><th>Fecha</th><th>Módulo</th><th>Acción</th><th>Registro</th><th>Campo</th><th>Anterior</th><th>Nuevo</th><th>Usuario</th></tr>
                {% for a in rows %}
                <tr>
                    <td>{{ a.id }}</td>
                    <td>{{ a.created_at }}</td>
                    <td>{{ a.modulo }}</td>
                    <td>{{ a.accion }}</td>
                    <td>{{ a.registro_id }}</td>
                    <td>{{ a.campo }}</td>
                    <td>{{ a.valor_anterior }}</td>
                    <td>{{ a.valor_nuevo }}</td>
                    <td>{{ a.usuario_nombre }}</td>
                </tr>
                {% else %}
                <tr><td colspan="9" class="muted">Sin eventos.</td></tr>
                {% endfor %}
            </table>
        </div>
    </div>
    """, rows=rows, modulos=modulos, request=request)


# ============================================================
# INTEGRACIONES / FACTURACIÓN.CL
# ============================================================

@app.route("/facturacion")
@login_required
@permission_required("integraciones")
def facturacion():
    return render_page("Facturación.cl", r"""
    <div class="page-head">
        <div>
            <h1>Integración Facturación.cl</h1>
            <p>Módulo preparado para conectar documentos electrónicos y evitar doble digitación.</p>
        </div>
    </div>

    <div class="card">
        <h2>Estado del módulo</h2>
        <div class="placeholder">
            Esta versión deja preparada la sección de integración, pero aún no consulta la API real.
            El siguiente paso técnico es conectar credenciales, WSDL/API y mapear documento → despacho.
        </div>
    </div>

    <div class="grid grid-2">
        <div class="card">
            <h3>Datos que debería traer</h3>
            <ul>
                <li>Factura, boleta o guía.</li>
                <li>PDF del documento.</li>
                <li>Monto total.</li>
                <li>Cliente y RUT.</li>
                <li>Fecha de emisión.</li>
                <li>Estado del documento.</li>
            </ul>
        </div>
        <div class="card">
            <h3>Uso operacional</h3>
            <ul>
                <li>Buscar documento por número.</li>
                <li>Precargar cliente y monto en despacho.</li>
                <li>Evitar documentos duplicados.</li>
                <li>Adjuntar link o PDF al registro.</li>
            </ul>
        </div>
    </div>
    """)


# ============================================================
# EXPORTACIONES EXCEL
# ============================================================

def excel_response(filename, sheet_title, columns, rows):
    wb = Workbook()
    ws = wb.active
    ws.title = sheet_title[:31]
    ws.append([label for key, label in columns])
    for row in rows:
        ws.append([row[key] if key in row.keys() else "" for key, label in columns])

    for col in ws.columns:
        max_len = 12
        col_letter = col[0].column_letter
        for cell in col:
            try:
                max_len = max(max_len, len(str(cell.value or "")) + 2)
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_len, 42)

    output = BytesIO()
    wb.save(output)
    output.seek(0)
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


@app.route("/exportar")
@login_required
@permission_required("exportar")
def exportar():
    return render_page("Exportar Excel", r"""
    <div class="page-head">
        <div>
            <h1>Exportar Excel / Reportes</h1>
            <p>Descarga datos operacionales y administrativos para respaldo o análisis.</p>
        </div>
    </div>

    <div class="grid grid-3">
        <a class="card" href="{{ url_for('export_despachos') }}"><h3>Despachos</h3><p class="muted">Registros de documentos, estados, clientes, usuarios y montos.</p></a>
        <a class="card" href="{{ url_for('export_mantenciones') }}"><h3>Mantenciones</h3><p class="muted">Historial de mantención, maquinaria, costo y responsable.</p></a>
        <a class="card" href="{{ url_for('export_auditoria') }}"><h3>Auditoría</h3><p class="muted">Eventos de edición y control por usuario.</p></a>
        <a class="card" href="{{ url_for('export_maquinarias') }}"><h3>Maquinarias</h3><p class="muted">Listado de maquinaria y estados.</p></a>
        <a class="card" href="{{ url_for('export_vehiculos') }}"><h3>Vehículos</h3><p class="muted">Patentes, documentos y vencimientos.</p></a>
        <a class="card danger-zone" href="{{ url_for('backup_db') }}"><h3>Backup base de datos</h3><p class="muted">Descarga un respaldo completo de usuarios, despachos, maquinarias, mantenciones, auditoría y configuración.</p></a>
    </div>
    """)




@app.route("/backup-db")
@login_required
@permission_required("administracion")
def backup_db():
    backup_path = backup_database_if_exists("manual")
    if not backup_path:
        flash("No se pudo crear respaldo. Verifica que exista base de datos y Disk persistente.", "error")
        return redirect(url_for("exportar"))

    write_audit("backup", "descargar", None, "database", DB_PATH, backup_path)
    return send_file(
        backup_path,
        as_attachment=True,
        download_name=f"backup_ferreteria_cloud_tool_{today_str()}.db",
        mimetype="application/octet-stream"
    )


@app.route("/export/despachos")
@login_required
@permission_required("consulta")
def export_despachos():
    q = request.args.get("q", "").strip()
    estado = request.args.get("estado", "").strip()
    desde = request.args.get("desde", "").strip()
    hasta = request.args.get("hasta", "").strip()

    wheres = []
    params = []
    if q:
        wheres.append("(numero_documento LIKE ? OR tipo_documento LIKE ? OR usuario_nombre LIKE ?)")
        params += [f"%{q}%", f"%{q}%", f"%{q}%"]
    if estado:
        wheres.append("estado = ?")
        params.append(estado)
    if desde:
        wheres.append("date(created_at) >= date(?)")
        params.append(desde)
    if hasta:
        wheres.append("date(created_at) <= date(?)")
        params.append(hasta)

    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""
    rows = query_all(f"SELECT * FROM despachos {where_sql} ORDER BY id DESC", params)
    columns = [
        ("id", "ID"),
        ("numero_documento", "Número documento"),
        ("tipo_documento", "Tipo documento"),
        ("estado", "Estado"),
        ("monto", "Monto"),
        ("usuario_nombre", "Usuario"),
        ("created_at", "Creado"),
        ("updated_at", "Actualizado"),
    ]
    return excel_response(f"despachos_rapidos_{today_str()}.xlsx", "Despachos", columns, rows)

@app.route("/export/mantenciones")
@login_required
@permission_required("mantenciones")
def export_mantenciones():
    rows = query_all("SELECT * FROM mantenciones ORDER BY fecha DESC, id DESC")
    columns = [
        ("id", "ID"), ("maquinaria_nombre", "Maquinaria"), ("fecha", "Fecha"), ("tipo_mantencion", "Tipo"),
        ("estado", "Estado"), ("responsable", "Responsable"), ("costo", "Costo"),
        ("observaciones", "Observaciones"), ("usuario_nombre", "Usuario"), ("created_at", "Creado")
    ]
    return excel_response(f"mantenciones_{today_str()}.xlsx", "Mantenciones", columns, rows)


@app.route("/export/auditoria")
@login_required
@permission_required("auditoria")
def export_auditoria():
    rows = query_all("SELECT * FROM audit_log ORDER BY id DESC")
    columns = [
        ("id", "ID"), ("created_at", "Fecha"), ("modulo", "Módulo"), ("accion", "Acción"),
        ("registro_id", "Registro ID"), ("campo", "Campo"), ("valor_anterior", "Valor anterior"),
        ("valor_nuevo", "Valor nuevo"), ("usuario_nombre", "Usuario")
    ]
    return excel_response(f"auditoria_{today_str()}.xlsx", "Auditoria", columns, rows)


@app.route("/export/maquinarias")
@login_required
@permission_required("maquinarias")
def export_maquinarias():
    rows = query_all("SELECT * FROM maquinarias ORDER BY nombre")
    columns = [
        ("id", "ID"), ("codigo", "Código"), ("nombre", "Nombre"), ("tipo", "Tipo"), ("marca", "Marca"),
        ("modelo", "Modelo"), ("anio", "Año"), ("patente", "Patente"), ("estado", "Estado"),
        ("observaciones", "Observaciones"), ("created_at", "Creado")
    ]
    return excel_response(f"maquinarias_{today_str()}.xlsx", "Maquinarias", columns, rows)


@app.route("/export/vehiculos")
@login_required
@permission_required("vehiculos")
def export_vehiculos():
    rows = query_all("SELECT * FROM vehiculos ORDER BY patente")
    columns = [
        ("id", "ID"), ("patente", "Patente"), ("tipo", "Tipo"), ("marca", "Marca"), ("modelo", "Modelo"),
        ("anio", "Año"), ("estado", "Estado"), ("permiso_circulacion_vencimiento", "Permiso circulación"),
        ("revision_tecnica_vencimiento", "Revisión técnica"), ("seguro_obligatorio_vencimiento", "SOAP"),
        ("observaciones", "Observaciones"), ("created_at", "Creado")
    ]
    return excel_response(f"vehiculos_{today_str()}.xlsx", "Vehiculos", columns, rows)


# ============================================================
# ARRANQUE
# ============================================================

init_db()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
