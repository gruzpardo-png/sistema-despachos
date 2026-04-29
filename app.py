import os
import re
from io import BytesIO
from functools import wraps
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from flask import (
    Flask,
    abort,
    flash,
    g,
    redirect,
    render_template_string,
    request,
    send_file,
    session,
    url_for,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import desc, or_
from werkzeug.security import check_password_hash, generate_password_hash
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment


# =========================
# Configuración base
# =========================

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "cambiar-esta-clave-en-render")

database_url = os.environ.get("DATABASE_URL", "sqlite:///despachos.db")
if database_url.startswith("postgres://"):
    database_url = database_url.replace("postgres://", "postgresql://", 1)

app.config["SQLALCHEMY_DATABASE_URI"] = database_url
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

CHILE_TZ = ZoneInfo("America/Santiago")

TIPOS_DOCUMENTO = [
    "Factura",
    "Boleta",
    "Guía de despacho",
    "Nota de crédito",
    "Otro",
]

ESTADOS_INICIALES = [
    "Pendiente",
    "Entregado - Retirado",
]

ESTADOS_TODOS = [
    "Pendiente",
    "Entregado - Retirado",
    "Anulado",
]


def now_chile():
    return datetime.now(CHILE_TZ).replace(tzinfo=None)


def parse_monto(value):
    """
    Convierte montos chilenos como:
    100000
    100.000
    $100.000
    100.000,00
    a entero en pesos.
    """
    text = str(value or "").strip()
    text = re.sub(r"[^\d,.-]", "", text)

    if not text:
        return 0

    # En Chile normalmente "." es miles y "," es decimal.
    text = text.replace(".", "")
    if "," in text:
        text = text.split(",")[0]

    try:
        return int(text)
    except ValueError:
        return 0


def is_admin():
    return bool(g.get("user") and g.user.rol == "admin")


def can_access_despacho(despacho):
    return is_admin() or (g.user and despacho.usuario_id == g.user.id)


# =========================
# Modelos
# =========================

class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    nombre = db.Column(db.String(120), nullable=False, default="")
    password_hash = db.Column(db.String(255), nullable=False)
    rol = db.Column(db.String(30), nullable=False, default="usuario")  # admin / usuario
    activo = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=now_chile)

    despachos = db.relationship("Despacho", back_populates="usuario", lazy=True)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Despacho(db.Model):
    __tablename__ = "despachos"

    id = db.Column(db.Integer, primary_key=True)

    numero_documento = db.Column(db.String(80), nullable=False, index=True)
    tipo_documento = db.Column(db.String(80), nullable=False)
    estado = db.Column(db.String(80), nullable=False, default="Pendiente", index=True)

    cliente = db.Column(db.String(180), nullable=False)
    telefono = db.Column(db.String(80), nullable=True)
    destino = db.Column(db.String(180), nullable=True)

    patente = db.Column(db.String(30), nullable=True)
    conductor = db.Column(db.String(120), nullable=True)
    pioneta = db.Column(db.String(120), nullable=True)

    monto = db.Column(db.Integer, nullable=False, default=0)

    usuario_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    usuario = db.relationship("User", back_populates="despachos")

    created_at = db.Column(db.DateTime, nullable=False, default=now_chile, index=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=now_chile, onupdate=now_chile)

    def to_row(self):
        return [
            self.id,
            self.created_at.strftime("%Y-%m-%d %H:%M"),
            self.numero_documento,
            self.tipo_documento,
            self.estado,
            self.cliente,
            self.telefono or "",
            self.destino or "",
            self.patente or "",
            self.conductor or "",
            self.pioneta or "",
            self.monto,
            self.usuario.nombre or self.usuario.username,
        ]


class AuditLog(db.Model):
    __tablename__ = "audit_logs"

    id = db.Column(db.Integer, primary_key=True)
    usuario_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True, index=True)
    accion = db.Column(db.String(120), nullable=False)
    detalle = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=now_chile, index=True)

    usuario = db.relationship("User", lazy=True)


# =========================
# Filtros y helpers Flask
# =========================

@app.template_filter("money")
def money(value):
    try:
        return "$" + f"{int(value or 0):,}".replace(",", ".")
    except Exception:
        return "$0"


@app.template_filter("dt")
def format_datetime(value):
    if not value:
        return ""
    return value.strftime("%d-%m-%Y %H:%M")


@app.before_request
def load_logged_user():
    g.user = None
    user_id = session.get("user_id")
    if user_id:
        g.user = db.session.get(User, user_id)
        if not g.user or not g.user.activo:
            session.clear()


def log_action(accion, detalle):
    db.session.add(
        AuditLog(
            usuario_id=session.get("user_id"),
            accion=accion,
            detalle=detalle,
        )
    )


def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.user:
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not g.user:
            return redirect(url_for("login"))
        if g.user.rol != "admin":
            abort(403)
        return fn(*args, **kwargs)
    return wrapper


# =========================
# Templates inline
# =========================

LAYOUT_START = """
<!doctype html>
<html lang="es">
<head>
    <meta charset="utf-8">
    <title>{{ title or "Sistema de Despachos" }}</title>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        :root {
            --bg: #f3f4f6;
            --card: #ffffff;
            --text: #111827;
            --muted: #6b7280;
            --border: #e5e7eb;
            --primary: #111827;
            --primary-soft: #f9fafb;
            --danger: #b91c1c;
            --success: #047857;
            --warning: #92400e;
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            font-family: Arial, Helvetica, sans-serif;
            background: var(--bg);
            color: var(--text);
            font-size: 14px;
        }
        a { color: inherit; text-decoration: none; }
        .topbar {
            background: #111827;
            color: white;
            padding: 12px 18px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            flex-wrap: wrap;
        }
        .brand { font-weight: 700; letter-spacing: .2px; }
        .nav {
            display: flex;
            gap: 8px;
            align-items: center;
            flex-wrap: wrap;
        }
        .nav a, .nav span {
            color: white;
            padding: 7px 10px;
            border-radius: 8px;
            background: rgba(255,255,255,.08);
            font-size: 13px;
        }
        .container {
            width: min(1180px, calc(100% - 24px));
            margin: 18px auto;
        }
        .grid {
            display: grid;
            grid-template-columns: 1.1fr .9fr;
            gap: 16px;
        }
        .card {
            background: var(--card);
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 16px;
            box-shadow: 0 6px 18px rgba(0,0,0,.04);
        }
        .card h1, .card h2, .card h3 {
            margin: 0 0 12px 0;
        }
        .muted { color: var(--muted); }
        .row {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
        }
        .row-3 {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
        }
        label {
            display: block;
            font-weight: 700;
            margin-bottom: 5px;
            font-size: 13px;
        }
        input, select {
            width: 100%;
            padding: 10px;
            border: 1px solid var(--border);
            border-radius: 10px;
            background: white;
            font-size: 14px;
        }
        button, .btn {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            border: 0;
            border-radius: 10px;
            padding: 10px 14px;
            background: var(--primary);
            color: white;
            cursor: pointer;
            font-weight: 700;
            font-size: 14px;
        }
        .btn-light {
            background: var(--primary-soft);
            color: var(--text);
            border: 1px solid var(--border);
        }
        .btn-danger { background: var(--danger); }
        .actions {
            display: flex;
            gap: 8px;
            flex-wrap: wrap;
            align-items: center;
            margin-top: 12px;
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-bottom: 16px;
        }
        .stat {
            background: white;
            border: 1px solid var(--border);
            border-radius: 14px;
            padding: 14px;
        }
        .stat .num {
            font-size: 24px;
            font-weight: 800;
            margin-top: 6px;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 12px;
            overflow: hidden;
        }
        th, td {
            padding: 9px 8px;
            border-bottom: 1px solid var(--border);
            text-align: left;
            vertical-align: middle;
            font-size: 13px;
        }
        th {
            background: #f9fafb;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: .03em;
            color: #374151;
        }
        .badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 800;
            white-space: nowrap;
        }
        .badge-pendiente { background: #fef3c7; color: var(--warning); }
        .badge-entregado { background: #d1fae5; color: var(--success); }
        .badge-anulado { background: #fee2e2; color: var(--danger); }
        .flash {
            padding: 10px 12px;
            border-radius: 10px;
            margin-bottom: 10px;
            border: 1px solid var(--border);
            background: white;
        }
        .flash.success { border-color: #bbf7d0; background: #f0fdf4; }
        .flash.error { border-color: #fecaca; background: #fef2f2; }
        .table-wrap {
            overflow-x: auto;
        }
        .small-form {
            display: flex;
            gap: 6px;
            align-items: center;
        }
        .small-form select {
            min-width: 145px;
            padding: 7px;
            font-size: 12px;
        }
        .small-form button {
            padding: 7px 9px;
            font-size: 12px;
        }
        @media (max-width: 900px) {
            .grid, .row, .row-3, .stats {
                grid-template-columns: 1fr;
            }
            .container { width: calc(100% - 14px); }
        }
    </style>
</head>
<body>
    <div class="topbar">
        <div class="brand">Sistema de Despachos</div>
        <div class="nav">
            {% if g.user %}
                <span>{{ g.user.nombre or g.user.username }} · {{ g.user.rol }}</span>
                <a href="{{ url_for('index') }}">Nuevo registro</a>
                <a href="{{ url_for('despachos') }}">Registros</a>
                <a href="{{ url_for('exportar') }}">Exportar Excel</a>
                {% if g.user.rol == "admin" %}
                    <a href="{{ url_for('usuarios') }}">Usuarios</a>
                    <a href="{{ url_for('auditoria') }}">Auditoría</a>
                {% endif %}
                <a href="{{ url_for('logout') }}">Salir</a>
            {% endif %}
        </div>
    </div>
    <div class="container">
        {% with messages = get_flashed_messages(with_categories=true) %}
            {% if messages %}
                {% for category, message in messages %}
                    <div class="flash {{ category }}">{{ message }}</div>
                {% endfor %}
            {% endif %}
        {% endwith %}
"""

LAYOUT_END = """
    </div>
</body>
</html>
"""


LOGIN_TEMPLATE = LAYOUT_START + """
<div class="card" style="max-width:430px;margin:50px auto;">
    <h1>Ingresar</h1>
    <p class="muted">Acceso al sistema de despachos.</p>

    <form method="post">
        <div style="margin-bottom:10px;">
            <label>Usuario</label>
            <input name="username" autocomplete="username" required autofocus>
        </div>

        <div style="margin-bottom:10px;">
            <label>Contraseña</label>
            <input name="password" type="password" autocomplete="current-password" required>
        </div>

        <button type="submit" style="width:100%;">Entrar</button>
    </form>

    <p class="muted" style="margin-top:14px;font-size:12px;">
        Primer ingreso por defecto: usuario <b>admin</b>, clave <b>admin123</b>.
        Cambia la clave usando variables de entorno en Render.
    </p>
</div>
""" + LAYOUT_END


INDEX_TEMPLATE = LAYOUT_START + """
<div class="stats">
    <div class="stat">
        <div class="muted">Documentos hoy</div>
        <div class="num">{{ resumen.documentos_hoy }}</div>
    </div>
    <div class="stat">
        <div class="muted">Monto hoy</div>
        <div class="num">{{ resumen.monto_hoy | money }}</div>
    </div>
    <div class="stat">
        <div class="muted">Pendientes</div>
        <div class="num">{{ resumen.pendientes }}</div>
    </div>
</div>

<div class="grid">
    <div class="card">
        <h1>Nuevo registro</h1>
        <p class="muted">Formulario simple para registrar documentos entregados, retirados o pendientes.</p>

        <form method="post">
            <div class="row">
                <div>
                    <label>Número documento *</label>
                    <input name="numero_documento" required>
                </div>
                <div>
                    <label>Tipo documento *</label>
                    <select name="tipo_documento" required>
                        {% for tipo in tipos_documento %}
                            <option value="{{ tipo }}">{{ tipo }}</option>
                        {% endfor %}
                    </select>
                </div>
            </div>

            <div class="row" style="margin-top:10px;">
                <div>
                    <label>Estado inicial *</label>
                    <select name="estado" required>
                        {% for estado in estados_iniciales %}
                            <option value="{{ estado }}">{{ estado }}</option>
                        {% endfor %}
                    </select>
                </div>
                <div>
                    <label>Monto documento *</label>
                    <input name="monto" inputmode="numeric" placeholder="Ej: 125.000" required>
                </div>
            </div>

            <div class="row" style="margin-top:10px;">
                <div>
                    <label>Cliente *</label>
                    <input name="cliente" required>
                </div>
                <div>
                    <label>Teléfono</label>
                    <input name="telefono">
                </div>
            </div>

            <div class="row" style="margin-top:10px;">
                <div>
                    <label>Destino</label>
                    <input name="destino" placeholder="Opcional">
                </div>
                <div>
                    <label>Patente</label>
                    <input name="patente" placeholder="Opcional">
                </div>
            </div>

            <div class="row" style="margin-top:10px;">
                <div>
                    <label>Conductor</label>
                    <input name="conductor" placeholder="Opcional, solo despacho interno">
                </div>
                <div>
                    <label>Pioneta</label>
                    <input name="pioneta" placeholder="Opcional, solo despacho interno">
                </div>
            </div>

            <div style="margin-top:10px;">
                <label>Usuario registrado automáticamente</label>
                <input value="{{ g.user.nombre or g.user.username }}" disabled>
            </div>

            <div class="actions">
                <button type="submit">Guardar registro</button>
                <a class="btn btn-light" href="{{ url_for('despachos') }}">Ver registros</a>
            </div>
        </form>
    </div>

    <div class="card">
        <h2>Registros recientes</h2>
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>Fecha</th>
                        <th>Doc.</th>
                        <th>Cliente</th>
                        <th>Estado</th>
                        <th>Monto</th>
                    </tr>
                </thead>
                <tbody>
                    {% for d in recientes %}
                    <tr>
                        <td>{{ d.created_at | dt }}</td>
                        <td>{{ d.numero_documento }}</td>
                        <td>{{ d.cliente }}</td>
                        <td>
                            {% if d.estado == "Pendiente" %}
                                <span class="badge badge-pendiente">Pendiente</span>
                            {% elif d.estado == "Anulado" %}
                                <span class="badge badge-anulado">Anulado</span>
                            {% else %}
                                <span class="badge badge-entregado">Entregado</span>
                            {% endif %}
                        </td>
                        <td>{{ d.monto | money }}</td>
                    </tr>
                    {% else %}
                    <tr>
                        <td colspan="5" class="muted">Sin registros todavía.</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>

        {% if g.user.rol == "admin" %}
        <h3 style="margin-top:18px;">Auditoría reciente</h3>
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>Fecha</th>
                        <th>Usuario</th>
                        <th>Acción</th>
                    </tr>
                </thead>
                <tbody>
                    {% for a in auditoria_reciente %}
                    <tr>
                        <td>{{ a.created_at | dt }}</td>
                        <td>{{ a.usuario.nombre if a.usuario else "Sistema" }}</td>
                        <td>{{ a.accion }}</td>
                    </tr>
                    {% else %}
                    <tr>
                        <td colspan="3" class="muted">Sin auditoría.</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% endif %}
    </div>
</div>
""" + LAYOUT_END


DESPACHOS_TEMPLATE = LAYOUT_START + """
<div class="card">
    <h1>Registros de despacho</h1>

    <form method="get" class="row-3" style="margin-bottom:12px;">
        <div>
            <label>Buscar</label>
            <input name="q" value="{{ request.args.get('q','') }}" placeholder="Documento, cliente, destino...">
        </div>
        <div>
            <label>Estado</label>
            <select name="estado">
                <option value="">Todos</option>
                {% for estado in estados_todos %}
                    <option value="{{ estado }}" {% if request.args.get('estado') == estado %}selected{% endif %}>{{ estado }}</option>
                {% endfor %}
            </select>
        </div>
        {% if g.user.rol == "admin" %}
        <div>
            <label>Usuario</label>
            <select name="usuario_id">
                <option value="">Todos</option>
                {% for u in usuarios %}
                    <option value="{{ u.id }}" {% if request.args.get('usuario_id') == u.id|string %}selected{% endif %}>
                        {{ u.nombre or u.username }}
                    </option>
                {% endfor %}
            </select>
        </div>
        {% endif %}
        <div class="actions">
            <button type="submit">Filtrar</button>
            <a class="btn btn-light" href="{{ url_for('despachos') }}">Limpiar</a>
            <a class="btn btn-light" href="{{ url_for('exportar', q=request.args.get('q',''), estado=request.args.get('estado',''), usuario_id=request.args.get('usuario_id','')) }}">Exportar filtrado</a>
        </div>
    </form>

    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>Fecha</th>
                    <th>N° Doc.</th>
                    <th>Tipo</th>
                    <th>Cliente</th>
                    <th>Teléfono</th>
                    <th>Destino</th>
                    <th>Patente</th>
                    <th>Conductor</th>
                    <th>Pioneta</th>
                    <th>Monto</th>
                    <th>Usuario</th>
                    <th>Estado</th>
                </tr>
            </thead>
            <tbody>
                {% for d in registros %}
                <tr>
                    <td>{{ d.created_at | dt }}</td>
                    <td><b>{{ d.numero_documento }}</b></td>
                    <td>{{ d.tipo_documento }}</td>
                    <td>{{ d.cliente }}</td>
                    <td>{{ d.telefono or "" }}</td>
                    <td>{{ d.destino or "" }}</td>
                    <td>{{ d.patente or "" }}</td>
                    <td>{{ d.conductor or "" }}</td>
                    <td>{{ d.pioneta or "" }}</td>
                    <td>{{ d.monto | money }}</td>
                    <td>{{ d.usuario.nombre or d.usuario.username }}</td>
                    <td>
                        <form class="small-form" method="post" action="{{ url_for('cambiar_estado', despacho_id=d.id) }}">
                            <select name="estado">
                                {% for estado in estados_todos %}
                                    <option value="{{ estado }}" {% if d.estado == estado %}selected{% endif %}>{{ estado }}</option>
                                {% endfor %}
                            </select>
                            <button type="submit">OK</button>
                        </form>
                    </td>
                </tr>
                {% else %}
                <tr>
                    <td colspan="12" class="muted">No hay registros para los filtros seleccionados.</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>
""" + LAYOUT_END


USUARIOS_TEMPLATE = LAYOUT_START + """
<div class="grid">
    <div class="card">
        <h1>Crear usuario</h1>
        <form method="post">
            <div style="margin-bottom:10px;">
                <label>Nombre visible</label>
                <input name="nombre" placeholder="Ej: Vendedor 1">
            </div>
            <div style="margin-bottom:10px;">
                <label>Usuario *</label>
                <input name="username" required>
            </div>
            <div style="margin-bottom:10px;">
                <label>Contraseña *</label>
                <input name="password" type="password" required>
            </div>
            <div style="margin-bottom:10px;">
                <label>Rol</label>
                <select name="rol">
                    <option value="usuario">Usuario</option>
                    <option value="admin">Administrador</option>
                </select>
            </div>
            <button type="submit">Crear usuario</button>
        </form>
    </div>

    <div class="card">
        <h2>Usuarios existentes</h2>
        <div class="table-wrap">
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Usuario</th>
                        <th>Nombre</th>
                        <th>Rol</th>
                        <th>Activo</th>
                    </tr>
                </thead>
                <tbody>
                    {% for u in usuarios %}
                    <tr>
                        <td>{{ u.id }}</td>
                        <td>{{ u.username }}</td>
                        <td>{{ u.nombre }}</td>
                        <td>{{ u.rol }}</td>
                        <td>{{ "Sí" if u.activo else "No" }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
</div>
""" + LAYOUT_END


AUDITORIA_TEMPLATE = LAYOUT_START + """
<div class="card">
    <h1>Auditoría</h1>
    <div class="table-wrap">
        <table>
            <thead>
                <tr>
                    <th>Fecha</th>
                    <th>Usuario</th>
                    <th>Acción</th>
                    <th>Detalle</th>
                </tr>
            </thead>
            <tbody>
                {% for a in logs %}
                <tr>
                    <td>{{ a.created_at | dt }}</td>
                    <td>{{ a.usuario.nombre if a.usuario else "Sistema" }}</td>
                    <td>{{ a.accion }}</td>
                    <td>{{ a.detalle }}</td>
                </tr>
                {% else %}
                <tr>
                    <td colspan="4" class="muted">Sin registros de auditoría.</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>
    </div>
</div>
""" + LAYOUT_END


# =========================
# Consultas
# =========================

def base_despachos_query():
    query = Despacho.query

    if not is_admin():
        query = query.filter(Despacho.usuario_id == g.user.id)

    q = request.args.get("q", "").strip()
    estado = request.args.get("estado", "").strip()
    usuario_id = request.args.get("usuario_id", "").strip()

    if q:
        like = f"%{q}%"
        query = query.filter(
            or_(
                Despacho.numero_documento.ilike(like),
                Despacho.cliente.ilike(like),
                Despacho.telefono.ilike(like),
                Despacho.destino.ilike(like),
                Despacho.patente.ilike(like),
                Despacho.conductor.ilike(like),
                Despacho.pioneta.ilike(like),
            )
        )

    if estado in ESTADOS_TODOS:
        query = query.filter(Despacho.estado == estado)

    if is_admin() and usuario_id.isdigit():
        query = query.filter(Despacho.usuario_id == int(usuario_id))

    return query


def calcular_resumen():
    inicio = now_chile().replace(hour=0, minute=0, second=0, microsecond=0)
    fin = inicio + timedelta(days=1)

    query = Despacho.query
    if not is_admin():
        query = query.filter(Despacho.usuario_id == g.user.id)

    hoy = query.filter(Despacho.created_at >= inicio, Despacho.created_at < fin)
    documentos_hoy = hoy.count()
    monto_hoy = hoy.with_entities(db.func.coalesce(db.func.sum(Despacho.monto), 0)).scalar() or 0
    pendientes = query.filter(Despacho.estado == "Pendiente").count()

    return {
        "documentos_hoy": documentos_hoy,
        "monto_hoy": monto_hoy,
        "pendientes": pendientes,
    }


# =========================
# Rutas
# =========================

@app.route("/login", methods=["GET", "POST"])
def login():
    if g.user:
        return redirect(url_for("index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")

        user = User.query.filter_by(username=username, activo=True).first()

        if user and user.check_password(password):
            session.clear()
            session["user_id"] = user.id
            log_action("LOGIN", f"Ingreso usuario {user.username}")
            db.session.commit()
            return redirect(url_for("index"))

        flash("Usuario o contraseña incorrectos.", "error")

    return render_template_string(LOGIN_TEMPLATE, title="Ingresar")


@app.route("/logout")
@login_required
def logout():
    log_action("LOGOUT", f"Salida usuario {g.user.username}")
    db.session.commit()
    session.clear()
    return redirect(url_for("login"))


@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    if request.method == "POST":
        numero_documento = request.form.get("numero_documento", "").strip()
        tipo_documento = request.form.get("tipo_documento", "").strip()
        estado = request.form.get("estado", "").strip()
        cliente = request.form.get("cliente", "").strip()

        if not numero_documento or not cliente:
            flash("Número de documento y cliente son obligatorios.", "error")
            return redirect(url_for("index"))

        if tipo_documento not in TIPOS_DOCUMENTO:
            flash("Tipo de documento no válido.", "error")
            return redirect(url_for("index"))

        if estado not in ESTADOS_INICIALES:
            flash("Estado inicial no válido.", "error")
            return redirect(url_for("index"))

        despacho = Despacho(
            numero_documento=numero_documento,
            tipo_documento=tipo_documento,
            estado=estado,
            cliente=cliente,
            telefono=request.form.get("telefono", "").strip() or None,
            destino=request.form.get("destino", "").strip() or None,
            patente=request.form.get("patente", "").strip().upper() or None,
            conductor=request.form.get("conductor", "").strip() or None,
            pioneta=request.form.get("pioneta", "").strip() or None,
            monto=parse_monto(request.form.get("monto", "")),
            usuario_id=g.user.id,
        )

        db.session.add(despacho)
        db.session.flush()

        log_action(
            "CREAR_DESPACHO",
            f"Registro #{despacho.id} doc {despacho.numero_documento} cliente {despacho.cliente} monto {despacho.monto}",
        )
        db.session.commit()

        flash("Registro guardado correctamente.", "success")
        return redirect(url_for("index"))

    query_recientes = Despacho.query
    if not is_admin():
        query_recientes = query_recientes.filter(Despacho.usuario_id == g.user.id)

    recientes = query_recientes.order_by(desc(Despacho.created_at)).limit(10).all()
    auditoria_reciente = []

    if is_admin():
        auditoria_reciente = AuditLog.query.order_by(desc(AuditLog.created_at)).limit(8).all()

    return render_template_string(
        INDEX_TEMPLATE,
        title="Nuevo registro",
        tipos_documento=TIPOS_DOCUMENTO,
        estados_iniciales=ESTADOS_INICIALES,
        recientes=recientes,
        auditoria_reciente=auditoria_reciente,
        resumen=calcular_resumen(),
    )


@app.route("/despachos")
@login_required
def despachos():
    registros = base_despachos_query().order_by(desc(Despacho.created_at)).limit(500).all()
    usuarios = User.query.order_by(User.nombre, User.username).all() if is_admin() else []

    return render_template_string(
        DESPACHOS_TEMPLATE,
        title="Registros",
        registros=registros,
        usuarios=usuarios,
        estados_todos=ESTADOS_TODOS,
    )


@app.route("/despachos/<int:despacho_id>/estado", methods=["POST"])
@login_required
def cambiar_estado(despacho_id):
    despacho = db.session.get(Despacho, despacho_id)
    if not despacho:
        abort(404)

    if not can_access_despacho(despacho):
        abort(403)

    nuevo_estado = request.form.get("estado", "").strip()
    if nuevo_estado not in ESTADOS_TODOS:
        flash("Estado no válido.", "error")
        return redirect(url_for("despachos"))

    estado_anterior = despacho.estado
    despacho.estado = nuevo_estado
    despacho.updated_at = now_chile()

    log_action(
        "CAMBIAR_ESTADO",
        f"Registro #{despacho.id} doc {despacho.numero_documento}: {estado_anterior} -> {nuevo_estado}",
    )
    db.session.commit()

    flash("Estado actualizado.", "success")
    return redirect(request.referrer or url_for("despachos"))


@app.route("/exportar")
@login_required
def exportar():
    registros = base_despachos_query().order_by(desc(Despacho.created_at)).all()

    wb = Workbook()
    ws = wb.active
    ws.title = "Despachos"

    headers = [
        "ID",
        "Fecha",
        "Número documento",
        "Tipo documento",
        "Estado",
        "Cliente",
        "Teléfono",
        "Destino",
        "Patente",
        "Conductor",
        "Pioneta",
        "Monto",
        "Usuario",
    ]

    ws.append(headers)

    header_fill = PatternFill("solid", fgColor="111827")
    header_font = Font(color="FFFFFF", bold=True)

    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")

    for d in registros:
        ws.append(d.to_row())

    for col in ws.columns:
        max_len = 0
        column_letter = col[0].column_letter
        for cell in col:
            value = str(cell.value or "")
            max_len = max(max_len, len(value))
        ws.column_dimensions[column_letter].width = min(max_len + 2, 35)

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"despachos_{now_chile().strftime('%Y%m%d_%H%M')}.xlsx"

    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/usuarios", methods=["GET", "POST"])
@admin_required
def usuarios():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        nombre = request.form.get("nombre", "").strip()
        password = request.form.get("password", "")
        rol = request.form.get("rol", "usuario").strip()

        if rol not in ["admin", "usuario"]:
            rol = "usuario"

        if not username or not password:
            flash("Usuario y contraseña son obligatorios.", "error")
            return redirect(url_for("usuarios"))

        if User.query.filter_by(username=username).first():
            flash("Ese usuario ya existe.", "error")
            return redirect(url_for("usuarios"))

        user = User(
            username=username,
            nombre=nombre or username,
            rol=rol,
            activo=True,
        )
        user.set_password(password)

        db.session.add(user)
        log_action("CREAR_USUARIO", f"Usuario creado: {username} rol {rol}")
        db.session.commit()

        flash("Usuario creado correctamente.", "success")
        return redirect(url_for("usuarios"))

    users = User.query.order_by(User.username).all()
    return render_template_string(
        USUARIOS_TEMPLATE,
        title="Usuarios",
        usuarios=users,
    )


@app.route("/auditoria")
@admin_required
def auditoria():
    logs = AuditLog.query.order_by(desc(AuditLog.created_at)).limit(300).all()
    return render_template_string(
        AUDITORIA_TEMPLATE,
        title="Auditoría",
        logs=logs,
    )


@app.route("/health")
def health():
    return {"status": "ok", "time": now_chile().isoformat()}


# =========================
# Inicialización segura
# IMPORTANTE:
# Aquí ya están definidos User, Despacho y AuditLog.
# Por eso no se usa ningún modelo inexistente como Machine.
# =========================

def create_admin_if_needed():
    with app.app_context():
        db.create_all()

        admin_username = os.environ.get("DEFAULT_ADMIN_USERNAME", "admin").strip().lower()
        admin_password = os.environ.get("DEFAULT_ADMIN_PASSWORD", "admin123")
        admin_name = os.environ.get("DEFAULT_ADMIN_NAME", "Administrador")

        admin = User.query.filter_by(username=admin_username).first()

        if not admin:
            admin = User(
                username=admin_username,
                nombre=admin_name,
                rol="admin",
                activo=True,
            )
            admin.set_password(admin_password)

            db.session.add(admin)
            db.session.add(
                AuditLog(
                    usuario_id=None,
                    accion="INIT_ADMIN",
                    detalle=f"Usuario administrador inicial creado: {admin_username}",
                )
            )
            db.session.commit()
            print(f"Administrador inicial creado: {admin_username}")
        else:
            print(f"Administrador inicial ya existe: {admin_username}")


create_admin_if_needed()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
