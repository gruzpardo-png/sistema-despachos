import os
import io
import secrets
from datetime import datetime, date, time
from functools import wraps
from zoneinfo import ZoneInfo

from flask import Flask, request, redirect, url_for, flash, session, send_file, render_template_string
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from openpyxl import Workbook


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))

database_url = os.environ.get("DATABASE_URL")
if database_url:
    database_url = database_url.replace("postgres://", "postgresql://", 1)
    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///despachos.db"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


CHILE_TZ = ZoneInfo("America/Santiago")


def chile_now():
    """Hora local de Chile, guardada sin zona horaria para compatibilidad con SQLite/PostgreSQL."""
    return datetime.now(CHILE_TZ).replace(tzinfo=None)


def chile_today():
    """Fecha actual en Chile, no en UTC del servidor Render."""
    return chile_now().date()


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    name = db.Column(db.String(160), nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(30), nullable=False, default="OPERADOR")
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=chile_now)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)


class Dispatch(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    numero_documento = db.Column(db.String(80), unique=True, nullable=False, index=True)
    tipo_documento = db.Column(db.String(40), nullable=False)
    estado = db.Column(db.String(40), nullable=False, default="ENTREGADO_RETIRADO", index=True)
    cliente = db.Column(db.String(180), nullable=True)
    telefono = db.Column(db.String(80), nullable=True)
    destino = db.Column(db.String(180), nullable=True)
    placa_patente = db.Column(db.String(40), nullable=True)
    conductor = db.Column(db.String(120), nullable=True)
    pioneta = db.Column(db.String(120), nullable=True)
    monto = db.Column(db.Integer, nullable=True)
    observacion = db.Column(db.Text, nullable=True)
    motivo_anulacion = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=chile_now, index=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=chile_now, onupdate=chile_now)
    created_by_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_by = db.relationship("User", foreign_keys=[created_by_id])


class AuditLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    action = db.Column(db.String(80), nullable=False)
    detail = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=chile_now, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    user = db.relationship("User")


class Machine(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    nombre = db.Column(db.String(160), nullable=False, index=True)
    tipo = db.Column(db.String(60), nullable=False, default="CAMIÓN", index=True)
    patente_codigo = db.Column(db.String(80), nullable=True, index=True)
    sucursal = db.Column(db.String(120), nullable=True)
    marca_modelo = db.Column(db.String(160), nullable=True)
    estado = db.Column(db.String(40), nullable=False, default="OPERATIVA", index=True)
    observacion = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=chile_now)


class Maintenance(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    machine_id = db.Column(db.Integer, db.ForeignKey("machine.id"), nullable=False, index=True)
    machine = db.relationship("Machine")

    fecha = db.Column(db.Date, nullable=False, default=chile_today, index=True)
    tipo_mantencion = db.Column(db.String(80), nullable=False, default="PREVENTIVA", index=True)
    estado = db.Column(db.String(40), nullable=False, default="REALIZADA", index=True)

    kilometraje_horometro = db.Column(db.String(80), nullable=True)
    proveedor_taller = db.Column(db.String(160), nullable=True)
    responsable = db.Column(db.String(160), nullable=True)
    costo = db.Column(db.Integer, nullable=True)

    detalle = db.Column(db.Text, nullable=False)
    proxima_fecha = db.Column(db.Date, nullable=True)
    proximo_km_horas = db.Column(db.String(80), nullable=True)

    created_at = db.Column(db.DateTime, nullable=False, default=chile_now, index=True)
    created_by_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_by = db.relationship("User", foreign_keys=[created_by_id])


def create_admin_if_needed():
    admin_username = os.environ.get("ADMIN_USERNAME", "admin")
    admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")
    admin_name = os.environ.get("ADMIN_NAME", "ADMINISTRADOR")
    existing = User.query.filter_by(username=admin_username).first()
    if not existing:
        admin = User(username=admin_username, name=admin_name, role="ADMIN", active=True)
        admin.set_password(admin_password)
        db.session.add(admin)
        db.session.commit()

    seed_machines = [
        ("JAC CZ JR 23", "CAMIÓN", "CZ JR 23", "Bodega / Despacho", "JAC"),
        ("KIA KJ HY 12", "CAMIÓN", "KJ HY 12", "Bodega / Despacho", "KIA"),
        ("KIA JB LL 75", "CAMIÓN", "JB LL 75", "Bodega / Despacho", "KIA"),
        ("LTRW80", "CAMIÓN", "LTRW80", "Bodega / Despacho", ""),
        ("LTRW79", "CAMIÓN", "LTRW79", "Bodega / Despacho", ""),
        ("Grúa horquilla Hangcha", "GRÚA HORQUILLA", "", "Bodega", "Hangcha"),
        ("Grúa horquilla Sucursal 1 - 1", "GRÚA HORQUILLA", "", "Sucursal 1", ""),
        ("Grúa horquilla Sucursal 1 - 2", "GRÚA HORQUILLA", "", "Sucursal 1", ""),
    ]

    for nombre, tipo, patente, sucursal, marca in seed_machines:
        if not Machine.query.filter_by(nombre=nombre).first():
            db.session.add(Machine(
                nombre=nombre,
                tipo=tipo,
                patente_codigo=patente,
                sucursal=sucursal,
                marca_modelo=marca,
                estado="OPERATIVA"
            ))
    db.session.commit()


with app.app_context():
    db.create_all()
    create_admin_if_needed()


def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return db.session.get(User, uid)


def login_required(view):
    @wraps(view)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return view(*args, **kwargs)
    return wrapper


def roles_required(*roles):
    def decorator(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user or user.role not in roles:
                flash("No tienes permisos para realizar esta acción.", "danger")
                return redirect(url_for("despachos"))
            return view(*args, **kwargs)
        return wrapper
    return decorator


def audit(action, detail=""):
    user = current_user()
    db.session.add(AuditLog(action=action, detail=detail, user_id=user.id if user else None))
    db.session.commit()


def parse_date(value, fallback):
    if not value:
        return fallback
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return fallback


def parse_int(value):
    if value is None:
        return None
    value = str(value).replace("$", "").replace(".", "").replace(",", "").strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def build_query():
    today = chile_today()
    start_date = parse_date(request.args.get("desde"), today)
    end_date = parse_date(request.args.get("hasta"), today)
    estado = request.args.get("estado", "").strip()
    usuario_id = request.args.get("usuario_id", "").strip()
    q = request.args.get("q", "").strip()

    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    query = Dispatch.query.filter(Dispatch.created_at.between(start_dt, end_dt))

    if estado:
        query = query.filter(Dispatch.estado == estado)

    if usuario_id:
        try:
            query = query.filter(Dispatch.created_by_id == int(usuario_id))
        except ValueError:
            usuario_id = ""

    if q:
        like = f"%{q}%"
        query = query.filter(
            db.or_(
                Dispatch.numero_documento.ilike(like),
                Dispatch.placa_patente.ilike(like),
                Dispatch.observacion.ilike(like),
            )
        )

    return query.order_by(Dispatch.created_at.desc()), start_date, end_date, estado, usuario_id, q


def summary_for(query):
    rows = query.all()
    return {
        "total": len(rows),
        "pendientes": sum(1 for r in rows if r.estado == "PENDIENTE"),
        "entregados": sum(1 for r in rows if r.estado == "ENTREGADO_RETIRADO"),
        "anulados": sum(1 for r in rows if r.estado == "ANULADO"),
        "monto_total": sum((r.monto or 0) for r in rows if r.estado != "ANULADO"),
    }


STYLE = """
<style>
*{box-sizing:border-box}
body{
  margin:0;
  font-family:Arial,sans-serif;
  background:#f4f7fb;
  color:#0f172a;
  font-size:14px;
}
.topbar{
  background:white;
  border-bottom:1px solid #e5e7eb;
  display:flex;
  justify-content:space-between;
  align-items:center;
  padding:10px 22px;
  position:sticky;
  top:0;
  z-index:5;
  min-height:58px;
}
.brand{display:flex;align-items:center;gap:10px}
.logo{height:34px;max-width:150px;object-fit:contain}
.brand-title{font-size:16px;font-weight:800}
.brand-sub{font-size:12px;color:#64748b}
.nav{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.nav a{text-decoration:none;font-weight:800;color:#0f172a;font-size:14px}
.nav .exit{color:#dc2626}
.pill{
  background:#ecfdf5;
  color:#047857;
  border:1px solid #86efac;
  border-radius:999px;
  padding:6px 10px;
  font-size:12px;
  font-weight:800;
}
.container{
  max-width:1540px;
  margin:auto;
  padding:18px 24px;
}
.card{
  background:white;
  border:1px solid #e2e8f0;
  border-radius:16px;
  padding:16px 18px;
  margin-bottom:14px;
  box-shadow:0 10px 24px rgba(15,23,42,.045);
}
h1{
  font-size:28px;
  margin:0 0 5px;
  line-height:1.1;
}
h2{
  font-size:21px;
  margin:0 0 14px;
  line-height:1.15;
}
.muted{color:#64748b;margin:4px 0 14px}
.grid{
  display:grid;
  grid-template-columns:repeat(4,minmax(0,1fr));
  gap:10px 14px;
}
.full{grid-column:1/-1}
label{
  font-weight:800;
  font-size:13px;
  display:block;
  margin-bottom:5px;
}
input,select,textarea{
  width:100%;
  border:1px solid #cbd5e1;
  border-radius:10px;
  padding:9px 11px;
  font:inherit;
  background:white;
  min-height:40px;
}
textarea{
  min-height:48px;
  resize:vertical;
}
.btn,button{
  border:0;
  border-radius:10px;
  padding:10px 15px;
  font-weight:800;
  cursor:pointer;
  background:#e2e8f0;
  text-decoration:none;
  color:#0f172a;
  min-height:38px;
}
.primary{background:#0f766e!important;color:white!important}
.danger-btn{background:#fee2e2;color:#991b1b}
.summary{
  display:grid;
  grid-template-columns:repeat(5,1fr);
  gap:12px;
}
.metric{
  padding:13px 15px;
}
.metric span{
  display:block;
  text-transform:uppercase;
  color:#64748b;
  font-size:11px;
  font-weight:800;
}
.metric strong{
  font-size:24px;
  line-height:1.05;
}
.filters{
  display:flex;
  gap:10px;
  align-items:end;
  flex-wrap:wrap;
}
.filters div{min-width:135px}
.filters .grow{flex:1}
.table-wrap{overflow:auto;max-height:650px}
table{border-collapse:collapse;width:100%;min-width:1300px}
th,td{
  border-bottom:1px solid #e5e7eb;
  padding:8px;
  text-align:left;
  font-size:12px;
  vertical-align:top;
}
th{background:#f8fafc;position:sticky;top:0}
.status{border-radius:999px;padding:4px 8px;font-size:10px;font-weight:900}
.ENTREGADO_RETIRADO{background:#dcfce7;color:#166534}
.PENDIENTE{background:#fef9c3;color:#854d0e}
.ANULADO,.FUERA{background:#fee2e2;color:#991b1b}
.REALIZADA,.OPERATIVA{background:#dcfce7;color:#166534}
.PROGRAMADA,.PENDIENTE{background:#fef9c3;color:#854d0e}
.OBSERVADA,.EN{background:#ffedd5;color:#9a3412}
.alert{padding:10px 12px;border-radius:10px;margin-bottom:8px;font-weight:700}
.alert-success{background:#dcfce7;color:#166534}
.alert-danger{background:#fee2e2;color:#991b1b}
.alert-warning{background:#fef9c3;color:#854d0e}
.login{min-height:calc(100vh - 90px);display:flex;justify-content:center;align-items:center}
.login-card{max-width:410px;width:100%;text-align:left}
.hint{background:#f8fafc;border-radius:10px;padding:10px;font-size:12px;margin-top:12px}
.rowline{display:flex;justify-content:space-between;border-bottom:1px solid #e5e7eb;padding:8px 0}
.compact-form-card{padding:14px 18px 16px}
.compact-form-card h2{margin-bottom:12px}
.dispatch-grid .obs-inline{grid-column:2 / -1}
.maintenance-grid .obs-inline{grid-column:3 / -1}
.simple-table{min-width:900px}
.compact-save{
  margin-top:2px;
  height:40px;
  min-height:40px;
  padding:9px 14px;
}
@media(max-width:900px){
  .topbar{flex-direction:column;align-items:flex-start}
  .grid,.summary{grid-template-columns:1fr}
  .container{padding:14px}
}
</style>
"""


def page(title, body):
    user = current_user()
    nav = ""
    if user:
        nav = f"""
        <nav class="nav">
            <a href="{url_for('despachos')}">Despachos</a>
            <a href="{url_for('dashboard')}">Dashboard</a>
            <a href="{url_for('consultas')}">Consultas</a>
            <a href="{url_for('mantenciones')}">Mantenciones</a>
            {'<a href="' + url_for('maquinarias') + '">Maquinarias</a>' if user.role in ['ADMIN', 'SUPERVISOR'] else ''}
            {'<a href="' + url_for('users') + '">Usuarios</a>' if user.role == 'ADMIN' else ''}
            <span class="pill">{user.name} · {user.role}</span>
            <a class="exit" href="{url_for('logout')}">Salir</a>
        </nav>
        """
    flashes = ""
    from flask import get_flashed_messages
    for category, msg in get_flashed_messages(with_categories=True):
        flashes += f'<div class="alert alert-{category}">{msg}</div>'
    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
{STYLE}
</head>
<body>
<header class="topbar">
  <div class="brand">
    <img class="logo" src="https://ferreteriasanpedro.cl/wp-content/uploads/2018/10/hnuevo-logo-e1539849032205.png" onerror="this.style.display='none'">
    <div><div class="brand-title">Ferretería San Pedro</div><div class="brand-sub">Sistema de entregas y retiros · Hora Chile</div></div>
  </div>
  {nav}
</header>
<main class="container">{flashes}{body}</main>
<script>
function pedirMotivo(id){{
  const motivo = prompt("Indica motivo de anulación:");
  if(!motivo || !motivo.trim()) return false;
  document.getElementById("motivo_"+id).value = motivo.trim();
  return confirm("¿Confirmas anular este registro?");
}}
</script>
</body>
</html>"""


def summary_html(resumen):
    money = f"${resumen['monto_total']:,.0f}".replace(",", ".")
    return f"""
    <section class="summary">
      <div class="card metric"><span>Registros</span><strong>{resumen['total']}</strong></div>
      <div class="card metric"><span>Pendientes</span><strong>{resumen['pendientes']}</strong></div>
      <div class="card metric"><span>Entregados / retirados</span><strong>{resumen['entregados']}</strong></div>
      <div class="card metric"><span>Anulados</span><strong>{resumen['anulados']}</strong></div>
      <div class="card metric"><span>Monto total</span><strong>{money}</strong></div>
    </section>
    """


def filters_html(desde, hasta, estado, usuario_id, q):
    def sel(v):
        return "selected" if estado == v else ""

    usuarios = User.query.filter_by(active=True).order_by(User.name.asc()).all()
    user_options = '<option value="">Todos</option>'
    for u in usuarios:
        selected = "selected" if str(u.id) == str(usuario_id) else ""
        user_options += f'<option value="{u.id}" {selected}>{u.name} ({u.username})</option>'

    return f"""
    <form class="card filters" method="get">
      <div><label>Desde</label><input type="date" name="desde" value="{desde}"></div>
      <div><label>Hasta</label><input type="date" name="hasta" value="{hasta}"></div>
      <div><label>Estado</label><select name="estado">
        <option value="">Todos</option>
        <option value="ENTREGADO_RETIRADO" {sel("ENTREGADO_RETIRADO")}>Entregado / Retirado</option>
        <option value="PENDIENTE" {sel("PENDIENTE")}>Pendiente</option>
        <option value="ANULADO" {sel("ANULADO")}>Anulado</option>
      </select></div>
      <div><label>Usuario</label><select name="usuario_id">{user_options}</select></div>
      <div class="grow"><label>Buscar</label><input name="q" value="{q}" placeholder="Documento, cliente, patente..."></div>
      <button class="btn" type="submit">Filtrar</button>
      <a class="btn primary" href="{url_for('exportar', desde=desde, hasta=hasta, estado=estado, usuario_id=usuario_id, q=q)}">Exportar Excel</a>
    </form>
    """


def table_html(registros, title="Registros"):
    rows = ""
    user = current_user()
    for r in registros:
        monto = f"${r.monto:,.0f}".replace(",", ".") if r.monto else ""
        estado_text = r.estado.replace("_", " / ")
        acciones = ""
        if r.estado != "ANULADO":
            acciones += f"""
            <form method="post" action="{url_for('cambiar_estado', dispatch_id=r.id)}" style="display:flex;gap:6px;margin-bottom:5px">
              <select name="estado"><option value="ENTREGADO_RETIRADO">Entregado</option><option value="PENDIENTE">Pendiente</option></select>
              <button type="submit">Cambiar</button>
            </form>
            """
            if user and user.role in ["ADMIN", "SUPERVISOR"]:
                acciones += f"""
                <form method="post" action="{url_for('anular', dispatch_id=r.id)}" onsubmit="return pedirMotivo({r.id})">
                  <input type="hidden" id="motivo_{r.id}" name="motivo_anulacion">
                  <button class="danger-btn" type="submit">Anular</button>
                </form>
                """
        else:
            acciones = r.motivo_anulacion or ""

        rows += f"""
        <tr>
          <td>{r.created_at.strftime("%Y-%m-%d %H:%M")}</td>
          <td><strong>{r.numero_documento}</strong></td>
          <td>{r.tipo_documento}</td>
          <td><span class="status {r.estado}">{estado_text}</span></td>
          <td>{monto}</td>
          <td>{r.placa_patente or ""}</td>
          <td>{r.created_by.name if r.created_by else ""}</td>
          <td>{r.observacion or ""}</td>
          <td>{acciones}</td>
        </tr>
        """
    if not rows:
        rows = '<tr><td colspan="9" style="text-align:center;color:#64748b;padding:25px">Sin registros.</td></tr>'
    return f"""
    <section class="card">
      <h2>{title}</h2>
      <div class="table-wrap">
        <table class="simple-table">
          <thead><tr>
            <th>Fecha</th><th>Documento</th><th>Tipo</th><th>Estado</th><th>Monto</th>
            <th>Patente</th><th>Usuario</th><th>Observación</th><th>Acción</th>
          </tr></thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </section>
    """


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        if not user or not user.active or not user.check_password(password):
            flash("Usuario o clave incorrecta.", "danger")
            return redirect(url_for("login"))
        session["user_id"] = user.id
        audit("LOGIN", f"Usuario {user.username} inició sesión")
        return redirect(url_for("despachos"))

    body = """
    <section class="login">
      <div class="card login-card">
        <h1>Ingreso al sistema</h1>
        <p class="muted">Registros de entregas, retiros y documentos pendientes.</p>
        <form method="post">
          <label>Usuario</label>
          <input name="username" required placeholder="admin">
          <br><br>
          <label>Clave</label>
          <input type="password" name="password" required placeholder="••••••••">
          <br><br>
          <button class="btn primary" type="submit" style="width:100%">Ingresar</button>
        </form>
        <div class="hint">Usuario inicial: <strong>admin</strong><br>Clave inicial: <strong>admin123</strong></div>
      </div>
    </section>
    """
    return page("Login", body)


@app.route("/logout")
@login_required
def logout():
    audit("LOGOUT", "Cierre de sesión")
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
@login_required
def home():
    return redirect(url_for("despachos"))


@app.route("/despachos", methods=["GET", "POST"])
@login_required
def despachos():
    if request.method == "POST":
        numero = request.form.get("numero_documento", "").strip()
        tipo = request.form.get("tipo_documento", "").strip()
        estado = request.form.get("estado", "ENTREGADO_RETIRADO").strip()

        if not numero or not tipo or not estado:
            flash("Número de documento, tipo y estado son obligatorios.", "warning")
            return redirect(url_for("despachos"))

        if Dispatch.query.filter_by(numero_documento=numero).first():
            flash("¡PELIGRO! MERCADERÍA YA RETIRADA O REGISTRADA CON ESTE DOCUMENTO.", "danger")
            return redirect(url_for("despachos"))

        despacho = Dispatch(
            numero_documento=numero,
            tipo_documento=tipo,
            estado=estado,
            cliente="",
            telefono="",
            destino="",
            placa_patente=request.form.get("placa_patente", "").strip().upper(),
            conductor="",
            pioneta="",
            monto=parse_int(request.form.get("monto")),
            observacion=request.form.get("observacion", "").strip(),
            created_by_id=session.get("user_id"),
        )
        db.session.add(despacho)
        db.session.commit()
        audit("CREAR_DESPACHO", f"Documento {despacho.numero_documento}")
        flash("Registro guardado correctamente.", "success")
        return redirect(url_for("despachos"))

    query, start_date, end_date, estado, usuario_id, q = build_query()
    registros = query.limit(300).all()
    resumen = summary_for(query)

    form = """
    <div><h1>Nuevo registro</h1><p class="muted">Registro simplificado de documentos entregados, retirados o pendientes.</p></div>
    <section class="card compact-form-card">
      <h2>Registrar documento</h2>
      <form method="post" class="grid dispatch-grid">
        <div><label>Número documento *</label><input name="numero_documento" required placeholder="Ej: 123456"></div>
        <div><label>Tipo documento *</label><select name="tipo_documento" required><option value="BOLETA">BOLETA</option><option value="FACTURA">FACTURA</option><option value="GUÍA">GUÍA</option></select></div>
        <div><label>Estado inicial *</label><select name="estado" required><option value="ENTREGADO_RETIRADO">ENTREGADO / RETIRADO</option><option value="PENDIENTE">PENDIENTE</option></select></div>
        <div><label>Monto documento</label><input name="monto" placeholder="Ej: 125000"></div>
        <div><label>Patente</label><input name="placa_patente" placeholder="Ej: ABCD12"></div>
        <div class="obs-inline"><label>Observación</label><input name="observacion" placeholder="Quién retira, autorización, condición especial, nota de bodega, etc."></div>
        <button class="btn primary full compact-save" type="submit">Guardar registro</button>
      </form>
    </section>
    """
    body = form + summary_html(resumen) + filters_html(start_date, end_date, estado, usuario_id, q) + table_html(registros, "Registros recientes")
    return page("Despachos", body)


@app.route("/dashboard")
@login_required
def dashboard():
    query, start_date, end_date, estado, usuario_id, q = build_query()
    registros = query.limit(1000).all()
    resumen = summary_for(query)

    by_user = {}
    by_estado = {}
    for r in registros:
        uname = r.created_by.name if r.created_by else "Sin usuario"
        if uname not in by_user:
            by_user[uname] = {"documentos": 0, "monto": 0}
        by_user[uname]["documentos"] += 1
        if r.estado != "ANULADO":
            by_user[uname]["monto"] += r.monto or 0

        by_estado[r.estado] = by_estado.get(r.estado, 0) + 1

    user_rows = ""
    for usuario, data in sorted(by_user.items(), key=lambda item: item[1]["monto"], reverse=True):
        monto = f"${data['monto']:,.0f}".replace(",", ".")
        user_rows += f"""
        <tr>
            <td><strong>{usuario}</strong></td>
            <td>{data['documentos']}</td>
            <td><strong>{monto}</strong></td>
        </tr>
        """
    if not user_rows:
        user_rows = '<tr><td colspan="3" style="text-align:center;color:#64748b;padding:20px">Sin datos.</td></tr>'

    estado_lines = "".join(
        f'<div class="rowline"><span>{k.replace("_"," / ")}</span><strong>{v}</strong></div>'
        for k, v in by_estado.items()
    ) or "<p class='muted'>Sin datos.</p>"

    body = f"""
    <h1>Dashboard</h1><p class="muted">Resumen operativo del periodo seleccionado.</p>
    {filters_html(start_date, end_date, estado, usuario_id, q)}
    {summary_html(resumen)}

    <section class="card">
      <h2>Reporte por usuario</h2>
      <p class="muted">Muestra cantidad de documentos y monto total por usuario para el periodo filtrado. Los montos no consideran registros anulados.</p>
      <div class="table-wrap">
        <table style="min-width:700px">
          <thead>
            <tr>
              <th>Usuario</th>
              <th>Número documentos</th>
              <th>Monto total</th>
            </tr>
          </thead>
          <tbody>{user_rows}</tbody>
        </table>
      </div>
    </section>

    <section class="grid">
      <div class="card"><h2>Por estado</h2>{estado_lines}</div>
    </section>

    {table_html(registros, "Últimos movimientos")}
    """
    return page("Dashboard", body)


@app.route("/consultas")
@login_required
def consultas():
    query, start_date, end_date, estado, usuario_id, q = build_query()
    registros = query.limit(1000).all()
    resumen = summary_for(query)
    body = f"""
    <h1>Consultas</h1><p class="muted">Historial completo con filtros y exportación a Excel.</p>
    {summary_html(resumen)}
    {filters_html(start_date, end_date, estado, usuario_id, q)}
    {table_html(registros, "Historial de documentos")}
    """
    return page("Consultas", body)


@app.route("/exportar")
@login_required
def exportar():
    query, start_date, end_date, estado, usuario_id, q = build_query()
    registros = query.all()
    wb = Workbook()
    ws = wb.active
    ws.title = "Despachos"
    headers = ["ID", "Número Documento", "Tipo Documento", "Estado", "Fecha y Hora", "Usuario", "Patente", "Monto", "Observación", "Motivo Anulación"]
    ws.append(headers)
    for r in registros:
        ws.append([r.id, r.numero_documento, r.tipo_documento, r.estado, r.created_at.strftime("%Y-%m-%d %H:%M:%S"), r.created_by.name if r.created_by else "", r.placa_patente or "", r.monto or 0, r.observacion or "", r.motivo_anulacion or ""])
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    filename = f"despachos_{start_date}_{end_date}.xlsx"
    audit("EXPORTAR_EXCEL", filename)
    return send_file(output, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/despachos/<int:dispatch_id>/anular", methods=["POST"])
@login_required
@roles_required("ADMIN", "SUPERVISOR")
def anular(dispatch_id):
    despacho = db.session.get(Dispatch, dispatch_id)
    if not despacho:
        flash("Registro no encontrado.", "danger")
        return redirect(request.referrer or url_for("consultas"))
    motivo = request.form.get("motivo_anulacion", "").strip()
    if not motivo:
        flash("Debes indicar el motivo de anulación.", "warning")
        return redirect(request.referrer or url_for("consultas"))
    despacho.estado = "ANULADO"
    despacho.motivo_anulacion = motivo
    despacho.updated_at = chile_now()
    db.session.commit()
    audit("ANULAR_DESPACHO", f"Documento {despacho.numero_documento}. Motivo: {motivo}")
    flash("Registro anulado correctamente.", "success")
    return redirect(request.referrer or url_for("consultas"))


@app.route("/despachos/<int:dispatch_id>/estado", methods=["POST"])
@login_required
def cambiar_estado(dispatch_id):
    despacho = db.session.get(Dispatch, dispatch_id)
    if not despacho:
        flash("Registro no encontrado.", "danger")
        return redirect(request.referrer or url_for("consultas"))
    nuevo_estado = request.form.get("estado", "").strip()
    if nuevo_estado not in {"PENDIENTE", "ENTREGADO_RETIRADO"}:
        flash("Estado inválido.", "warning")
        return redirect(request.referrer or url_for("consultas"))
    despacho.estado = nuevo_estado
    despacho.updated_at = chile_now()
    db.session.commit()
    audit("CAMBIAR_ESTADO", f"Documento {despacho.numero_documento} a {nuevo_estado}")
    flash("Estado actualizado.", "success")
    return redirect(request.referrer or url_for("consultas"))


def parse_date_optional(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def maintenance_filters():
    today = chile_today()
    start_date = parse_date(request.args.get("desde"), date(today.year, 1, 1))
    end_date = parse_date(request.args.get("hasta"), today)
    machine_id = request.args.get("machine_id", "").strip()
    estado = request.args.get("estado", "").strip()
    tipo = request.args.get("tipo", "").strip()
    q = request.args.get("q", "").strip()

    query = Maintenance.query.filter(Maintenance.fecha.between(start_date, end_date))

    if machine_id:
        try:
            query = query.filter(Maintenance.machine_id == int(machine_id))
        except ValueError:
            machine_id = ""

    if estado:
        query = query.filter(Maintenance.estado == estado)

    if tipo:
        query = query.filter(Maintenance.tipo_mantencion == tipo)

    if q:
        like = f"%{q}%"
        query = query.join(Machine).filter(
            db.or_(
                Machine.nombre.ilike(like),
                Machine.patente_codigo.ilike(like),
                Maintenance.detalle.ilike(like),
                Maintenance.proveedor_taller.ilike(like),
                Maintenance.responsable.ilike(like),
            )
        )

    return query.order_by(Maintenance.fecha.desc(), Maintenance.created_at.desc()), start_date, end_date, machine_id, estado, tipo, q


def maintenance_summary(registros):
    total = len(registros)
    costo_total = sum((m.costo or 0) for m in registros)
    pendientes = sum(1 for m in registros if m.estado == "PENDIENTE")
    realizadas = sum(1 for m in registros if m.estado == "REALIZADA")
    programadas = sum(1 for m in registros if m.estado == "PROGRAMADA")
    return total, costo_total, pendientes, realizadas, programadas


def maintenance_table(registros):
    rows = ""
    for m in registros:
        costo = f"${m.costo:,.0f}".replace(",", ".") if m.costo else ""
        proxima = m.proxima_fecha.strftime("%Y-%m-%d") if m.proxima_fecha else ""
        rows += f"""
        <tr>
          <td>{m.fecha.strftime("%Y-%m-%d")}</td>
          <td><strong>{m.machine.nombre}</strong><br><small>{m.machine.tipo} · {m.machine.patente_codigo or ""}</small></td>
          <td>{m.tipo_mantencion}</td>
          <td><span class="status {m.estado}">{m.estado}</span></td>
          <td>{m.kilometraje_horometro or ""}</td>
          <td>{m.proveedor_taller or ""}</td>
          <td>{m.responsable or ""}</td>
          <td>{costo}</td>
          <td>{m.detalle}</td>
          <td>{proxima}<br><small>{m.proximo_km_horas or ""}</small></td>
          <td>{m.created_by.name if m.created_by else ""}</td>
        </tr>
        """
    if not rows:
        rows = '<tr><td colspan="11" style="text-align:center;color:#64748b;padding:24px">Sin mantenciones para los filtros seleccionados.</td></tr>'
    return f"""
    <section class="card">
      <h2>Historial de mantenciones</h2>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Fecha</th><th>Maquinaria</th><th>Tipo</th><th>Estado</th><th>KM/Horómetro</th>
              <th>Proveedor/Taller</th><th>Responsable</th><th>Costo</th><th>Detalle</th><th>Próxima</th><th>Usuario</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </section>
    """


@app.route("/mantenciones", methods=["GET", "POST"])
@login_required
def mantenciones():
    if request.method == "POST":
        machine_id = request.form.get("machine_id", "").strip()
        fecha = parse_date_optional(request.form.get("fecha")) or chile_today()
        detalle = request.form.get("detalle", "").strip()

        if not machine_id or not detalle:
            flash("Debes seleccionar maquinaria e ingresar el detalle de la mantención.", "warning")
            return redirect(url_for("mantenciones"))

        machine = db.session.get(Machine, int(machine_id))
        if not machine:
            flash("Maquinaria no encontrada.", "danger")
            return redirect(url_for("mantenciones"))

        mant = Maintenance(
            machine_id=machine.id,
            fecha=fecha,
            tipo_mantencion=request.form.get("tipo_mantencion", "PREVENTIVA").strip(),
            estado=request.form.get("estado", "REALIZADA").strip(),
            kilometraje_horometro=request.form.get("kilometraje_horometro", "").strip(),
            proveedor_taller=request.form.get("proveedor_taller", "").strip(),
            responsable=request.form.get("responsable", "").strip(),
            costo=parse_int(request.form.get("costo")),
            detalle=detalle,
            proxima_fecha=parse_date_optional(request.form.get("proxima_fecha")),
            proximo_km_horas=request.form.get("proximo_km_horas", "").strip(),
            created_by_id=session.get("user_id"),
        )
        db.session.add(mant)

        nuevo_estado_maquina = request.form.get("estado_maquina", "").strip()
        if nuevo_estado_maquina:
            machine.estado = nuevo_estado_maquina

        db.session.commit()
        audit("CREAR_MANTENCION", f"{machine.nombre} - {mant.tipo_mantencion}")
        flash("Mantención registrada correctamente.", "success")
        return redirect(url_for("mantenciones"))

    query, start_date, end_date, machine_id, estado, tipo, q = maintenance_filters()
    registros = query.limit(1000).all()
    total, costo_total, pendientes, realizadas, programadas = maintenance_summary(registros)
    costo_txt = f"${costo_total:,.0f}".replace(",", ".")

    machines = Machine.query.order_by(Machine.tipo.asc(), Machine.nombre.asc()).all()
    machine_options = "".join(f'<option value="{m.id}">{m.nombre} · {m.tipo}</option>' for m in machines)

    filter_machine_options = '<option value="">Todas</option>'
    for m in machines:
        selected = "selected" if str(m.id) == str(machine_id) else ""
        filter_machine_options += f'<option value="{m.id}" {selected}>{m.nombre}</option>'

    def sel_estado(v):
        return "selected" if estado == v else ""

    def sel_tipo(v):
        return "selected" if tipo == v else ""

    body = f"""
    <h1>Mantenciones de maquinarias</h1>
    <p class="muted">Control de mantenciones para camiones, grúas horquilla y equipos de bodega.</p>

    <section class="summary">
      <div class="card metric"><span>Registros</span><strong>{total}</strong></div>
      <div class="card metric"><span>Realizadas</span><strong>{realizadas}</strong></div>
      <div class="card metric"><span>Pendientes</span><strong>{pendientes}</strong></div>
      <div class="card metric"><span>Programadas</span><strong>{programadas}</strong></div>
      <div class="card metric"><span>Costo periodo</span><strong>{costo_txt}</strong></div>
    </section>

    <section class="card compact-form-card">
      <h2>Registrar mantención</h2>
      <form method="post" class="grid maintenance-grid">
        <div><label>Maquinaria *</label><select name="machine_id" required>{machine_options}</select></div>
        <div><label>Fecha</label><input type="date" name="fecha" value="{chile_today()}"></div>
        <div><label>Tipo mantención</label><select name="tipo_mantencion"><option value="PREVENTIVA">PREVENTIVA</option><option value="CORRECTIVA">CORRECTIVA</option><option value="REVISIÓN">REVISIÓN</option><option value="CAMBIO ACEITE/FILTROS">CAMBIO ACEITE/FILTROS</option><option value="NEUMÁTICOS">NEUMÁTICOS</option><option value="FRENOS">FRENOS</option><option value="ELÉCTRICA">ELÉCTRICA</option><option value="OTRA">OTRA</option></select></div>
        <div><label>Estado mantención</label><select name="estado"><option value="REALIZADA">REALIZADA</option><option value="PENDIENTE">PENDIENTE</option><option value="PROGRAMADA">PROGRAMADA</option><option value="OBSERVADA">OBSERVADA</option></select></div>
        <div><label>KM / Horómetro</label><input name="kilometraje_horometro" placeholder="Ej: 120.000 km / 2.300 h"></div>
        <div><label>Proveedor / Taller</label><input name="proveedor_taller" placeholder="Taller, mecánico, proveedor"></div>
        <div><label>Responsable</label><input name="responsable" placeholder="Quién coordina o revisa"></div>
        <div><label>Costo</label><input name="costo" placeholder="Ej: 85000"></div>
        <div><label>Próxima fecha</label><input type="date" name="proxima_fecha"></div>
        <div><label>Próximo KM/Horas</label><input name="proximo_km_horas" placeholder="Ej: 130.000 km / 2.500 h"></div>
        <div><label>Estado maquinaria</label><select name="estado_maquina"><option value="">No cambiar</option><option value="OPERATIVA">OPERATIVA</option><option value="EN MANTENCIÓN">EN MANTENCIÓN</option><option value="FUERA DE SERVICIO">FUERA DE SERVICIO</option></select></div>
        <div class="obs-inline"><label>Detalle *</label><input name="detalle" required placeholder="Trabajo realizado, repuestos, falla detectada, recomendación, etc."></div>
        <button class="btn primary full compact-save" type="submit">Guardar mantención</button>
      </form>
    </section>

    <form class="card filters" method="get">
      <div><label>Desde</label><input type="date" name="desde" value="{start_date}"></div>
      <div><label>Hasta</label><input type="date" name="hasta" value="{end_date}"></div>
      <div><label>Maquinaria</label><select name="machine_id">{filter_machine_options}</select></div>
      <div><label>Estado</label><select name="estado"><option value="">Todos</option><option value="REALIZADA" {sel_estado("REALIZADA")}>Realizada</option><option value="PENDIENTE" {sel_estado("PENDIENTE")}>Pendiente</option><option value="PROGRAMADA" {sel_estado("PROGRAMADA")}>Programada</option><option value="OBSERVADA" {sel_estado("OBSERVADA")}>Observada</option></select></div>
      <div><label>Tipo</label><select name="tipo"><option value="">Todos</option><option value="PREVENTIVA" {sel_tipo("PREVENTIVA")}>Preventiva</option><option value="CORRECTIVA" {sel_tipo("CORRECTIVA")}>Correctiva</option><option value="REVISIÓN" {sel_tipo("REVISIÓN")}>Revisión</option><option value="CAMBIO ACEITE/FILTROS" {sel_tipo("CAMBIO ACEITE/FILTROS")}>Aceite/Filtros</option><option value="NEUMÁTICOS" {sel_tipo("NEUMÁTICOS")}>Neumáticos</option><option value="FRENOS" {sel_tipo("FRENOS")}>Frenos</option><option value="ELÉCTRICA" {sel_tipo("ELÉCTRICA")}>Eléctrica</option><option value="OTRA" {sel_tipo("OTRA")}>Otra</option></select></div>
      <div class="grow"><label>Buscar</label><input name="q" value="{q}" placeholder="Maquinaria, taller, responsable, detalle..."></div>
      <button class="btn" type="submit">Filtrar</button>
    </form>

    {maintenance_table(registros)}
    """
    return page("Mantenciones", body)


@app.route("/maquinarias", methods=["GET", "POST"])
@login_required
@roles_required("ADMIN", "SUPERVISOR")
def maquinarias():
    if request.method == "POST":
        nombre = request.form.get("nombre", "").strip()
        tipo = request.form.get("tipo", "CAMIÓN").strip()
        if not nombre:
            flash("El nombre de la maquinaria es obligatorio.", "warning")
            return redirect(url_for("maquinarias"))

        machine = Machine(
            nombre=nombre,
            tipo=tipo,
            patente_codigo=request.form.get("patente_codigo", "").strip().upper(),
            sucursal=request.form.get("sucursal", "").strip(),
            marca_modelo=request.form.get("marca_modelo", "").strip(),
            estado=request.form.get("estado", "OPERATIVA").strip(),
            observacion=request.form.get("observacion", "").strip(),
        )
        db.session.add(machine)
        db.session.commit()
        audit("CREAR_MAQUINARIA", nombre)
        flash("Maquinaria agregada correctamente.", "success")
        return redirect(url_for("maquinarias"))

    machines = Machine.query.order_by(Machine.tipo.asc(), Machine.nombre.asc()).all()
    rows = ""
    for m in machines:
        rows += f"""
        <tr>
          <td><strong>{m.nombre}</strong></td>
          <td>{m.tipo}</td>
          <td>{m.patente_codigo or ""}</td>
          <td>{m.sucursal or ""}</td>
          <td>{m.marca_modelo or ""}</td>
          <td><span class="status {m.estado}">{m.estado}</span></td>
          <td>{m.observacion or ""}</td>
        </tr>
        """
    body = f"""
    <h1>Maquinarias</h1>
    <p class="muted">Maestro de equipos disponibles para control de mantenciones.</p>

    <section class="card compact-form-card">
      <h2>Agregar maquinaria</h2>
      <form method="post" class="grid">
        <div><label>Nombre *</label><input name="nombre" required placeholder="Ej: Camión KIA KJ HY 12"></div>
        <div><label>Tipo</label><select name="tipo"><option value="CAMIÓN">CAMIÓN</option><option value="GRÚA HORQUILLA">GRÚA HORQUILLA</option><option value="CAMIONETA">CAMIONETA</option><option value="OTRA">OTRA</option></select></div>
        <div><label>Patente / Código</label><input name="patente_codigo" placeholder="Ej: KJ HY 12"></div>
        <div><label>Sucursal</label><input name="sucursal" placeholder="Ej: Sucursal 1"></div>
        <div><label>Marca / Modelo</label><input name="marca_modelo" placeholder="Ej: KIA / Hangcha"></div>
        <div><label>Estado</label><select name="estado"><option value="OPERATIVA">OPERATIVA</option><option value="EN MANTENCIÓN">EN MANTENCIÓN</option><option value="FUERA DE SERVICIO">FUERA DE SERVICIO</option></select></div>
        <div class="obs-inline"><label>Observación</label><input name="observacion" placeholder="Dato relevante, ubicación, condición, etc."></div>
        <button class="btn primary full compact-save" type="submit">Agregar maquinaria</button>
      </form>
    </section>

    <section class="card">
      <h2>Maquinarias registradas</h2>
      <div class="table-wrap">
        <table class="simple-table">
          <thead><tr><th>Nombre</th><th>Tipo</th><th>Patente/Código</th><th>Sucursal</th><th>Marca/Modelo</th><th>Estado</th><th>Observación</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </section>
    """
    return page("Maquinarias", body)


@app.route("/users", methods=["GET", "POST"])
@login_required
@roles_required("ADMIN")
def users():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        name = request.form.get("name", "").strip()
        role = request.form.get("role", "OPERADOR").strip()
        password = request.form.get("password", "").strip()

        if not username or not name or not password:
            flash("Usuario, nombre y clave son obligatorios.", "warning")
            return redirect(url_for("users"))

        if role not in {"ADMIN", "SUPERVISOR", "OPERADOR", "LECTURA"}:
            flash("Rol inválido.", "warning")
            return redirect(url_for("users"))

        if len(password) < 6:
            flash("La clave debe tener al menos 6 caracteres.", "warning")
            return redirect(url_for("users"))

        if User.query.filter_by(username=username).first():
            flash("Ese usuario ya existe.", "danger")
            return redirect(url_for("users"))

        user = User(username=username, name=name, role=role, active=True)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        audit("CREAR_USUARIO", username)
        flash("Usuario creado correctamente.", "success")
        return redirect(url_for("users"))

    usuarios = User.query.order_by(User.created_at.desc()).all()

    rows = ""
    for u in usuarios:
        checked_active = "selected" if u.active else ""
        checked_inactive = "" if u.active else "selected"
        role_options = ""
        for role_value in ["ADMIN", "SUPERVISOR", "OPERADOR", "LECTURA"]:
            selected = "selected" if u.role == role_value else ""
            role_options += f'<option value="{role_value}" {selected}>{role_value}</option>'

        rows += f"""
        <tr>
          <td>
            <form method="post" action="{url_for('editar_usuario', user_id=u.id)}" id="edit_user_{u.id}"></form>
            <input form="edit_user_{u.id}" name="username" value="{u.username}" required>
          </td>
          <td>
            <input form="edit_user_{u.id}" name="name" value="{u.name}" required>
          </td>
          <td>
            <select form="edit_user_{u.id}" name="role">{role_options}</select>
          </td>
          <td>
            <select form="edit_user_{u.id}" name="active">
              <option value="1" {checked_active}>Sí</option>
              <option value="0" {checked_inactive}>No</option>
            </select>
          </td>
          <td>
            <input form="edit_user_{u.id}" name="password" type="password" placeholder="Nueva clave opcional">
          </td>
          <td>{u.created_at.strftime('%Y-%m-%d')}</td>
          <td style="min-width:220px">
            <button form="edit_user_{u.id}" class="btn primary" type="submit">Guardar</button>
            <form method="post" action="{url_for('toggle_usuario', user_id=u.id)}" style="display:inline">
              <button class="btn danger-btn" type="submit">{'Desactivar' if u.active else 'Activar'}</button>
            </form>
          </td>
        </tr>
        """

    body = f"""
    <h1>Usuarios</h1>
    <p class="muted">Administración completa de usuarios internos: crear, editar nombre, rol, estado y clave.</p>

    <section class="card">
      <h2>Crear usuario</h2>
      <form method="post" class="grid">
        <div>
          <label>Usuario</label>
          <input name="username" required placeholder="ej: camilo">
        </div>
        <div>
          <label>Nombre visible</label>
          <input name="name" required placeholder="ej: CAMILO_LLANCA">
        </div>
        <div>
          <label>Rol</label>
          <select name="role">
            <option value="ADMIN">ADMIN</option>
            <option value="SUPERVISOR">SUPERVISOR</option>
            <option value="OPERADOR">OPERADOR</option>
            <option value="LECTURA">LECTURA</option>
          </select>
        </div>
        <div>
          <label>Clave</label>
          <input name="password" type="password" required placeholder="mínimo 6 caracteres">
        </div>
        <button class="btn primary full" type="submit">Crear usuario</button>
      </form>
    </section>

    <section class="card">
      <h2>Usuarios registrados</h2>
      <p class="muted">Para cambiar clave, escribe una nueva clave en “Nueva clave opcional”. Si lo dejas vacío, conserva la clave actual.</p>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Usuario</th>
              <th>Nombre visible</th>
              <th>Rol</th>
              <th>Activo</th>
              <th>Nueva clave</th>
              <th>Creado</th>
              <th>Acciones</th>
            </tr>
          </thead>
          <tbody>{rows}</tbody>
        </table>
      </div>
    </section>

    <section class="card">
      <h2>Roles recomendados</h2>
      <div class="rowline"><span><strong>ADMIN</strong> — controla usuarios, anula registros y administra el sistema.</span></div>
      <div class="rowline"><span><strong>SUPERVISOR</strong> — puede anular registros y revisar operación.</span></div>
      <div class="rowline"><span><strong>OPERADOR</strong> — puede crear registros y cambiar pendientes/entregados.</span></div>
      <div class="rowline"><span><strong>LECTURA</strong> — recomendado para consulta sin operación sensible.</span></div>
    </section>
    """
    return page("Usuarios", body)


@app.route("/users/<int:user_id>/editar", methods=["POST"])
@login_required
@roles_required("ADMIN")
def editar_usuario(user_id):
    usuario = db.session.get(User, user_id)
    if not usuario:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for("users"))

    username = request.form.get("username", "").strip()
    name = request.form.get("name", "").strip()
    role = request.form.get("role", "OPERADOR").strip()
    active = request.form.get("active") == "1"
    password = request.form.get("password", "").strip()

    if not username or not name:
        flash("Usuario y nombre son obligatorios.", "warning")
        return redirect(url_for("users"))

    if role not in {"ADMIN", "SUPERVISOR", "OPERADOR", "LECTURA"}:
        flash("Rol inválido.", "warning")
        return redirect(url_for("users"))

    existing = User.query.filter(User.username == username, User.id != user_id).first()
    if existing:
        flash("Ya existe otro usuario con ese nombre de usuario.", "danger")
        return redirect(url_for("users"))

    current = current_user()
    if usuario.id == current.id and role != "ADMIN":
        flash("No puedes quitarte tu propio rol ADMIN.", "danger")
        return redirect(url_for("users"))

    if usuario.id == current.id and not active:
        flash("No puedes desactivar tu propio usuario.", "danger")
        return redirect(url_for("users"))

    usuario.username = username
    usuario.name = name
    usuario.role = role
    usuario.active = active

    if password:
        if len(password) < 6:
            flash("La nueva clave debe tener al menos 6 caracteres.", "warning")
            return redirect(url_for("users"))
        usuario.set_password(password)

    db.session.commit()
    audit("EDITAR_USUARIO", f"Usuario {usuario.username} actualizado")
    flash("Usuario actualizado correctamente.", "success")
    return redirect(url_for("users"))


@app.route("/users/<int:user_id>/toggle", methods=["POST"])
@login_required
@roles_required("ADMIN")
def toggle_usuario(user_id):
    usuario = db.session.get(User, user_id)
    if not usuario:
        flash("Usuario no encontrado.", "danger")
        return redirect(url_for("users"))

    if usuario.id == current_user().id:
        flash("No puedes desactivar tu propio usuario.", "danger")
        return redirect(url_for("users"))

    usuario.active = not usuario.active
    db.session.commit()
    estado = "activado" if usuario.active else "desactivado"
    audit("CAMBIAR_ESTADO_USUARIO", f"Usuario {usuario.username} {estado}")
    flash(f"Usuario {estado} correctamente.", "success")
    return redirect(url_for("users"))



@app.errorhandler(404)
def not_found(error):
    return redirect(url_for("despachos"))


if __name__ == "__main__":
    app.run(debug=True)
