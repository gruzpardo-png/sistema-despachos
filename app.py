import os
import io
import secrets
from datetime import datetime, date, time
from functools import wraps

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


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False, index=True)
    name = db.Column(db.String(160), nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(30), nullable=False, default="OPERADOR")
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

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
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    created_by = db.relationship("User", foreign_keys=[created_by_id])


class AuditLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    action = db.Column(db.String(80), nullable=False)
    detail = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("user.id"), nullable=True)
    user = db.relationship("User")


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
    today = date.today()
    start_date = parse_date(request.args.get("desde"), today)
    end_date = parse_date(request.args.get("hasta"), today)
    estado = request.args.get("estado", "").strip()
    q = request.args.get("q", "").strip()

    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    query = Dispatch.query.filter(Dispatch.created_at.between(start_dt, end_dt))

    if estado:
        query = query.filter(Dispatch.estado == estado)

    if q:
        like = f"%{q}%"
        query = query.filter(
            db.or_(
                Dispatch.numero_documento.ilike(like),
                Dispatch.cliente.ilike(like),
                Dispatch.telefono.ilike(like),
                Dispatch.destino.ilike(like),
                Dispatch.placa_patente.ilike(like),
                Dispatch.conductor.ilike(like),
                Dispatch.pioneta.ilike(like),
                Dispatch.observacion.ilike(like),
            )
        )

    return query.order_by(Dispatch.created_at.desc()), start_date, end_date, estado, q


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
*{box-sizing:border-box}body{margin:0;font-family:Arial,sans-serif;background:#f4f7fb;color:#0f172a}.topbar{background:white;border-bottom:1px solid #e5e7eb;display:flex;justify-content:space-between;align-items:center;padding:16px 26px;position:sticky;top:0;z-index:5}.brand{display:flex;align-items:center;gap:14px}.logo{height:44px;max-width:180px;object-fit:contain}.brand-title{font-size:18px;font-weight:800}.brand-sub{font-size:13px;color:#64748b}.nav{display:flex;gap:14px;align-items:center;flex-wrap:wrap}.nav a{text-decoration:none;font-weight:800;color:#0f172a}.nav .exit{color:#dc2626}.pill{background:#ecfdf5;color:#047857;border:1px solid #86efac;border-radius:999px;padding:8px 12px;font-size:12px;font-weight:800}.container{max-width:1500px;margin:auto;padding:28px}.card{background:white;border:1px solid #e2e8f0;border-radius:18px;padding:22px;margin-bottom:20px;box-shadow:0 16px 35px rgba(15,23,42,.06)}h1{font-size:34px;margin:0 0 6px}.muted{color:#64748b}.grid{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:15px}.full{grid-column:1/-1}label{font-weight:800;font-size:14px;display:block;margin-bottom:7px}input,select,textarea{width:100%;border:1px solid #cbd5e1;border-radius:12px;padding:13px;font:inherit;background:white}.btn,button{border:0;border-radius:12px;padding:13px 17px;font-weight:800;cursor:pointer;background:#e2e8f0;text-decoration:none;color:#0f172a}.primary{background:#0f766e!important;color:white!important}.danger-btn{background:#fee2e2;color:#991b1b}.summary{display:grid;grid-template-columns:repeat(5,1fr);gap:14px}.metric span{display:block;text-transform:uppercase;color:#64748b;font-size:12px;font-weight:800}.metric strong{font-size:30px}.filters{display:flex;gap:12px;align-items:end;flex-wrap:wrap}.filters div{min-width:150px}.filters .grow{flex:1}.table-wrap{overflow:auto;max-height:650px}table{border-collapse:collapse;width:100%;min-width:1300px}th,td{border-bottom:1px solid #e5e7eb;padding:10px;text-align:left;font-size:13px;vertical-align:top}th{background:#f8fafc;position:sticky;top:0}.status{border-radius:999px;padding:5px 9px;font-size:11px;font-weight:900}.ENTREGADO_RETIRADO{background:#dcfce7;color:#166534}.PENDIENTE{background:#fef9c3;color:#854d0e}.ANULADO{background:#fee2e2;color:#991b1b}.alert{padding:12px 14px;border-radius:12px;margin-bottom:10px;font-weight:700}.alert-success{background:#dcfce7;color:#166534}.alert-danger{background:#fee2e2;color:#991b1b}.alert-warning{background:#fef9c3;color:#854d0e}.login{min-height:calc(100vh - 90px);display:flex;justify-content:center;align-items:center}.login-card{max-width:430px;width:100%;text-align:left}.hint{background:#f8fafc;border-radius:12px;padding:12px;font-size:13px;margin-top:14px}.rowline{display:flex;justify-content:space-between;border-bottom:1px solid #e5e7eb;padding:10px 0}@media(max-width:900px){.topbar{flex-direction:column;align-items:flex-start}.grid,.summary{grid-template-columns:1fr}.container{padding:16px}}
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
    <div><div class="brand-title">Ferretería San Pedro</div><div class="brand-sub">Sistema de entregas y retiros</div></div>
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


def filters_html(desde, hasta, estado, q):
    def sel(v):
        return "selected" if estado == v else ""
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
      <div class="grow"><label>Buscar</label><input name="q" value="{q}" placeholder="Documento, cliente, patente..."></div>
      <button class="btn" type="submit">Filtrar</button>
      <a class="btn primary" href="{url_for('exportar', desde=desde, hasta=hasta, estado=estado, q=q)}">Exportar Excel</a>
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
          <td>{r.cliente or ""}</td>
          <td>{r.telefono or ""}</td>
          <td>{r.destino or ""}</td>
          <td>{r.placa_patente or ""}</td>
          <td>{r.conductor or ""}</td>
          <td>{r.pioneta or ""}</td>
          <td>{r.created_by.name if r.created_by else ""}</td>
          <td>{r.observacion or ""}</td>
          <td>{acciones}</td>
        </tr>
        """
    if not rows:
        rows = '<tr><td colspan="14" style="text-align:center;color:#64748b;padding:25px">Sin registros.</td></tr>'
    return f"""
    <section class="card">
      <h2>{title}</h2>
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>Fecha</th><th>Documento</th><th>Tipo</th><th>Estado</th><th>Monto</th>
            <th>Cliente</th><th>Teléfono</th><th>Destino</th><th>Patente</th>
            <th>Conductor</th><th>Pioneta</th><th>Usuario</th><th>Observación</th><th>Acción</th>
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
            cliente=request.form.get("cliente", "").strip(),
            telefono=request.form.get("telefono", "").strip(),
            destino=request.form.get("destino", "").strip(),
            placa_patente=request.form.get("placa_patente", "").strip().upper(),
            conductor=request.form.get("conductor", "").strip(),
            pioneta=request.form.get("pioneta", "").strip(),
            monto=parse_int(request.form.get("monto")),
            observacion=request.form.get("observacion", "").strip(),
            created_by_id=session.get("user_id"),
        )
        db.session.add(despacho)
        db.session.commit()
        audit("CREAR_DESPACHO", f"Documento {despacho.numero_documento}")
        flash("Registro guardado correctamente.", "success")
        return redirect(url_for("despachos"))

    query, start_date, end_date, estado, q = build_query()
    registros = query.limit(300).all()
    resumen = summary_for(query)

    form = """
    <div><h1>Nuevo registro</h1><p class="muted">Formulario rápido para registrar documentos entregados, retirados o pendientes.</p></div>
    <section class="card">
      <h2>Registrar documento</h2>
      <form method="post" class="grid">
        <div><label>Número documento *</label><input name="numero_documento" required placeholder="Ej: 123456"></div>
        <div><label>Tipo documento *</label><select name="tipo_documento" required><option value="BOLETA">BOLETA</option><option value="FACTURA">FACTURA</option><option value="GUÍA">GUÍA</option></select></div>
        <div><label>Estado inicial *</label><select name="estado" required><option value="ENTREGADO_RETIRADO">ENTREGADO / RETIRADO</option><option value="PENDIENTE">PENDIENTE</option></select></div>
        <div><label>Monto documento</label><input name="monto" placeholder="Ej: 125000"></div>
        <div><label>Cliente</label><input name="cliente" placeholder="Cliente o empresa"></div>
        <div><label>Teléfono</label><input name="telefono"></div>
        <div><label>Destino</label><input name="destino" placeholder="Comuna / referencia"></div>
        <div><label>Patente</label><input name="placa_patente"></div>
        <div><label>Conductor</label><input name="conductor" placeholder="Solo despacho interno"></div>
        <div><label>Pioneta</label><input name="pioneta" placeholder="Solo despacho interno"></div>
        <div class="full"><label>Observación</label><textarea name="observacion" rows="3" placeholder="Quién retira, condición especial, autorización, etc."></textarea></div>
        <button class="btn primary full" type="submit">Guardar registro</button>
      </form>
    </section>
    """
    body = form + summary_html(resumen) + filters_html(start_date, end_date, estado, q) + table_html(registros, "Registros recientes")
    return page("Despachos", body)


@app.route("/dashboard")
@login_required
def dashboard():
    query, start_date, end_date, estado, q = build_query()
    registros = query.limit(500).all()
    resumen = summary_for(query)
    by_user = {}
    by_estado = {}
    for r in registros:
        uname = r.created_by.name if r.created_by else "Sin usuario"
        by_user[uname] = by_user.get(uname, 0) + 1
        by_estado[r.estado] = by_estado.get(r.estado, 0) + 1

    user_lines = "".join(f'<div class="rowline"><span>{k}</span><strong>{v}</strong></div>' for k, v in by_user.items()) or "<p class='muted'>Sin datos.</p>"
    estado_lines = "".join(f'<div class="rowline"><span>{k.replace("_"," / ")}</span><strong>{v}</strong></div>' for k, v in by_estado.items()) or "<p class='muted'>Sin datos.</p>"

    body = f"""
    <h1>Dashboard</h1><p class="muted">Resumen operativo del periodo seleccionado.</p>
    {filters_html(start_date, end_date, estado, q)}
    {summary_html(resumen)}
    <section class="grid">
      <div class="card"><h2>Por usuario</h2>{user_lines}</div>
      <div class="card"><h2>Por estado</h2>{estado_lines}</div>
    </section>
    {table_html(registros, "Últimos movimientos")}
    """
    return page("Dashboard", body)


@app.route("/consultas")
@login_required
def consultas():
    query, start_date, end_date, estado, q = build_query()
    registros = query.limit(1000).all()
    resumen = summary_for(query)
    body = f"""
    <h1>Consultas</h1><p class="muted">Historial completo con filtros y exportación a Excel.</p>
    {summary_html(resumen)}
    {filters_html(start_date, end_date, estado, q)}
    {table_html(registros, "Historial de documentos")}
    """
    return page("Consultas", body)


@app.route("/exportar")
@login_required
def exportar():
    query, start_date, end_date, estado, q = build_query()
    registros = query.all()
    wb = Workbook()
    ws = wb.active
    ws.title = "Despachos"
    headers = ["ID", "Número Documento", "Tipo Documento", "Estado", "Fecha y Hora", "Usuario", "Cliente", "Teléfono", "Destino", "Patente", "Conductor", "Pioneta", "Monto", "Observación", "Motivo Anulación"]
    ws.append(headers)
    for r in registros:
        ws.append([r.id, r.numero_documento, r.tipo_documento, r.estado, r.created_at.strftime("%Y-%m-%d %H:%M:%S"), r.created_by.name if r.created_by else "", r.cliente or "", r.telefono or "", r.destino or "", r.placa_patente or "", r.conductor or "", r.pioneta or "", r.monto or 0, r.observacion or "", r.motivo_anulacion or ""])
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
    despacho.updated_at = datetime.utcnow()
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
    despacho.updated_at = datetime.utcnow()
    db.session.commit()
    audit("CAMBIAR_ESTADO", f"Documento {despacho.numero_documento} a {nuevo_estado}")
    flash("Estado actualizado.", "success")
    return redirect(request.referrer or url_for("consultas"))


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
    rows = "".join(f"<tr><td>{u.username}</td><td>{u.name}</td><td>{u.role}</td><td>{'Sí' if u.active else 'No'}</td><td>{u.created_at.strftime('%Y-%m-%d')}</td></tr>" for u in usuarios)
    body = f"""
    <h1>Usuarios</h1><p class="muted">Administración de usuarios internos.</p>
    <section class="card">
      <h2>Crear usuario</h2>
      <form method="post" class="grid">
        <div><label>Usuario</label><input name="username" required></div>
        <div><label>Nombre</label><input name="name" required></div>
        <div><label>Rol</label><select name="role"><option value="ADMIN">ADMIN</option><option value="SUPERVISOR">SUPERVISOR</option><option value="OPERADOR">OPERADOR</option><option value="LECTURA">LECTURA</option></select></div>
        <div><label>Clave</label><input name="password" type="password" required></div>
        <button class="btn primary full" type="submit">Crear usuario</button>
      </form>
    </section>
    <section class="card"><h2>Usuarios registrados</h2><div class="table-wrap"><table><thead><tr><th>Usuario</th><th>Nombre</th><th>Rol</th><th>Activo</th><th>Creado</th></tr></thead><tbody>{rows}</tbody></table></div></section>
    """
    return page("Usuarios", body)


@app.errorhandler(404)
def not_found(error):
    return redirect(url_for("despachos"))


if __name__ == "__main__":
    app.run(debug=True)
