import os
import csv
import io
import secrets
from datetime import datetime, date, time
from functools import wraps

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, session, send_file
)
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
from openpyxl import Workbook


app = Flask(__name__)

# -----------------------------
# Configuración
# -----------------------------
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))

database_url = os.environ.get("DATABASE_URL")
if database_url:
    # Render PostgreSQL a veces entrega postgres:// y SQLAlchemy espera postgresql://
    database_url = database_url.replace("postgres://", "postgresql://", 1)
    app.config["SQLALCHEMY_DATABASE_URI"] = database_url
else:
    app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///despachos.db"

app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app)


# -----------------------------
# Modelos
# -----------------------------
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


# -----------------------------
# Inicialización
# -----------------------------
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


# -----------------------------
# Helpers
# -----------------------------
def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    return db.session.get(User, uid)


@app.context_processor
def inject_globals():
    return {
        "current_user": current_user(),
        "now": datetime.now(),
        "app_name": "Sistema de Despachos",
    }


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
    log = AuditLog(action=action, detail=detail, user_id=user.id if user else None)
    db.session.add(log)
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
    total = len(rows)
    pendientes = sum(1 for r in rows if r.estado == "PENDIENTE")
    entregados = sum(1 for r in rows if r.estado == "ENTREGADO_RETIRADO")
    anulados = sum(1 for r in rows if r.estado == "ANULADO")
    monto_total = sum((r.monto or 0) for r in rows if r.estado != "ANULADO")
    return {
        "total": total,
        "pendientes": pendientes,
        "entregados": entregados,
        "anulados": anulados,
        "monto_total": monto_total,
    }


# -----------------------------
# Rutas
# -----------------------------
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")

        user = User.query.filter_by(username=username).first()
        if not user or not user.active or not user.check_password(password):
            flash("Usuario o clave incorrecta.", "danger")
            return render_template("login.html")

        session["user_id"] = user.id
        audit("LOGIN", f"Usuario {user.username} inició sesión")
        return redirect(url_for("despachos"))

    return render_template("login.html")


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
            flash("Número de documento, tipo de documento y estado son obligatorios.", "warning")
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

    return render_template(
        "despachos.html",
        registros=registros,
        resumen=resumen,
        desde=start_date,
        hasta=end_date,
        estado=estado,
        q=q,
    )


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

    return render_template(
        "dashboard.html",
        registros=registros,
        resumen=resumen,
        by_user=by_user,
        by_estado=by_estado,
        desde=start_date,
        hasta=end_date,
        estado=estado,
        q=q,
    )


@app.route("/consultas")
@login_required
def consultas():
    query, start_date, end_date, estado, q = build_query()
    registros = query.limit(1000).all()
    resumen = summary_for(query)

    return render_template(
        "consultas.html",
        registros=registros,
        resumen=resumen,
        desde=start_date,
        hasta=end_date,
        estado=estado,
        q=q,
    )


@app.route("/exportar")
@login_required
def exportar():
    query, start_date, end_date, estado, q = build_query()
    registros = query.all()

    wb = Workbook()
    ws = wb.active
    ws.title = "Despachos"

    headers = [
        "ID", "Número Documento", "Tipo Documento", "Estado", "Fecha y Hora",
        "Usuario", "Cliente", "Teléfono", "Destino", "Patente",
        "Conductor", "Pioneta", "Monto", "Observación", "Motivo Anulación"
    ]
    ws.append(headers)

    for r in registros:
        ws.append([
            r.id,
            r.numero_documento,
            r.tipo_documento,
            r.estado,
            r.created_at.strftime("%Y-%m-%d %H:%M:%S"),
            r.created_by.name if r.created_by else "",
            r.cliente or "",
            r.telefono or "",
            r.destino or "",
            r.placa_patente or "",
            r.conductor or "",
            r.pioneta or "",
            r.monto or 0,
            r.observacion or "",
            r.motivo_anulacion or "",
        ])

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"despachos_{start_date}_{end_date}.xlsx"
    audit("EXPORTAR_EXCEL", filename)

    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


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
    if nuevo_estado not in {"PENDIENTE", "ENTREGADO_RETIRADO", "ANULADO"}:
        flash("Estado inválido.", "warning")
        return redirect(request.referrer or url_for("consultas"))

    if nuevo_estado == "ANULADO":
        flash("Para anular usa la opción de anulación con motivo.", "warning")
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
    return render_template("users.html", usuarios=usuarios)


@app.errorhandler(404)
def not_found(error):
    return redirect(url_for("despachos"))


if __name__ == "__main__":
    app.run(debug=True)
