import os
import secrets
from datetime import datetime, date, time
from functools import wraps
from io import BytesIO
from zoneinfo import ZoneInfo

from flask import Flask, flash, redirect, render_template, request, send_file, session, url_for
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_, inspect, text
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter


def normalize_database_url(raw_url: str | None) -> str:
    if raw_url:
        if raw_url.startswith("postgres://"):
            return raw_url.replace("postgres://", "postgresql+psycopg2://", 1)
        if raw_url.startswith("postgresql://"):
            return raw_url.replace("postgresql://", "postgresql+psycopg2://", 1)
        return raw_url
    return "sqlite:///despachos.db"


APP_TZ = ZoneInfo(os.environ.get("APP_TIMEZONE", "America/Santiago"))
DOC_TYPES = ["BOLETA", "FACTURA", "GUIA", "OTRO"]
STATUSES = ["PENDIENTE", "ENTREGADO", "ANULADO"]
ROLES = ["ADMIN", "SUPERVISOR", "OPERADOR", "LECTURA"]

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.config["SQLALCHEMY_DATABASE_URI"] = normalize_database_url(os.environ.get("DATABASE_URL"))
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 10
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

db = SQLAlchemy(app)


def now_local() -> datetime:
    return datetime.now(APP_TZ).replace(tzinfo=None)


class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    username = db.Column(db.String(60), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="OPERADOR")
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=now_local)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)


class Delivery(db.Model):
    __tablename__ = "deliveries"
    id = db.Column(db.Integer, primary_key=True)
    doc_number = db.Column(db.String(50), nullable=False, unique=True, index=True)
    doc_type = db.Column(db.String(30), nullable=False)
    status = db.Column(db.String(20), nullable=False, default="ENTREGADO", index=True)
    amount = db.Column(db.Float, nullable=True, default=0)
    customer = db.Column(db.String(160), nullable=True)
    phone = db.Column(db.String(60), nullable=True)
    destination = db.Column(db.String(180), nullable=True)
    license_plate = db.Column(db.String(20), nullable=True)
    driver = db.Column(db.String(120), nullable=True)
    assistant = db.Column(db.String(120), nullable=True)
    observation = db.Column(db.Text, nullable=True)
    registered_by_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    cancelled_by_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    cancelled_reason = db.Column(db.Text, nullable=True)
    delivered_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=now_local, index=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=now_local, onupdate=now_local)

    registered_by = db.relationship("User", foreign_keys=[registered_by_id])
    cancelled_by = db.relationship("User", foreign_keys=[cancelled_by_id])


class AuditLog(db.Model):
    __tablename__ = "audit_logs"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    action = db.Column(db.String(60), nullable=False)
    entity = db.Column(db.String(60), nullable=False)
    entity_id = db.Column(db.Integer, nullable=True)
    detail = db.Column(db.Text, nullable=True)
    ip_address = db.Column(db.String(80), nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=now_local, index=True)
    user = db.relationship("User")


def clean_text(value: str | None, upper: bool = False) -> str:
    value = (value or "").strip()
    return value.upper() if upper else value


def parse_amount(value: str | None) -> float | None:
    raw = (value or "").strip().replace("$", "").replace(".", "").replace(",", ".")
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def money(value) -> str:
    try:
        number = float(value or 0)
    except (TypeError, ValueError):
        number = 0
    return "$" + f"{number:,.0f}".replace(",", ".")


def current_user() -> User | None:
    user_id = session.get("user_id")
    if not user_id:
        return None
    return db.session.get(User, user_id)


@app.context_processor
def inject_context():
    return {
        "current_user": current_user(),
        "doc_types": DOC_TYPES,
        "statuses": STATUSES,
        "roles": ROLES,
        "money": money,
        "today": date.today().strftime("%Y-%m-%d"),
    }


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        user = current_user()
        if not user or not user.active:
            flash("Debes iniciar sesión.", "warning")
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)
    return wrapped


def role_required(*allowed_roles):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            user = current_user()
            if not user or user.role not in allowed_roles:
                flash("No tienes permisos para esta acción.", "danger")
                return redirect(url_for("despachos"))
            return view(*args, **kwargs)
        return wrapped
    return decorator


def audit(action: str, entity: str, entity_id: int | None = None, detail: str | None = None) -> None:
    db.session.add(AuditLog(
        user_id=session.get("user_id"), action=action, entity=entity, entity_id=entity_id,
        detail=detail, ip_address=request.headers.get("X-Forwarded-For", request.remote_addr)
    ))


def parse_date_param(name: str, fallback: date) -> date:
    raw = request.args.get(name) or request.form.get(name)
    if not raw:
        return fallback
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return fallback


def delivery_query(default_today: bool = True):
    fallback_start = date.today() if default_today else date(2020, 1, 1)
    start_date = parse_date_param("start", fallback_start)
    end_date = parse_date_param("end", date.today())
    q = clean_text(request.args.get("q"))
    status = clean_text(request.args.get("status"), upper=True)
    doc_type = clean_text(request.args.get("doc_type"), upper=True)
    user_id = request.args.get("user_id") or ""

    query = Delivery.query.filter(Delivery.created_at.between(datetime.combine(start_date, time.min), datetime.combine(end_date, time.max)))
    if status in STATUSES:
        query = query.filter(Delivery.status == status)
    if doc_type in DOC_TYPES:
        query = query.filter(Delivery.doc_type == doc_type)
    if user_id.isdigit():
        query = query.filter(Delivery.registered_by_id == int(user_id))
    if q:
        like = f"%{q}%"
        query = query.filter(or_(
            Delivery.doc_number.ilike(like), Delivery.customer.ilike(like), Delivery.phone.ilike(like),
            Delivery.destination.ilike(like), Delivery.license_plate.ilike(like), Delivery.driver.ilike(like),
            Delivery.assistant.ilike(like), Delivery.observation.ilike(like)
        ))
    return query.order_by(Delivery.created_at.desc()), start_date, end_date, q, status, doc_type, user_id


def get_summary(start_date: date | None = None, end_date: date | None = None) -> dict:
    if not start_date:
        start_date = date.today()
    if not end_date:
        end_date = date.today()
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    base = Delivery.query.filter(Delivery.created_at.between(start_dt, end_dt))
    rows = base.all()
    return {
        "total": len(rows),
        "pending": sum(1 for r in rows if r.status == "PENDIENTE"),
        "delivered": sum(1 for r in rows if r.status == "ENTREGADO"),
        "cancelled": sum(1 for r in rows if r.status == "ANULADO"),
        "amount": sum(float(r.amount or 0) for r in rows if r.status != "ANULADO"),
    }


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user():
        return redirect(url_for("despachos"))
    if request.method == "POST":
        username = clean_text(request.form.get("username")).lower()
        password = request.form.get("password") or ""
        user = User.query.filter(func.lower(User.username) == username).first()
        if user and user.active and user.check_password(password):
            session.permanent = True
            session["user_id"] = user.id
            audit("LOGIN", "USER", user.id, "Inicio de sesión correcto")
            db.session.commit()
            return redirect(request.args.get("next") or url_for("despachos"))
        flash("Usuario o contraseña incorrectos.", "danger")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    audit("LOGOUT", "USER", session.get("user_id"), "Cierre de sesión")
    db.session.commit()
    session.clear()
    return redirect(url_for("login"))


@app.route("/")
def root():
    return redirect(url_for("despachos"))


@app.route("/despachos")
@login_required
def despachos():
    query, start_date, end_date, q, status, doc_type, user_id = delivery_query(default_today=True)
    deliveries = query.limit(150).all()
    summary = get_summary(start_date, end_date)
    return render_template("despachos.html", deliveries=deliveries, summary=summary, start_date=start_date, end_date=end_date, q=q, selected_status=status, selected_doc_type=doc_type, selected_user_id=user_id)


@app.route("/dashboard")
@login_required
def dashboard():
    today_summary = get_summary(date.today(), date.today())
    month_start = date(date.today().year, date.today().month, 1)
    month_summary = get_summary(month_start, date.today())
    recent_logs = AuditLog.query.order_by(AuditLog.created_at.desc()).limit(12).all()
    recent_deliveries = Delivery.query.order_by(Delivery.created_at.desc()).limit(12).all()
    users_activity = db.session.query(User.name, func.count(Delivery.id).label("total")).join(Delivery, Delivery.registered_by_id == User.id).filter(Delivery.created_at >= datetime.combine(month_start, time.min)).group_by(User.name).order_by(text("total DESC")).limit(8).all()
    return render_template("dashboard.html", today_summary=today_summary, month_summary=month_summary, recent_logs=recent_logs, recent_deliveries=recent_deliveries, users_activity=users_activity)


@app.route("/consultas")
@login_required
def consultas():
    query, start_date, end_date, q, status, doc_type, user_id = delivery_query(default_today=False)
    deliveries = query.limit(1000).all()
    summary = get_summary(start_date, end_date)
    users = User.query.order_by(User.name.asc()).all()
    return render_template("consultas.html", deliveries=deliveries, summary=summary, users=users, start_date=start_date, end_date=end_date, q=q, selected_status=status, selected_doc_type=doc_type, selected_user_id=user_id)


@app.route("/deliveries", methods=["POST"])
@login_required
@role_required("ADMIN", "SUPERVISOR", "OPERADOR")
def create_delivery():
    doc_number = clean_text(request.form.get("doc_number"), upper=True)
    doc_type = clean_text(request.form.get("doc_type"), upper=True)
    status = clean_text(request.form.get("status"), upper=True) or "ENTREGADO"
    amount = parse_amount(request.form.get("amount"))
    observation = clean_text(request.form.get("observation"))

    if not doc_number or not doc_type:
        flash("Número de documento y tipo de documento son obligatorios.", "warning")
        return redirect(request.referrer or url_for("despachos"))
    if doc_type not in DOC_TYPES:
        flash("Tipo de documento inválido. Se eliminó Nota de Venta; usa Boleta, Factura, Guía u Otro.", "warning")
        return redirect(request.referrer or url_for("despachos"))
    if status not in ["PENDIENTE", "ENTREGADO"]:
        status = "ENTREGADO"
    if amount is not None and amount < 0:
        flash("El monto no puede ser negativo.", "warning")
        return redirect(request.referrer or url_for("despachos"))

    existing = Delivery.query.filter(func.upper(Delivery.doc_number) == doc_number).first()
    if existing:
        flash(f"PELIGRO: el documento {doc_number} ya está registrado desde {existing.created_at.strftime('%d-%m-%Y %H:%M')} por {existing.registered_by.name}.", "danger")
        audit("DUPLICATE_ATTEMPT", "DELIVERY", existing.id, f"Intento duplicado documento {doc_number}")
        db.session.commit()
        return redirect(url_for("consultas", q=doc_number))

    d = Delivery(
        doc_number=doc_number, doc_type=doc_type, status=status, amount=amount or 0,
        customer=clean_text(request.form.get("customer"), upper=True),
        phone=clean_text(request.form.get("phone")),
        destination=clean_text(request.form.get("destination"), upper=True),
        license_plate=clean_text(request.form.get("license_plate"), upper=True),
        driver=clean_text(request.form.get("driver"), upper=True),
        assistant=clean_text(request.form.get("assistant"), upper=True),
        observation=observation,
        registered_by_id=current_user().id,
        delivered_at=now_local() if status == "ENTREGADO" else None,
    )
    db.session.add(d)
    db.session.flush()
    audit("CREATE", "DELIVERY", d.id, f"Documento {d.doc_number} registrado como {d.status}. Monto: {money(d.amount)}")
    db.session.commit()
    flash(f"Documento {doc_number} guardado correctamente.", "success")
    return redirect(url_for("despachos", q=doc_number))


@app.route("/deliveries/<int:delivery_id>/status", methods=["POST"])
@login_required
@role_required("ADMIN", "SUPERVISOR", "OPERADOR")
def update_delivery_status(delivery_id: int):
    delivery = db.session.get(Delivery, delivery_id)
    if not delivery:
        flash("Registro no encontrado.", "warning")
        return redirect(request.referrer or url_for("despachos"))
    new_status = clean_text(request.form.get("status"), upper=True)
    if new_status not in ["PENDIENTE", "ENTREGADO"]:
        flash("Estado inválido.", "warning")
        return redirect(request.referrer or url_for("despachos"))
    old = delivery.status
    delivery.status = new_status
    delivery.delivered_at = now_local() if new_status == "ENTREGADO" else None
    audit("STATUS_UPDATE", "DELIVERY", delivery.id, f"{old} -> {new_status}")
    db.session.commit()
    flash("Estado actualizado.", "success")
    return redirect(request.referrer or url_for("despachos"))


@app.route("/deliveries/<int:delivery_id>/cancel", methods=["POST"])
@login_required
@role_required("ADMIN", "SUPERVISOR")
def cancel_delivery(delivery_id: int):
    delivery = db.session.get(Delivery, delivery_id)
    if not delivery:
        flash("Registro no encontrado.", "warning")
        return redirect(request.referrer or url_for("despachos"))
    reason = clean_text(request.form.get("cancelled_reason"))
    if not reason:
        flash("Debes ingresar motivo de anulación.", "warning")
        return redirect(request.referrer or url_for("despachos"))
    old = delivery.status
    delivery.status = "ANULADO"
    delivery.cancelled_by_id = current_user().id
    delivery.cancelled_reason = reason
    audit("CANCEL", "DELIVERY", delivery.id, f"{old} -> ANULADO. Motivo: {reason}")
    db.session.commit()
    flash("Registro anulado correctamente.", "success")
    return redirect(request.referrer or url_for("despachos"))


@app.route("/export")
@login_required
def export_excel():
    query, start_date, end_date, q, status, doc_type, user_id = delivery_query(default_today=False)
    rows = query.limit(20000).all()
    wb = Workbook()
    ws = wb.active
    ws.title = "Despachos"
    headers = ["Número Documento", "Tipo Documento", "Estado", "Monto", "Fecha y Hora", "Usuario", "Cliente", "Teléfono", "Destino", "Patente", "Conductor", "Pioneta", "Observación", "Motivo Anulación"]
    ws.append(headers)
    for d in rows:
        ws.append([d.doc_number, d.doc_type, d.status, d.amount or 0, d.created_at.strftime("%d-%m-%Y %H:%M"), d.registered_by.name if d.registered_by else "", d.customer or "", d.phone or "", d.destination or "", d.license_plate or "", d.driver or "", d.assistant or "", d.observation or "", d.cancelled_reason or ""])
    header_fill = PatternFill("solid", fgColor="0F766E")
    header_font = Font(color="FFFFFF", bold=True)
    border = Border(bottom=Side(style="thin", color="D1D5DB"))
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")
        cell.border = border
    for column_cells in ws.columns:
        max_length = max(len(str(cell.value or "")) for cell in column_cells)
        ws.column_dimensions[get_column_letter(column_cells[0].column)].width = min(max(max_length + 2, 12), 48)
    ws.freeze_panes = "A2"
    output = BytesIO(); wb.save(output); output.seek(0)
    filename = f"DESPACHOS_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
    audit("EXPORT", "DELIVERY", None, f"Exportación {filename}. Registros: {len(rows)}")
    db.session.commit()
    return send_file(output, as_attachment=True, download_name=filename, mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


@app.route("/users", methods=["GET", "POST"])
@login_required
@role_required("ADMIN")
def users():
    if request.method == "POST":
        name = clean_text(request.form.get("name"), upper=True)
        username = clean_text(request.form.get("username")).lower()
        password = request.form.get("password") or ""
        role = clean_text(request.form.get("role"), upper=True)
        if not name or not username or len(password) < 6 or role not in ROLES:
            flash("Completa nombre, usuario, clave de mínimo 6 caracteres y rol válido.", "warning")
            return redirect(url_for("users"))
        if User.query.filter(func.lower(User.username) == username).first():
            flash("Ese usuario ya existe.", "warning")
            return redirect(url_for("users"))
        user = User(name=name, username=username, role=role, active=True)
        user.set_password(password)
        db.session.add(user); db.session.flush()
        audit("CREATE", "USER", user.id, f"Usuario {username} creado con rol {role}")
        db.session.commit()
        flash("Usuario creado correctamente.", "success")
    all_users = User.query.order_by(User.active.desc(), User.name.asc()).all()
    return render_template("users.html", users=all_users)


@app.route("/users/<int:user_id>/toggle", methods=["POST"])
@login_required
@role_required("ADMIN")
def toggle_user(user_id: int):
    user = db.session.get(User, user_id)
    if not user:
        flash("Usuario no encontrado.", "warning")
        return redirect(url_for("users"))
    if user.id == current_user().id:
        flash("No puedes desactivar tu propio usuario.", "warning")
        return redirect(url_for("users"))
    user.active = not user.active
    audit("TOGGLE_ACTIVE", "USER", user.id, f"Activo: {user.active}")
    db.session.commit()
    return redirect(url_for("users"))


@app.route("/health")
def health():
    return {"status": "ok", "service": "despachos_v3"}


def create_default_admin() -> None:
    username = os.environ.get("ADMIN_USERNAME", "admin").lower()
    password = os.environ.get("ADMIN_PASSWORD", "admin123")
    name = os.environ.get("ADMIN_NAME", "ADMINISTRADOR")
    if not User.query.filter(func.lower(User.username) == username).first():
        admin = User(name=name, username=username, role="ADMIN", active=True)
        admin.set_password(password)
        db.session.add(admin)
        db.session.commit()


def add_column_if_missing(table: str, column: str, ddl: str) -> None:
    engine = db.engine
    inspector = inspect(engine)
    if table not in inspector.get_table_names():
        return
    existing = {c["name"] for c in inspector.get_columns(table)}
    if column not in existing:
        with engine.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {ddl}"))


def lightweight_migrations() -> None:
    # Permite reutilizar una base creada con la v2 sin perder registros.
    add_column_if_missing("deliveries", "amount", "amount FLOAT DEFAULT 0")


with app.app_context():
    db.create_all()
    lightweight_migrations()
    create_default_admin()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
