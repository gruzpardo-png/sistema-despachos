import os
import secrets
from datetime import datetime, date, time
from functools import wraps
from io import BytesIO
from decimal import Decimal, InvalidOperation

from flask import (
    Flask,
    flash,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import or_, func, text, inspect, case
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


app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.config["SQLALCHEMY_DATABASE_URI"] = normalize_database_url(os.environ.get("DATABASE_URL"))
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["PERMANENT_SESSION_LIFETIME"] = 60 * 60 * 10
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

db = SQLAlchemy(app)

DOC_TYPES = ["BOLETA", "FACTURA", "GUIA", "OTRO"]
STATUSES = ["PENDIENTE", "ENTREGADO", "ANULADO"]
INITIAL_STATUSES = ["ENTREGADO", "PENDIENTE"]
ROLES = ["ADMIN", "SUPERVISOR", "OPERADOR", "LECTURA"]


class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    username = db.Column(db.String(60), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="OPERADOR")
    active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    deliveries = db.relationship("Delivery", foreign_keys="Delivery.registered_by_id", backref="registered_by")

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)


class Delivery(db.Model):
    __tablename__ = "deliveries"

    id = db.Column(db.Integer, primary_key=True)
    doc_number = db.Column(db.String(50), nullable=False, unique=True, index=True)
    doc_type = db.Column(db.String(30), nullable=False)
    customer = db.Column(db.String(160), nullable=True)
    phone = db.Column(db.String(60), nullable=True)
    # Se mantiene en BD por compatibilidad con versiones anteriores, pero ya no se usa en el formulario.
    address = db.Column(db.String(220), nullable=True)
    destination = db.Column(db.String(180), nullable=True)
    observation = db.Column(db.Text, nullable=True)
    license_plate = db.Column(db.String(20), nullable=True)
    driver = db.Column(db.String(120), nullable=True)
    assistant = db.Column(db.String(120), nullable=True)
    amount = db.Column(db.Numeric(14, 2), nullable=False, default=0)
    status = db.Column(db.String(20), nullable=False, default="ENTREGADO", index=True)
    registered_by_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    cancelled_by_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=True)
    cancelled_reason = db.Column(db.Text, nullable=True)
    delivered_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

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
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    user = db.relationship("User")


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
        "initial_statuses": INITIAL_STATUSES,
        "roles": ROLES,
        "now": datetime.now(),
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
                return redirect(url_for("dashboard"))
            return view(*args, **kwargs)
        return wrapped
    return decorator


def audit(action: str, entity: str, entity_id: int | None = None, detail: str | None = None) -> None:
    db.session.add(AuditLog(
        user_id=session.get("user_id"),
        action=action,
        entity=entity,
        entity_id=entity_id,
        detail=detail,
        ip_address=request.headers.get("X-Forwarded-For", request.remote_addr),
    ))


def clean_text(value: str | None, upper: bool = False) -> str:
    value = (value or "").strip()
    return value.upper() if upper else value


def parse_amount(value: str | None) -> Decimal:
    raw = clean_text(value).replace("$", "").replace(".", "").replace(",", ".")
    if not raw:
        return Decimal("0")
    try:
        amount = Decimal(raw)
        return amount if amount >= 0 else Decimal("0")
    except (InvalidOperation, ValueError):
        return Decimal("0")


def money(value) -> str:
    try:
        return f"${int(value or 0):,}".replace(",", ".")
    except Exception:
        return "$0"


@app.template_filter("money")
def money_filter(value):
    return money(value)


def parse_date_param(name: str, fallback: date) -> date:
    raw = request.args.get(name) or request.form.get(name)
    if not raw:
        return fallback
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        return fallback


def query_deliveries(default_today: bool = True):
    today = date.today()
    start_date = parse_date_param("start", today if default_today else date(today.year, today.month, 1))
    end_date = parse_date_param("end", today)
    q = clean_text(request.args.get("q"))
    status = clean_text(request.args.get("status"), upper=True)
    user_id = request.args.get("user_id", "")

    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)

    query = Delivery.query.filter(Delivery.created_at.between(start_dt, end_dt))

    if status and status in STATUSES:
        query = query.filter(Delivery.status == status)

    if user_id:
        try:
            query = query.filter(Delivery.registered_by_id == int(user_id))
        except ValueError:
            user_id = ""

    if q:
        like = f"%{q}%"
        query = query.filter(or_(
            Delivery.doc_number.ilike(like),
            Delivery.customer.ilike(like),
            Delivery.phone.ilike(like),
            Delivery.destination.ilike(like),
            Delivery.license_plate.ilike(like),
            Delivery.driver.ilike(like),
            Delivery.assistant.ilike(like),
            Delivery.observation.ilike(like),
            User.name.ilike(like),
            User.username.ilike(like),
        )).join(User, Delivery.registered_by_id == User.id)

    return query.order_by(Delivery.created_at.desc()), start_date, end_date, q, status, user_id


def make_summary(start_date: date | None = None, end_date: date | None = None):
    today_start = datetime.combine(start_date or date.today(), time.min)
    today_end = datetime.combine(end_date or date.today(), time.max)
    base = Delivery.query.filter(Delivery.created_at.between(today_start, today_end))
    pending = Delivery.query.filter(Delivery.status == "PENDIENTE").count()
    total_amount = base.with_entities(func.coalesce(func.sum(Delivery.amount), 0)).scalar() or 0
    return {
        "total": base.count(),
        "pending": pending,
        "delivered": base.filter(Delivery.status == "ENTREGADO").count(),
        "cancelled": base.filter(Delivery.status == "ANULADO").count(),
        "amount": total_amount,
    }


def user_summary(start_date: date, end_date: date):
    start_dt = datetime.combine(start_date, time.min)
    end_dt = datetime.combine(end_date, time.max)
    rows = (
        db.session.query(
            User.name.label("name"),
            User.username.label("username"),
            func.count(Delivery.id).label("qty"),
            func.coalesce(func.sum(Delivery.amount), 0).label("amount"),
            func.sum(case((Delivery.status == "ENTREGADO", 1), else_=0)).label("delivered"),
            func.sum(case((Delivery.status == "PENDIENTE", 1), else_=0)).label("pending"),
            func.sum(case((Delivery.status == "ANULADO", 1), else_=0)).label("cancelled"),
        )
        .join(Delivery, Delivery.registered_by_id == User.id)
        .filter(Delivery.created_at.between(start_dt, end_dt))
        .group_by(User.id, User.name, User.username)
        .order_by(func.count(Delivery.id).desc())
        .all()
    )
    return rows


@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user():
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        username = clean_text(request.form.get("username"), upper=False).lower()
        password = request.form.get("password") or ""
        user = User.query.filter(func.lower(User.username) == username).first()
        if user and user.active and user.check_password(password):
            session.permanent = True
            session["user_id"] = user.id
            audit("LOGIN", "USER", user.id, "Inicio de sesión correcto")
            db.session.commit()
            return redirect(request.args.get("next") or url_for("dashboard"))
        flash("Usuario o contraseña incorrectos.", "danger")
    return render_template("login.html")


@app.route("/logout")
@login_required
def logout():
    audit("LOGOUT", "USER", session.get("user_id"), "Cierre de sesión")
    db.session.commit()
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("login"))


@app.route("/")
@login_required
def dashboard():
    summary = make_summary()
    recent_logs = []
    if current_user().role in ["ADMIN", "SUPERVISOR"]:
        recent_logs = AuditLog.query.order_by(AuditLog.created_at.desc()).limit(10).all()
    return render_template("index.html", summary=summary, recent_logs=recent_logs)


@app.route("/consultas")
@login_required
def consultas():
    query, start_date, end_date, q, status, user_id = query_deliveries(default_today=False)
    deliveries = query.limit(700).all()
    summary = make_summary(start_date, end_date)
    users = User.query.order_by(User.name.asc()).all()
    per_user = user_summary(start_date, end_date)
    return render_template(
        "consultas.html",
        deliveries=deliveries,
        summary=summary,
        per_user=per_user,
        users=users,
        start_date=start_date,
        end_date=end_date,
        q=q,
        selected_status=status,
        selected_user_id=user_id,
    )


@app.route("/deliveries", methods=["POST"])
@login_required
@role_required("ADMIN", "SUPERVISOR", "OPERADOR")
def create_delivery():
    user = current_user()
    doc_number = clean_text(request.form.get("doc_number"), upper=True)
    doc_type = clean_text(request.form.get("doc_type"), upper=True)
    status = clean_text(request.form.get("status"), upper=True) or "ENTREGADO"
    amount = parse_amount(request.form.get("amount"))

    if not doc_number or not doc_type:
        flash("Número de documento y tipo de documento son obligatorios.", "warning")
        return redirect(url_for("dashboard"))
    if doc_type not in DOC_TYPES:
        flash("Tipo de documento inválido.", "warning")
        return redirect(url_for("dashboard"))
    if status not in INITIAL_STATUSES:
        status = "ENTREGADO"

    existing = Delivery.query.filter(func.upper(Delivery.doc_number) == doc_number).first()
    if existing:
        flash(
            f"PELIGRO: El documento {doc_number} ya fue registrado el "
            f"{existing.created_at.strftime('%d-%m-%Y %H:%M')} por {existing.registered_by.name}.",
            "danger",
        )
        audit("DUPLICATE_ATTEMPT", "DELIVERY", existing.id, f"Intento duplicado documento {doc_number}")
        db.session.commit()
        return redirect(url_for("consultas", q=doc_number))

    delivery = Delivery(
        doc_number=doc_number,
        doc_type=doc_type,
        customer=clean_text(request.form.get("customer"), upper=True),
        phone=clean_text(request.form.get("phone")),
        address="",
        destination=clean_text(request.form.get("destination"), upper=True),
        observation=clean_text(request.form.get("observation")),
        license_plate=clean_text(request.form.get("license_plate"), upper=True),
        driver=clean_text(request.form.get("driver"), upper=True),
        assistant=clean_text(request.form.get("assistant"), upper=True),
        amount=amount,
        status=status,
        registered_by_id=user.id,
        delivered_at=datetime.utcnow() if status == "ENTREGADO" else None,
    )
    db.session.add(delivery)
    db.session.flush()
    audit("CREATE", "DELIVERY", delivery.id, f"Documento {delivery.doc_number} registrado como {delivery.status}. Monto: {money(delivery.amount)}")
    db.session.commit()
    flash(f"Documento {doc_number} registrado correctamente por {user.name}.", "success")
    return redirect(url_for("dashboard"))


@app.route("/deliveries/<int:delivery_id>/status", methods=["POST"])
@login_required
@role_required("ADMIN", "SUPERVISOR", "OPERADOR")
def update_delivery_status(delivery_id: int):
    delivery = db.session.get(Delivery, delivery_id)
    if not delivery:
        flash("Registro no encontrado.", "warning")
        return redirect(url_for("consultas"))
    new_status = clean_text(request.form.get("status"), upper=True)
    if new_status not in ["PENDIENTE", "ENTREGADO"]:
        flash("Estado inválido.", "warning")
        return redirect(url_for("consultas"))
    old_status = delivery.status
    delivery.status = new_status
    if new_status == "ENTREGADO" and not delivery.delivered_at:
        delivery.delivered_at = datetime.utcnow()
    audit("STATUS_UPDATE", "DELIVERY", delivery.id, f"{old_status} -> {new_status}")
    db.session.commit()
    flash(f"Estado actualizado a {new_status}.", "success")
    return redirect(request.referrer or url_for("consultas"))


@app.route("/deliveries/<int:delivery_id>/cancel", methods=["POST"])
@login_required
@role_required("ADMIN", "SUPERVISOR")
def cancel_delivery(delivery_id: int):
    delivery = db.session.get(Delivery, delivery_id)
    if not delivery:
        flash("Registro no encontrado.", "warning")
        return redirect(url_for("consultas"))
    reason = clean_text(request.form.get("cancelled_reason"))
    if not reason:
        flash("Debes ingresar un motivo de anulación.", "warning")
        return redirect(request.referrer or url_for("consultas"))
    old_status = delivery.status
    delivery.status = "ANULADO"
    delivery.cancelled_by_id = current_user().id
    delivery.cancelled_reason = reason
    audit("CANCEL", "DELIVERY", delivery.id, f"{old_status} -> ANULADO. Motivo: {reason}")
    db.session.commit()
    flash(f"Documento {delivery.doc_number} anulado correctamente.", "success")
    return redirect(request.referrer or url_for("consultas"))


@app.route("/export")
@login_required
def export_excel():
    query, start_date, end_date, q, status, user_id = query_deliveries(default_today=False)
    rows = query.limit(10000).all()
    wb = Workbook()
    ws = wb.active
    ws.title = "Despachos"
    headers = [
        "Documento", "Tipo", "Estado", "Fecha registro", "Usuario", "Rol usuario",
        "Cliente", "Teléfono", "Destino", "Patente", "Conductor", "Pioneta", "Monto",
        "Observación", "Motivo anulación"
    ]
    ws.append(headers)
    for item in rows:
        ws.append([
            item.doc_number,
            item.doc_type,
            item.status,
            item.created_at.strftime("%d-%m-%Y %H:%M"),
            item.registered_by.name if item.registered_by else "",
            item.registered_by.role if item.registered_by else "",
            item.customer or "",
            item.phone or "",
            item.destination or "",
            item.license_plate or "",
            item.driver or "",
            item.assistant or "",
            float(item.amount or 0),
            item.observation or "",
            item.cancelled_reason or "",
        ])
    header_fill = PatternFill("solid", fgColor="1F2937")
    header_font = Font(color="FFFFFF", bold=True)
    border = Border(bottom=Side(style="thin", color="D1D5DB"))
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center")
        cell.border = border
    for column_cells in ws.columns:
        max_length = max(len(str(cell.value or "")) for cell in column_cells)
        ws.column_dimensions[get_column_letter(column_cells[0].column)].width = min(max(max_length + 2, 12), 45)
    ws.freeze_panes = "A2"
    output = BytesIO()
    wb.save(output)
    output.seek(0)
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
        username = clean_text(request.form.get("username"), upper=False).lower()
        password = request.form.get("password") or ""
        role = clean_text(request.form.get("role"), upper=True)
        if not name or not username or len(password) < 6 or role not in ROLES:
            flash("Completa nombre, usuario, contraseña de mínimo 6 caracteres y rol válido.", "warning")
            return redirect(url_for("users"))
        if User.query.filter(func.lower(User.username) == username).first():
            flash("Ese usuario ya existe.", "warning")
            return redirect(url_for("users"))
        user = User(name=name, username=username, role=role, active=True)
        user.set_password(password)
        db.session.add(user)
        db.session.flush()
        audit("CREATE", "USER", user.id, f"Usuario {username} creado con rol {role}")
        db.session.commit()
        flash("Usuario creado correctamente.", "success")
        return redirect(url_for("users"))
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
    flash("Usuario actualizado.", "success")
    return redirect(url_for("users"))


@app.route("/health")
def health():
    return {"status": "ok", "service": "despachos"}


def create_default_admin() -> None:
    admin_username = os.environ.get("ADMIN_USERNAME", "admin").lower()
    admin_password = os.environ.get("ADMIN_PASSWORD", "admin123")
    admin_name = os.environ.get("ADMIN_NAME", "ADMINISTRADOR")
    if not User.query.filter(func.lower(User.username) == admin_username).first():
        admin = User(name=admin_name, username=admin_username, role="ADMIN", active=True)
        admin.set_password(admin_password)
        db.session.add(admin)
        db.session.commit()


def ensure_schema() -> None:
    """Migración mínima para bases ya creadas en Render."""
    inspector = inspect(db.engine)
    if "deliveries" not in inspector.get_table_names():
        return
    columns = {c["name"] for c in inspector.get_columns("deliveries")}
    if "amount" not in columns:
        db.session.execute(text("ALTER TABLE deliveries ADD COLUMN amount NUMERIC(14,2) NOT NULL DEFAULT 0"))
        db.session.commit()


with app.app_context():
    db.create_all()
    ensure_schema()
    create_default_admin()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=True)
