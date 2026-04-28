# Sistema profesional de despachos - Ferretería San Pedro

Aplicación web para registrar entregas/retiros de mercadería, bloquear documentos duplicados, consultar registros, exportar Excel y mantener auditoría interna.

## Funciones principales

- Login con usuarios y roles.
- Roles: ADMIN, SUPERVISOR, OPERADOR, LECTURA.
- Registro de documento, cliente, teléfono, dirección, destino, transporte y observación.
- Bloqueo de documentos duplicados.
- Estados: PENDIENTE, EN_RUTA, ENTREGADO, ANULADO.
- Anulación solo con motivo y solo por ADMIN/SUPERVISOR.
- Auditoría de login, creación, cambios de estado, anulaciones y exportaciones.
- Consulta por fecha, estado y búsqueda general.
- Exportación Excel.
- Compatible con SQLite local y PostgreSQL en Render.

## Uso local

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
python app.py
```

Abrir:

```text
http://localhost:5000
```

Usuario inicial local:

```text
usuario: admin
clave: admin123
```

## Variables de entorno recomendadas en Render

```text
SECRET_KEY=generada por Render
ADMIN_USERNAME=admin
ADMIN_PASSWORD=una_clave_segura
ADMIN_NAME=ADMINISTRADOR
DATABASE_URL=URL de PostgreSQL de Render
```

## Comandos Render

Build command:

```bash
pip install -r requirements.txt
```

Start command:

```bash
gunicorn app:app
```

## Nota operacional

El sistema registra como `ENTREGADO` por defecto porque el software original funcionaba como control de mercadería ya retirada. Si se quiere usar como agenda de despachos, cambiar el estado inicial por defecto a `PENDIENTE`.
