# Ferretería Cloud Tool - v4 Administración

Versión actualizada lista para subir a Render.

## Incluye

- Login con usuarios.
- Roles y permisos.
- Dashboard visible solo para administradores.
- Módulo Despachos.
- Módulo Mantenciones.
- Módulo Consulta.
- Módulo Administración / Configuración.
- Usuarios y permisos.
- Maquinarias editable solo por admin.
- Vehículos / patentes.
- Conductores y pionetas.
- Auditoría de cambios.
- Exportación a Excel.
- Sección futura para Facturación.cl.

## Usuarios iniciales

Admin:

- Usuario: `admin`
- Contraseña: `admin123`

Operador:

- Usuario: `operador`
- Contraseña: `operador123`

Cambia estas claves después de entrar en Administración > Usuarios.

## Subir a Render

1. Descomprime este ZIP.
2. Sube todos los archivos a tu repositorio GitHub.
3. En Render, crea un nuevo Web Service conectado a ese repositorio.
4. Render debería detectar `render.yaml`.
5. Si lo haces manual:
   - Build command: `pip install -r requirements.txt`
   - Start command: `gunicorn app:app`
6. Agrega estas variables:
   - `SECRET_KEY`: una clave larga aleatoria.
   - `DATABASE_PATH`: `/data/ferreteria_cloud_tool.db`
7. Agrega un Disk:
   - Mount path: `/data`
   - Size: 1 GB

## Importante

SQLite en Render necesita Disk persistente. Si no agregas Disk, la base puede perderse al reiniciar.
