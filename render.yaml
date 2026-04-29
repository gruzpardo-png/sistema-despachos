# Ferretería Cloud Tool - v4.2 Data Safe

Versión actualizada para subir a Render sin perder datos anteriores.

## Cambios principales

- Despacho rápido para operadores.
- Estados de despacho operativos:
  - Entregado por defecto.
  - Pendiente.
- Dashboard solo administradores.
- Administración / Configuración solo administradores.
- Usuarios y permisos.
- Maquinarias.
- Vehículos / patentes.
- Conductores / pionetas.
- Mantenciones.
- Auditoría.
- Exportación Excel.
- Backup completo de base de datos desde el sistema.
- Respaldo automático antes de migraciones.

## Regla importante de actualización

Esta versión está preparada para actualizar sin borrar datos, siempre que la base SQLite esté en un Disk persistente de Render.

La variable debe ser:

```txt
DATABASE_PATH=/data/ferreteria_cloud_tool.db
```

Y el servicio debe tener Disk:

```txt
Mount path: /data
```

Si la base queda dentro del contenedor normal, por ejemplo `ferreteria_cloud_tool.db` sin `/data`, Render puede reemplazarla en cada deploy.

## Qué NO hace esta versión

- No usa `DROP TABLE`.
- No borra tablas.
- No borra usuarios.
- No borra despachos.
- No borra maquinarias.
- No borra mantenciones.
- No reinicia la configuración.
- No reemplaza la base existente.

## Qué SÍ hace esta versión

- Usa `CREATE TABLE IF NOT EXISTS`.
- Usa `ALTER TABLE ADD COLUMN` solo si falta una columna.
- Crea respaldo automático antes de migrar.
- Permite descargar backup manual desde:
  - Exportar Excel / Reportes > Backup base de datos.

## Usuarios iniciales

Solo se crean si no existen.

```txt
Admin:
usuario: admin
clave: admin123

Operador:
usuario: operador
clave: operador123
```

Si esos usuarios ya existen en tu base, no se reemplazan.

## Antes de subir una actualización

1. Entra como admin.
2. Ve a Exportar Excel / Reportes.
3. Descarga `Backup base de datos`.
4. Sube el nuevo código.
5. Verifica que `DATABASE_PATH` siga siendo `/data/ferreteria_cloud_tool.db`.
6. Verifica que el Disk siga montado en `/data`.

## Render

El archivo `render.yaml` ya viene configurado con:

```yaml
DATABASE_PATH: /data/ferreteria_cloud_tool.db
disk:
  mountPath: /data
```

Si ya tienes un servicio creado en Render, revisa manualmente que el Disk esté agregado.
