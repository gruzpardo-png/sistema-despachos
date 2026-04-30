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


## v4.4 Ventas IA Elias

Incluye:

- Módulo Ventas / Cotización IA.
- Asistente de ventas Elias con OpenAI.
- Entrada por texto y por imagen.
- Importador Excel de maestra de productos.
- Cálculo de:
  - venta bruta
  - venta neta estimada
  - precio compra neto
  - contribución en pesos
  - margen porcentual
  - stock disponible
- Registro de última actualización de productos.
- Exportación de cotización a Excel.
- Hora del sistema configurada para Chile (`America/Santiago`).

### Variables de entorno en Render

```txt
OPENAI_API_KEY=tu_api_key
OPENAI_MODEL=gpt-5.4-mini
IVA_RATE=0.19
DATABASE_PATH=/data/ferreteria_cloud_tool.db
```

Si tu cuenta no tiene acceso a `gpt-5.4-mini`, cambia `OPENAI_MODEL` por el modelo disponible en tu panel de OpenAI.

### Columnas esperadas para importar productos

```txt
Código Producto
Descripción
Precio Compra Neto
Precio Venta Bruto
Stock
Activo
```

También soporta planillas con más columnas, como la exportación de Facturación.cl/ERP.


## v4.4.1 Disk Safe

Corrige diagnóstico de SQLite en Render.

Si ves:
sqlite3.OperationalError: unable to open database file

Revisa:
DATABASE_PATH=/data/ferreteria_cloud_tool.db
Disk Mount Path=/data

Si usas otro mount path, por ejemplo /var/data:
DATABASE_PATH=/var/data/ferreteria_cloud_tool.db

Ruta admin de diagnóstico:
https://tu-dominio/debug-db


## v4.5 Ventas Chat Elias

Cambios:
- Elias ahora funciona como chat conversacional para vendedores.
- La cotización NO se genera automáticamente.
- El vendedor conversa con Elias, adjunta imagen o pega lista, y luego presiona "Generar cotización".
- Matching de productos más estricto para evitar productos equivocados.
- Las coincidencias dudosas quedan como REVISAR y no se suman al total.
- Agrega tablas ventas_chat_sesiones y ventas_chat_mensajes.
- No borra base de datos ni tablas existentes.


## v4.5.1 Fix OpenAI

Corrige el problema de despliegue cuando Render no instala la librería OpenAI.

`requirements.txt` debe contener:

```txt
openai>=1.100.0
```

Después de subir esta versión, en Render ejecutar:

```txt
Manual Deploy -> Clear build cache & deploy
```

Variables necesarias:

```txt
OPENAI_API_KEY=tu_api_key
OPENAI_MODEL=gpt-5.4-mini
DATABASE_PATH=/data/ferreteria_cloud_tool.db
IVA_RATE=0.19
```


## v4.6 Chat Ventas Integrado

- Conversación de Elias rediseñada como chat moderno.
- Los mensajes se muestran en orden natural y el chat baja automáticamente al último mensaje.
- La cotización generada aparece dentro del mismo chat como tarjeta comercial.
- Desde la tarjeta se puede abrir detalle, descargar PDF o exportar Excel.
- El vendedor puede seguir conversando después de generar la cotización.
- Agrega exportación PDF con formato comercial básico.
- Matching estricto: líneas dudosas quedan como REVISAR y no se suman al total.
