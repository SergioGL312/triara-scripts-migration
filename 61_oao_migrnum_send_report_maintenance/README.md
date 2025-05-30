# Script 61_oao_migrnum_send_report_maintenance

## Descripción

Este script automatiza el envío del reporte semanal de **mantenimientos programados en Exadata** para el proyecto de migración a Oracle Cloud (OCI), región 2. Evalúa el contenido de un archivo CSV que lista los mantenimientos y envía un correo en función del estado del mismo.

## Funcionalidades

- Genera un rango de fechas para el reporte: desde hoy a las 19:00 hasta dentro de 7 días.
- Verifica si existe el archivo de reporte.
- Analiza el contenido del archivo:
  - Si contiene la frase `"No existen mantenimientos programados."`, envía un correo sin adjuntos notificando que no hay mantenimientos.
  - Si contiene información de mantenimientos, lo adjunta al correo.
  - Si no existe el archivo, envía un mensaje de error.

## Configuración

```python
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "tu_correo@gmail.com"
SMTP_PASSWORD = "tu_contraseña_de_aplicación"
```

## Uso

```bash
python 61_oao_migrnum_send_report_maintenance.py
```

## Recomendaciones

* Verifica que los reportes existan en la ruta correcta antes de ejecutar el script.
* Asegúrate de tener habilitado el acceso SMTP y una contraseña de aplicación para el correo remitente si usas Gmail.