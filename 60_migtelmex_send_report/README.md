# Script 60_migtelmex_send_report

## Descripción

Este script envía automáticamente un correo con los reportes de respaldo de bases de datos **Oracle** y **MySQL** correspondientes al día anterior. Si los archivos existen, se adjuntan al correo; en caso contrario, se notifica el error indicando que no se generaron los reportes esperados.

## Funcionalidades

- Envío de correos mediante SMTP (servidor Gmail).
- Adjunta archivos de respaldo si están disponibles.
- Reporta errores si faltan uno o ambos archivos.
- Configurable para múltiples destinatarios y BCC.

## Configuración

Asegúrate de definir correctamente las siguientes variables en el script:

```python
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "tu_correo@gmail.com"
SMTP_PASSWORD = "tu_contraseña_de_aplicación"
```

## Uso

```bash
python 60_migtelmex_send_report.py
```

## Recomendaciones

* Verifica que los reportes existan en la ruta correcta antes de ejecutar el script.
* Asegúrate de tener habilitado el acceso SMTP y una contraseña de aplicación para el correo remitente si usas Gmail.
