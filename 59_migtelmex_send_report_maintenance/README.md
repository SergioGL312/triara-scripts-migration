# Script: 59_migtelmex_send_report_maintenance

## Descripción

Este script genera y envía por correo electrónico un reporte de mantenimientos programados para infraestructuras Exadata. El reporte cubre un rango de 7 días a partir del día de ejecución. Se genera un archivo CSV y se adjunta al correo si hay mantenimientos detectados.

## Funcionalidades

- Generación de un archivo CSV con mantenimientos programados.
- Envío de correo electrónico utilizando el comando `mailx`.
- Incluye la opción de adjuntar el archivo generado.
- Soporte para envío con BCC.
- Verificación si existen mantenimientos o no para personalizar el mensaje.

## Flujo general

1. Se calcula la fecha actual y la de 7 días en adelante.
2. Se genera un archivo CSV con la información de mantenimiento (simulada en este caso).
3. Se envía un correo:
   - Si no hay mantenimientos, solo se manda un texto.
   - Si hay mantenimientos, se adjunta el archivo.
   - Si hay error al generar el archivo, se informa por correo.

## Uso

```bash
python 59_migtelmex_send_report_maintenance.py
```