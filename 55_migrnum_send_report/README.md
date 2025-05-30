# Script 55_migrnum_send_report

## Descripción

Este script analiza los reportes diarios de respaldos de bases de datos Oracle y MySQL para determinar si han sido exitosos o no. Posteriormente, envía un correo electrónico con el resumen del estado de los respaldos e incluye los archivos CSV como adjuntos.

El script busca archivos CSV generados el día anterior dentro de rutas predefinidas.

---


## Información del correo

- **Servidor SMTP**: `smtp.gmail.com` (puerto 587)
- **Remitente**: `sergigl312@gmail.com` (cambiar por el remitente)
- **SMTP_PASSWORD**: `htjr gewv pyom vtbb` (generar una clave)
- **Destinatarios**:
  - Principales: definidos en la lista `TO_RECIPIENTS`
  - Copia oculta (BCC): `oracle.cloud@triara.com`

---
## Funcionalidades principales

* Analiza archivos CSV de reportes de respaldo para bases de datos Oracle y MySQL.
* Clasifica los respaldos en exitosos y fallidos.
* Envía un correo con un resumen del estado de los respaldos.
* Adjunta los reportes originales en el correo.

## Uso

```bash
python 55_migrnum_send_report.py
```

## Recomendaciones

* Verifica que los reportes existan en la ruta correcta antes de ejecutar el script.
* Asegúrate de tener habilitado el acceso SMTP y una contraseña de aplicación para el correo remitente si usas Gmail.
