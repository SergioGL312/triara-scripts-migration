# Script 54_migrnum-send-report-maintenance

## Descripción

Este script automatiza el envío por correo electrónico del **reporte de mantenimientos de Exadata** generado por el script `53_migrnum_exadata_maintenance_report.py`.

Evalúa si el archivo del reporte existe y, según su contenido:

- Envía el reporte como adjunto si hay mantenimientos programados.
- Envía un aviso sin adjunto si no hay mantenimientos.
- Envía un mensaje de error si el archivo no se generó correctamente.

---

## Información del correo

- **Servidor SMTP**: `smtp.gmail.com` (puerto 587)
- **Remitente**: `sergigl312@gmail.com` (cambiar por el remitente)
- **SMTP_PASSWORD**: `htjr gewv pyom vtbb` (generar una clave)
- **Destinatarios**:
  - Principales: definidos en la lista `TO_RECIPIENTS`
  - Copia oculta (BCC): `oracle.cloud@triara.com`
- **Asunto del correo**: incluye la fecha del rango de mantenimientos (`YYYY-MM-DD`)
- **Adjunto (opcional)**:  
  `backup-report/reports/rnum/rnum-maintenance-exadata-YYYY-MM-DD.csv`

---

### Seguridad SMTP

Actualmente, el script contiene credenciales en texto plano:

```python
SMTP_USER = "sergigl312@gmail.com"
SMTP_PASSWORD = "htjr gewv pyom vtbb"
```

## Uso
```bash
pip install -r requirements.txt
python 54_migrnum-send-report-maintenance.pu
```

