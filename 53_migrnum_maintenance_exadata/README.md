# Script 53_migrnum_maintenance_exadata

## Descripción
Este script genera un **reporte en formato CSV** que detalla los **mantenimientos programados para infraestructuras Exadata en Oracle Cloud Infrastructure (OCI)** dentro de **todos los compartimentos activos** del tenant especificado.

Incluye información detallada como:

*Nombre del Exadata
*Tipo de mantenimiento
*Hora programada en UTC
*Hora programada en horario de Ciudad de México
*Tiempo estimado de parchado
*Los datos del mantenimiento programado abarcan un rango de 7 días a partir de las 7:00 p.m. (hora UTC) del día actual.

## Librerías externas necesarias

* [`oci`](https://pypi.org/project/oci/): Oracle Cloud Infrastructure SDK for Python.

## Uso
```bash
pip install -r requirements.txt
python 53_migrnum_exadata_maintenance_report.py <TENANT_NAME>
```

* Si no se especifica el tenant, usará `"DEFAULT"` del archivo `~/.oci/config`.

## Recomendaciones

* Asegúrate de tener configurado tu archivo `~/.oci/config` con la clave del tenant deseado.
* Asegúrate de tener permisos adecuados para consultar los recursos del tenancy.