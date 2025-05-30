# Script 52_migrnum_bkp_report_oracle

## Descripción

Este script genera un **reporte en formato CSV** que detalla los **backups realizados el día anterior** para bases de datos MySQL dentro de **todos los compartimentos activos** del tenant especificado en Oracle Cloud Infrastructure (OCI).

Incluye información detallada como:

* Nombre del compartimento
* Nombre de la base de datos
* Nombre del backup
* Tamaño en GB
* Tipo
* Estado
* Tiempos de inicio y fin
* OCID del backup

## Librerías externas necesarias

* [`oci`](https://pypi.org/project/oci/): Oracle Cloud Infrastructure SDK for Python.

## Uso
```bash
pip install -r requirements.txt
python 52_migrnum_bkp_report_oracle.py <TENANT_NAME>
```

* Si no se especifica el tenant, usará `"DEFAULT"` del archivo `~/.oci/config`.

## Recomendaciones

* Asegúrate de tener configurado tu archivo `~/.oci/config` con la clave del tenant deseado.
* Asegúrate de tener permisos adecuados para consultar los recursos del tenancy.
