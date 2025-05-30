# Script 51_migrnum_bkp_report_mysql

## Descripción

Este script genera un reporte en formato CSV con los backups de bases de datos MySQL realizados en el día anterior dentro de todos los compartimentos activos del tenant especificado en Oracle Cloud Infrastructure (OCI). La información incluye nombre del compartimento, nombre de la base de datos, nombre del último backup, tamaño, tipo, estado, fecha de inicio y el OCID.

## Librerías externas necesarias

* [`oci`](https://pypi.org/project/oci/): Oracle Cloud Infrastructure SDK for Python.

## Uso
```bash
pip install -r requirements.txt
python 51_migrnum_bkp_report_mysql.py <TENANT_NAME>
````

* Si no se especifica el tenant, usará `"DEFAULT"` del archivo `~/.oci/config`.

## Recomendaciones

* Asegúrate de tener configurado tu archivo `~/.oci/config` con la clave del tenant deseado.
* Asegúrate de tener permisos adecuados para consultar los recursos del tenancy.
