# Script 57_migtelmex_bkp_report_oracle

## Descripción

Este script permite obtener los respaldos de bases de datos **Oracle** en Oracle Cloud Infrastructure (OCI) generados el día anterior, recorriendo todos los compartimientos activos. El resultado se guarda en un archivo CSV y se registra en un archivo de log.

## Funcionalidades

- Autenticación mediante archivo de configuración OCI (`~/.oci/config`) y perfil específico.
- Recorrido de todos los compartimientos activos dentro del tenant.
- Obtención de DB Homes y sus bases de datos correspondientes.
- Consulta de respaldos por base de datos.
- Filtrado de respaldos por fecha (solo los generados ayer).
- Generación de archivo CSV con la siguiente información:
  - Nombre del compartimiento
  - Nombre de la base de datos
  - Nombre del respaldo
  - Tamaño (GB)
  - Tipo de respaldo
  - Estado
  - Fecha y hora de inicio del respaldo
  - Fecha y hora de finalización
  - OCID del respaldo
- Log de eventos y errores almacenado en archivo.

## Librerías externas necesarias

* [`oci`](https://pypi.org/project/oci/): Oracle Cloud Infrastructure SDK for Python.

## Uso
```bash
pip install -r requirements.txt
python 57_migtelmex_bkp_report_oracle.py
```

## Recomendaciones

* Asegúrate de tener configurado tu archivo `~/.oci/config` con la clave del tenant deseado.
* Asegúrate de tener permisos adecuados para consultar los recursos del tenancy.