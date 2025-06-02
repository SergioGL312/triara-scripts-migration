# Script 62_pase_maintenance_exadata

## Descripción

Este script permite obtener información sobre mantenimientos programados de infraestructuras Exadata en Oracle Cloud Infrastructure (OCI) dentro de los próximos 7 días. Recorre todos los compartimientos activos del tenant, identifica las infraestructuras Exadata disponibles, y consulta los mantenimientos planificados. El resultado se guarda en un archivo CSV y se registra en un archivo de log.

## Funcionalidades

- Autenticación mediante archivo de configuración OCI (`~/.oci/config`) y perfil específico.
- Recorrido de todos los compartimientos activos dentro del tenancy.
- Detección de infraestructuras Exadata disponibles.
- Consulta de mantenimientos programados en los próximos 7 días.
- Conversión de tiempo UTC a horario de México.
- Cálculo de duración estimada de mantenimiento.
- Generación de archivo CSV con la siguiente información:
  - Nombre de la infraestructura Exadata
  - Tipo de mantenimiento
  - Fecha y hora programada (UTC)
  - Fecha y hora programada (hora México)
  - Tiempo estimado de mantenimiento
- Registro de eventos y errores en un archivo de log.

## Librerías externas necesarias

* [`oci`](https://pypi.org/project/oci/): Oracle Cloud Infrastructure SDK for Python.

## Uso

```bash
pip install -r requirements.txt
python 62_pase_maintenance_exadata.py <DEFAULT>
```

* Si no se especifica el tenant, usará `"DEFAULT"` del archivo `~/.oci/config`.

## Recomendaciones

* Asegúrate de tener configurado tu archivo `~/.oci/config` con la clave del tenant deseado.
* Asegúrate de tener permisos adecuados para consultar los recursos del tenancy.