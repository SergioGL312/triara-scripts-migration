import os
import time
import logging
from pathlib import Path
from typing import List, Tuple

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

DIRECTORIES = [
    "/home/opc/backup-report/rnum/",
    "/home/backup-report/reports/telmex/"
]

FILE_CONFIGS = [
    ("backups-db-oracle", 7),
    ("backups-db-mysql", 7)
]

def validate_directory(directory: str) -> Path:
    path = Path(directory)
    
    if not path.exists():
        raise FileNotFoundError(f"Directorio no encontrado: {directory}")
    if not path.is_dir():
        raise NotADirectoryError(f"La ruta no es un directorio: {directory}")
    if not os.access(directory, os.R_OK | os.W_OK):
        raise PermissionError(f"Permisos insuficientes para: {directory}")
    
    return path

def delete_old_files(directory: Path, file_prefix: str, max_to_keep: int) -> None:
    try:
        logger.debug(f"Procesando directorio: {directory}")
        
        file_pattern = f"{file_prefix}-*"
        matching_files = list(directory.glob(file_pattern))
        
        if not matching_files:
            logger.debug(f"No se encontraron archivos con patrón: {file_pattern}")
            return
        
        cutoff_time = time.time() - (6 * 24 * 60 * 60)
        old_files = [f for f in matching_files if f.stat().st_mtime < cutoff_time]
        
        old_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        if len(old_files) > max_to_keep:
            files_to_delete = old_files[max_to_keep:]
            
            for file_path in files_to_delete:
                try:
                    file_path.unlink()
                    logger.info(f"Eliminado: {file_path}")
                except OSError as e:
                    logger.error(f"Error al eliminar {file_path}: {e}")
        else:
            logger.debug(f"No hay suficientes archivos antiguos para eliminar ({len(old_files)} <= {max_to_keep})")
    
    except Exception as e:
        logger.error(f"Error procesando {directory}: {e}")
        raise

def main() -> None:
    try:
        logger.info("Iniciando limpieza de backups antiguos")
        for dir_path in DIRECTORIES:
            try:
                validated_dir = validate_directory(dir_path)
                for file_prefix, max_to_keep in FILE_CONFIGS:
                    delete_old_files(validated_dir, file_prefix, max_to_keep)
            except (FileNotFoundError, PermissionError) as e:
                logger.warning(str(e))
                continue
        
        logger.info("Proceso de limpieza completado")
    
    except Exception as e:
        logger.error(f"Error en el proceso principal: {e}")
        raise

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Error crítico: {e}")
        exit(1)
    exit(0)