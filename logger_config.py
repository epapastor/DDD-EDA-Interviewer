import logging
import json
from datetime import datetime

class JsonFormatter(logging.Formatter):
    """
    Formateador personalizado para convertir logs en objetos JSON
    """
    def format(self, record):
        log_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "service": record.name,
            "message": record.getMessage(),
            "session_id": getattr(record, 'session_id', 'N/A') # Trazabilidad
        }
        # Si hay una excepción, la incluimos
        if record.exc_info:
            log_record["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_record)

def get_logger(service_name):
    logger = logging.getLogger(service_name)
    logger.setLevel(logging.INFO)
    
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    return logger