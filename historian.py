import os
import sys
import json
from datetime import datetime
from confluent_kafka import Consumer
from pydantic import BaseModel, Field, validator
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from logger_config import get_logger

# 1. Definimos el contrato con campos opcionales para evitar que el script muera
# Definimos la clase heredando de BaseModel para habilitar la validación automática de tipos
class MessageEnvelope(BaseModel):
    
    # Identificador único del agente: Fundamental para el trazado (Lineage) del dato
    agent_id: str 
    
    # ID de sesión: Permite agrupar eventos y reconstruir el estado en sistemas distribuidos
    session_id: str 
    
    # Marca de tiempo: Usamos Field para asegurar que si no viene, se genere en UTC al vuelo
    # En Data Engineering, el timestamp es vital para el ordenamiento en Kafka/Redpanda
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # Tipo de evento: La clave del contrato. Define qué lógica debe disparar el consumidor
    event_type: str
    
    # Payload: Diccionario flexible para los datos del mensaje (el "cuerpo" del evento)
    # Usamos Any para permitir diferentes estructuras internas según el agente
    payload: Dict[str, Any]

    # Decorador que actúa como un "filtro de calidad" específico para el campo event_type
    @validator('event_type')
    def validate_event(cls, v):
        # Lista blanca (White List): Solo permitimos estos estados para evitar "Data Drift"
        allowed = ["INFO_CLASSIFIED", "QUESTION_GENERATED", "STRATEGIC_ANALYSIS_CREATED", "FINISHED"]
        
        # Si el valor (v) no está en la lista, lanzamos un error preventivo (Fail-Fast)
        if v not in allowed:
            raise ValueError(f"Evento '{v}' no reconocido por el contrato de datos")
        
        # Si es válido, devolvemos el valor para que Pydantic termine la instanciación
        return v

c = Consumer({'bootstrap.servers': 'redpanda:29092', 
    'group.id': 'moderator-group', 
    'auto.offset.reset': 'earliest'})
# ... (Configuración de Kafka igual) ...
c.subscribe(['domain.strategic.analysis'])

logger = get_logger("historian-service")


def save_to_lake(envelope: MessageEnvelope):
    try: 
        # Si timestamp es string, lo convertimos a datetime para sacar el año/mes/día
        dt = envelope.timestamp
        if isinstance(dt, str):
            try:
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            except:
                dt = datetime.utcnow() # Fallback si el formato es raro

        path = f"data/raw/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        os.makedirs(path, exist_ok=True)
        safe_session = "".join([c for c in envelope.session_id if c.isalnum() or c in ('-','_')])
        file_path = os.path.join(path, f"{safe_session}.jsonl")
        
        with open(file_path, "a", encoding='utf-8') as f:
            # Compatibilidad Pydantic V1/V2
            out_data = envelope.model_dump_json() if hasattr(envelope, 'model_dump_json') else envelope.json()
            f.write(out_data + "\n")
            f.flush()
            os.fsync(f.fileno())
        return file_path
    except Exception as e:
        print(f"🚨 Error interno guardando: {e}")
        return None


while True:
    msg = c.poll(1.0)
    if msg is None: continue
    
    try:
        raw_json = json.loads(msg.value().decode('utf-8'))
        
        # LOG de depuración: Descomenta esto para ver qué llega exactamente
        # print(f"DEBUG: Recibido -> {raw_json}")

        if "metadata" in raw_json:
            flat_data = {**raw_json["metadata"], "payload": raw_json["payload"]}
        else:
            flat_data = raw_json

        # VALIDACIÓN
        validated_envelope = MessageEnvelope(**flat_data)
        # Log con contexto adicional (session_id)
        session_id = validated_envelope.session_id
        logger.info(f"Guardando evento en Data Lake", extra={'session_id': session_id})
        ruta = save_to_lake(validated_envelope)
        if ruta:
            print(f"💾 [OK] {validated_envelope.event_type} -> {ruta}")

    except Exception as e:
        # Aquí verás exactamente por qué Pydantic rechaza el mensaje
        print(f"⚠️ ERROR DE VALIDACIÓN: {e}")
        print(f"Contenido problemático: {raw_json}")