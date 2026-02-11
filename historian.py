import os
import sys
import json
from datetime import datetime
from confluent_kafka import Consumer
from dotenv import load_dotenv

# Configuración de salida
sys.stdout.reconfigure(line_buffering=True)
load_dotenv()

# Configuración Kafka
c = Consumer({
    'bootstrap.servers': 'redpanda:29092',
    'group.id': 'historian-group',
    'auto.offset.reset': 'earliest'
})

# Escuchamos el análisis estratégico. 
# OPCIONAL: Podrías escuchar también 'ai.questions' si quieres guardar el chat completo.
c.subscribe(['domain.strategic.analysis'])

LOG_DIR = "./output"
os.makedirs(LOG_DIR, exist_ok=True)

def escribir_en_disco_por_sesion(envelope):
    """
    Escribe el evento en un archivo específico para la sesión.
    Esto permite tener trazabilidad aislada por cada entrevista.
    """
    try:
        # 1. Extraemos el ID de sesión para nombrar el archivo
        meta = envelope.get('metadata', {})
        session_id = meta.get('session_id', 'unknown_session')
        
        # Sanitizamos el nombre del archivo para evitar errores en SO
        safe_session_id = "".join([c for c in session_id if c.isalnum() or c in ('-','_')])
        filename = f"{LOG_DIR}/history_{safe_session_id}.jsonl"

        # 2. Escribimos el sobre completo (Metadata + Payload)
        # Modo 'a' (append) para añadir al historial sin borrar lo anterior
        with open(filename, 'a', encoding='utf-8') as f:
            # json.dumps asegura que se escriba en una sola línea (JSONL standard)
            linea = json.dumps(envelope, ensure_ascii=False)
            f.write(linea + "\n")
            
            # 3. Persistencia Sólida (Flush + Fsync)
            f.flush()
            os.fsync(f.fileno()) 
            
        return True, filename
    except Exception as e:
        print(f"🚨 Error escribiendo en disco: {e}", flush=True)
        return False, None

print(f"📚 HISTORIADOR: Listo para archivar en carpeta '{LOG_DIR}/'...", flush=True)

while True:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error Kafka: {msg.error()}", flush=True)
        continue

    try:
        # --- 1. DESEMPAQUETADO ---
        # Ahora recibimos un JSON Envelope, no texto plano
        mensaje_json = msg.value().decode('utf-8')
        envelope = json.loads(mensaje_json)
        
        # Validamos que sea un mensaje con la estructura nueva
        if 'metadata' not in envelope:
            # Soporte legacy por si llega un mensaje viejo
            envelope = {
                "metadata": {"session_id": "legacy_data", "timestamp": datetime.now().isoformat()},
                "payload": {"contenido": envelope}
            }

        # --- 2. PERSISTENCIA (ACTING) ---
        exito, ruta_fichero = escribir_en_disco_por_sesion(envelope)
        
        if exito:
            agent = envelope['metadata'].get('agent_id', 'unknown')
            print(f"💾 Evento de [{agent}] guardado en: {ruta_fichero}", flush=True)

    except json.JSONDecodeError:
        print(f"⚠️ Error: Llegó un mensaje que no es JSON válido. Ignorando.", flush=True)
    except Exception as e:
        print(f"🚨 Error crítico en Historian: {e}", flush=True)