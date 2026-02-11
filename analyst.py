import os
import sys
import redis
import json
from datetime import datetime
from groq import Groq
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

# --- 1. FUNCIÓN DE ESTANDARIZACIÓN (El Sobre) ---
def formato_mensaje(contenido, agent_id, event_type, session_id, categoria=None):
    return {
        "metadata": {
            "agent_id": agent_id,
            "timestamp": datetime.now().isoformat(),
            "session_id": session_id,
            "event_type": event_type
        },
        "payload": {
            "categoria": categoria,
            "contenido": contenido
        }
    }

sys.stdout.reconfigure(line_buffering=True)
load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

# Conexión a Redis
memoria = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Configuración Kafka
p = Producer({'bootstrap.servers': 'redpanda:29092'})
c = Consumer({
    'bootstrap.servers': 'redpanda:29092', 
    'group.id': 'analyst-final-group', 
    'auto.offset.reset': 'earliest'
})

# Escuchamos los datos limpios y estandarizados del Pre-Analyst
c.subscribe(['domain.preprocessed'])

print("🏗️ STRATEGIC-ANALYST: Esperando sobres en 'domain.preprocessed'...", flush=True)

while True:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error Kafka: {msg.error()}", flush=True)
        continue
    
    try:
        # --- 1. DESEMPAQUETADO (INPUT) ---
        # Leemos el sobre que envió el Pre-Analyst
        data_in = json.loads(msg.value().decode('utf-8'))
        
        # Extraemos metadatos vitales
        meta = data_in.get('metadata', {})
        payload = data_in.get('payload', {})
        
        session_id = meta.get('session_id', 'unknown_session')
        
        # El contenido del payload es el diccionario: {"Procesos": "Descripción..."}
        dato_contenido = payload.get('contenido')
        
        # Identificamos la llave (Objetivo, Procesos o Entities)
        categoria_recibida = list(dato_contenido.keys())[0]
        texto_real = dato_contenido[categoria_recibida]

        print(f"📥 Analizando '{categoria_recibida}' de sesión {session_id}...", flush=True)

        # --- 2. GUARDAR EN REDIS (Deduplicación por Sesión) ---
        # IMPORTANTE: Usamos session_id en la clave para no mezclar entrevistas
        key_redis = f"session:{session_id}:{categoria_recibida.lower()}"
        memoria.sadd(key_redis, texto_real)

        # --- 3. RECUPERAR CONTEXTO (Scoped por Sesión) ---
        # Solo recuperamos lo que pertenece a ESTA entrevista
        todos_los_objetivos = list(memoria.smembers(f"session:{session_id}:objetivo"))
        todos_los_procesos = list(memoria.smembers(f"session:{session_id}:procesos"))
        todas_las_entidades = list(memoria.smembers(f"session:{session_id}:entities"))

        # --- 4. PROMPT DE ANÁLISIS ESTRATÉGICO ---
        prompt_final = f"""
        Eres un Arquitecto DDD. Tu misión es analizar este nuevo dato dentro del contexto global.

        DATO NUEVO RECIBIDO ({categoria_recibida}): 
        "{texto_real}"

        CONTEXTO ACUMULADO DE LA SESIÓN '{session_id}':
        - Objetivos: {todos_los_objetivos}
        - Procesos: {todos_los_procesos}
        - Entidades: {todas_las_entidades}

        TAREA:
        1. Clasifica este dato en [Core | Supporting | Generic] basándote en su importancia para los Objetivos.
        2. Si es un 'Proceso', indica cuántos Bounded Contexts atraviesa.
        3. Si es un 'Objetivo', diferencia entre Valor de Empresa (Común) y Valor de Usuario (Personal).
        4. Define Requisitos Funcionales breves (usando el dato + tu conocimiento experto).
        
        Responde de forma concisa.
        """

        res = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "system", "content": prompt_final}]
        )

        analisis = res.choices[0].message.content
        print(f"\n--- ANÁLISIS ESTRATÉGICO ({categoria_recibida}) ---\n{analisis}", flush=True)
        
        # --- 5. EMPAQUETADO (OUTPUT) ---
        # Enviamos el análisis final empaquetado para el Historian y el Moderador
        mensaje_salida = formato_mensaje(
            contenido=analisis,
            agent_id="analyst-final",
            event_type="STRATEGIC_ANALYSIS_CREATED", # Event Type claro para el Moderador
            session_id=session_id,
            categoria=categoria_recibida # Útil si el Historian quiere filtrar por categoría
        )

        p.produce('domain.strategic.analysis', value=json.dumps(mensaje_salida))
        p.flush()

    except Exception as e:
        print(f"🚨 Error en Strategic-Analyst: {e}", flush=True)
        # Opcional: Podrías enviar un evento de ERROR a un tópico de logs