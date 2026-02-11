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

# Memoria para consultar el estado actual
memoria = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Configuración Kafka
p = Producer({'bootstrap.servers': 'redpanda:29092'})
c = Consumer({
    'bootstrap.servers': 'redpanda:29092', 
    'group.id': 'moderator-group', 
    'auto.offset.reset': 'earliest'
})

# Escuchamos el análisis estratégico final
c.subscribe(['domain.strategic.analysis'])

def evaluar_con_matriz_ddd(contexto):
    """
    Usa el LLM para evaluar si la 'calidad' del conocimiento es suficiente.
    """
    prompt_critico = f"""
    Eres un auditor de arquitectura DDD. Tu objetivo es determinar si la entrevista 
    ha alcanzado la 'Saturación de Información'.

    CONTEXTO RECOGIDO:
    {contexto}

    CRITERIOS DE CIERRE:
    1. ¿Están claros los límites del sistema (lo que NO hace)?
    2. ¿Se identifican al menos dos áreas de lenguaje ubicuo distintas (potenciales Bounded Contexts)?
    3. ¿Se entiende el flujo de valor principal?

    Si la respuesta es SÍ a todo, responde: "TERMINAR".
    Si falta algo, indica específicamente qué concepto falta profundizar (ej: "Faltan entidades del proceso de cobro").
    """
    
    try:
        res = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "system", "content": prompt_critico}]
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        print(f"🚨 Error en llamada a Groq: {e}", flush=True)
        return "ERROR_LLM"

print("⚖️ MODERADOR: Vigilando la calidad del modelo DDD...", flush=True)

while True:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error Kafka: {msg.error()}", flush=True)
        continue
    
    try:
        # --- 1. DESEMPAQUETADO (INPUT) ---
        data_in = json.loads(msg.value().decode('utf-8'))
        meta = data_in.get('metadata', {})
        
        session_id = meta.get('session_id', 'unknown_session')
        event_type = meta.get('event_type')

        # Filtramos: Solo nos interesa actuar cuando hay un nuevo análisis estratégico
        if event_type != "STRATEGIC_ANALYSIS_CREATED":
            continue

        print(f"🧐 Evaluando sesión: {session_id}...", flush=True)

        # --- 2. RECUPERAR CONTEXTO (Scoped por Sesión) ---
        # Consultamos Redis usando el session_id para aislar los datos
        contexto_actual = {
            "objetivos": list(memoria.smembers(f"session:{session_id}:objetivo")),
            "procesos": list(memoria.smembers(f"session:{session_id}:procesos")),
            "entidades": list(memoria.smembers(f"session:{session_id}:entities"))
        }

        # --- 3. LÓGICA DE SATURACIÓN ---
        # Solo llamamos al criterio de la IA si hay una base mínima 
        # (ej: al menos 1 proceso registrado para no gastar tokens en el primer saludo)
        if len(contexto_actual["procesos"]) > 0:
            
            veredicto = evaluar_con_matriz_ddd(json.dumps(contexto_actual))
            
            if "TERMINAR" in veredicto.upper():
                print(f"🏁 MODERADOR: Saturación alcanzada en {session_id}. Cerrando.", flush=True)
                
                # --- 4. EMPAQUETADO (OUTPUT - FINAL) ---
                # Enviamos la señal de FIN con el formato estándar
                mensaje_fin = formato_mensaje(
                    contenido="Suficiente información DDD acumulada.",
                    agent_id="moderator",
                    event_type="FINISHED", # Event Type crítico que escucha el Interviewer
                    session_id=session_id,
                    categoria="SYSTEM_STATUS"
                )
                
                p.produce('interview.status', value=json.dumps(mensaje_fin))
                p.flush()
                
            else:
                print(f"⏳ Aún hay huecos en {session_id}. Sugerencia: {veredicto}", flush=True)
                # Opcional: Podrías enviar feedback al Interviewer
                # p.produce('moderator.feedback', value=json.dumps(formato_mensaje(veredicto, "moderator", "FEEDBACK", session_id)))

    except Exception as e:
        print(f"🚨 Error en Moderador: {e}", flush=True)