import os
import sys
import redis
import json
from groq import Groq
from confluent_kafka import Producer, Consumer
from dotenv import load_dotenv
from datetime import datetime

# --- UTILIDAD DE FORMATO ---
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

# Forzar logs inmediatos
sys.stdout.reconfigure(line_buffering=True)
load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))
memoria = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# --- CONFIGURACIÓN KAFKA ---
p = Producer({'bootstrap.servers': 'redpanda:29092'})

# UN SOLO CONSUMIDOR PARA TODO
# Usamos un solo consumidor para evitar problemas de sincronización
c = Consumer({
    'bootstrap.servers': 'redpanda:29092',
    'group.id': 'interviewer-group',
    'auto.offset.reset': 'earliest'
})

# Nos suscribimos a AMBOS tópicos a la vez
c.subscribe(['user.answers', 'interview.status'])

print(f"🤖 INTERVIEWER: Escuchando 'user.answers' y 'interview.status'...", flush=True)

def enviar_despedida(texto, session_destina):
    """Genera el mensaje final y lo envía a Kafka."""
    # IMPORTANTE: Pasamos session_destina como argumento, no usamos global SESSION_ID
    msg = formato_mensaje(texto, "interviewer", "INTERVIEW_CLOSED", session_destina)
    p.produce('ai.questions', value=json.dumps(msg))
    p.flush()

while True:
    # Solo hacemos UN poll. Kafka nos dará el siguiente mensaje, venga del tópico que venga.
    msg = c.poll(1.0)
    
    if msg is None: continue
    if msg.error():
        print(f"⚠️ Error Kafka: {msg.error()}", flush=True)
        continue

    try:
        # Descodificamos el mensaje
        data = json.loads(msg.value().decode('utf-8'))
        meta = data.get('metadata', {})
        payload = data.get('payload', {})
        
        # Obtenemos el tipo de evento y la sesión del mensaje actual
        event_type = meta.get('event_type')
        current_session_id = meta.get('session_id', 'unknown')

        # --- LÓGICA DE RAMIFICACIÓN (ROUTING) ---
        
        # CASO 1: ORDEN DE FINALIZACIÓN (Viene del Moderador)
        if event_type == "FINISHED":
            print(f"🏁 Orden de cierre recibida para sesión: {current_session_id}")
            enviar_despedida(
                "¡Muchas gracias! Tenemos información suficiente para el diseño DDD. El análisis estratégico ha sido guardado.", 
                current_session_id
            )
            # No hacemos break para no matar el servicio, solo 'continue' para esperar otros usuarios
            continue

        # CASO 2: RESPUESTA DEL USUARIO (Viene del Chat)
        # Importante: Verificamos que sea un mensaje de usuario real
        elif event_type == "USER_RESPONSE": 
            
            user_text = payload.get('contenido', '')
            source_agent = meta.get('agent_id', 'unknown_user')
            
            print(f"📩 Procesando respuesta de {source_agent} ({current_session_id}): '{user_text}'", flush=True)

            # 1. Recuperar historial de Redis específico de ESA sesión
            key_history = f"chat_history:{current_session_id}"
            historial_raw = memoria.lrange(key_history, -6, -1)
            
            # 2. Construir Prompt con Contexto
            prompt_sistema = f"""Eres un entrevistador experto en DDD.
            Tu objetivo es obtener información sobre:
            1- Objetivos y límites de la empresa.
            2- Procesos de negocio (fin a fin).
            3- Entidades y conceptos clave (Lenguaje Ubicuo).

            INSTRUCCIÓN: Haz UNA sola pregunta CORTA. 
            Si el usuario divaga, recondúcelo. Si falta detalle en un proceso, profundiza.
            
            HISTORIAL RECIENTE:
            {historial_raw}
            """

            # 3. Consultar a Groq
            res = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": prompt_sistema},
                    {"role": "user", "content": f"El usuario ({source_agent}) dice: {user_text}"}
                ]
            )
            pregunta_ia = res.choices[0].message.content

            # 4. Persistir en Redis
            memoria.rpush(key_history, f"USER: {user_text}")
            memoria.rpush(key_history, f"AI: {pregunta_ia}")
            
            # 5. Enviar a Kafka
            mensaje_salida = formato_mensaje(
                contenido=pregunta_ia,
                agent_id="interviewer",
                event_type="QUESTION_GENERATED",
                session_id=current_session_id 
            )

            p.produce('ai.questions', value=json.dumps(mensaje_salida))
            p.flush()
            
            print(f"💡 Nueva pregunta enviada a {current_session_id}", flush=True)

        else:
            # Ignoramos eventos que no nos interesan (ruido)
            pass

    except Exception as e:
        print(f"🚨 Error en Interviewer: {e}", flush=True)