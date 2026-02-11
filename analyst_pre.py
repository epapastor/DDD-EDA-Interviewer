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

def validar_un_solo_concepto(res_texto):
    """Valida que la IA devuelva un JSON con exactamente una de las llaves permitidas."""
    try:
        res_dict = json.loads(res_texto)
        categorias = ["Objetivo", "Procesos", "Entities"]
        llaves_encontradas = [k for k in categorias if k in res_dict]
        
        if len(llaves_encontradas) == 1:
            # Retorna: Éxito, Diccionario completo, Nombre de la categoría
            return True, res_dict, llaves_encontradas[0]
        elif len(llaves_encontradas) > 1:
            return False, f"Error: Devolviste múltiples llaves ({llaves_encontradas}). Solo quiero una.", None
        else:
            return False, f"Error: No usaste ninguna de las llaves: {categorias}", None
    except json.JSONDecodeError:
        return False, "Error: El formato no es un JSON válido.", None

# Configuración de salida y carga de entorno
sys.stdout.reconfigure(line_buffering=True)
load_dotenv()

# Inicialización de clientes
client = Groq(api_key=os.getenv("GROQ_API_KEY"))
memoria = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)

# Configuración Kafka
p = Producer({'bootstrap.servers': 'redpanda:29092'})
c = Consumer({
    'bootstrap.servers': 'redpanda:29092', 
    'group.id': 'pre-analyst-group', 
    'auto.offset.reset': 'earliest'
})
c.subscribe(['user.answers'])

print("🧹 PRE-ANALYST: Esperando sobres en 'user.answers'...", flush=True)

prompt_base = '''Eres un extractor de información experto en DDD.
Recibirás información del usuario y debes clasificarla en UNA de estas tres categorías:
1- Objetivo: Valor que entrega o no entrega la empresa (límites).
2- Procesos: Flujos del negocio de principio a fin.
3- Entities: Conceptos, lenguaje ubicuo y sobre qué cosas se decide.

TU SALIDA DEBE SER UN JSON CON ESTE FORMATO:
{"NombreDeLaCategoria": "Información sintetizada"}

REGLA CRÍTICA: Solo puedes usar una llave: "Objetivo", "Procesos" o "Entities".
'''

while True:
    msg = c.poll(1.0)
    if msg is None: continue
    if msg.error():
        print(f"Error Kafka: {msg.error()}", flush=True)
        continue
    
    try:
        # --- DESEMPAQUETADO DEL SOBRE ENTRANTE ---
        # Asumimos que el input ya viene con el formato estándar (metadata + payload)
        data_in = json.loads(msg.value().decode('utf-8'))
        
        # Extraemos lo que necesitamos para trabajar
        user_text = data_in['payload']['contenido']
        session_id = data_in['metadata']['session_id'] # ¡Vital para no perder el hilo!
        
        print(f"📥 Procesando mensaje de sesión: {session_id}", flush=True)

    except Exception as e:
        # Fallback por si llega texto plano antiguo o sucio
        print(f"⚠️ Formato no estándar recibido: {e}. Tratando como texto plano...", flush=True)
        user_text = msg.value().decode('utf-8')
        session_id = "unknown_session"

    intentos = 0
    max_intentos = 3
    feedback_error = ""
    exito = False

    # --- BUCLE DE SELF-CORRECTION ---
    while intentos < max_intentos and not exito:
        try:
            res = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": prompt_base + feedback_error},
                    {"role": "user", "content": user_text}
                ],
                response_format={"type": "json_object"}
            )
            
            pre_data_raw = res.choices[0].message.content
            
            # Validamos la estructura
            valido, data_dict, categoria_detectada = validar_un_solo_concepto(pre_data_raw)
            
            if valido:
                # 1. Guardar en Redis (Con namespace por sesión)
                # Ejemplo: session:user123:entidades_raw
                memoria.rpush(f"session:{session_id}:entidades_raw", json.dumps(data_dict))
                
                # 2. EMPAQUETADO DE SALIDA
                mensaje_salida = formato_mensaje(
                    contenido=data_dict,               # El diccionario completo: {"Objetivo": "..."}
                    agent_id="pre-analyst",            # Quién soy
                    event_type="INFO_CLASSIFIED",      # Qué hice
                    session_id=session_id,             # A qué entrevista pertenece
                    categoria=categoria_detectada      # Metadato extra útil para el router
                )
                
                # 3. Publicar en Kafka
                p.produce('domain.preprocessed', value=json.dumps(mensaje_salida))
                p.flush()
                
                print(f"✅ [{categoria_detectada}] Clasificado y enviado.", flush=True)
                exito = True
            else:
                intentos += 1
                feedback_error = f"\nERROR PREVIO: {data_dict}. Instrucción: {categoria_detectada}" # categoria_detectada trae el mensaje de error aquí
                print(f"⚠️ Reintento {intentos}/3...", flush=True)

        except Exception as e:
            print(f"🚨 Error en bucle LLM: {e}", flush=True)
            break

    if not exito:
        print(f"❌ Mensaje descartado tras {max_intentos} intentos fallidos.", flush=True)