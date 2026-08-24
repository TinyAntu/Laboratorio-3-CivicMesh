import os
import json
import uuid
import time
from github import Github, Auth
from google import genai

# Directorios de sistema/entornos que se ignorarán
DIRECTORIOS_IGNORADOS = {
    ".git", 
    ".github", 
    "__pycache__", 
    ".venv", 
    "venv", 
    "env", 
    ".pytest_cache", 
    "build", 
    "dist"
}

CONFIG_PATH = os.path.join("config", "bug_agent_config.json")
DEFAULT_CONFIG = {
    "include_directories": ["network", "domains"],
    "exclude_files": ["__init__.py"],
    "max_files_per_run": 5,
    "min_lines_per_file": 15,
    "delay_between_requests_sec": 2,
    "scan_mode": "priority"
}

def cargar_configuracion():
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                config = json.load(f)
                print(f"Configuración cargada desde {CONFIG_PATH}")
                return {**DEFAULT_CONFIG, **config}
        except Exception as e:
            print(f"Error al leer {CONFIG_PATH}, usando configuración por defecto: {e}")
    else:
        print(f"No se encontró {CONFIG_PATH}, usando configuración por defecto.")
    return DEFAULT_CONFIG

def analizar_codigo_con_ia(nombre_archivo, contenido_codigo):
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: Falta GEMINI_API_KEY en el entorno.")
        return {"encontro_bug": False}

    client = genai.Client(api_key=api_key)
    
    prompt = f"""
    Eres un Agente Revisor de Bugs para el proyecto CivicMesh (Framework P2P Publish/Subscribe en Python).
    Analiza el siguiente código fuente Python en búsqueda de errores de implementación, incluyendo:
    - Problemas de concurrencia o sockets/red en la capa Gossip o Pub/Sub.
    - Manejo incorrecto o falta de semillas (seeds) para reproducibilidad estocástica.
    - Errores en la función 'should_forward' (TTL, prioridad, fanout).
    - Desviaciones en las fórmulas de percepción/memoria EMA o generadores estocásticos (Poisson).
    
    Clasifica cualquier bug encontrado en dos categorías:
    1. Mecánico: Errores sintácticos simples, typos de variables, imports no utilizados o formato.
    2. Complejo (Humano): Afecta la semántica del protocolo P2P/Gossip, la lógica de simulación, la reproducibilidad de semillas o las fórmulas matemáticas.

    Devuelve ÚNICAMENTE un JSON estricto con este formato:
    {{
        "encontro_bug": true/false,
        "es_mecanico": true/false,
        "descripcion": "Descripción concisa del problema detectado",
        "codigo_corregido": "Si es_mecanico es true, entrega el código fuente Python completo y corregido. Si es false, deja un string vacío."
    }}

    Archivo ({nombre_archivo}):
    {contenido_codigo}
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-3.6-flash',
            contents=prompt
        )
        texto_limpio = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(texto_limpio)
    except Exception as e:
        print(f"Error detallado de la IA en {nombre_archivo}: {e}")
        return {"encontro_bug": False}

def obtener_archivos_python_recursivo(repo, path=""):
    archivos_python = []
    try:
        contenidos = repo.get_contents(path)
        for contenido in contenidos:
            if contenido.type == "dir":
                if contenido.name not in DIRECTORIOS_IGNORADOS:
                    archivos_python.extend(obtener_archivos_python_recursivo(repo, contenido.path))
            elif contenido.name.endswith(".py"):
                archivos_python.append(contenido)
    except Exception as e:
        print(f"Error al explorar la ruta '{path}': {e}")
    return archivos_python

def obtener_archivos_objetivo(repo, config):
    directorios_incluidos = config.get("include_directories", [])
    archivos_excluidos = set(config.get("exclude_files", []))
    
    candidatos = []
    
    if directorios_incluidos:
        print(f"Escaneando directorios configurados: {directorios_incluidos}")
        for dir_target in directorios_incluidos:
            candidatos.extend(obtener_archivos_python_recursivo(repo, path=dir_target))
    else:
        print("Escaneando todo el repositorio dinámicamente...")
        candidatos = obtener_archivos_python_recursivo(repo, path="")
        
    # Filtrar por nombre excluido
    filtrados = [f for f in candidatos if os.path.basename(f.name) not in archivos_excluidos]
    
    return filtrados

def main():
    token = os.getenv("GITHUB_TOKEN")
    repo_name = os.getenv("GITHUB_REPOSITORY")
    
    if not token or not repo_name:
        print("Error: Faltan variables de entorno de GitHub (GITHUB_TOKEN, GITHUB_REPOSITORY).")
        exit(1)

    config = cargar_configuracion()
    max_archivos = config.get("max_files_per_run", 5)
    min_lineas = config.get("min_lines_per_file", 15)
    delay_sec = config.get("delay_between_requests_sec", 2)

    auth = Auth.Token(token)
    g = Github(auth=auth)
    repo = g.get_repo(repo_name)
    
    archivos_candidatos = obtener_archivos_objetivo(repo, config)
    
    if not archivos_candidatos:
        print("No se encontraron archivos .py candidatos para analizar.")
        return

    print(f"Se encontraron {len(archivos_candidatos)} archivos elegibles. Se analizarán un máximo de {max_archivos}.")
    
    archivos_procesados = 0
    
    for archivo in archivos_candidatos:
        if archivos_procesados >= max_archivos:
            print(f"Se alcanzó el límite configurado de {max_archivos} archivos por ejecución para proteger cuota de tokens.")
            break

        try:
            contenido = archivo.decoded_content.decode("utf-8")
        except Exception as e:
            print(f"No se pudo leer {archivo.path}: {e}")
            continue

        num_lineas = len(contenido.splitlines())
        if num_lineas < min_lineas:
            print(f"Saltando {archivo.path} (posee {num_lineas} líneas, menor al mínimo configurado de {min_lineas}).")
            continue

        archivos_procesados += 1
        print(f"[{archivos_procesados}/{max_archivos}] Analizando {archivo.path} ({num_lineas} líneas)...")
        
        analisis = analizar_codigo_con_ia(archivo.path, contenido)
        
        # Pausa para proteger Rate Limit (RPM/TPM)
        if delay_sec > 0:
            time.sleep(delay_sec)

        if not analisis.get("encontro_bug", False):
            print(f"✅ Sin bugs en {archivo.path}")
            continue
            
        print(f"⚠️ Bug detectado en {archivo.path}.")
        
        if analisis.get("es_mecanico", False):
            nuevo_codigo = analisis.get("codigo_corregido", "")
            if not nuevo_codigo:
                print("El análisis indicó bug mecánico pero no entregó código corregido.")
                continue

            rama_base = repo.get_branch("main")
            nueva_rama = f"auto-fix-bug-{uuid.uuid4().hex[:6]}"
            repo.create_git_ref(ref=f"refs/heads/{nueva_rama}", sha=rama_base.commit.sha)
            
            repo.update_file(
                path=archivo.path,
                message=f"fix(agent): corrección mecánica automática en {archivo.path}",
                content=nuevo_codigo,
                sha=archivo.sha,
                branch=nueva_rama
            )
            
            pr = repo.create_pull(
                title=f"fix(agent): Corrección automática de código en {archivo.path}",
                body=f"El Agente de Bugs detectó un error mecánico en `{archivo.path}` y propone este parche.\n\n**Descripción:** {analisis.get('descripcion')}",
                head=nueva_rama,
                base="main"
            )
            try:
                pr.add_to_labels("agent: auto-fix", "bug")
            except Exception:
                pass
            print(f"PR de corrección automática creado: {pr.html_url}")
            
        else:
            issue = repo.create_issue(
                title=f"⚠️ Bug detectado en {archivo.path}",
                body=f"**Requiere intervención humana**\n\n**Descripción:** {analisis.get('descripcion', 'Sin descripción')}\n\n*Nota: El problema afecta la semántica o lógica del protocolo, por lo que no se aplicó un fix automático.*",
                labels=["bug", "agent"]
            )
            print(f"Issue creado para revisión humana: {issue.html_url}")

if __name__ == "__main__":
    main()