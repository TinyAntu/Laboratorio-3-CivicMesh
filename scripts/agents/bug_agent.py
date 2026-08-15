import os
import json
import uuid
from github import Github, Auth
from google import genai

# Directorios de sistema/entornos que se ignorarán en la búsqueda dinámica
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

def analizar_codigo_con_ia(nombre_archivo, contenido_codigo):
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
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
    """
    Busca archivos .py de forma completamente dinámica a partir de la raíz del repo,
    sin asumir nombres de carpetas fijas.
    """
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

def main():
    token = os.getenv("GITHUB_TOKEN")
    repo_name = os.getenv("GITHUB_REPOSITORY")
    
    if not token or not repo_name:
        print("Error: Faltan variables de entorno de GitHub (GITHUB_TOKEN, GITHUB_REPOSITORY).")
        exit(1)

    auth = Auth.Token(token)
    g = Github(auth=auth)
    repo = g.get_repo(repo_name)
    
    # Búsqueda dinámica de archivos .py desde la raíz (path="")
    todos_los_archivos_py = obtener_archivos_python_recursivo(repo, path="")
    
    if not todos_los_archivos_py:
        print("No se encontraron archivos .py en el repositorio.")
        return

    for archivo in todos_los_archivos_py:
        print(f"Analizando {archivo.path}...")
        try:
            contenido = archivo.decoded_content.decode("utf-8")
        except Exception as e:
            print(f"No se pudo leer {archivo.path}: {e}")
            continue

        analisis = analizar_codigo_con_ia(archivo.path, contenido)
        
        if not analisis.get("encontro_bug", False):
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