import os
import json
import uuid
from github import Github, Auth
from google import genai

def analizar_texto_con_ia(contenido_archivo):
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        print("Error: No se encontró GEMINI_API_KEY. Saliendo...")
        exit(1)
        
    client = genai.Client(api_key=api_key)
    
    prompt = f"""
    Eres el Agente Documentador para el proyecto CivicMesh (Framework P2P Publish/Subscribe para Monitoreo Ciudadano Distribuido en Python).
    Analiza el archivo de documentación. Debes evaluar los siguientes aspectos:

    1. Errores mecánicos: Ortografía, sintaxis Markdown, enlaces rotos, formato faltante o incoherencias simples de fecha.
    2. Errores o vacíos técnicos que requieren criterio humano:
       - Si es README.md: Debe validar la presencia de secciones sobre instalación local, ejecución en Docker Compose, despliegue en Slurm (separando nodos CPU y GPU), convención del Shared FS ($CIVICMESH_RUNS/<run_id>/), semillas estocásticas (seeds), dataset de calidad del aire (SINCA/Open-Meteo), acceso al frontend de métricas, tabla de roles del equipo y evidencia de los 3 agentes de IA.
       - Si es CHANGELOG.md: Debe validar el cumplimiento estricto del estándar 'Keep a Changelog' y sus secciones (Added, Changed, Fixed, etc.).

    Devuelve ÚNICAMENTE un JSON con este formato exacto:
    {{
        "encontro_problemas": true/false,
        "es_mecanico": true/false,
        "motivo": "Explicación breve del problema",
        "contenido_corregido": "Si es_mecanico es true, pon aquí el texto completo corregido. Si es false, deja un string vacío."
    }}

    Archivo a analizar:
    {contenido_archivo}
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-3.6-flash', 
            contents=prompt
        )
        texto_limpio = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(texto_limpio)
    except Exception as e:
        print(f"Error detallado de la IA: {e}") 
        return {"encontro_problemas": False}

def main():
    token = os.getenv("GITHUB_TOKEN")
    repo_name = os.getenv("GITHUB_REPOSITORY")
    
    if not token or not repo_name:
        print("Error: Faltan variables de entorno de GitHub.")
        exit(1)

    auth = Auth.Token(token)
    g = Github(auth=auth)
    repo = g.get_repo(repo_name)
    
    archivos_objetivo = ["README.md", "CHANGELOG.md"]
    
    for ruta_archivo in archivos_objetivo:
        try:
            archivo_repo = repo.get_contents(ruta_archivo)
            contenido_actual = archivo_repo.decoded_content.decode("utf-8")
        except Exception:
            print(f"No se pudo encontrar {ruta_archivo}. Saltando...")
            continue
            
        print(f"Analizando {ruta_archivo}...")
        analisis = analizar_texto_con_ia(contenido_actual)
        
        if not analisis.get("encontro_problemas", False):
            print(f"✅ La IA no encontró problemas en {ruta_archivo}.")
            continue
            
        print(f"⚠️ Problemas encontrados en {ruta_archivo}.")
        
        if analisis.get("es_mecanico", False):
            print("El problema es mecánico. Creando fix automático...")
            nuevo_contenido = analisis["contenido_corregido"]
            
            rama_base = repo.get_branch("main")
            nombre_nueva_rama = f"auto-fix-doc-{uuid.uuid4().hex[:6]}"
            repo.create_git_ref(ref=f"refs/heads/{nombre_nueva_rama}", sha=rama_base.commit.sha)
            
            repo.update_file(
                path=archivo_repo.path,
                message=f"docs(agent): corrección automática en {ruta_archivo}",
                content=nuevo_contenido,
                sha=archivo_repo.sha,
                branch=nombre_nueva_rama
            )
            
            pr = repo.create_pull(
                title=f"Fix automático de documentación en {ruta_archivo}",
                body="El agente documentador detectó y corrigió un error mecánico.",
                head=nombre_nueva_rama,
                base="main"
            )
            
            try:
                pr.add_to_labels("agent: auto-fix")
            except Exception:
                print("No se pudo añadir la etiqueta.")
            
            print(f"PR creado con éxito: {pr.html_url}")
            
        else:
            print("El problema requiere juicio técnico. Abriendo Issue...")
            motivo = analisis.get("motivo", "Falta documentación técnica.")
            
            issue = repo.create_issue(
                title=f"Documentación deficiente en {ruta_archivo}",
                body=f"Requiere intervención humana: {motivo}",
                labels=["documentation", "agent"]
            )
            print(f"Issue creado con éxito: {issue.html_url}")

if __name__ == "__main__":
    main()