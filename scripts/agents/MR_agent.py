import os
import json
from github import Github, Auth
from google import genai

def evaluar_diff_pr_con_ia(diff_texto, pr_title, pr_body):
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return {"es_mecanico": False, "razonamiento": "Falta GEMINI_API_KEY en el entorno."}
        
    client = genai.Client(api_key=api_key) 
    
    prompt = f"""
    Eres el Agente Revisor de Merge Requests para el proyecto CivicMesh (Framework P2P Publish/Subscribe en Python).
    Analiza las diferencias (diff) y el contexto de este PR para clasificarlo según las siguientes reglas:

    - Es 'mecánico': Solo modifica documentación, formato, refactorizaciones cosméticas o comentarios que NO alteran el comportamiento del protocolo P2P, firmas de API, ni la lógica de simulación.
    - Requiere revisión humana: Altera la lógica del protocolo Gossip/Membresía, el reenvío en Pub/Sub (should_forward, TTL, prioridad, fanout), generadores estocásticos (Poisson/percepción), replay de aire, scripts de Slurm/Docker o la suite de tests.

    Título del PR: {pr_title}
    Descripción: {pr_body}

    Devuelve ÚNICAMENTE un JSON estricto con este formato:
    {{
        "es_mecanico": true/false,
        "razonamiento": "Explica brevemente por qué tomaste la decisión."
    }}

    Diff del PR:
    {diff_texto}
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-3.6-flash',
            contents=prompt
        )
        texto_limpio = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(texto_limpio)
    except Exception as e:
        print(f"Error detallado en la IA: {e}")
        return {"es_mecanico": False, "razonamiento": f"Fallo en el análisis de IA ({e}). Se requiere humano por seguridad."}

def main():
    token = os.getenv("GITHUB_TOKEN")
    repo_name = os.getenv("GITHUB_REPOSITORY")
    pr_number = os.getenv("PR_NUMBER")
    
    if not all([token, repo_name, pr_number]):
        print("Error: Faltan variables de entorno de GitHub.")
        exit(1)
        
    auth = Auth.Token(token)
    g = Github(auth=auth)
    repo = g.get_repo(repo_name)
    pr = repo.get_pull(int(pr_number))
    
    # Capturar título y descripción del PR con fallback si está vacío
    pr_title = pr.title
    pr_body = pr.body if pr.body else "Sin descripción provista por el desarrollador."
    
    # Comprobar el estado del CI en el último commit
    commits = list(pr.get_commits())
    ci_exitoso = True
    if commits:
        ultimo_commit = commits[-1]
        estados = ultimo_commit.get_statuses()
        for estado in estados:
            if estado.state != "success" and estado.context != "Agente Revisor de MR IA":
                ci_exitoso = False
                break

    archivos_cambiados = pr.get_files()
    diff_completo = ""
    for archivo in archivos_cambiados:
        patch = archivo.patch if archivo.patch else "Sin cambios legibles"
        diff_completo += f"--- {archivo.filename}\n+++ {archivo.filename}\n{patch}\n\n"

    # Pasar los parámetros de contexto a la función
    analisis = evaluar_diff_pr_con_ia(diff_completo, pr_title, pr_body)
    
    comentario = "🤖 **Evaluación del Agente Revisor de MR**\n\n"
    
    if not ci_exitoso:
        comentario += "❌ **Estado del CI:** El pipeline de integración continua ha fallado o está pendiente. Por favor, revisa los logs.\n\n"
    else:
        comentario += "✅ **Estado del CI:** Los tests pasaron correctamente.\n\n"

    if analisis.get("es_mecanico", False) and ci_exitoso:
        comentario += "**Veredicto:** 🟢 Mecánico y listo para revisión final.\n"
    else:
        comentario += "**Veredicto:** 🟡 Requiere revisión humana.\n"
        
    comentario += f"**Análisis:** {analisis.get('razonamiento', 'Sin razón provista.')}\n\n"
    comentario += "*Nota de seguridad: Este agente nunca fusionará el código automáticamente a main.*"
    
    pr.create_issue_comment(comentario)
    print("Comentario publicado exitosamente en el PR.")

if __name__ == "__main__":
    main()