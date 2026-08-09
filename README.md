# Laboratorio-3-CivicMesh
En este laboratorio se implementara CivicMesh: una aplicación de monitoreo ciudadano distribuido basada en una capa de comunicación reutilizable (gossip + pub/sub por tópico geográfico: comuna o región).

### 1. Organización del Equipo (Roles)

De acuerdo a lo solicitado en el punto 3 del enunciado, a continuación se detallan los roles y responsabilidades:

| Rol | Encargado | Responsabilidades Concretas |
| :--- | :--- | :--- |
| **1. Líder de Capa de Red / Gossip** | Benjamin Moya | Membresía, descubrimiento, tolerancia a fallos. |
| **2. Líder de Capa Pub/Sub** |  | Tópicos, suscripciones, should_forward, fanout. |
| **3. Líder de Datos** |  | Ingesta/cache Dominio B (SINCA/Open-Meteo → replay); generadores Poisson y percepción (Sección 4.3); configuración de tasas/sesgos. |
| **4. Líder de Analítica y Estadística** | Sebastian de la Fuente | métricas de convergencia/divergencia; experimentos de caída/partición; frontend de estadísticas (Sección 5.4). |
| **5. Líder de CI/CD, Git y agentes** | Alonso Henriquez | Pipeline CI verde con tests; Dockerfile y docker-compose; ramas/issues/MR; los tres agentes del Lab 2; scripts sbatch/shared FS; README (local,Compose, clúster y cómo abrir la UI). |
