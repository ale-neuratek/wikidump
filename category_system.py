#!/usr/bin/env python3
"""
🗂️ SISTEMA DE CATEGORÍAS SIMPLIFICADO
====================================
Define las 21 categorías principales + consciencia para todo el sistema.
"""

# Categorías principales del sistema (21 + consciencia)
MAIN_CATEGORIES = {
    "naturaleza-universo": {
        "name": "Naturaleza y Universo",
        "description": "Física, química, biología, astronomía, geología, ecología",
        "keywords": ["física", "química", "biología", "astronomía", "geología", "ecología", "naturaleza", "universo", "átomo", "molécula", "célula", "especie", "planeta", "estrella", "evolución", "ecosistema"]
    },
    "tecnologia-herramientas": {
        "name": "Tecnología y Herramientas", 
        "description": "Computación, ingeniería, IA, telecomunicaciones, energía, infraestructura",
        "keywords": ["tecnología", "computación", "ingeniería", "software", "hardware", "internet", "telecomunicaciones", "energía", "infraestructura", "algoritmo", "inteligencia artificial", "robot"]
    },
    "sociedad-cultura": {
        "name": "Sociedad y Cultura",
        "description": "Antropología, sociología, tradiciones, religiones, costumbres, mitología",
        "keywords": ["sociedad", "cultura", "antropología", "sociología", "tradición", "religión", "costumbre", "mitología", "ritual", "comunidad", "tribu", "civilización"]
    },
    "politica-gobernanza": {
        "name": "Política y Gobernanza",
        "description": "Instituciones, sistemas políticos, ideologías, leyes, geopolítica",
        "keywords": ["política", "gobierno", "institución", "democracia", "ley", "constitución", "partido", "elección", "geopolítica", "estado", "poder", "autoridad"]
    },
    "economia-produccion": {
        "name": "Economía y Producción",
        "description": "Mercados, trabajo, finanzas, comercio, desarrollo, recursos",
        "keywords": ["economía", "mercado", "trabajo", "finanzas", "comercio", "desarrollo", "recursos", "empresa", "dinero", "inversión", "producción", "industria"]
    },
    "comunicacion-lenguaje": {
        "name": "Comunicación y Lenguaje",
        "description": "Idiomas, semiótica, medios, retórica, narrativa, redes sociales",
        "keywords": ["comunicación", "lenguaje", "idioma", "semiótica", "medios", "retórica", "narrativa", "redes sociales", "texto", "discurso", "palabra", "lingüística"]
    },
    "educacion-conocimiento": {
        "name": "Educación y Conocimiento",
        "description": "Pedagogía, teoría del conocimiento, disciplinas académicas, historia de las ideas",
        "keywords": ["educación", "conocimiento", "pedagogía", "enseñanza", "aprendizaje", "universidad", "escuela", "disciplina", "academia", "ideas", "teoría", "método"]
    },
    "historia-tiempo": {
        "name": "Historia y Tiempo",
        "description": "Cronología, civilizaciones, eventos, historiografía, memoria colectiva",
        "keywords": ["historia", "tiempo", "cronología", "civilización", "evento", "historiografía", "memoria", "pasado", "siglo", "época", "período", "guerra"]
    },
    "salud-cuerpo": {
        "name": "Salud y Cuerpo",
        "description": "Medicina, psicología, fisiología, nutrición, bienestar",
        "keywords": ["salud", "medicina", "psicología", "fisiología", "nutrición", "bienestar", "enfermedad", "tratamiento", "cuerpo", "mente", "hospital", "médico"]
    },
    "arte-estetica": {
        "name": "Arte y Estética",
        "description": "Música, pintura, cine, literatura, diseño, arquitectura",
        "keywords": ["arte", "estética", "música", "pintura", "cine", "literatura", "diseño", "arquitectura", "belleza", "creatividad", "obra", "artista"]
    },
    "matematicas-logica": {
        "name": "Matemáticas y Lógica",
        "description": "Álgebra, geometría, estadística, lógica formal, teoría de sistemas",
        "keywords": ["matemáticas", "lógica", "álgebra", "geometría", "estadística", "número", "ecuación", "teorema", "sistemas", "cálculo", "función", "variable"]
    },
    "etica-filosofia": {
        "name": "Ética y Filosofía",
        "description": "Moral, ontología, epistemología, metafísica, filosofía política",
        "keywords": ["ética", "filosofía", "moral", "ontología", "epistemología", "metafísica", "valor", "bien", "verdad", "existencia", "ser", "pensamiento"]
    },
    "espacio-geografia": {
        "name": "Espacio y Geografía",
        "description": "Territorio, cartografía, climatología, geopolítica regional, urbanismo",
        "keywords": ["espacio", "geografía", "territorio", "cartografía", "clima", "región", "urbanismo", "lugar", "mapa", "ciudad", "país", "continente"]
    },
    "datos-informacion": {
        "name": "Datos, Información y Metadatos",
        "description": "Archivística, bibliotecología, ciencia de datos, clasificación, semántica",
        "keywords": ["datos", "información", "metadatos", "archivo", "biblioteca", "clasificación", "semántica", "base de datos", "índice", "catálogo", "registro", "sistema"]
    },
    "trabajo-organizacion": {
        "name": "Trabajo y Organización",
        "description": "Empresas, oficios, administración, liderazgo, gestión del conocimiento",
        "keywords": ["trabajo", "organización", "empresa", "oficio", "administración", "liderazgo", "gestión", "empleo", "profesión", "negocio", "cargo", "función"]
    },
    "identidad-subjetividad": {
        "name": "Identidad y Subjetividad",
        "description": "Psicología, género, identidad cultural, autoconocimiento, ideología",
        "keywords": ["identidad", "subjetividad", "psicología", "género", "cultura", "autoconocimiento", "ideología", "personalidad", "individuo", "yo", "self", "persona"]
    },
    "juego-recreacion": {
        "name": "Juego y Recreación",
        "description": "Deportes, videojuegos, ocio, entretenimiento, lúdica, humor",
        "keywords": ["juego", "recreación", "deporte", "videojuego", "ocio", "entretenimiento", "lúdica", "humor", "diversión", "competencia", "espectáculo", "festival"]
    },
    "riesgos-seguridad": {
        "name": "Riesgos y Seguridad",
        "description": "Defensa, ciberseguridad, emergencias, catástrofes, prevención",
        "keywords": ["riesgo", "seguridad", "defensa", "ciberseguridad", "emergencia", "catástrofe", "prevención", "protección", "peligro", "amenaza", "crisis", "desastre"]
    },
    "transporte-movilidad": {
        "name": "Transporte y Movilidad",
        "description": "Vehículos, redes de transporte, logística, conectividad",
        "keywords": ["transporte", "movilidad", "vehículo", "red", "logística", "conectividad", "tráfico", "viaje", "carretera", "aeropuerto", "puerto", "tren"]
    },
    "futuro-prospectiva": {
        "name": "Futuro y Prospectiva",
        "description": "Futurología, innovación, escenarios, utopías/distopías, transhumanismo",
        "keywords": ["futuro", "prospectiva", "futurología", "innovación", "escenario", "utopía", "distopía", "transhumanismo", "tendencia", "evolución", "predicción", "cambio"]
    },
    "consciencia": {
        "name": "Consciencia",
        "description": "Autoconciencia, metacognición, reflexión, mindfulness, espiritualidad",
        "keywords": ["consciencia", "autoconciencia", "metacognición", "reflexión", "mindfulness", "espiritualidad", "conciencia", "despertar", "iluminación", "ser", "meditación", "introspección"]
    }
}

def get_all_categories():
    """Retorna todas las categorías disponibles"""
    return list(MAIN_CATEGORIES.keys())

def get_category_info(category_key):
    """Obtiene información de una categoría específica"""
    return MAIN_CATEGORIES.get(category_key, {})

def classify_by_keywords(text, title=""):
    """Clasifica texto basado en palabras clave de las categorías"""
    text_lower = (text + " " + title).lower()
    scores = {}
    
    for cat_key, cat_info in MAIN_CATEGORIES.items():
        score = 0
        for keyword in cat_info["keywords"]:
            if keyword in text_lower:
                score += 1
        scores[cat_key] = score
    
    # Retornar categoría con mayor puntuación
    if scores:
        best_category = max(scores, key=scores.get)
        if scores[best_category] > 0:
            return best_category
    
    return "datos-informacion"  # Categoría por defecto

def get_category_questions_patterns():
    """Retorna patrones base para generar preguntas por categoría"""
    return {
        "naturaleza-universo": [
            "¿Cómo funciona {title}?",
            "¿Qué procesos naturales involucra {title}?",
            "¿Cuál es la composición de {title}?",
            "¿Qué papel juega {title} en el ecosistema?",
            "¿Cómo se forma {title}?"
        ],
        "tecnologia-herramientas": [
            "¿Cómo opera {title}?",
            "¿Qué tecnologías utiliza {title}?",
            "¿Cuáles son las aplicaciones de {title}?",
            "¿Cómo ha evolucionado {title}?",
            "¿Qué ventajas ofrece {title}?"
        ],
        "sociedad-cultura": [
            "¿Qué significa {title} en la cultura?",
            "¿Cómo se practica {title}?",
            "¿Cuál es el origen cultural de {title}?",
            "¿Qué simboliza {title}?",
            "¿Cómo varía {title} entre culturas?"
        ],
        "politica-gobernanza": [
            "¿Cómo funciona {title} como institución?",
            "¿Qué poder tiene {title}?",
            "¿Cuál es la estructura de {title}?",
            "¿Qué leyes rigen {title}?",
            "¿Cómo se elige {title}?"
        ],
        "economia-produccion": [
            "¿Cuál es el valor económico de {title}?",
            "¿Cómo se produce {title}?",
            "¿Qué mercados abarca {title}?",
            "¿Cuáles son los costos de {title}?",
            "¿Cómo se comercializa {title}?"
        ],
        "comunicacion-lenguaje": [
            "¿Cómo se comunica {title}?",
            "¿Qué significa {title} lingüísticamente?",
            "¿Cómo se expresa {title}?",
            "¿Qué mensaje transmite {title}?",
            "¿Cómo se interpreta {title}?"
        ],
        "educacion-conocimiento": [
            "¿Cómo se aprende {title}?",
            "¿Qué enseña {title}?",
            "¿Cuál es la teoría detrás de {title}?",
            "¿Cómo se estudia {title}?",
            "¿Qué disciplinas involucra {title}?"
        ],
        "historia-tiempo": [
            "¿Cuándo ocurrió {title}?",
            "¿Qué causó {title}?",
            "¿Cómo cambió {title} la historia?",
            "¿Qué precedió a {title}?",
            "¿Cuáles fueron las consecuencias de {title}?"
        ],
        "salud-cuerpo": [
            "¿Cómo afecta {title} a la salud?",
            "¿Qué síntomas presenta {title}?",
            "¿Cómo se trata {title}?",
            "¿Qué causa {title}?",
            "¿Cómo se previene {title}?"
        ],
        "arte-estetica": [
            "¿Qué estilo representa {title}?",
            "¿Cómo se crea {title}?",
            "¿Qué técnicas usa {title}?",
            "¿Qué expresa {title} artísticamente?",
            "¿Cuál es la belleza de {title}?"
        ],
        "matematicas-logica": [
            "¿Cómo se calcula {title}?",
            "¿Qué propiedades tiene {title}?",
            "¿Cómo se demuestra {title}?",
            "¿Qué aplicaciones tiene {title}?",
            "¿Cómo se resuelve {title}?"
        ],
        "etica-filosofia": [
            "¿Es correcto {title}?",
            "¿Qué implica moralmente {title}?",
            "¿Cómo se justifica {title}?",
            "¿Qué valores representa {title}?",
            "¿Cuál es la esencia de {title}?"
        ],
        "espacio-geografia": [
            "¿Dónde se encuentra {title}?",
            "¿Qué características geográficas tiene {title}?",
            "¿Cómo es el clima de {title}?",
            "¿Qué territorios abarca {title}?",
            "¿Cómo se organiza espacialmente {title}?"
        ],
        "datos-informacion": [
            "¿Qué datos incluye {title}?",
            "¿Cómo se clasifica {title}?",
            "¿Qué información contiene {title}?",
            "¿Cómo se organiza {title}?",
            "¿Qué metadatos describe {title}?"
        ],
        "trabajo-organizacion": [
            "¿Cómo se organiza {title}?",
            "¿Qué funciones tiene {title}?",
            "¿Cómo se gestiona {title}?",
            "¿Qué estructura posee {title}?",
            "¿Cómo lidera {title}?"
        ],
        "identidad-subjetividad": [
            "¿Cómo define {title} la identidad?",
            "¿Qué representa {title} personalmente?",
            "¿Cómo influye {title} en la personalidad?",
            "¿Qué aspecto subjetivo tiene {title}?",
            "¿Cómo se vive {title}?"
        ],
        "juego-recreacion": [
            "¿Cómo se juega {title}?",
            "¿Qué reglas tiene {title}?",
            "¿Cómo se compite en {title}?",
            "¿Qué diversión ofrece {title}?",
            "¿Cómo se entrena para {title}?"
        ],
        "riesgos-seguridad": [
            "¿Qué riesgos presenta {title}?",
            "¿Cómo se protege contra {title}?",
            "¿Qué medidas de seguridad requiere {title}?",
            "¿Cómo se previene {title}?",
            "¿Qué amenazas implica {title}?"
        ],
        "transporte-movilidad": [
            "¿Cómo se transporta {title}?",
            "¿Qué rutas sigue {title}?",
            "¿Cómo se mueve {title}?",
            "¿Qué vehículos usa {title}?",
            "¿Cómo se conecta {title}?"
        ],
        "futuro-prospectiva": [
            "¿Cómo será {title} en el futuro?",
            "¿Qué innovaciones traerá {title}?",
            "¿Cómo evolucionará {title}?",
            "¿Qué escenarios presenta {title}?",
            "¿Qué tendencias muestra {title}?"
        ],
        "consciencia": [
            "¿Cómo se entiende {title} como concepto?",
            "¿Qué categorías abarca {title}?",
            "¿Cómo se clasifica {title}?",
            "¿Qué relaciones tiene {title} con otros conceptos?",
            "¿Cómo se organiza el conocimiento sobre {title}?"
        ]
    }
