#!/usr/bin/env python3
"""
🧠 CONTENT MANAGER INTELIGENTE - Con índices de confianza y conversaciones profundas
==================================================================================
Módulo avanzado para generación de contenido y categorización inteligente
Incluye índices de confianza, conversaciones en profundidad y análisis de calidad

MEJORAS IMPLEMENTADAS:
- Índices de confianza para cada categorización
- Conversaciones "en profundidad" con contenido completo
- Inferencia inteligente de preguntas por tipo de título
- Métricas de calidad y cercanía
- Sistema de retroalimentación para mejora continua
- Plantillas contextuales específicas por tipo
- Análisis de artículos con baja confianza
"""

import re
import json
import hashlib
import math
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict, Counter
from datetime import datetime


class TitleInferenceEngine:
    """Motor de inferencia inteligente para generar preguntas basadas en el título"""
    
    def __init__(self):
        self.title_patterns = {
            # PERSONAS (biografías)
            'persona': {
                'patterns': [
                    r'^[A-Z][a-záéíóúñü]+ [A-Z][a-záéíóúñü]+$',  # Nombre Apellido
                    r'^[A-Z][a-záéíóúñü]+ [A-Z][a-záéíóúñü]+ [A-Z][a-záéíóúñü]+$',  # Nombre Apellido Apellido
                    r'(?:nació|nacido|nacida|fallecido|murió)',
                ],
                'questions': [
                    "¿Quién fue {title}?",
                    "¿Cuándo nació {title}?", 
                    "¿Dónde nació {title}?",
                    "¿Qué hizo {title}?",
                    "¿Por qué es conocido {title}?",
                    "Háblame en profundidad sobre la vida de {title}"
                ],
                'weight': 9
            },
            
            # FECHAS Y EVENTOS HISTÓRICOS
            'fecha_evento': {
                'patterns': [
                    r'\b(?:19|20)\d{2}\b',  # Años
                    r'\b(?:siglo (?:XVI|XVII|XVIII|XIX|XX|XXI))\b',
                    r'(?:guerra|batalla|revolución|independencia)',
                    r'(?:enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)',
                ],
                'questions': [
                    "¿Qué sucedió en {title}?",
                    "¿Cuándo ocurrió {title}?",
                    "¿Dónde tuvo lugar {title}?",
                    "¿Cuáles fueron las consecuencias de {title}?",
                    "¿Quiénes participaron en {title}?",
                    "Explícame detalladamente todo sobre {title}"
                ],
                'weight': 8
            },
            
            # LUGARES GEOGRÁFICOS
            'lugar': {
                'patterns': [
                    r'(?:ciudad|pueblo|municipio|localidad)',
                    r'(?:provincia|región|departamento|estado)',
                    r'(?:país|nación|república|reino)',
                    r'(?:río|montaña|lago|valle|isla)',
                ],
                'questions': [
                    "¿Dónde está {title}?",
                    "¿En qué país se encuentra {title}?",
                    "¿Cuántos habitantes tiene {title}?",
                    "¿Cuáles son las características de {title}?",
                    "¿Cuál es la historia de {title}?",
                    "Descríbeme completamente {title} con todos sus detalles"
                ],
                'weight': 8
            },
            
            # ESPECIES BIOLÓGICAS
            'especie': {
                'patterns': [
                    r'\b[A-Z][a-z]+ [a-z]+\b',  # Nombre científico
                    r'(?:especie|género|familia) de',
                    r'(?:mamífero|ave|pez|reptil|insecto|planta)',
                ],
                'questions': [
                    "¿Qué tipo de especie es {title}?",
                    "¿Dónde vive {title}?",
                    "¿Cuáles son las características de {title}?",
                    "¿Cómo se reproduce {title}?",
                    "¿De qué se alimenta {title}?",
                    "Explica todo lo que sabes sobre {title} en detalle"
                ],
                'weight': 7
            },
            
            # OBRAS ARTÍSTICAS
            'obra_arte': {
                'patterns': [
                    r'(?:álbum|disco|canción|single)',
                    r'(?:película|libro|novela|obra)',
                    r'(?:pintura|escultura|cuadro)',
                ],
                'questions': [
                    "¿Qué es {title}?",
                    "¿Quién creó {title}?",
                    "¿Cuándo se lanzó {title}?",
                    "¿De qué trata {title}?",
                    "¿Cuál es la importancia de {title}?",
                    "Analiza profundamente {title} y su significado"
                ],
                'weight': 7
            },
            
            # CONCEPTOS TÉCNICOS/CIENTÍFICOS
            'concepto': {
                'patterns': [
                    r'(?:teoría|ley|principio|concepto)',
                    r'(?:método|técnica|proceso|sistema)',
                    r'(?:enfermedad|síndrome|trastorno)',
                ],
                'questions': [
                    "¿Qué es {title}?",
                    "¿Cómo funciona {title}?",
                    "¿Para qué sirve {title}?",
                    "¿Quién desarrolló {title}?",
                    "¿Cuáles son las aplicaciones de {title}?",
                    "Explica en detalle todo sobre {title} y sus implicaciones"
                ],
                'weight': 6
            }
        }
    
    def infer_type(self, title: str) -> str:
        """Infiere el tipo de contenido basado solo en el título - versión mejorada"""
        text = title.lower()
        
        # Reglas específicas prioritarias
        # 1. Nombres científicos (dos palabras en latín)
        if re.match(r'^[A-Z][a-z]+ [a-z]+$', title) and len(title.split()) == 2:
            # Verificar si contiene palabras típicas de especies
            if any(word in text for word in ['panthera', 'canis', 'felis', 'homo', 'sus', 'equus']):
                return 'especie'
        
        # 2. Eventos históricos con números o palabras clave
        if any(word in text for word in ['guerra', 'batalla', 'revolución', 'independencia', 'conflicto']):
            return 'fecha_evento'
        
        # 3. Obras literarias/artísticas
        if any(phrase in text for phrase in ['don ', 'el ', 'la ', 'los ', 'las ']) and len(title.split()) > 2:
            if any(word in text for word in ['quijote', 'miserables', 'divina', 'comedia', 'novel', 'obra']):
                return 'obra_arte'
        
        # 4. Nombres de personas (patrón más estricto)
        words = title.split()
        if len(words) == 2:
            # Verificar que sean nombres propios típicos
            if all(word[0].isupper() and word[1:].islower() for word in words):
                # Excluir títulos que no son nombres
                if not any(excluded in text for excluded in ['guerra', 'batalla', 'de la', 'del ']):
                    return 'persona'
        
        # Sistema de puntuación original como fallback
        best_type = 'concepto'  # default
        max_score = 0
        
        for type_name, config in self.title_patterns.items():
            score = 0
            
            for pattern in config['patterns']:
                matches = len(re.findall(pattern, text, re.IGNORECASE))
                score += matches * 2
            
            # Bonus por longitud apropiada del título
            title_words = len(title.split())
            if type_name == 'persona' and 2 <= title_words <= 3:
                score += 3
            elif type_name == 'lugar' and 1 <= title_words <= 4:
                score += 2
            elif type_name == 'fecha_evento' and title_words >= 3:
                score += 2
            
            weighted_score = score * config['weight']
            
            if weighted_score > max_score:
                max_score = weighted_score
                best_type = type_name
        
        return best_type
    
    def infer_question_type(self, title: str, content: str) -> Tuple[str, float]:
        """Infiere el tipo de pregunta más apropiado para el título"""
        text = f"{title} {content}".lower()
        
        best_type = 'concepto'  # default
        max_score = 0
        
        for type_name, config in self.title_patterns.items():
            score = 0
            
            for pattern in config['patterns']:
                matches = len(re.findall(pattern, text, re.IGNORECASE))
                score += matches * 2
            
            # Bonus por longitud apropiada del título
            title_words = len(title.split())
            if type_name == 'persona' and 2 <= title_words <= 3:
                score += 3
            elif type_name == 'lugar' and 1 <= title_words <= 4:
                score += 2
            
            weighted_score = score * config['weight']
            
            if weighted_score > max_score:
                max_score = weighted_score
                best_type = type_name
        
        # Calcular confianza (0.0 a 1.0)
        confidence = min(max_score / 20.0, 1.0) if max_score > 0 else 0.3
        
        return best_type, confidence
    
    def generate_questions(self, title: str, question_type: str) -> List[str]:
        """Genera preguntas apropiadas para el tipo identificado"""
        if question_type not in self.title_patterns:
            question_type = 'concepto'
        
        questions = []
        templates = self.title_patterns[question_type]['questions']
        
        for template in templates:
            question = template.format(title=title)
            questions.append(question)
        
        return questions


class ConfidenceMetrics:
    """Sistema de métricas de confianza para evaluar calidad de categorización"""
    
    def __init__(self):
        self.metrics_history = []
        self.low_confidence_articles = []
        
    def calculate_confidence(self, title: str, content: str, category: str, 
                           subcategory: str, question_type: str) -> Dict[str, float]:
        """Calcula múltiples métricas de confianza"""
        
        # 1. Confianza de categorización (basada en keywords y patterns)
        category_confidence = self._calculate_category_confidence(content, category)
        
        # 2. Confianza de tipo de pregunta
        question_confidence = self._calculate_question_confidence(title, question_type)
        
        # 3. Confianza de calidad de contenido
        content_confidence = self._calculate_content_confidence(content)
        
        # 4. Confianza de especificidad del título
        title_confidence = self._calculate_title_confidence(title)
        
        # 5. Confianza global (promedio ponderado)
        global_confidence = (
            category_confidence * 0.3 +
            question_confidence * 0.25 +
            content_confidence * 0.25 +
            title_confidence * 0.2
        )
        
        metrics = {
            'global_confidence': global_confidence,
            'category_confidence': category_confidence,
            'question_confidence': question_confidence,
            'content_confidence': content_confidence,
            'title_confidence': title_confidence,
            'content_length': len(content),
            'title_words': len(title.split())
        }
        
        # Registrar métricas
        self.metrics_history.append({
            'title': title,
            'category': category,
            'metrics': metrics,
            'timestamp': datetime.now().isoformat()
        })
        
        # Identificar artículos de baja confianza
        if global_confidence < 0.6:
            self.low_confidence_articles.append({
                'title': title,
                'category': category,
                'confidence': global_confidence,
                'issues': self._identify_issues(metrics)
            })
        
        return metrics
    
    def _calculate_category_confidence(self, content: str, category: str) -> float:
        """Confianza de la categorización"""
        category_keywords = {
            'arte': ['música', 'álbum', 'canción', 'pintura', 'artista', 'obra'],
            'geografia': ['ciudad', 'país', 'región', 'provincia', 'territorio'],
            'historia': ['nació', 'murió', 'siglo', 'guerra', 'batalla', 'histórico'],
            'biologia': ['especie', 'género', 'familia', 'animal', 'planta'],
            'ciencias': ['teoría', 'investigación', 'científico', 'descubrimiento'],
            'deportes': ['equipo', 'club', 'liga', 'campeonato', 'deportivo'],
            'politica': ['gobierno', 'presidente', 'político', 'partido'],
            'tecnologia': ['software', 'internet', 'tecnología', 'digital'],
            'medicina': ['enfermedad', 'tratamiento', 'médico', 'hospital'],
            'economia': ['empresa', 'mercado', 'economía', 'negocio'],
            'educacion': ['universidad', 'escuela', 'educación', 'enseñanza']
        }
        
        if category not in category_keywords:
            return 0.5
        
        keywords = category_keywords[category]
        content_lower = content.lower()
        
        matches = sum(1 for keyword in keywords if keyword in content_lower)
        confidence = min(matches / len(keywords), 1.0)
        
        return max(confidence, 0.1)
    
    def _calculate_question_confidence(self, title: str, question_type: str) -> float:
        """Confianza del tipo de pregunta inferido"""
        title_length = len(title.split())
        
        if question_type == 'persona' and 2 <= title_length <= 4:
            return 0.9
        elif question_type == 'lugar' and 1 <= title_length <= 3:
            return 0.8
        elif question_type == 'fecha_evento' and any(word.isdigit() for word in title.split()):
            return 0.85
        elif question_type == 'especie' and title_length == 2:
            return 0.8
        else:
            return 0.6
    
    def _calculate_content_confidence(self, content: str) -> float:
        """Confianza basada en la calidad del contenido"""
        content_length = len(content)
        sentences = content.count('.')
        
        if content_length < 100:
            length_score = content_length / 100.0
        elif content_length > 5000:
            length_score = max(0.5, 1.0 - (content_length - 5000) / 10000.0)
        else:
            length_score = 1.0
        
        structure_score = min(sentences / 10.0, 1.0) if sentences > 0 else 0.1
        
        return (length_score + structure_score) / 2.0
    
    def _calculate_title_confidence(self, title: str) -> float:
        """Confianza basada en la especificidad del título"""
        title_words = len(title.split())
        
        if 1 <= title_words <= 5:
            return 0.9
        elif 6 <= title_words <= 8:
            return 0.7
        else:
            return 0.5
    
    def _identify_issues(self, metrics: Dict[str, float]) -> List[str]:
        """Identifica problemas específicos en métricas bajas"""
        issues = []
        
        if metrics['category_confidence'] < 0.5:
            issues.append("Categorización incierta")
        if metrics['content_confidence'] < 0.5:
            issues.append("Contenido de baja calidad")
        if metrics['question_confidence'] < 0.5:
            issues.append("Tipo de pregunta inadecuado")
        if metrics['title_confidence'] < 0.5:
            issues.append("Título poco específico")
        if metrics['content_length'] < 100:
            issues.append("Contenido muy corto")
        
        return issues
    
    def get_low_confidence_report(self) -> Dict:
        """Genera reporte de artículos con baja confianza"""
        return {
            'total_low_confidence': len(self.low_confidence_articles),
            'articles': self.low_confidence_articles[-50:],
            'common_issues': self._get_common_issues(),
            'recommendations': self._get_recommendations()
        }
    
    def _get_common_issues(self) -> Dict[str, int]:
        """Analiza problemas más comunes"""
        issue_counts = Counter()
        for article in self.low_confidence_articles:
            for issue in article['issues']:
                issue_counts[issue] += 1
        return dict(issue_counts.most_common(10))
    
    def _get_recommendations(self) -> List[str]:
        """Genera recomendaciones de mejora"""
        common_issues = self._get_common_issues()
        recommendations = []
        
        if 'Categorización incierta' in common_issues:
            recommendations.append("Mejorar patrones de keywords por categoría")
        if 'Contenido muy corto' in common_issues:
            recommendations.append("Filtrar artículos con contenido < 100 caracteres")
        if 'Tipo de pregunta inadecuado' in common_issues:
            recommendations.append("Refinar patrones de inferencia de preguntas")
        
        return recommendations


class IntelligentCategorizer:
    """Categorizador inteligente con sistema de confianza avanzado"""
    
    def __init__(self):
        self.setup_intelligent_patterns()
        self.setup_subcategory_patterns()  # ¡FALTABA ESTA LÍNEA!
        self.setup_conversation_templates()
        self.title_inference = TitleInferenceEngine()
        self.confidence_metrics = ConfidenceMetrics()
        
        # Estadísticas thread-safe (atómicas)
        self.category_corrections = defaultdict(int)
        self.confidence_scores = defaultdict(list)
        
    def setup_intelligent_patterns(self):
        """Configura patrones inteligentes mejorados"""
        
        # CATEGORÍAS PRINCIPALES con patrones específicos para alta confianza
        self.categories = {
            'arte': {
                'keywords': ['música', 'álbum', 'canción', 'banda', 'compositor', 'pintura', 'artista', 'obra', 'teatro', 'danza'],
                'patterns': [
                    r'\b(?:álbum|disco|canción|single)\b',
                    r'\b(?:banda|grupo musical|cantante|músico)\b',
                    r'\b(?:reggae|rock|pop|jazz|clásica|folk)\b',
                    r'\b(?:pintor|escultor|artista|galería|museo)\b'
                ],
                'weight': 10
            },
            'geografia': {
                'keywords': ['país', 'ciudad', 'región', 'provincia', 'municipio', 'capital', 'territorio'],
                'patterns': [
                    r'\b(?:ciudad|pueblo|municipio|localidad)\b',
                    r'\b(?:provincia|región|departamento|estado)\b',
                    r'\b(?:país|nación|república|reino)\b',
                    r'\bes una (?:ciudad|provincia|región|país)\b'
                ],
                'weight': 8
            },
            'historia': {
                'keywords': ['nació', 'murió', 'biografía', 'vida', 'histórico', 'siglo', 'guerra', 'batalla'],
                'patterns': [
                    r'\b(?:nació|nacido|nacida)\b.*\b(?:murió|fallecido)\b',
                    r'\b(?:\d{4})\s*[-–]\s*(?:\d{4})\b',
                    r'\bfue un(?:a)? (?:escritor|político|militar)\b',
                    r'\b(?:siglo (?:XVIII|XIX|XX|XXI))\b'
                ],
                'weight': 9
            },
            'ciencias': {
                'keywords': ['científico', 'investigación', 'teoría', 'experimento', 'método', 'descubrimiento'],
                'patterns': [
                    r'\b(?:teoría|ley|principio|descubrimiento)\b',
                    r'\b(?:investigación|experimento|estudio)\b',
                    r'\b(?:científico|investigador|premio Nobel)\b'
                ],
                'weight': 7
            },
            'biologia': {
                'keywords': ['especie', 'género', 'familia', 'animal', 'planta', 'mamífero', 'ave'],
                'patterns': [
                    r'\b(?:especie|género|familia) de\b',
                    r'\b[A-Z][a-z]+ [a-z]+\b.*\b(?:especie|animal|planta)\b',
                    r'\b(?:mamífero|ave|pez|reptil|insecto)\b'
                ],
                'weight': 8
            },
            'tecnologia': {
                'keywords': ['computadora', 'software', 'internet', 'tecnología', 'digital', 'programa'],
                'patterns': [
                    r'\b(?:software|hardware|programa|aplicación)\b',
                    r'\b(?:internet|web|digital|tecnología)\b'
                ],
                'weight': 7
            },
            'deportes': {
                'keywords': ['fútbol', 'equipo', 'club', 'deportivo', 'liga', 'campeonato'],
                'patterns': [
                    r'\b(?:club|equipo) (?:de fútbol|deportivo)\b',
                    r'\b(?:liga|campeonato|torneo)\b'
                ],
                'weight': 8
            },
            'politica': {
                'keywords': ['gobierno', 'presidente', 'ministro', 'político', 'partido', 'elección'],
                'patterns': [
                    r'\b(?:presidente|ministro|gobernador)\b',
                    r'\b(?:partido político|gobierno)\b'
                ],
                'weight': 7
            },
            'medicina': {
                'keywords': ['enfermedad', 'tratamiento', 'médico', 'hospital', 'salud'],
                'patterns': [
                    r'\b(?:enfermedad|síntoma|tratamiento)\b',
                    r'\b(?:médico|doctor|hospital)\b'
                ],
                'weight': 7
            },
            'economia': {
                'keywords': ['empresa', 'mercado', 'economía', 'negocio', 'comercio'],
                'patterns': [
                    r'\b(?:empresa|corporación)\b',
                    r'\b(?:mercado|economía)\b'
                ],
                'weight': 6
            },
            'educacion': {
                'keywords': ['universidad', 'colegio', 'escuela', 'educación', 'enseñanza'],
                'patterns': [
                    r'\b(?:universidad|colegio|escuela)\b',
                    r'\b(?:educación|enseñanza)\b'
                ],
                'weight': 6
            }
        }
        
        # PATRONES MUSICALES Y ARTÍSTICOS ESPECÍFICOS
        self.music_patterns = [
            r'\b(?:álbum|disco)\b.*\b(?:de|por)\b.*\b(?:banda|grupo|cantante|artista)\b',
            r'\b(?:banda|grupo)\b.*\b(?:álbum|disco|canción)\b',
            r'\b(?:single|sencillo)\b.*\b(?:de|por)\b',
            r'\b(?:reggae|rock|pop|jazz|blues|folk|metal|punk|electronic)\b.*\b(?:álbum|banda|artista)\b',
            r'\b(?:compositor|músico|cantante)\b.*\b(?:conocido|famoso)\b'
        ]
        
        # PATRONES ARTÍSTICOS GENERALES
        self.art_patterns = [
            r'\b(?:pintor|escultor|artista)\b.*\b(?:conocido|famoso|creó|pintó)\b',
            r'\b(?:obra|pintura|escultura)\b.*\b(?:de|por)\b.*\b(?:artista|pintor)\b',
            r'\b(?:estilo|movimiento)\b.*\b(?:artístico|pictórico)\b'
        ]
        
    def setup_conversation_templates(self):
        """Plantillas inteligentes basadas en tipo de contenido con preguntas en profundidad"""
        self.conversation_templates = {
            'persona': {
                'basic': [
                    "¿Quién fue {topic}?",
                    "¿Cuándo nació {topic}?",
                    "¿Cuál fue la principal contribución de {topic}?"
                ],
                'deep': "Dame toda la información disponible sobre {topic}. Quiero conocer su biografía completa, obra, logros y legado histórico."
            },
            'lugar': {
                'basic': [
                    "¿Dónde está {topic}?",
                    "¿Cuál es la importancia de {topic}?",
                    "¿Cuáles son las características principales de {topic}?"
                ],
                'deep': "Proporciona información completa sobre {topic}. Incluye ubicación exacta, historia, geografía, población, economía, cultura y todos los detalles relevantes."
            },
            'fecha_evento': {
                'basic': [
                    "¿Qué sucedió en {topic}?",
                    "¿Cuándo ocurrió {topic}?",
                    "¿Cuál fue la importancia de {topic}?"
                ],
                'deep': "Explica completamente {topic}. Quiero conocer todos los antecedentes, el desarrollo del evento, las consecuencias y su significado histórico."
            },
            'especie': {
                'basic': [
                    "¿Qué tipo de especie es {topic}?",
                    "¿Dónde vive {topic}?",
                    "¿Cuáles son las características de {topic}?"
                ],
                'deep': "Dame información completa sobre {topic}. Incluye clasificación taxonómica, hábitat, comportamiento, alimentación, reproducción, distribución y estado de conservación."
            },
            'obra_arte': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Quién creó {topic}?",
                    "¿Cuándo se creó {topic}?"
                ],
                'deep': "Analiza en profundidad {topic}. Quiero conocer todo sobre su creación, contexto histórico, técnica utilizada, significado, influencia cultural y recepción crítica."
            },
            'concepto': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Cómo funciona {topic}?",
                    "¿Para qué se utiliza {topic}?"
                ],
                'deep': "Explica exhaustivamente {topic}. Incluye definición completa, principios fundamentales, aplicaciones, desarrollo histórico y relevancia actual."
            },
            # Plantillas por categoría (fallback)
            'arte': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Quién creó {topic}?",
                    "¿Cuál es su importancia artística?"
                ],
                'deep': "Proporciona un análisis artístico completo de {topic}. Incluye todo sobre su creación, estilo, contexto cultural, técnica y significado en la historia del arte."
            },
            'geografia': {
                'basic': [
                    "¿Dónde está {topic}?",
                    "¿Cuántos habitantes tiene {topic}?",
                    "¿Cuáles son sus características?"
                ],
                'deep': "Describe completamente {topic} desde todos los aspectos geográficos. Incluye ubicación, demografía, geografía física, economía, historia y características culturales."
            },
            'historia': {
                'basic': [
                    "¿Quién fue {topic}?",
                    "¿Cuándo vivió {topic}?",
                    "¿Por qué es histórica​mente importante {topic}?"
                ],
                'deep': "Explica en detalle toda la importancia histórica de {topic}. Incluye contexto completo, eventos relevantes, influencia en su época y legado histórico."
            },
            'ciencias': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Cómo funciona {topic}?",
                    "¿Cuál es su importancia científica?"
                ],
                'deep': "Proporciona una explicación científica completa de {topic}. Incluye principios teóricos, funcionamiento, aplicaciones, investigación actual y relevancia en el campo científico."
            },
            'biologia': {
                'basic': [
                    "¿Qué tipo de ser vivo es {topic}?",
                    "¿Dónde vive {topic}?",
                    "¿Cómo se comporta {topic}?"
                ],
                'deep': "Dame información biológica completa sobre {topic}. Incluye taxonomía, anatomía, fisiología, comportamiento, ecología, evolución y relación con otros organismos."
            },
            'tecnologia': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Cómo funciona {topic}?",
                    "¿Para qué se usa {topic}?"
                ],
                'deep': "Explica completamente {topic} desde el punto de vista tecnológico. Incluye principios técnicos, funcionamiento, aplicaciones, desarrollo histórico e impacto tecnológico."
            },
            'deportes': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Dónde se practica {topic}?",
                    "¿Cuáles son las reglas de {topic}?"
                ],
                'deep': "Describe completamente {topic} desde todos los aspectos deportivos. Incluye historia, reglas, técnicas, competiciones importantes y figuras destacadas."
            },
            'politica': {
                'basic': [
                    "¿Quién fue {topic}?",
                    "¿Qué cargo ocupó {topic}?",
                    "¿Cuál fue su importancia política?"
                ],
                'deep': "Analiza completamente la figura política de {topic}. Incluye trayectoria política, ideología, logros, controversias y impacto en la historia política."
            },
            'economia': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Cómo funciona {topic}?",
                    "¿Cuál es su importancia económica?"
                ],
                'deep': "Explica completamente {topic} desde el punto de vista económico. Incluye principios económicos, funcionamiento, impacto en mercados y relevancia actual."
            },
            'medicina': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Cómo se trata {topic}?",
                    "¿Cuáles son los síntomas de {topic}?"
                ],
                'deep': "Proporciona información médica completa sobre {topic}. Incluye definición, causas, síntomas, diagnóstico, tratamiento y pronóstico."
            },
            'educacion': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Dónde se enseña {topic}?",
                    "¿Para qué sirve {topic}?"
                ],
                'deep': "Explica completamente {topic} desde el punto de vista educativo. Incluye metodología, objetivos, aplicaciones y relevancia en el sistema educativo."
            },
            # Template general mejorado
            'general': {
                'basic': [
                    "¿Qué es {topic}?",
                    "¿Cuál es la importancia de {topic}?",
                    "¿Cuáles son las características principales de {topic}?"
                ],
                'deep': "Dame toda la información disponible sobre {topic}. Quiero conocer todos los detalles, contexto, importancia y cualquier aspecto relevante."
            }
        }
        
    def setup_subcategory_patterns(self):
        """Patrones rápidos para subcategorías"""
        self.subcategory_patterns = {
            'arte': {
                'musica-albums': r'(?:álbum|disco)',
                'musica-reggae': r'(?:reggae|Jamaica)',
                'musica-rock': r'(?:rock|metal)',
                'pintura': r'(?:pintura|pintor)',
                'literatura': r'(?:novela|poeta|escritor)'
            },
            'geografia': {
                'paises': r'(?:país|nación)',
                'ciudades': r'(?:ciudad|municipio)',
                'regiones': r'(?:región|provincia)'
            },
            'historia': {
                'biografias': r'(?:nació|nacido).*(?:murió|fallecido)',
                'eventos': r'(?:guerra|batalla|revolución)',
                'periodos': r'(?:siglo|época|era)'
            }
        }
        
    def categorize_article_fast(self, title: str, content: str) -> Tuple[str, str, float]:
        """
        Categorización ultra-rápida optimizada para throughput masivo
        Returns: (category, subcategory, confidence)
        """
        text = f"{title} {content}".lower()
        
        # 1. VERIFICAR PATRONES MUSICALES Y ARTÍSTICOS PRIMERO
        for pattern in self.music_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                subcategory = self.identify_subcategory_fast('arte', text)
                return 'arte', subcategory, 0.90
                
        for pattern in self.art_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                subcategory = self.identify_subcategory_fast('arte', text)
                return 'arte', subcategory, 0.85
        
        # 2. SCORING RÁPIDO POR CATEGORÍA
        best_category = 'general'
        max_score = 0
        
        for category, config in self.categories.items():
            score = 0
            
            # Keywords (más rápido que regex)
            for keyword in config['keywords']:
                if keyword in text:
                    score += 2
                    
            # Solo aplicar regex si ya hay score de keywords
            if score > 0:
                for pattern in config['patterns']:
                    if re.search(pattern, text, re.IGNORECASE):
                        score += 3
                        
                score *= config['weight']
                
                if score > max_score:
                    max_score = score
                    best_category = category
        
        # 3. CALCULAR CONFIANZA RÁPIDA
        confidence = min(max_score / 20.0, 1.0) if max_score > 0 else 0.3
        
        # 4. SUBCATEGORÍA RÁPIDA
        subcategory = self.identify_subcategory_fast(best_category, text)
        
        return best_category, subcategory, confidence
        
    def identify_subcategory_fast(self, category: str, text: str) -> str:
        """Identificación rápida de subcategoría"""
        if category not in self.subcategory_patterns:
            return 'general'
            
        for subcategory, pattern in self.subcategory_patterns[category].items():
            if re.search(pattern, text, re.IGNORECASE):
                return subcategory
                
        return 'general'
    
    def generate_conversations_fast(self, title: str, content: str, category: str, subcategory: str) -> List[Dict]:
        """Generación inteligente de conversaciones con análisis en profundidad"""
        conversations = []
        
        # Inferir tipo de contenido desde el título
        content_type = self.title_inference.infer_type(title)
        
        # Obtener plantillas para este tipo
        templates = self.conversation_templates.get(content_type, self.conversation_templates['general'])
        
        # Generar preguntas básicas (máximo 3 para dejar espacio a la pregunta profunda)
        basic_questions = templates.get('basic', templates if isinstance(templates, list) else [])
        for question_template in basic_questions[:3]:  # Máximo 3 básicas
            question = question_template.format(topic=title)
            
            # Generar respuesta contextual
            answer = self._generate_contextual_answer(question, title, content, content_type)
            
            # Calcular métricas de confianza
            confidence = self.confidence_metrics.calculate_confidence(
                title=title,
                content=content,
                category=category,
                subcategory=subcategory,
                question_type=self.classify_question_type_fast(question)
            )
            
            conversations.append({
                'question': question,
                'answer': answer,
                'category': category,
                'subcategory': subcategory,
                'content_type': content_type,
                'confidence_score': confidence.get('global_confidence', 0.8),
                'conversation_type': self.classify_question_type_fast(question)
            })
        
        # SIEMPRE generar pregunta de análisis en profundidad
        if 'deep' in templates:
            deep_question = templates['deep'].format(topic=title)
            deep_answer = self._generate_deep_analysis(title, content, content_type)
            
            confidence = self.confidence_metrics.calculate_confidence(
                title=title,
                content=content,
                category=category,
                subcategory=subcategory,
                question_type='deep_analysis'
            )
            
            conversations.append({
                'question': deep_question,
                'answer': deep_answer,
                'category': category,
                'subcategory': subcategory,
                'content_type': content_type,
                'confidence_score': confidence.get('global_confidence', 0.9),
                'conversation_type': 'deep_analysis',
                'analysis_type': 'comprehensive'
            })
            
        return conversations
        
    def _generate_contextual_answer(self, question: str, title: str, content: str, content_type: str) -> str:
        """Genera respuestas contextuales basadas en el tipo de pregunta y contenido"""
        
        # Determinar el tiempo verbal apropiado según el tipo de entidad
        verb_tense = self._determine_verb_tense(title, content, content_type)
        
        # Buscar información específica según el tipo de pregunta
        if 'cuándo' in question.lower() or 'año' in question.lower():
            # Buscar fechas y eventos temporales
            dates = re.findall(r'\b(?:en )?(\d{1,2} de \w+ de \d{4}|\d{4}|\w+ de \d{4})\b', content)
            if dates:
                return f"En relación a {title}, las fechas relevantes son: {', '.join(dates[:3])}"
                
        elif 'dónde' in question.lower():
            # Buscar ubicaciones geográficas
            locations = re.findall(r'(?:en|de|desde) ([A-Z][a-záéíóúñü][a-záéíóúñü\s]+?)(?:[.,;]|$)', content)
            if locations:
                clean_locations = [loc.strip() for loc in locations[:2] if len(loc.strip()) > 2]
                if clean_locations:
                    return f"{title} se localiza o tiene relación con: {', '.join(clean_locations)}"
        
        elif 'quién' in question.lower():
            # Buscar personas mencionadas
            people = re.findall(r'\b([A-Z][a-záéíóúñü]+ [A-Z][a-záéíóúñü]+)\b', content)
            if people and content_type == 'persona':
                bio_info = self._extract_biographical_info(content, verb_tense)
                return f"{title} {bio_info}"
                
        elif 'qué' in question.lower():
            # Definiciones y descripciones con tiempo verbal apropiado
            first_sentence = self._extract_definition_sentence(content, title, verb_tense)
            if first_sentence:
                return first_sentence
        
        # Respuesta contextual genérica pero informativa
        sentences = content.split('.')[:3]
        relevant_sentence = ""
        for sentence in sentences:
            sentence = sentence.strip()
            if 50 < len(sentence) < 300 and title.lower() in sentence.lower():
                relevant_sentence = sentence
                break
        
        if relevant_sentence:
            return relevant_sentence
        
        # Fallback inteligente por tipo de contenido con tiempo verbal correcto
        fallback_responses = {
            'persona': f"{title} {verb_tense['ser']} una persona notable cuya vida y obra han tenido impacto significativo.",
            'lugar': f"{title} {verb_tense['ser']} una ubicación geográfica con características e historia particulares.",
            'fecha_evento': f"{title} representa un acontecimiento histórico de importancia.",
            'especie': f"{title} {verb_tense['ser']} una especie biológica con características específicas.",
            'obra_arte': f"{title} {verb_tense['ser']} una obra artística o cultural relevante.",
            'concepto': f"{title} {verb_tense['ser']} un concepto que requiere explicación detallada."
        }
        
        return fallback_responses.get(content_type, f"{title} {verb_tense['ser']} un tema de interés que merece explicación detallada.")
    
    def _determine_verb_tense(self, title: str, content: str, content_type: str) -> dict:
        """Determina si usar 'es/fue' basado en el análisis temporal"""
        
        # Para personas: detectar si están vivas o muertas
        if content_type == 'persona':
            is_deceased = self._is_person_deceased(content)
            if is_deceased:
                return {'ser': 'fue', 'estar': 'estuvo', 'tener': 'tuvo'}
            else:
                return {'ser': 'es', 'estar': 'está', 'tener': 'tiene'}
        
        # Para organizaciones: detectar si siguen existiendo
        elif self._is_organization(content):
            is_defunct = self._is_organization_defunct(content)
            if is_defunct:
                return {'ser': 'fue', 'estar': 'estuvo', 'tener': 'tuvo'}
            else:
                return {'ser': 'es', 'estar': 'está', 'tener': 'tiene'}
        
        # Para eventos históricos
        elif content_type == 'fecha_evento' or self._is_historical_event(content):
            return {'ser': 'fue', 'estar': 'estuvo', 'tener': 'tuvo'}
        
        # Default: presente
        return {'ser': 'es', 'estar': 'está', 'tener': 'tiene'}
    
    def _is_person_deceased(self, content: str) -> bool:
        """Detecta si una persona ha fallecido"""
        deceased_patterns = [
            r'\b(?:murió|falleció|fallecido|muerto|†)\b',
            r'\b\d{4}\s*[-–]\s*\d{4}\b',  # Años de nacimiento-muerte
            r'\b(?:difunto|finado|extinto)\b',
            r'\b(?:fue enterrado|fue sepultado)\b',
            r'\b(?:su muerte|su fallecimiento)\b'
        ]
        
        for pattern in deceased_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        
        return False
    
    def _is_organization(self, content: str) -> bool:
        """Detecta si el artículo es sobre una organización"""
        org_patterns = [
            r'\b(?:empresa|corporación|compañía|organización)\b',
            r'\b(?:fundada|creada|establecida) en\b',
            r'\b(?:club|equipo|banda|grupo)\b',
            r'\b(?:partido|movimiento|asociación)\b',
            r'\b(?:gobierno|ministerio|departamento)\b'
        ]
        
        for pattern in org_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        
        return False
    
    def _is_organization_defunct(self, content: str) -> bool:
        """Detecta si una organización ya no existe"""
        defunct_patterns = [
            r'\b(?:desaparecida|extinta|disuelta|cerrada)\b',
            r'\b(?:fue disuelta|fue cerrada|cesó sus operaciones)\b',
            r'\b(?:hasta \d{4}|terminó en \d{4})\b',
            r'\b(?:ya no existe|no existe más)\b',
            r'\b(?:ex-|antigua|former)\b',
            r'\b\d{4}\s*[-–]\s*\d{4}\b',  # Años de inicio-fin
            r'\b(?:quebró|en bancarrota|liquidada)\b'
        ]
        
        for pattern in defunct_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        
        return False
    
    def _is_historical_event(self, content: str) -> bool:
        """Detecta si es un evento histórico"""
        historical_patterns = [
            r'\b(?:guerra|batalla|revolución|conflicto)\b',
            r'\b(?:siglo|año|época|era)\b.*\b(?:XVIII|XIX|XX|XXI|\d{4})\b',
            r'\b(?:acontecimiento|evento|suceso) (?:histórico|importante)\b'
        ]
        
        for pattern in historical_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        
        return False
    
    def _generate_deep_analysis(self, title: str, content: str, content_type: str) -> str:
        """Genera análisis en profundidad basado en el tipo de contenido"""
        
        # Para preguntas profundas, siempre incluir el contenido completo pero organizado
        clean_content = self._clean_and_organize_content(content)
        
        # Si el contenido es muy corto, devolver tal como está
        if len(clean_content) < 200:
            return clean_content
        
        # Si el contenido es largo, organizarlo por secciones
        if len(clean_content) > 2000:
            return self._organize_long_content(title, clean_content, content_type)
        
        # Para contenido medio, devolver completo con introducción contextual
        return f"**{title}**\n\n{clean_content}"
    
    def _clean_and_organize_content(self, content: str) -> str:
        """Limpia y organiza el contenido del artículo"""
        
        # Eliminar elementos de Wiki markup que no aportan valor
        content = re.sub(r'\[\[Archivo:.*?\]\]', '', content)
        content = re.sub(r'\[\[Imagen:.*?\]\]', '', content)
        content = re.sub(r'\{\{.*?\}\}', '', content, flags=re.DOTALL)
        content = re.sub(r'miniatura\|.*?\|', '', content)
        content = re.sub(r'thumb\|.*?\|', '', content)
        content = re.sub(r'<ref.*?</ref>', '', content, flags=re.DOTALL)
        content = re.sub(r'<ref.*?/>', '', content)
        
        # Limpiar enlaces pero mantener el texto
        content = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', content)
        content = re.sub(r'\[\[([^\]]+)\]\]', r'\1', content)
        
        # Limpiar formato
        content = re.sub(r"'''([^']+)'''", r'\1', content)
        content = re.sub(r"''([^']+)''", r'\1', content)
        content = re.sub(r'==([^=]+)==', r'\n\n**\1**\n', content)
        content = re.sub(r'===([^=]+)===', r'\n\n***\1***\n', content)
        
        # Normalizar espacios
        content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
        content = re.sub(r' +', ' ', content)
        
        return content.strip()
    
    def _organize_long_content(self, title: str, content: str, content_type: str) -> str:
        """Organiza contenido largo por secciones"""
        
        # Dividir por secciones marcadas con **
        sections = re.split(r'\n\n\*\*([^*]+)\*\*\n', content)
        
        organized_parts = [f"**{title}**\n"]
        
        # Agregar resumen inicial (primeros párrafos)
        first_part = sections[0].strip()
        if first_part:
            paragraphs = first_part.split('\n\n')
            intro = '\n\n'.join(paragraphs[:2])  # Primeros 2 párrafos
            if intro:
                organized_parts.append(f"**Información General:**\n{intro}\n")
        
        # Agregar secciones organizadas
        for i in range(1, len(sections), 2):
            if i + 1 < len(sections):
                section_title = sections[i].strip()
                section_content = sections[i + 1].strip()
                
                if section_content and len(section_content) > 50:
                    # Limitar cada sección a un párrafo razonable
                    section_paragraphs = section_content.split('\n\n')
                    section_summary = '\n\n'.join(section_paragraphs[:2])
                    organized_parts.append(f"**{section_title}:**\n{section_summary}\n")
        
        # Si no hay secciones bien definidas, dividir por párrafos
        if len(organized_parts) == 1:
            paragraphs = content.split('\n\n')
            for i, para in enumerate(paragraphs[:5]):  # Máximo 5 párrafos
                if len(para.strip()) > 100:
                    organized_parts.append(para.strip())
        
        result = '\n\n'.join(organized_parts)
        
        # Asegurar que no sea excesivamente largo
        if len(result) > 3000:
            result = result[:3000] + "...\n\n*[Artículo completo disponible en la fuente original]*"
        
        return result
    
    def _analyze_person(self, title: str, paragraphs: List[str]) -> str:
        """Análisis profundo de personas"""
        analysis_parts = []
        
        # Información biográfica
        for p in paragraphs[:2]:
            if any(keyword in p.lower() for keyword in ['nació', 'nacido', 'nacida', 'vida', 'biografía']):
                analysis_parts.append(f"Biografía: {p}")
                break
        
        # Logros y obra
        for p in paragraphs:
            if any(keyword in p.lower() for keyword in ['obra', 'trabajo', 'carrera', 'logro', 'contribución']):
                analysis_parts.append(f"Contribuciones: {p}")
                break
        
        # Legado
        for p in paragraphs:
            if any(keyword in p.lower() for keyword in ['legado', 'influencia', 'importancia', 'reconocimiento']):
                analysis_parts.append(f"Legado: {p}")
                break
        
        if not analysis_parts:
            analysis_parts = paragraphs[:3]
        
        return f"{title} - Análisis completo:\n\n" + "\n\n".join(analysis_parts)
    
    def _analyze_location(self, title: str, paragraphs: List[str]) -> str:
        """Análisis profundo de lugares"""
        return f"{title} - Análisis geográfico completo:\n\n" + "\n\n".join(paragraphs[:3])
    
    def _analyze_event(self, title: str, paragraphs: List[str]) -> str:
        """Análisis profundo de eventos históricos"""
        return f"{title} - Análisis histórico completo:\n\n" + "\n\n".join(paragraphs[:3])
    
    def _analyze_species(self, title: str, paragraphs: List[str]) -> str:
        """Análisis profundo de especies"""
        return f"{title} - Análisis biológico completo:\n\n" + "\n\n".join(paragraphs[:3])
    
    def _analyze_artwork(self, title: str, paragraphs: List[str]) -> str:
        """Análisis profundo de obras artísticas"""
        return f"{title} - Análisis artístico completo:\n\n" + "\n\n".join(paragraphs[:3])
    
    def _analyze_general(self, title: str, paragraphs: List[str]) -> str:
        """Análisis profundo general"""
        return f"{title} - Análisis detallado:\n\n" + "\n\n".join(paragraphs[:3])
    
    def _extract_definition_sentence(self, content: str, title: str, verb_tense: dict) -> str:
        """Extrae la oración de definición principal con tiempo verbal apropiado"""
        sentences = content.split('.')
        for sentence in sentences[:5]:
            sentence = sentence.strip()
            if (title.lower() in sentence.lower() and 
                any(verb in sentence.lower() for verb in ['es', 'fue', 'son', 'era', 'constituye']) and
                50 < len(sentence) < 400):
                # Ajustar tiempo verbal en la oración encontrada
                if 'fue' in sentence.lower() and verb_tense['ser'] == 'es':
                    sentence = re.sub(r'\bfue\b', 'es', sentence, flags=re.IGNORECASE)
                elif 'es' in sentence.lower() and verb_tense['ser'] == 'fue':
                    sentence = re.sub(r'\bes\b', 'fue', sentence, flags=re.IGNORECASE)
                return sentence
        return ""
    
    def _extract_biographical_info(self, content: str, verb_tense: dict) -> str:
        """Extrae información biográfica clave con tiempo verbal apropiado"""
        bio_patterns = [
            r'(?:nació|nacido|nacida).{0,100}',
            r'(?:fue|era).{0,100}',
            r'(?:\d{4}).{0,100}'
        ]
        
        for pattern in bio_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                bio_info = matches[0][:200]
                # Ajustar tiempo verbal
                if verb_tense['ser'] == 'fue':
                    bio_info = re.sub(r'\bes\b', 'fue', bio_info, flags=re.IGNORECASE)
                return bio_info
        
        return f"{verb_tense['ser']} una figura notable"
        
    def classify_question_type_fast(self, question: str) -> str:
        """Clasificación rápida de tipo de pregunta"""
        if 'qué es' in question or 'define' in question:
            return 'definicion'
        elif 'cuándo' in question or 'año' in question:
            return 'temporal'
        elif 'dónde' in question:
            return 'espacial'
        elif 'por qué' in question:
            return 'explicacion'
        else:
            return 'general'


class CategoryManager:
    """Gestor de categorías con límite de 100 carpetas máximo"""
    
    def __init__(self, max_categories: int = 100):
        self.max_categories = max_categories
        self.category_counts = defaultdict(int)
        self.subcategory_counts = defaultdict(lambda: defaultdict(int))
        self.final_categories = {}
        self.generic_categories = {}
        self.finalized = False
        self.total_articles = 0
        
    def register_category(self, category: str, subcategory: str = 'general'):
        """Registra una categoría encontrada durante el procesamiento"""
        if self.finalized:
            return self.get_final_category(category, subcategory)
            
        self.category_counts[category] += 1
        self.subcategory_counts[category][subcategory] += 1
        self.total_articles += 1
        
        # Crear nombre de categoría combinada
        return f"{category}-{subcategory}" if subcategory != 'general' else category
    
    def get_final_category(self, category: str, subcategory: str = 'general') -> str:
        """Obtiene la categoría final después de la finalización"""
        if not self.finalized:
            return self.register_category(category, subcategory)
            
        # Intentar encontrar la combinación exacta
        combination_name = f"{category}-{subcategory}" if subcategory != 'general' else category
        
        if combination_name in self.final_categories:
            return self.final_categories[combination_name]
            
        # Si la categoría principal tiene una versión genérica
        if category in self.generic_categories:
            return self.generic_categories[category]
            
        # Fallback a genérico
        return 'generico'
    
    def get_category_stats(self) -> Dict:
        """Estadísticas de las categorías"""
        if not self.finalized:
            return {
                'status': 'not_finalized',
                'categories_found': len(self.category_counts),
                'total_articles': self.total_articles
            }
            
        return {
            'status': 'finalized',
            'total_categories_found': len(self.category_counts),
            'final_categories_count': len(self.final_categories),
            'total_articles': self.total_articles,
            'most_popular': dict(sorted(self.category_counts.items(), key=lambda x: x[1], reverse=True)[:10]),
            'final_categories': list(self.final_categories.keys()),
            'generic_categories': self.generic_categories
        }

    def finalize_categories(self):
        """Finaliza las categorías - simplemente marca como finalizado para CategoryManager"""
        if not self.finalized:
            print(f"🏷️ CATEGORYMANAGER: Finalizando {len(self.category_counts)} categorías")
            self.finalized = True
            print(f"✅ CATEGORYMANAGER: Categorías finalizadas")

    def get_all_categories(self) -> List[str]:
        """Retorna todas las categorías encontradas"""
        return list(self.category_counts.keys())


class ContentManager:
    """Gestor de contenido principal optimizado para máximo throughput"""
    
    def __init__(self, use_category_manager: bool = True):
        self.categorizer = IntelligentCategorizer()
        self.category_manager = CategoryManager() if use_category_manager else None
        self.title_inference = TitleInferenceEngine()
        self.confidence_metrics = ConfidenceMetrics()
        # Acceder a las plantillas desde el categorizador
        self.conversation_templates = self.categorizer.conversation_templates
        self.stats = {
            'articles_processed': 0,
            'conversations_generated': 0,
            'categorization_time': 0,
            'conversation_time': 0,
            'conversation_errors': 0
        }
        
    def process_article(self, article: Dict) -> Optional[Dict]:
        """
        Procesa un artículo completo de forma ultra-rápida
        Returns: Dict con categoría, subcategoría, conversaciones
        """
        title = article.get('title', '').strip()
        content = article.get('content', '').strip()
        
        if not title or not content or len(content) < 50:
            return None
            
        try:
            # Categorización ultra-rápida
            category, subcategory, confidence = self.categorizer.categorize_article_fast(title, content)
            
            # Registrar en CategoryManager si está habilitado
            final_category = category
            if self.category_manager:
                final_category = self.category_manager.register_category(category, subcategory)
            
            # Generación rápida de conversaciones
            conversations = self.categorizer.generate_conversations_fast(title, content, category, subcategory)
            
            # Resultado optimizado
            result = {
                'title': title,
                'category': category,
                'subcategory': subcategory,
                'final_category': final_category,  # Para usar en la escritura de archivos
                'confidence': confidence,
                'conversations': conversations,
                'metadata': {
                    'content_length': len(content),
                    'conversations_count': len(conversations),
                    'processed_at': datetime.now().isoformat()
                }
            }
            
            self.stats['articles_processed'] += 1
            self.stats['conversations_generated'] += len(conversations)
            
            return result
            
        except Exception as e:
            return None
    
    def process_article_batch(self, articles: List[Dict]) -> List[Dict]:
        """Procesa un batch completo de artículos de forma masiva"""
        results = []
        
        for article in articles:
            result = self.process_article(article)
            if result:
                results.append(result)
                
        return results
    
    def finalize_categories(self):
        """Finaliza las categorías si se está usando CategoryManager"""
        if self.category_manager:
            self.category_manager.finalize_categories()
    
    def get_final_category(self, category: str, subcategory: str = 'general') -> str:
        """Obtiene la categoría final para un artículo"""
        if self.category_manager:
            return self.category_manager.get_final_category(category, subcategory)
        return f"{category}-{subcategory}" if subcategory != 'general' else category
    
    def get_categories(self) -> List[str]:
        """Lista de categorías disponibles"""
        return ['arte', 'geografia', 'historia', 'ciencias', 'biologia', 'tecnologia', 
                'deportes', 'politica', 'medicina', 'economia', 'educacion', 'general']
    
    def generate_consciencia_category(self, categories_found: list, output_dir: str, total_articles: int = 0) -> dict:
        """Genera la categoría consciencia con reconocimiento de Wikipedia y análisis completo de categorías"""
        
        from pathlib import Path
        import json
        from datetime import datetime
        
        # Obtener categorías reales del CategoryManager si está disponible
        if self.category_manager:
            actual_categories = self.category_manager.get_all_categories()
            if actual_categories:
                categories_found = actual_categories
                print(f"🏷️ CONSCIENCIA: Usando categorías del CategoryManager: {len(categories_found)} categorías")
            else:
                print(f"🏷️ CONSCIENCIA: CategoryManager vacío, usando categorías pasadas: {len(categories_found)} categorías")
        else:
            print(f"🏷️ CONSCIENCIA: Sin CategoryManager, usando categorías pasadas: {len(categories_found)} categorías")
        
        # Crear directorio consciencia
        consciencia_dir = Path(output_dir) / "consciencia"
        consciencia_dir.mkdir(parents=True, exist_ok=True)
        
        # Crear conversaciones sobre conocimiento y categorías
        consciencia_conversations = []
        
        # 1. RECONOCIMIENTO DE WIKIPEDIA Y FUENTE DEL CONOCIMIENTO
        wikipedia_article = {
            'title': 'Reconocimiento de Wikipedia como Fuente de Conocimiento',
            'content': f'''Este sistema está basado en el conocimiento contenido en Wikipedia, 
            la enciclopedia libre más grande del mundo. Wikipedia representa el conocimiento colaborativo 
            de millones de contribuyentes que han trabajado para crear y mantener artículos sobre 
            prácticamente todos los temas del conocimiento humano. 
            
            El contenido procesado proviene de artículos de Wikipedia en español, que contiene información 
            verificable, neutral y de fuentes confiables. Este sistema ha procesado {total_articles:,} artículos 
            de Wikipedia, organizándolos en {len(categories_found)} categorías principales para facilitar 
            la búsqueda y exploración del conocimiento.
            
            Wikipedia es una fuente invaluable de conocimiento porque:
            - Contiene información actualizada constantemente
            - Es revisada por una comunidad global de editores
            - Cita fuentes verificables y confiables
            - Mantiene neutralidad en los puntos de vista
            - Es de acceso libre y gratuito para toda la humanidad
            
            Este sistema reconoce y honra el trabajo de todos los editores de Wikipedia que han hecho 
            posible esta vasta base de conocimiento.'''
        }
        
        wikipedia_result = self.process_article(wikipedia_article)
        if wikipedia_result:
            for conv in wikipedia_result['conversations']:
                consciencia_conversations.append({
                    'question': conv['question'],
                    'answer': conv['answer'],
                    'category': 'consciencia',
                    'subcategory': 'reconocimiento_wikipedia',
                    'conversation_type': 'wikipedia_recognition'
                })
        
        # 2. SISTEMA DE CATEGORIZACIÓN Y ORGANIZACIÓN DEL CONOCIMIENTO
        categorization_article = {
            'title': 'Sistema de Categorización del Conocimiento',
            'content': f'''Este sistema ha analizado y categorizado {total_articles:,} artículos de Wikipedia 
            en {len(categories_found)} categorías fundamentales. El proceso de categorización utiliza algoritmos 
            inteligentes que analizan el contenido de cada artículo para determinar su categoría más apropiada.
            
            Las categorías fundamentales identificadas representan las principales áreas del conocimiento humano:
            {", ".join(categories_found)}
            
            El sistema funciona mediante:
            - Análisis semántico del contenido de cada artículo
            - Identificación de palabras clave y patrones temáticos
            - Asignación a categorías basada en contenido y contexto
            - Organización jerárquica del conocimiento
            - Generación de conversaciones contextuales para cada tema
            
            Esta organización permite acceso eficiente a información específica y facilita la exploración 
            interdisciplinaria del conocimiento. Cada categoría contiene artículos especializados con 
            conversaciones diseñadas para responder preguntas comunes y proporcionar análisis profundos.'''
        }
        
        categorization_result = self.process_article(categorization_article)
        if categorization_result:
            for conv in categorization_result['conversations']:
                consciencia_conversations.append({
                    'question': conv['question'],
                    'answer': conv['answer'],
                    'category': 'consciencia',
                    'subcategory': 'sistema_categorizacion',
                    'conversation_type': 'categorization_explanation'
                })
        
        # 3. EXPLICACIÓN DETALLADA DE CADA CATEGORÍA ENCONTRADA
        for i, category in enumerate(categories_found):
            # Obtener información específica de cada categoría
            category_info = self._get_category_explanation(category)
            
            category_article = {
                'title': f'Categoría {category.title()}: {category_info["description"]}',
                'content': f'''La categoría "{category}" es una de las {len(categories_found)} categorías fundamentales 
                identificadas en este sistema de conocimiento. 
                
                **Descripción:** {category_info["description"]}
                
                **Alcance:** {category_info["scope"]}
                
                **Tipos de contenido:** {category_info["content_types"]}
                
                **Importancia:** Esta categoría es fundamental porque {category_info["importance"]}
                
                **Ejemplos de temas:** {category_info["examples"]}
                
                Los artículos en esta categoría han sido procesados y organizados para proporcionar 
                respuestas especializadas sobre {category}. Cada artículo incluye conversaciones 
                contextuales que permiten explorar el tema desde múltiples perspectivas.
                
                La información en esta categoría proviene de Wikipedia y mantiene los estándares 
                de verificabilidad y neutralidad de la enciclopedia.'''
            }
            
            category_result = self.process_article(category_article)
            if category_result:
                for conv in category_result['conversations']:
                    consciencia_conversations.append({
                        'question': conv['question'],
                        'answer': conv['answer'],
                        'category': 'consciencia',
                        'subcategory': f'explicacion_{category}',
                        'conversation_type': 'category_explanation'
                    })
        
        # 4. CAPACIDADES Y FUNCIONAMIENTO DEL SISTEMA
        capabilities_article = {
            'title': 'Capacidades y Funcionamiento del Sistema de Conocimiento',
            'content': f'''Este sistema de conocimiento está diseñado para proporcionar acceso inteligente 
            a la información contenida en Wikipedia. Sus capacidades incluyen:
            
            **Capacidades de Búsqueda:**
            - Respuestas a preguntas específicas sobre cualquiera de las {len(categories_found)} categorías
            - Análisis profundo de temas mediante conversaciones contextuales
            - Explicaciones detalladas que van desde conceptos básicos hasta análisis avanzados
            - Conexiones interdisciplinarias entre diferentes áreas del conocimiento
            
            **Organización del Conocimiento:**
            - Categorización automática de {total_articles:,} artículos de Wikipedia
            - Estructuración jerárquica por temas y subtemas
            - Generación de conversaciones contextuales para cada artículo
            - Mantenimiento de la trazabilidad hacia las fuentes originales de Wikipedia
            
            **Tipos de Respuestas:**
            - Definiciones claras y precisas
            - Explicaciones históricas y contextuales
            - Datos geográficos y demográficos
            - Información científica y técnica
            - Análisis cultural y social
            - Biografías y perfiles detallados
            
            El sistema está en constante evolución y puede actualizarse con nuevo contenido de Wikipedia 
            para mantener la información actualizada y relevante.'''
        }
        
        capabilities_result = self.process_article(capabilities_article)
        if capabilities_result:
            for conv in capabilities_result['conversations']:
                consciencia_conversations.append({
                    'question': conv['question'],
                    'answer': conv['answer'],
                    'category': 'consciencia',
                    'subcategory': 'capacidades_sistema',
                    'conversation_type': 'system_capabilities'
                })
        
        # Escribir conversaciones en archivos JSONL
        conversations_per_file = 50000
        file_counter = 0
        
        for i in range(0, len(consciencia_conversations), conversations_per_file):
            file_counter += 1
            output_file = consciencia_dir / f"consciencia_{file_counter:04d}.jsonl"
            
            with open(output_file, 'w', encoding='utf-8') as f:
                batch = consciencia_conversations[i:i + conversations_per_file]
                for conv in batch:
                    # Formato de conversación estándar
                    conversation_record = {
                        'conversation': [
                            {'role': 'user', 'content': conv['question']},
                            {'role': 'assistant', 'content': conv['answer']}
                        ],
                        'metadata': {
                            'source_article': 'Sistema de Consciencia',
                            'category': 'consciencia',
                            'subcategory': conv['subcategory'],
                            'conversation_type': conv['conversation_type'],
                            'generation_date': datetime.now().isoformat(),
                            'categories_available': categories_found[:20]  # Primeras 20 categorías
                        }
                    }
                    f.write(json.dumps(conversation_record, ensure_ascii=False) + '\n')
        
        # Crear metadata
        metadata_file = consciencia_dir / "metadata_consciencia.json"
        metadata = {
            'category': 'consciencia',
            'total_conversations': len(consciencia_conversations),
            'total_files': file_counter,
            'categories_found': categories_found,
            'total_articles_processed': total_articles,
            'description': 'Conversaciones sobre el conocimiento disponible, reconocimiento de Wikipedia, categorización y capacidades del sistema',
            'generation_date': datetime.now().isoformat(),
            'conversation_types': ['wikipedia_recognition', 'categorization_explanation', 'category_explanation', 'system_capabilities'],
            'note': 'Categoría consciencia: Conocimiento sobre el sistema y sus fuentes'
        }
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        return {
            'total_conversations': len(consciencia_conversations),
            'total_files': file_counter,
            'categories_described': len(categories_found),
            'output_directory': str(consciencia_dir)
        }
    
    def _get_category_explanation(self, category: str) -> dict:
        """Proporciona explicaciones detalladas para cada categoría"""
        explanations = {
            'arte': {
                'description': 'Creaciones y expresiones culturales humanas',
                'scope': 'Música, pintura, literatura, teatro, danza, escultura, arquitectura, cine y todas las formas de expresión artística',
                'content_types': 'Biografías de artistas, obras específicas, movimientos artísticos, técnicas, historia del arte',
                'importance': 'representa la creatividad y expresión cultural de la humanidad a lo largo de la historia',
                'examples': 'pintores famosos, álbumes musicales, obras literarias, películas, esculturas, movimientos artísticos'
            },
            'geografia': {
                'description': 'Estudio de lugares, territorios y características terrestres',
                'scope': 'Países, ciudades, regiones, características físicas, demografía, clima, recursos naturales',
                'content_types': 'Información sobre lugares, datos demográficos, características geográficas, recursos naturales',
                'importance': 'nos ayuda a entender nuestro mundo, ubicaciones, culturas y características territoriales',
                'examples': 'países, ciudades, montañas, ríos, regiones, capitales, fronteras'
            },
            'historia': {
                'description': 'Registro y análisis de eventos y personalidades del pasado',
                'scope': 'Biografías, eventos históricos, períodos, civilizaciones, guerras, revoluciones',
                'content_types': 'Biografías de personajes históricos, descripciones de eventos, análisis de períodos',
                'importance': 'preserva la memoria humana y nos ayuda a entender el presente a través del pasado',
                'examples': 'personajes históricos, guerras, revoluciones, imperios, descubrimientos, fechas importantes'
            },
            'ciencias': {
                'description': 'Conocimiento sistemático sobre el mundo natural y técnico',
                'scope': 'Física, química, matemáticas, investigación, teorías, descubrimientos científicos',
                'content_types': 'Teorías científicas, biografías de científicos, experimentos, descubrimientos',
                'importance': 'expande nuestro entendimiento del universo y permite el progreso tecnológico',
                'examples': 'teorías científicas, científicos famosos, experimentos, descubrimientos, leyes naturales'
            },
            'biologia': {
                'description': 'Estudio de los seres vivos y sus procesos',
                'scope': 'Especies animales y vegetales, ecosistemas, evolución, genética, anatomía',
                'content_types': 'Descripciones de especies, información sobre ecosistemas, procesos biológicos',
                'importance': 'nos ayuda a entender la vida en todas sus formas y nuestra relación con otros seres vivos',
                'examples': 'especies animales, plantas, ecosistemas, procesos evolutivos, características biológicas'
            },
            'tecnologia': {
                'description': 'Herramientas, sistemas y procesos técnicos',
                'scope': 'Computación, internet, software, hardware, innovaciones tecnológicas',
                'content_types': 'Información sobre tecnologías, empresas tecnológicas, innovaciones',
                'importance': 'transforma la manera en que vivimos, trabajamos y nos comunicamos',
                'examples': 'computadoras, software, internet, aplicaciones, empresas tecnológicas'
            },
            'deportes': {
                'description': 'Actividades físicas competitivas y recreativas',
                'scope': 'Diferentes disciplinas deportivas, equipos, competiciones, atletas',
                'content_types': 'Información sobre deportes, equipos, atletas, competiciones, reglas',
                'importance': 'promueve la salud, la competencia sana y la unión social a través del deporte',
                'examples': 'fútbol, basketball, olimpiadas, equipos deportivos, atletas famosos'
            },
            'politica': {
                'description': 'Organización del poder y gobierno en las sociedades',
                'scope': 'Gobiernos, políticos, partidos, sistemas políticos, elecciones',
                'content_types': 'Biografías de políticos, información sobre gobiernos, sistemas políticos',
                'importance': 'define cómo se organizan las sociedades y se toman decisiones colectivas',
                'examples': 'presidentes, gobiernos, partidos políticos, elecciones, sistemas de gobierno'
            },
            'medicina': {
                'description': 'Ciencia y práctica de la salud humana',
                'scope': 'Enfermedades, tratamientos, anatomía, medicina preventiva, investigación médica',
                'content_types': 'Información sobre enfermedades, tratamientos, procedimientos médicos',
                'importance': 'preserva y mejora la salud humana, combate enfermedades y extiende la vida',
                'examples': 'enfermedades, tratamientos, medicamentos, procedimientos médicos, especialidades médicas'
            },
            'economia': {
                'description': 'Sistemas de producción, distribución y consumo',
                'scope': 'Empresas, mercados, comercio, finanzas, sistemas económicos',
                'content_types': 'Información sobre empresas, mercados, teorías económicas, sistemas financieros',
                'importance': 'organiza los recursos y la actividad económica de las sociedades',
                'examples': 'empresas, mercados, monedas, comercio, sistemas económicos'
            },
            'educacion': {
                'description': 'Procesos de enseñanza y aprendizaje',
                'scope': 'Instituciones educativas, métodos pedagógicos, sistemas educativos',
                'content_types': 'Información sobre universidades, escuelas, métodos educativos, pedagogía',
                'importance': 'transmite conocimiento y habilidades para el desarrollo personal y social',
                'examples': 'universidades, escuelas, métodos de enseñanza, sistemas educativos'
            },
            'general': {
                'description': 'Conocimiento diverso que no se limita a categorías específicas',
                'scope': 'Temas variados, conceptos multidisciplinarios, información general',
                'content_types': 'Información diversa, conceptos generales, temas multidisciplinarios',
                'importance': 'abarca conocimiento que conecta diferentes áreas y proporciona contexto general',
                'examples': 'conceptos generales, temas multidisciplinarios, información diversa'
            }
        }
        
        return explanations.get(category, {
            'description': f'Área especializada del conocimiento: {category}',
            'scope': f'Temas relacionados con {category} y sus subdisciplinas',
            'content_types': f'Información especializada sobre {category}',
            'importance': f'contribuye al conocimiento humano en el área de {category}',
            'examples': f'temas específicos de {category}'
        })
    
    def get_stats(self) -> Dict:
        """Estadísticas del procesador"""
        stats = self.stats.copy()
        if self.category_manager:
            stats['category_stats'] = self.category_manager.get_category_stats()
        return stats
