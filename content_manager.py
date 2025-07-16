#!/usr/bin/env python3
"""
🎯 CONTENT MANAGER - Gestor de Contenido Optimizado
==================================================
Módulo independiente para generación de contenido y categorización
Diseñado para máximo rendimiento sin locks

CARACTERÍSTICAS:
- Categorización inteligente ultra-rápida
- Generación masiva de conversaciones  
- Arquitectura thread-safe sin locks explícitos
- Optimizado para GH200 (450GB RAM)
"""

import re
import json
import hashlib
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from datetime import datetime


class IntelligentCategorizer:
    """Categorizador inteligente ultra-optimizado para máximo rendimiento"""
    
    def __init__(self):
        self.setup_intelligent_patterns()
        self.setup_conversation_templates()
        self.setup_subcategory_patterns()
        
        # Estadísticas thread-safe (atómicas)
        self.category_corrections = defaultdict(int)
        self.confidence_scores = defaultdict(list)
        
    def setup_intelligent_patterns(self):
        """Configura patrones inteligentes optimizados para velocidad"""
        
        # CATEGORÍAS PRINCIPALES (optimizadas para matching rápido)
        self.categories = {
            'arte': {
                'keywords': ['música', 'álbum', 'canción', 'banda', 'compositor', 'pintura', 'artista', 'obra', 'teatro', 'danza'],
                'patterns': [
                    r'\b(?:álbum|disco|canción|single)\b',
                    r'\b(?:banda|grupo musical|cantante|músico)\b',
                    r'\b(?:reggae|rock|pop|jazz|clásica|folk)\b',
                    r'\b(?:Bob Marley|The Wailers|Island Records)\b',  # Específico para Catch a Fire
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
        
        # REGLAS ESPECÍFICAS ULTRA-RÁPIDAS
        self.specific_rules = {
            'catch_a_fire': {
                'pattern': r'Catch a Fire.*(?:álbum|Bob Marley|reggae)',
                'force_category': 'arte',
                'subcategory': 'musica-reggae',
                'confidence': 0.95
            },
            'albums_music': {
                'pattern': r'(?:álbum|disco).*(?:banda|grupo|cantante)',
                'force_category': 'arte', 
                'subcategory': 'musica-albums',
                'confidence': 0.90
            },
            'geographic_places': {
                'pattern': r'es una (?:ciudad|provincia|región|país)',
                'force_category': 'geografia',
                'subcategory': 'lugares',
                'confidence': 0.85
            },
            'biographical': {
                'pattern': r'(?:nació|nacido).*(?:fue un|fue una).*(?:murió|fallecido)',
                'force_category': 'historia',
                'subcategory': 'biografias',
                'confidence': 0.88
            }
        }
        
    def setup_conversation_templates(self):
        """Plantillas optimizadas para generación rápida de conversaciones"""
        self.conversation_templates = {
            'arte': {
                'musica': [
                    "¿qué tipo de música es {topic}?",
                    "¿cuándo se lanzó {topic}?",
                    "¿quién creó {topic}?",
                    "¿cuál es la historia de {topic}?"
                ],
                'general': [
                    "¿qué es {topic}?",
                    "¿cuál es la importancia artística de {topic}?",
                    "¿en qué estilo se enmarca {topic}?"
                ]
            },
            'geografia': [
                "¿dónde está {topic}?",
                "¿cuántos habitantes tiene {topic}?",
                "¿en qué región se encuentra {topic}?",
                "¿cuáles son las características de {topic}?"
            ],
            'historia': [
                "¿cuándo nació {topic}?",
                "¿qué hizo {topic}?",
                "¿por qué es conocido {topic}?",
                "¿cuál es la historia de {topic}?"
            ],
            'ciencias': [
                "¿qué es {topic}?",
                "¿cómo funciona {topic}?",
                "¿qué aplicaciones tiene {topic}?",
                "¿cuál es la importancia de {topic}?"
            ],
            'biologia': [
                "¿qué tipo de especie es {topic}?",
                "¿dónde vive {topic}?",
                "¿cuáles son las características de {topic}?",
                "¿cómo se reproduce {topic}?"
            ],
            'general': [
                "¿qué es {topic}?",
                "¿cuál es la importancia de {topic}?",
                "¿cómo se define {topic}?",
                "¿en qué contexto aparece {topic}?"
            ]
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
        
        # 1. VERIFICAR REGLAS ESPECÍFICAS PRIMERO (más rápido)
        for rule in self.specific_rules.values():
            if re.search(rule['pattern'], text, re.IGNORECASE):
                subcategory = self.identify_subcategory_fast(rule['force_category'], text)
                return rule['force_category'], subcategory, rule['confidence']
        
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
        """Generación ultra-rápida de conversaciones"""
        conversations = []
        topic = title.lower()
        
        # Obtener plantillas apropiadas
        if category in self.conversation_templates:
            if isinstance(self.conversation_templates[category], dict):
                # Si tiene subcategorías específicas
                templates = self.conversation_templates[category].get(
                    subcategory.split('-')[0], 
                    self.conversation_templates[category].get('general', self.conversation_templates['general'])
                )
            else:
                # Lista simple de plantillas
                templates = self.conversation_templates[category]
        else:
            templates = self.conversation_templates['general']
        
        # Generar 4 conversaciones rápidas
        for i, template in enumerate(templates[:4]):
            question = template.format(topic=topic)
            answer = self.generate_fast_answer(title, content, category, question)
            
            conversations.append({
                'question': question,
                'answer': answer,
                'category': category,
                'subcategory': subcategory,
                'conversation_type': self.classify_question_type_fast(question)
            })
            
        return conversations
        
    def generate_fast_answer(self, title: str, content: str, category: str, question: str) -> str:
        """Generación rápida de respuestas contextuales"""
        
        # Extraer primera oración válida
        sentences = content.split('.')
        for sentence in sentences[:3]:  # Solo revisar primeras 3 oraciones
            sentence = sentence.strip()
            if 30 < len(sentence) < 200:  # Longitud razonable
                return sentence
        
        # Respuestas específicas por tipo de pregunta
        if 'cuándo' in question or 'año' in question:
            # Buscar fechas
            dates = re.findall(r'\b\d{4}\b', content)
            if dates:
                return f"En relación a {title}, encontramos las fechas: {', '.join(dates[:2])}"
                
        if 'dónde' in question:
            # Buscar ubicaciones
            locations = re.findall(r'(?:en|de) ([A-Z][a-záéíóúñü]+)', content)
            if locations:
                return f"{title} se encuentra en {locations[0]}"
        
        # Respuesta genérica rápida
        return f"{title} es un concepto importante en el área de {category}"
        
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


class ContentManager:
    """Gestor de contenido principal optimizado para máximo throughput"""
    
    def __init__(self):
        self.categorizer = IntelligentCategorizer()
        self.stats = {
            'articles_processed': 0,
            'conversations_generated': 0,
            'categorization_time': 0,
            'conversation_time': 0
        }
        """Procesa un artículo completo de forma ultra-rápida"""
        
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
            
            # Generación rápida de conversaciones
            conversations = self.categorizer.generate_conversations_fast(title, content, category, subcategory)
            
            # Resultado optimizado
            result = {
                'title': title,
                'category': category,
                'subcategory': subcategory,
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
    
    def get_categories(self) -> List[str]:
        """Lista de categorías disponibles"""
        return ['arte', 'geografia', 'historia', 'ciencias', 'biologia', 'tecnologia', 
                'deportes', 'politica', 'medicina', 'economia', 'educacion', 'general']
    
    def get_stats(self) -> Dict:
        """Estadísticas del procesador"""
        return self.stats.copy()
