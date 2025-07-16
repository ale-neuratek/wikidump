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
- Sistema de categorías limitado a 100 máximo
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
    
    def finalize_categories(self):
        """Finaliza las categorías limitándolas a máximo 100"""
        if self.finalized:
            return
            
        print(f"🏷️ FINALIZANDO CATEGORÍAS: {len(self.category_counts)} categorías principales encontradas")
        print(f"📊 Total de artículos procesados: {self.total_articles:,}")
        
        # 1. Crear lista ordenada de todas las combinaciones categoría-subcategoría
        all_combinations = []
        
        for category, count in self.category_counts.items():
            for subcategory, sub_count in self.subcategory_counts[category].items():
                combination_name = f"{category}-{subcategory}" if subcategory != 'general' else category
                all_combinations.append({
                    'name': combination_name,
                    'category': category,
                    'subcategory': subcategory,
                    'count': sub_count,
                    'category_total': count,
                    'percentage': (sub_count / self.total_articles) * 100 if self.total_articles > 0 else 0
                })
        
        # 2. Ordenar por cantidad (descendente)
        all_combinations.sort(key=lambda x: x['count'], reverse=True)
        
        print(f"📈 Top 10 categorías más populares:")
        for i, combo in enumerate(all_combinations[:10]):
            print(f"   {i+1:2d}. {combo['name']}: {combo['count']:,} artículos ({combo['percentage']:.1f}%)")
        
        # 3. Seleccionar las top 90 (dejando 10% para genérico)
        top_90 = all_combinations[:90]
        
        # 4. Crear las categorías finales
        for combo in top_90:
            self.final_categories[combo['name']] = combo['name']
            
        # 5. Crear categorías genéricas para las principales no incluidas
        remaining_categories = {}
        for combo in all_combinations[90:]:
            main_cat = combo['category']
            if main_cat not in remaining_categories:
                remaining_categories[main_cat] = 0
            remaining_categories[main_cat] += combo['count']
        
        # Ordenar categorías restantes por cantidad
        sorted_remaining = sorted(remaining_categories.items(), key=lambda x: x[1], reverse=True)
        
        # 6. Agregar 'generico' como primera categoría genérica
        self.final_categories['generico'] = 'generico'
        generic_used = 1
        
        # 7. Agregar generico-{categoria} para las 8 más populares restantes
        for main_cat, count in sorted_remaining[:8]:
            if generic_used < 10:  # Máximo 10 categorías genéricas
                generic_name = f"generico-{main_cat}"
                self.final_categories[generic_name] = generic_name
                self.generic_categories[main_cat] = generic_name
                generic_used += 1
        
        # 8. Última categoría genérica para overflow
        if generic_used < 10:
            self.final_categories['generico-otros'] = 'generico-otros'
            generic_used += 1
        
        print(f"✅ CATEGORÍAS FINALIZADAS: {len(self.final_categories)} categorías")
        print(f"   📊 Específicas: {len(top_90)}")
        print(f"   🗂️ Genéricas: {generic_used}")
        print(f"   📋 Categorías genéricas: {list(self.generic_categories.keys())}")
        
        self.finalized = True
        
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


class ContentManager:
    """Gestor de contenido principal optimizado para máximo throughput"""
    
    def __init__(self, use_category_manager: bool = True):
        self.categorizer = IntelligentCategorizer()
        self.category_manager = CategoryManager() if use_category_manager else None
        self.stats = {
            'articles_processed': 0,
            'conversations_generated': 0,
            'categorization_time': 0,
            'conversation_time': 0
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
    
    def get_stats(self) -> Dict:
        """Estadísticas del procesador"""
        stats = self.stats.copy()
        if self.category_manager:
            stats['category_stats'] = self.category_manager.get_category_stats()
        return stats
