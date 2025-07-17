#!/usr/bin/env python3
"""
🧠 CONTENT MANAGER REFACTORIZADO - Con FormationSystem para generación
==================================================================================
Módulo refactorizado que usa FormationSystem para generar conversaciones de alta calidad.
El content_manager SOLO CONSUME configuraciones, no las modifica.

ARQUITECTURA:
- formation/fundamental.jsonl: Fuente de configuración (solo lectura)
- formation_system: Interface para acceso a configuraciones
- test_fundamental_quality_content.py: Tool para optimizar formation_training/fundamental.jsonl
"""
import json
import re
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path

# Importar FormationSystem 
import sys
sys.path.append(str(Path(__file__).parent / "formation_fundamental"))
from formation_system import FormationFundamentalSystem


class FormationLoader:
    """Cargador de configuraciones desde archivos JSON"""
    
    def __init__(self, formation_dir: str = "formation"):
        self.formation_dir = Path(formation_dir)
        self.fundamental_data = {}
        self.categories_data = {}
        self.load_all_configurations()
    
    def load_all_configurations(self):
        """Carga todas las configuraciones desde los archivos JSON"""
        # Cargar fundamental.jsonl
        fundamental_path = self.formation_dir / "fundamental.jsonl"
        if fundamental_path.exists():
            self.fundamental_data = self.load_jsonl(fundamental_path)
        
        # Cargar archivos de categorías
        for category_file in self.formation_dir.glob("*.jsonl"):
            if category_file.name != "fundamental.jsonl":
                category_name = category_file.stem
                self.categories_data[category_name] = self.load_jsonl(category_file)
    
    def load_jsonl(self, file_path: Path) -> Dict:
        """Carga un archivo JSONL y organiza los datos por tipo"""
        data = {}
        if not file_path.exists():
            return data
            
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        item = json.loads(line)
                        data_type = item.get('type', 'unknown')
                        data[data_type] = item.get('data', {})
        except Exception as e:
            print(f"Error cargando {file_path}: {e}")
        
        return data
    
    def get_fundamental_data(self, data_type: str) -> Any:
        """Obtiene datos fundamentales por tipo"""
        return self.fundamental_data.get(data_type, {})
    
    def get_category_data(self, category: str, data_type: str) -> Any:
        """Obtiene datos específicos de una categoría"""
        return self.categories_data.get(category, {}).get(data_type, {})
    
    def get_all_categories(self) -> List[str]:
        """Obtiene todas las categorías disponibles"""
        return list(self.categories_data.keys())


class TitleInferenceEngine:
    """Motor de inferencia inteligente para generar preguntas basadas en el título"""
    
    def __init__(self, formation_loader: FormationLoader):
        self.formation_loader = formation_loader
        self.title_patterns = self._load_title_patterns()
        
    def _load_title_patterns(self) -> Dict:
        """Carga los patrones de títulos desde la configuración fundamental"""
        patterns = {}
        
        # Cargar cada tipo de patrón
        pattern_types = ['persona', 'fecha_evento', 'lugar', 'especie', 'obra_arte', 'concepto']
        for pattern_type in pattern_types:
            pattern_data = self.formation_loader.get_fundamental_data('title_patterns')
            if pattern_type in pattern_data:
                patterns[pattern_type] = pattern_data[pattern_type]
        
        return patterns
    
    def infer_type(self, title: str) -> str:
        """Infiere el tipo de contenido basado solo en el título"""
        text = title.lower()
        
        # Reglas específicas prioritarias
        if re.match(r'^[A-Z][a-z]+ [a-z]+$', title) and len(title.split()) == 2:
            if any(word in text for word in ['panthera', 'canis', 'felis', 'homo', 'sus', 'equus']):
                return 'especie'
        
        if any(word in text for word in ['guerra', 'batalla', 'revolución', 'independencia', 'conflicto']):
            return 'fecha_evento'
        
        if any(phrase in text for phrase in ['don ', 'el ', 'la ', 'los ', 'las ']) and len(title.split()) > 2:
            if any(word in text for word in ['quijote', 'miserables', 'divina', 'comedia', 'novel', 'obra']):
                return 'obra_arte'
        
        words = title.split()
        if len(words) == 2:
            if all(word[0].isupper() and word[1:].islower() for word in words):
                if not any(excluded in text for excluded in ['guerra', 'batalla', 'de la', 'del ']):
                    return 'persona'
        
        # Sistema de puntuación con patrones cargados
        best_type = 'concepto'
        max_score = 0
        
        for type_name, config in self.title_patterns.items():
            score = 0
            
            for pattern in config['patterns']:
                matches = len(re.findall(pattern, text, re.IGNORECASE))
                score += matches * 2
            
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
        
        best_type = 'concepto'
        max_score = 0
        
        for type_name, config in self.title_patterns.items():
            score = 0
            
            for pattern in config['patterns']:
                matches = len(re.findall(pattern, text, re.IGNORECASE))
                score += matches * 2
            
            title_words = len(title.split())
            if type_name == 'persona' and 2 <= title_words <= 3:
                score += 3
            elif type_name == 'lugar' and 1 <= title_words <= 4:
                score += 2
            
            weighted_score = score * config['weight']
            
            if weighted_score > max_score:
                max_score = weighted_score
                best_type = type_name
        
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
    
    def __init__(self, formation_loader: FormationLoader):
        self.formation_loader = formation_loader
        self.metrics_history = []
        self.low_confidence_articles = []
        
    def calculate_confidence(self, title: str, content: str, category: str, 
                           subcategory: str, question_type: str) -> Dict[str, float]:
        """Calcula múltiples métricas de confianza"""
        
        category_confidence = self._calculate_category_confidence(content, category)
        question_confidence = self._calculate_question_confidence(title, question_type)
        content_confidence = self._calculate_content_confidence(content)
        title_confidence = self._calculate_title_confidence(title)
        
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
        
        self.metrics_history.append({
            'title': title,
            'category': category,
            'metrics': metrics,
            'timestamp': datetime.now().isoformat()
        })
        
        if global_confidence < 0.6:
            self.low_confidence_articles.append({
                'title': title,
                'category': category,
                'confidence': global_confidence,
                'issues': self._identify_issues(metrics)
            })
        
        return metrics
    
    def _calculate_category_confidence(self, content: str, category: str) -> float:
        """Confianza de la categorización usando keywords desde configuración"""
        category_keywords = self.formation_loader.get_category_data(category, 'category_keywords')
        
        if not category_keywords:
            return 0.5
        
        content_lower = content.lower()
        matches = sum(1 for keyword in category_keywords if keyword in content_lower)
        confidence = min(matches / len(category_keywords), 1.0)
        
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


class IntelligentCategorizer:
    """Categorizador inteligente con configuración cargada desde JSON"""
    
    def __init__(self, formation_loader: FormationLoader):
        self.formation_loader = formation_loader
        self.setup_intelligent_patterns()
        self.setup_subcategory_patterns()
        self.setup_conversation_templates()
        self.title_inference = TitleInferenceEngine(formation_loader)
        self.confidence_metrics = ConfidenceMetrics(formation_loader)
        
        self.category_corrections = defaultdict(int)
        self.confidence_scores = defaultdict(list)
        
    def setup_intelligent_patterns(self):
        """Configura patrones inteligentes desde archivos JSON"""
        self.categories = {}
        
        # Cargar configuración de cada categoría
        for category in self.formation_loader.get_all_categories():
            category_config = self.formation_loader.get_category_data(category, 'category_config')
            if category_config:
                self.categories[category] = category_config
        
        # Cargar patrones especiales desde fundamental
        self.music_patterns = self.formation_loader.get_fundamental_data('music_patterns')
        self.art_patterns = self.formation_loader.get_fundamental_data('art_patterns')
        
    def setup_subcategory_patterns(self):
        """Patrones para subcategorías desde archivos JSON"""
        self.subcategory_patterns = {}
        
        for category in self.formation_loader.get_all_categories():
            subcategory_patterns = self.formation_loader.get_category_data(category, 'subcategory_patterns')
            if subcategory_patterns:
                self.subcategory_patterns[category] = subcategory_patterns
        
    def setup_conversation_templates(self):
        """Plantillas de conversación desde archivos JSON"""
        # Cargar templates generales desde fundamental
        self.conversation_templates = self.formation_loader.get_fundamental_data('conversation_templates_general')
        
        # Cargar templates específicos por categoría
        for category in self.formation_loader.get_all_categories():
            category_templates = self.formation_loader.get_category_data(category, 'conversation_templates')
            if category_templates:
                self.conversation_templates[category] = category_templates
        
    def categorize_article_fast(self, title: str, content: str) -> Tuple[str, str, float]:
        """Categorización ultra-rápida optimizada"""
        text = f"{title} {content}".lower()
        
        # Verificar patrones musicales y artísticos primero
        if self.music_patterns:
            for pattern in self.music_patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    subcategory = self.identify_subcategory_fast('arte', text)
                    return 'arte', subcategory, 0.90
                    
        if self.art_patterns:
            for pattern in self.art_patterns:
                if re.search(pattern, text, re.IGNORECASE):
                    subcategory = self.identify_subcategory_fast('arte', text)
                    return 'arte', subcategory, 0.85
        
        # Scoring por categoría
        best_category = 'general'
        max_score = 0
        
        for category, config in self.categories.items():
            score = 0
            
            # Keywords
            if 'keywords' in config:
                for keyword in config['keywords']:
                    if keyword in text:
                        score += 2
                        
            # Patterns
            if score > 0 and 'patterns' in config:
                for pattern in config['patterns']:
                    if re.search(pattern, text, re.IGNORECASE):
                        score += 3
                        
                score *= config.get('weight', 1)
                
                if score > max_score:
                    max_score = score
                    best_category = category
        
        confidence = min(max_score / 20.0, 1.0) if max_score > 0 else 0.3
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
        """Generación inteligente de conversaciones con plantillas desde JSON"""
        conversations = []
        
        content_type = self.title_inference.infer_type(title)
        
        # Obtener plantillas
        templates = self.conversation_templates.get(content_type, 
                   self.conversation_templates.get(category, 
                   self.conversation_templates.get('general', {})))
        
        # Generar preguntas básicas
        if isinstance(templates, dict) and 'basic' in templates:
            basic_questions = templates['basic'][:3]
            for question_template in basic_questions:
                question = question_template.format(topic=title)
                answer = self._generate_contextual_answer(question, title, content, content_type)
                
                confidence = self.confidence_metrics.calculate_confidence(
                    title=title, content=content, category=category,
                    subcategory=subcategory, question_type=self.classify_question_type_fast(question)
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
        
        # Pregunta profunda
        if isinstance(templates, dict) and 'deep' in templates:
            deep_question = templates['deep'].format(topic=title)
            deep_answer = self._generate_deep_analysis(title, content, content_type)
            
            confidence = self.confidence_metrics.calculate_confidence(
                title=title, content=content, category=category,
                subcategory=subcategory, question_type='deep_analysis'
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
        """Genera respuestas contextuales"""
        verb_tense = self._determine_verb_tense(title, content, content_type)
        
        if 'cuándo' in question.lower() or 'año' in question.lower():
            dates = re.findall(r'\b(?:en )?(\d{1,2} de \w+ de \d{4}|\d{4}|\w+ de \d{4})\b', content)
            if dates:
                return f"En relación a {title}, las fechas relevantes son: {', '.join(dates[:3])}"
                
        elif 'dónde' in question.lower():
            locations = re.findall(r'(?:en|de|desde) ([A-Z][a-záéíóúñü][a-záéíóúñü\s]+?)(?:[.,;]|$)', content)
            if locations:
                clean_locations = [loc.strip() for loc in locations[:2] if len(loc.strip()) > 2]
                if clean_locations:
                    return f"{title} se localiza o tiene relación con: {', '.join(clean_locations)}"
        
        elif 'quién' in question.lower():
            if content_type == 'persona':
                bio_info = self._extract_biographical_info(content, verb_tense)
                return f"{title} {bio_info}"
                
        elif 'qué' in question.lower():
            first_sentence = self._extract_definition_sentence(content, title, verb_tense)
            if first_sentence:
                return first_sentence
        
        # Respuesta contextual genérica
        sentences = content.split('.')[:3]
        for sentence in sentences:
            sentence = sentence.strip()
            if 50 < len(sentence) < 300 and title.lower() in sentence.lower():
                return sentence
        
        # Fallback con plantillas desde JSON
        fallback_responses = self.formation_loader.get_fundamental_data('fallback_responses')
        verb_ser = verb_tense.get('ser', 'es')
        
        if content_type in fallback_responses:
            return fallback_responses[content_type].format(title=title, verb_ser=verb_ser)
        
        return f"{title} {verb_ser} un tema de interés que merece explicación detallada."
    
    def _determine_verb_tense(self, title: str, content: str, content_type: str) -> dict:
        """Determina tiempo verbal usando patrones desde JSON"""
        verb_tenses = self.formation_loader.get_fundamental_data('verb_tenses')
        
        if content_type == 'persona':
            if self._is_person_deceased(content):
                return verb_tenses.get('past', {'ser': 'fue', 'estar': 'estuvo', 'tener': 'tuvo'})
            else:
                return verb_tenses.get('present', {'ser': 'es', 'estar': 'está', 'tener': 'tiene'})
        
        elif self._is_organization(content):
            if self._is_organization_defunct(content):
                return verb_tenses.get('past', {'ser': 'fue', 'estar': 'estuvo', 'tener': 'tuvo'})
            else:
                return verb_tenses.get('present', {'ser': 'es', 'estar': 'está', 'tener': 'tiene'})
        
        elif content_type == 'fecha_evento' or self._is_historical_event(content):
            return verb_tenses.get('past', {'ser': 'fue', 'estar': 'estuvo', 'tener': 'tuvo'})
        
        return verb_tenses.get('present', {'ser': 'es', 'estar': 'está', 'tener': 'tiene'})
    
    def _is_person_deceased(self, content: str) -> bool:
        """Detecta si una persona ha fallecido usando patrones desde JSON"""
        deceased_patterns = self.formation_loader.get_fundamental_data('deceased_patterns')
        if not deceased_patterns:
            return False
            
        for pattern in deceased_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False
    
    def _is_organization(self, content: str) -> bool:
        """Detecta si el artículo es sobre una organización"""
        org_patterns = self.formation_loader.get_fundamental_data('organization_patterns')
        if not org_patterns:
            return False
            
        for pattern in org_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False
    
    def _is_organization_defunct(self, content: str) -> bool:
        """Detecta si una organización ya no existe"""
        defunct_patterns = self.formation_loader.get_fundamental_data('defunct_patterns')
        if not defunct_patterns:
            return False
            
        for pattern in defunct_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False
    
    def _is_historical_event(self, content: str) -> bool:
        """Detecta si es un evento histórico"""
        historical_patterns = self.formation_loader.get_fundamental_data('historical_patterns')
        if not historical_patterns:
            return False
            
        for pattern in historical_patterns:
            if re.search(pattern, content, re.IGNORECASE):
                return True
        return False
    
    def _generate_deep_analysis(self, title: str, content: str, content_type: str) -> str:
        """Genera análisis en profundidad"""
        clean_content = self._clean_and_organize_content(content)
        
        if len(clean_content) < 200:
            return clean_content
        
        if len(clean_content) > 2000:
            return self._organize_long_content(title, clean_content, content_type)
        
        return f"**{title}**\n\n{clean_content}"
    
    def _clean_and_organize_content(self, content: str) -> str:
        """Limpia y organiza el contenido del artículo"""
        content = re.sub(r'\[\[Archivo:.*?\]\]', '', content)
        content = re.sub(r'\[\[Imagen:.*?\]\]', '', content)
        content = re.sub(r'\{\{.*?\}\}', '', content, flags=re.DOTALL)
        content = re.sub(r'miniatura\|.*?\|', '', content)
        content = re.sub(r'thumb\|.*?\|', '', content)
        content = re.sub(r'<ref.*?</ref>', '', content, flags=re.DOTALL)
        content = re.sub(r'<ref.*?/>', '', content)
        content = re.sub(r'\[\[([^|\]]+)\|([^\]]+)\]\]', r'\2', content)
        content = re.sub(r'\[\[([^\]]+)\]\]', r'\1', content)
        content = re.sub(r"'''([^']+)'''", r'\1', content)
        content = re.sub(r"''([^']+)''", r'\1', content)
        content = re.sub(r'==([^=]+)==', r'\n\n**\1**\n', content)
        content = re.sub(r'===([^=]+)===', r'\n\n***\1***\n', content)
        content = re.sub(r'\n\s*\n\s*\n', '\n\n', content)
        content = re.sub(r' +', ' ', content)
        
        return content.strip()
    
    def _organize_long_content(self, title: str, content: str, content_type: str) -> str:
        """Organiza contenido largo por secciones"""
        sections = re.split(r'\n\n\*\*([^*]+)\*\*\n', content)
        organized_parts = [f"**{title}**\n"]
        
        first_part = sections[0].strip()
        if first_part:
            paragraphs = first_part.split('\n\n')
            intro = '\n\n'.join(paragraphs[:2])
            if intro:
                organized_parts.append(f"**Información General:**\n{intro}\n")
        
        for i in range(1, len(sections), 2):
            if i + 1 < len(sections):
                section_title = sections[i].strip()
                section_content = sections[i + 1].strip()
                
                if section_content and len(section_content) > 50:
                    section_paragraphs = section_content.split('\n\n')
                    section_summary = '\n\n'.join(section_paragraphs[:2])
                    organized_parts.append(f"**{section_title}:**\n{section_summary}\n")
        
        if len(organized_parts) == 1:
            paragraphs = content.split('\n\n')
            for i, para in enumerate(paragraphs[:5]):
                if len(para.strip()) > 100:
                    organized_parts.append(para.strip())
        
        result = '\n\n'.join(organized_parts)
        
        if len(result) > 3000:
            result = result[:3000] + "...\n\n*[Artículo completo disponible en la fuente original]*"
        
        return result
    
    def _extract_definition_sentence(self, content: str, title: str, verb_tense: dict) -> str:
        """Extrae la oración de definición principal"""
        sentences = content.split('.')
        for sentence in sentences[:5]:
            sentence = sentence.strip()
            if (title.lower() in sentence.lower() and 
                any(verb in sentence.lower() for verb in ['es', 'fue', 'son', 'era', 'constituye']) and
                50 < len(sentence) < 400):
                if 'fue' in sentence.lower() and verb_tense['ser'] == 'es':
                    sentence = re.sub(r'\bfue\b', 'es', sentence, flags=re.IGNORECASE)
                elif 'es' in sentence.lower() and verb_tense['ser'] == 'fue':
                    sentence = re.sub(r'\bes\b', 'fue', sentence, flags=re.IGNORECASE)
                return sentence
        return ""
    
    def _extract_biographical_info(self, content: str, verb_tense: dict) -> str:
        """Extrae información biográfica clave"""
        bio_patterns = [
            r'(?:nació|nacido|nacida).{0,100}',
            r'(?:fue|era).{0,100}',
            r'(?:\d{4}).{0,100}'
        ]
        
        for pattern in bio_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            if matches:
                bio_info = matches[0][:200]
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
        
        return f"{category}-{subcategory}" if subcategory != 'general' else category
    
    def get_final_category(self, category: str, subcategory: str = 'general') -> str:
        """Obtiene la categoría final después de la finalización"""
        if not self.finalized:
            return self.register_category(category, subcategory)
            
        combination_name = f"{category}-{subcategory}" if subcategory != 'general' else category
        
        if combination_name in self.final_categories:
            return self.final_categories[combination_name]
            
        if category in self.generic_categories:
            return self.generic_categories[category]
            
        return 'generico'
    
    def finalize_categories(self):
        """Finaliza las categorías"""
        if not self.finalized:
            print(f"🏷️ CATEGORYMANAGER: Finalizando {len(self.category_counts)} categorías")
            self.finalized = True
            print(f"✅ CATEGORYMANAGER: Categorías finalizadas")

    def get_all_categories(self) -> List[str]:
        """Retorna todas las categorías encontradas"""
        return list(self.category_counts.keys())


class ContentManager:
    """Gestor de contenido principal que usa FormationSystem para configuraciones"""
    
    def __init__(self, use_category_manager: bool = True, formation_dir: str = "formation"):
        # Usar FormationSystem para acceso a configuraciones (solo lectura)
        self.formation_system = FormationFundamentalSystem(formation_training_dir=formation_dir)
        
        # Cargar configuraciones desde formation_system
        self.formation_loader = FormationLoader(formation_dir)  # Mantener compatibilidad
        self.categorizer = IntelligentCategorizer(self.formation_loader)
        self.category_manager = CategoryManager() if use_category_manager else None
        self.title_inference = TitleInferenceEngine(self.formation_loader)
        self.confidence_metrics = ConfidenceMetrics(self.formation_loader)
        
        # Cargar plantillas desde FormationSystem (no desde formation_loader)
        self.conversation_templates = self.formation_system.get_conversation_templates()
        
        self.stats = {
            'articles_processed': 0,
            'conversations_generated': 0,
            'categorization_time': 0,
            'conversation_time': 0,
            'conversation_errors': 0
        }
    
    def add_pattern_to_training(self, pattern_data: Dict[str, Any]) -> bool:
        """
        NUEVO MÉTODO: Permite que test_fundamental_quality_content.py agregue patrones
        al fundamental.jsonl de formation_training (NO al de formation)
        
        Args:
            pattern_data: Datos del patrón a agregar
            
        Returns:
            bool: True si se agregó exitosamente
        """
        try:
            training_fundamental = Path("formation_training") / "fundamental.jsonl"
            
            # Crear entrada para el patrón
            pattern_entry = {
                'type': pattern_data.get('type', 'pattern_optimization'),
                'data': pattern_data,
                'timestamp': datetime.now().isoformat(),
                'source': 'test_fundamental_quality_content'
            }
            
            # Agregar al archivo de training (NO al de formation)
            with open(training_fundamental, 'a', encoding='utf-8') as f:
                json.dump(pattern_entry, f, ensure_ascii=False)
                f.write('\n')
            
            return True
            
        except Exception as e:
            print(f"❌ Error agregando patrón a training: {e}")
            return False
    
    def validate_conversation_quality(self, conversation: Dict[str, Any]) -> Dict[str, Any]:
        """
        NUEVO MÉTODO: Usa formation_system para validar calidad de conversaciones
        durante la generación (sin modificar configuraciones)
        
        Args:
            conversation: Conversación a validar
            
        Returns:
            dict: Resultado de validación con métricas
        """
        try:
            # Usar formation_system para validación (solo lectura)
            validation_result = self.formation_system.validate_conversation(conversation)
            return validation_result
            
        except Exception as e:
            return {
                'is_valid': False,
                'quality_score': 0.0,
                'issues': [f'Error en validación: {e}'],
                'metrics': {}
            }
    
    def get_optimized_templates(self, content_type: str = None) -> Dict[str, Any]:
        """
        NUEVO MÉTODO: Obtiene templates optimizados desde formation_system
        
        Args:
            content_type: Tipo de contenido específico
            
        Returns:
            dict: Templates optimizados
        """
        try:
            return self.formation_system.get_conversation_templates(content_type)
        except Exception as e:
            print(f"⚠️ Error obteniendo templates optimizados: {e}")
            # Fallback a templates básicos
            return self.conversation_templates
    
    def process_article(self, article: Dict) -> Optional[Dict]:
        """Procesa un artículo completo de forma ultra-rápida"""
        title = article.get('title', '').strip()
        content = article.get('content', '').strip()
        
        if not title or not content or len(content) < 50:
            return None
            
        try:
            category, subcategory, confidence = self.categorizer.categorize_article_fast(title, content)
            
            final_category = category
            if self.category_manager:
                final_category = self.category_manager.register_category(category, subcategory)
            
            conversations = self.categorizer.generate_conversations_fast(title, content, category, subcategory)
            
            # NUEVO: Validar calidad de conversaciones usando formation_system
            validated_conversations = []
            for conv in conversations:
                validation = self.validate_conversation_quality(conv)
                if validation.get('is_valid', False) and validation.get('quality_score', 0) > 0.5:
                    conv['quality_score'] = validation['quality_score']
                    conv['quality_metrics'] = validation.get('metrics', {})
                    validated_conversations.append(conv)
            
            self.stats['articles_processed'] += 1
            self.stats['conversations_generated'] += len(validated_conversations)
            
            return {
                'title': title,
                'content': content,
                'category': final_category,
                'subcategory': subcategory,
                'conversations': validated_conversations,  # Usar conversaciones validadas
                'confidence': confidence,
                'content_type': self.title_inference.infer_type(title)
            }
            
        except Exception as e:
            self.stats['conversation_errors'] += 1
            print(f"Error procesando artículo '{title}': {e}")
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
        """Lista de categorías disponibles desde la configuración JSON"""
        return self.formation_loader.get_all_categories()
    
    def generate_consciencia_category(self, categories_found: list, output_dir: str, total_articles: int = 0) -> dict:
        """Genera la categoría consciencia con reconocimiento de Wikipedia"""
        from pathlib import Path
        
        output_path = Path(output_dir) / "consciencia"
        output_path.mkdir(parents=True, exist_ok=True)
        
        consciencia_conversations = []
        
        # Usar plantillas desde JSON para generar conversaciones de consciencia
        consciencia_template_data = self.formation_loader.get_fundamental_data('consciencia_template')
        
        if consciencia_template_data:
            question = consciencia_template_data.get('question', 'Explica las categorías encontradas.')
            answer_template = consciencia_template_data.get('answer_template', 'Categorías: {categories_list}')
            
            # Formatear la respuesta con los datos específicos
            answer = answer_template.format(
                num_categories=len(categories_found),
                categories_list=', '.join(categories_found),
                total_articles=total_articles
            )
            
            consciencia_template = {
                'question': question,
                'answer': answer
            }
        else:
            # Fallback si no se encuentra el template
            consciencia_template = {
                'question': 'Explica qué categorías de conocimiento encontraste en este procesamiento de Wikipedia.',
                'answer': f"Se encontraron {len(categories_found)} categorías: {', '.join(categories_found)}. Total de artículos: {total_articles:,}"
            }
        
        consciencia_conversations.append(consciencia_template)
        
        # Guardar archivo JSONL
        output_file = output_path / "consciencia_categorias.jsonl"
        with open(output_file, 'w', encoding='utf-8') as f:
            for conv in consciencia_conversations:
                json.dump(conv, f, ensure_ascii=False)
                f.write('\n')
        
        return {
            'conversations_generated': len(consciencia_conversations),
            'files_created': 1,
            'categories_described': len(categories_found),
            'output_path': str(output_path)
        }


if __name__ == "__main__":
    # Ejemplo de uso
    content_manager = ContentManager()
    
    # Ejemplo de procesamiento
    test_article = {
        'title': 'Ludwig van Beethoven',
        'content': 'Ludwig van Beethoven fue un compositor y pianista alemán. Nació en 1770 y murió en 1827. Es considerado uno de los compositores más importantes de la música clásica.'
    }
    
    result = content_manager.process_article(test_article)
    if result:
        print(f"Artículo procesado: {result['title']}")
        print(f"Categoría: {result['category']}")
        print(f"Conversaciones generadas: {len(result['conversations'])}")
