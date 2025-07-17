#!/usr/bin/env python3
"""
🧠 SIMPLIFIED CONTENT MANAGER - Gestor de contenido simplificado y legible
========================================================================
Sistema simplificado para generar conversaciones de alta calidad a partir
de artículos de Wikipedia. Fácil de leer, mantener y entender.

RESPONSABILIDADES PRINCIPALES:
- Procesar artículos de entrada
- Categorizar contenido automáticamente  
- Generar preguntas fundamentales y específicas
- Validar calidad de conversaciones
- Exportar resultados en formato JSONL

ARQUITECTURA SIMPLIFICADA:
- SimpleFormationSystem: Gestiona todas las configuraciones
- IntelligentQuestionGenerator: Genera preguntas con ranking
- CategoryManager: Clasifica contenido por categorías
- QualityValidator: Valida calidad de conversaciones
"""

import json
import re
import time
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path

# Importar sistema simplificado de formación
import sys
sys.path.append(str(Path(__file__).parent / "formation_fundamental"))
from simple_formation_system import SimpleFormationSystem
from intelligent_question_generator import IntelligentQuestionGenerator


class CategoryManager:
    """Gestor de categorización de contenido simplificado"""
    
    def __init__(self, formation_system: SimpleFormationSystem):
        self.formation_system = formation_system
        self.category_patterns = formation_system.get_title_patterns()
        
    def categorize_article(self, title: str, content: str) -> str:
        """Categoriza un artículo basado en título y contenido"""
        # Usar el sistema de formación para categorizar
        category = self.formation_system.get_category_for_title(title)
        
        if category == 'general':
            # Intento adicional basado en contenido
            category = self._categorize_by_content(content)
        
        return category
    
    def _categorize_by_content(self, content: str) -> str:
        """Categorización adicional basada en contenido"""
        content_lower = content.lower()
        
        # Patrones expandidos por contenido (MEJORADO)
        content_patterns = {
            'historia-tiempo': ['nacido', 'nació', 'murió', 'biografía', 'histórico', 'siglo', 'época', 'guerra', 'batalla', 'fecha', 'año', 'cronología'],
            'naturaleza-universo': ['ciencia', 'científico', 'investigación', 'física', 'química', 'biología', 'átomo', 'célula', 'planeta', 'especie', 'natural', 'género', 'familia', 'taxonomía'],
            'politica-gobernanza': ['política', 'político', 'gobierno', 'estado', 'ley', 'constitución', 'democracia', 'elección', 'partido', 'enmienda', 'cláusula'],
            'arte-estetica': ['arte', 'artista', 'música', 'pintura', 'literatura', 'teatro', 'cine', 'cultural', 'estético', 'belleza'],
            'tecnologia-herramientas': ['tecnología', 'computadora', 'software', 'internet', 'digital', 'algoritmo', 'programación', 'ingeniería'],
            'salud-cuerpo': ['salud', 'medicina', 'médico', 'hospital', 'enfermedad', 'tratamiento', 'psicológico', 'mental', 'físico'],
            'economia-produccion': ['economía', 'económico', 'empresa', 'negocio', 'mercado', 'finanzas', 'comercio', 'producción', 'trabajo'],
            'educacion-conocimiento': ['educación', 'educativo', 'universidad', 'escuela', 'enseñanza', 'aprendizaje', 'conocimiento', 'académico'],
            'comunicacion-lenguaje': ['comunicación', 'lenguaje', 'idioma', 'texto', 'discurso', 'narrativa', 'medios', 'información', 'lingüística'],
            'juego-recreacion': [
                # Deportes generales
                'deporte', 'deportivo', 'deportista', 'juego', 'entretenimiento', 'competencia', 'ocio', 'recreación', 'diversión',
                'atleta', 'olimpico', 'medallista', 'competición', 'campeonato', 'torneo',
                # Piragüismo específico
                'piragüismo', 'canoa', 'kayak', 'remo', 'paleta', 'aguas bravas',
                # Otros deportes
                'baloncesto', 'nba', 'draft', 'basket', 'basketball'
            ],
            'espacio-geografia': ['geografía', 'geográfico', 'territorio', 'región', 'lugar', 'ciudad', 'país', 'mapa', 'ubicación', 'provincia', 'distrito', 'cantón'],
            'transporte-movilidad': ['transporte', 'vehículo', 'carretera', 'aeropuerto', 'tren', 'viaje', 'movilidad', 'estación', 'línea', 'metro'],
            'matematicas-logica': ['matemática', 'matemático', 'número', 'ecuación', 'cálculo', 'estadística', 'lógica', 'teorema', 'regresión', 'logística', 'multinomial'],
            'etica-filosofia': ['filosofía', 'filosófico', 'ética', 'moral', 'valor', 'principio', 'creencia', 'pensamiento'],
            'sociedad-cultura': ['sociedad', 'social', 'cultura', 'cultural', 'tradición', 'religión', 'comunidad', 'antropología'],
            'consciencia': ['consciencia', 'conciencia', 'autoconciencia', 'reflexión', 'mindfulness', 'espiritual', 'meditación']
        }
        
        # Buscar coincidencias y contar frecuencia
        category_scores = {}
        for category, keywords in content_patterns.items():
            score = 0
            for keyword in keywords:
                # Contar todas las apariciones de la palabra clave
                score += content_lower.count(keyword)
            
            if score > 0:
                category_scores[category] = score
        
        # Devolver la categoría con mayor puntaje
        if category_scores:
            best_category = max(category_scores.items(), key=lambda x: x[1])[0]
            return best_category
        
        return 'general'


class QuestionGenerator:
    """Generador de preguntas simplificado"""
    
    def __init__(self, formation_system: SimpleFormationSystem, 
                 intelligent_generator: IntelligentQuestionGenerator):
        self.formation_system = formation_system
        self.intelligent_generator = intelligent_generator
        
    def generate_fundamental_questions(self, title: str, content: str, 
                                     max_questions: int = 5) -> List[Dict]:
        """Genera preguntas fundamentales de alta calidad"""
        questions = []
        
        # Pregunta obligatoria: explicación completa
        questions.append({
            'question': f'Explícame en detalle todo sobre {title} basándote en el artículo completo',
            'answer': self._generate_comprehensive_answer(title, content),
            'question_type': 'fundamental',
            'subtype': 'complete_explanation',
            'quality_score': 0.95,
            'priority': 1.0,
            'is_mandatory': True
        })
        
        # Preguntas fundamentales básicas
        fundamental_templates = [
            f'¿Qué es {title}?',
            f'¿Cuáles son las características principales de {title}?',
            f'¿Cuál es la importancia de {title}?',
            f'¿Cómo funciona {title}?'
        ]
        
        for i, template in enumerate(fundamental_templates[:max_questions-1]):
            answer = self._generate_targeted_answer(template, title, content)
            quality = self.formation_system.validate_conversation_quality(template, answer)
            
            if quality >= self.formation_system.get_quality_threshold('fundamental'):
                questions.append({
                    'question': template,
                    'answer': answer,
                    'question_type': 'fundamental',
                    'subtype': f'basic_{i}',
                    'quality_score': quality,
                    'priority': 0.9 - (i * 0.1),
                    'is_mandatory': False
                })
        
        return questions
    
    def generate_specific_questions(self, title: str, content: str, category: str,
                                  max_questions: int = 5) -> List[Dict]:
        """Genera preguntas específicas por categoría"""
        questions = []
        
        # Usar el generador inteligente para preguntas específicas
        try:
            article_data = {'title': title, 'content': content}
            intelligent_questions = self.intelligent_generator.generate_questions_for_article(
                article_data, category
            )
            
            for q_data in intelligent_questions.get('questions', [])[:max_questions]:
                if q_data.get('type') == 'specific':
                    questions.append({
                        'question': q_data['question'],
                        'answer': q_data['answer'],
                        'question_type': 'specific',
                        'subtype': q_data.get('subtype', 'category_specific'),
                        'quality_score': q_data.get('quality_score', 0.7),
                        'priority': q_data.get('priority', 0.7),
                        'category': category
                    })
                    
        except Exception as e:
            print(f"⚠️ Error generando preguntas específicas: {e}")
            # Fallback a preguntas básicas por categoría
            questions = self._generate_fallback_specific_questions(title, content, category, max_questions)
        
        return questions
    
    def _generate_comprehensive_answer(self, title: str, content: str) -> str:
        """Genera respuesta comprehensiva usando todo el contenido"""
        # Usar el contenido completo pero estructurado
        if len(content) > 1000:
            summary = content[:500] + "..."
            return f"Según el artículo completo sobre {title}: {summary}"
        else:
            return f"Según el artículo sobre {title}: {content}"
    
    def _generate_targeted_answer(self, question: str, title: str, content: str) -> str:
        """Genera respuesta específica para una pregunta"""
        # Simplificado: usar contenido relevante
        if "qué es" in question.lower():
            # Buscar definición en el primer párrafo
            first_paragraph = content.split('.')[0] if content else ""
            return f"{title} es {first_paragraph}."
        elif "características" in question.lower():
            return f"Las características principales de {title} incluyen: {content[:300]}..."
        elif "importancia" in question.lower():
            return f"La importancia de {title} radica en {content[:250]}..."
        elif "funciona" in question.lower():
            return f"{title} funciona de la siguiente manera: {content[:300]}..."
        else:
            return f"Respecto a {title}: {content[:400]}..."
    
    def _generate_fallback_specific_questions(self, title: str, content: str, 
                                            category: str, max_questions: int) -> List[Dict]:
        """Preguntas específicas de fallback por categoría"""
        fallback_questions = []
        
        category_templates = {
            'historia-tiempo': [
                f'¿Cuándo ocurrió {title}?',
                f'¿Qué contexto histórico rodeó a {title}?'
            ],
            'naturaleza-universo': [
                f'¿Cómo se relaciona {title} con la naturaleza?',
                f'¿Qué procesos naturales involucra {title}?'
            ],
            'tecnologia-herramientas': [
                f'¿Cómo se utiliza {title}?',
                f'¿Qué aplicaciones tiene {title}?'
            ]
        }
        
        templates = category_templates.get(category, [
            f'¿Cómo se aplica {title} en {category}?',
            f'¿Qué relación tiene {title} con {category}?'
        ])
        
        for i, template in enumerate(templates[:max_questions]):
            answer = self._generate_targeted_answer(template, title, content)
            fallback_questions.append({
                'question': template,
                'answer': answer,
                'question_type': 'specific',
                'subtype': 'fallback',
                'quality_score': 0.6,
                'priority': 0.5,
                'category': category
            })
        
        return fallback_questions


class QualityValidator:
    """Validador de calidad de conversaciones"""
    
    def __init__(self, formation_system: SimpleFormationSystem):
        self.formation_system = formation_system
        
    def validate_conversation_set(self, conversations: List[Dict]) -> Dict[str, Any]:
        """Valida un conjunto de conversaciones"""
        results = {
            'total_conversations': len(conversations),
            'valid_conversations': 0,
            'quality_scores': [],
            'issues': []
        }
        
        for conv in conversations:
            question = conv.get('question', '')
            answer = conv.get('answer', '')
            
            # Validar calidad individual
            quality = self.formation_system.validate_conversation_quality(question, answer)
            results['quality_scores'].append(quality)
            
            # Determinar si es válida
            threshold = self.formation_system.get_quality_threshold(conv.get('question_type', 'fundamental'))
            if quality >= threshold:
                results['valid_conversations'] += 1
            else:
                results['issues'].append(f"Baja calidad en: {question[:50]}...")
        
        results['average_quality'] = sum(results['quality_scores']) / len(results['quality_scores']) if results['quality_scores'] else 0
        results['success_rate'] = results['valid_conversations'] / results['total_conversations'] if results['total_conversations'] > 0 else 0
        
        return results


class SimplifiedContentManager:
    """Gestor de contenido principal simplificado"""
    
    def __init__(self, formation_dir: str = "formation", 
                 fundamental_questions: int = 5, specific_questions: int = 5):
        print("🧠 Inicializando Content Manager Simplificado...")
        
        # Configuración
        self.fundamental_questions = fundamental_questions
        self.specific_questions = specific_questions
        self.total_questions = fundamental_questions + specific_questions
        
        # Inicializar sistemas
        self.formation_system = SimpleFormationSystem(formation_dir)
        self.intelligent_generator = IntelligentQuestionGenerator(
            questions_per_article=self.total_questions
        )
        
        # Inicializar gestores
        self.category_manager = CategoryManager(self.formation_system)
        self.question_generator = QuestionGenerator(self.formation_system, self.intelligent_generator)
        self.quality_validator = QualityValidator(self.formation_system)
        
        # Estadísticas
        self.stats = {
            'articles_processed': 0,
            'conversations_generated': 0,
            'categories_used': set(),
            'quality_scores': [],
            'processing_times': []
        }
        
        print(f"✅ Content Manager inicializado")
        print(f"   📊 Preguntas fundamentales: {fundamental_questions}")
        print(f"   📊 Preguntas específicas: {specific_questions}")
        print(f"   📁 Directorio de formación: {formation_dir}")
    
    def process_article(self, article: Dict[str, str]) -> Dict[str, Any]:
        """Procesa un artículo y genera conversaciones"""
        start_time = time.time()
        
        title = article.get('title', 'Sin título')
        content = article.get('content', '')
        
        if not content.strip():
            return {'error': 'Contenido vacío', 'conversations': []}
        
        try:
            # 1. Categorizar artículo
            category = self.category_manager.categorize_article(title, content)
            self.stats['categories_used'].add(category)
            
            # 2. Generar preguntas fundamentales
            fundamental_conversations = self.question_generator.generate_fundamental_questions(
                title, content, self.fundamental_questions
            )
            
            # 3. Generar preguntas específicas
            specific_conversations = self.question_generator.generate_specific_questions(
                title, content, category, self.specific_questions
            )
            
            # 4. Combinar conversaciones
            all_conversations = fundamental_conversations + specific_conversations
            
            # 5. Validar calidad
            validation_results = self.quality_validator.validate_conversation_set(all_conversations)
            
            # 6. Actualizar estadísticas
            processing_time = time.time() - start_time
            self.stats['articles_processed'] += 1
            self.stats['conversations_generated'] += len(all_conversations)
            self.stats['quality_scores'].extend(validation_results['quality_scores'])
            self.stats['processing_times'].append(processing_time)
            
            result = {
                'title': title,
                'category': category,
                'conversations': all_conversations,
                'validation': validation_results,
                'processing_time': processing_time,
                'total_conversations': len(all_conversations)
            }
            
            return result
            
        except Exception as e:
            print(f"❌ Error procesando artículo '{title}': {e}")
            return {'error': str(e), 'conversations': []}
    
    def process_articles_batch(self, articles: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Procesa un lote de artículos"""
        print(f"📦 Procesando lote de {len(articles)} artículos...")
        
        results = []
        for i, article in enumerate(articles):
            if i % 10 == 0:
                print(f"   Progreso: {i}/{len(articles)} artículos procesados")
            
            result = self.process_article(article)
            results.append(result)
        
        print(f"✅ Lote completado: {len(results)} artículos procesados")
        return results
    
    def export_conversations(self, results: List[Dict[str, Any]], output_file: str):
        """Exporta conversaciones a archivo JSONL"""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        conversations_exported = 0
        
        with open(output_path, 'w', encoding='utf-8') as f:
            for result in results:
                if 'conversations' in result and result['conversations']:
                    for conv in result['conversations']:
                        # Añadir metadatos
                        conv_export = {
                            **conv,
                            'title': result.get('title', ''),
                            'category': result.get('category', ''),
                            'timestamp': datetime.now().isoformat()
                        }
                        f.write(json.dumps(conv_export, ensure_ascii=False) + '\n')
                        conversations_exported += 1
        
        print(f"💾 Exportadas {conversations_exported} conversaciones a {output_path}")
        return conversations_exported
    
    def get_statistics(self) -> Dict[str, Any]:
        """Obtiene estadísticas del procesamiento"""
        return {
            'articles_processed': self.stats['articles_processed'],
            'conversations_generated': self.stats['conversations_generated'],
            'categories_used': list(self.stats['categories_used']),
            'average_quality': sum(self.stats['quality_scores']) / len(self.stats['quality_scores']) if self.stats['quality_scores'] else 0,
            'average_processing_time': sum(self.stats['processing_times']) / len(self.stats['processing_times']) if self.stats['processing_times'] else 0,
            'conversations_per_article': self.stats['conversations_generated'] / max(1, self.stats['articles_processed'])
        }
    
    def print_statistics(self):
        """Imprime estadísticas legibles"""
        stats = self.get_statistics()
        
        print("\n📊 ESTADÍSTICAS DE PROCESAMIENTO")
        print("=" * 50)
        print(f"📄 Artículos procesados: {stats['articles_processed']:,}")
        print(f"💬 Conversaciones generadas: {stats['conversations_generated']:,}")
        print(f"📂 Categorías utilizadas: {len(stats['categories_used'])}")
        print(f"⭐ Calidad promedio: {stats['average_quality']:.3f}")
        print(f"⏱️ Tiempo promedio por artículo: {stats['average_processing_time']:.2f}s")
        print(f"📊 Conversaciones por artículo: {stats['conversations_per_article']:.1f}")
        print("\n📂 Categorías utilizadas:")
        for category in sorted(stats['categories_used']):
            print(f"   • {category}")
    
    def generate_consciencia_category(self, categories_found: List[str], 
                                    output_dir: str, total_articles: int) -> Dict[str, Any]:
        """Genera categoría especial 'consciencia' que describe las otras categorías"""
        print(f"🧠 Generando categoría consciencia con {len(categories_found)} categorías...")
        
        # Crear descripción de las categorías encontradas
        categories_description = ", ".join(categories_found)
        consciencia_title = "Consciencia del Sistema de Categorización"
        consciencia_content = f"""
        Este artículo representa la consciencia del sistema de categorización utilizado.
        Las categorías identificadas en este dataset son: {categories_description}.
        
        Cada categoría representa un dominio específico del conocimiento humano:
        - Total de artículos procesados: {total_articles}
        - Categorías descubiertas: {len(categories_found)}
        - Distribución diversa del conocimiento abarcado
        
        Esta categoría consciencia sirve como metadescripción del proceso de 
        clasificación automática del conocimiento en dominios específicos.
        """
        
        # Generar conversaciones para la categoría consciencia
        consciencia_article = {
            'title': consciencia_title,
            'content': consciencia_content.strip(),
            'url': 'system://consciencia'
        }
        
        # Procesar como artículo normal
        conversations = []
        try:
            result = self.process_article(consciencia_article)
            conversations.append(result)
            
            # Guardar en directorio especial
            consciencia_dir = Path(output_dir) / "consciencia_completa"
            consciencia_dir.mkdir(exist_ok=True, parents=True)
            
            output_file = consciencia_dir / "consciencia.jsonl"
            conversations_exported = self.export_conversations(conversations, str(output_file))
            
        except Exception as e:
            print(f"⚠️ Error generando consciencia: {e}")
            conversations_exported = 0
        
        return {
            'total_conversations': len(conversations),
            'total_files': 1 if conversations_exported > 0 else 0,
            'categories_described': len(categories_found),
            'output_dir': str(consciencia_dir) if 'consciencia_dir' in locals() else output_dir
        }
    
    def save_low_quality_conversations(self) -> str:
        """Método stub para compatibilidad - retorna directorio donde se guardarían"""
        return "data_conversations_low_quality"


# === FUNCIONES DE UTILIDAD ===

def create_content_manager(formation_dir: str = "formation", 
                          fundamental_questions: int = 5, 
                          specific_questions: int = 5) -> SimplifiedContentManager:
    """Factory function para crear el gestor de contenido"""
    return SimplifiedContentManager(formation_dir, fundamental_questions, specific_questions)

def process_jsonl_file(input_file: str, output_file: str, 
                      fundamental_questions: int = 5, specific_questions: int = 5):
    """Procesa un archivo JSONL completo"""
    print(f"🚀 Procesando archivo: {input_file}")
    
    # Crear gestor de contenido
    content_manager = create_content_manager(
        fundamental_questions=fundamental_questions,
        specific_questions=specific_questions
    )
    
    # Cargar artículos
    articles = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                articles.append(json.loads(line.strip()))
    
    print(f"📚 Cargados {len(articles)} artículos")
    
    # Procesar artículos
    results = content_manager.process_articles_batch(articles)
    
    # Exportar resultados
    conversations_exported = content_manager.export_conversations(results, output_file)
    
    print(f"✅ Procesamiento completado: {conversations_exported} conversaciones exportadas")


def create_content_manager(formation_dir: str = "formation",
                          fundamental_questions: int = 5, 
                          specific_questions: int = 5) -> SimplifiedContentManager:
    """Función helper para crear un content manager simplificado"""
    return SimplifiedContentManager(formation_dir, fundamental_questions, specific_questions)


# === COMPATIBILIDAD CON CÓDIGO EXISTENTE ===

# Alias para mantener compatibilidad
ContentManager = SimplifiedContentManager
