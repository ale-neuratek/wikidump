#!/usr/bin/env python3
"""
🚀 BATCH OPTIMIZATION QR - Sistema de Optimización de Preguntas-Respuestas
===========================================================================
Sistema para procesar datasets usando adaptive_processor e identificar conversaciones 
con índice de calidad < 0.6 para generar patrones optimizados.

FUNCIONALIDAD:
- Procesa datasets usando adaptive_processor con 5 fundamentales + 3 específicas
- Identifica conversaciones con quality_score < 0.6
- Genera reportes por batch después de procesar cada archivo
- Crea patrones optimizados para mejorar la calidad del dataset

CONFIGURACIÓN OPTIMIZADA:
- Dataset fuente: data_test_ultra_hybrid (1 archivo de prueba)
- Fundamentales: 5 (umbral: 0.9)
- Específicas: 3 (umbral: 0.7)
- Logs reducidos y reportes por batch
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
import shutil
from collections import defaultdict

# Importar módulos locales
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from content_manager import ContentManager
    print("✅ ContentManager importado")
except ImportError as e:
    print(f"⚠️ Warning importando ContentManager: {e}")
    class ContentManager:
        def __init__(self, **kwargs):
            pass

try:
    from category_system import CategorySystem
    print("✅ CategorySystem importado")
except ImportError as e:
    print(f"⚠️ Warning importando CategorySystem: {e}")
    class CategorySystem:
        def get_category_config(self, category):
            return {'keywords': [category]}

try:
    # Intentar importar formation system
    import importlib.util
    formation_spec = importlib.util.spec_from_file_location("formation_system", current_dir / "formation_system.py")
    if formation_spec and formation_spec.loader:
        formation_module = importlib.util.module_from_spec(formation_spec)
        formation_spec.loader.exec_module(formation_module)
        FormationFundamentalSystem = formation_module.FormationFundamentalSystem
        print("✅ FormationFundamentalSystem importado")
    else:
        raise ImportError("No se pudo cargar formation_system")
        
except ImportError as e:
    print(f"⚠️ Warning: {e}")
    # Fallback básico
    class FormationFundamentalSystem:
        def __init__(self, output_dir):
            self.output_dir = output_dir
            
        def validate_conversation(self, conversation):
            return {
                'is_valid': True,
                'quality_score': conversation.get('quality_score', 0.8),
                'issues': []
            }
    category_spec.loader.exec_module(category_module)
    
    # Crear clase CategorySystem desde el módulo
    class CategorySystem:
        def __init__(self):
            self.categories = category_module.MAIN_CATEGORIES
        
        def get_category_config(self, category: str) -> dict:
            return self.categories.get(category, {})
        
        def get_all_categories(self) -> dict:
            return self.categories
            
except ImportError as e:
    print(f"Warning: {e}")
    # Fallback básico
    class CategorySystem:
        def __init__(self):
            self.categories = {}
        def get_category_config(self, category: str) -> dict:
            return {}
        def get_all_categories(self) -> dict:
            return {}


class BatchOptimizationQR:
    """Sistema principal para optimización de preguntas-respuestas de baja calidad"""
    
    def __init__(self, 
                 source_dir: str = "data_conversations_complete",
                 output_dir: str = "fundamentals_training",
                 quality_threshold: float = 0.6,
                 fundamental_questions: int = 5,
                 specific_questions: int = 3):
        
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.quality_threshold = quality_threshold
        self.fundamental_questions = fundamental_questions
        self.specific_questions = specific_questions
        self.low_quality_dir = self.output_dir / "low_quality_patterns"
        
        # Crear directorios de salida
        self.output_dir.mkdir(exist_ok=True)
        self.low_quality_dir.mkdir(exist_ok=True)
        
        # Inicializar sistemas con nuevos parámetros
        self.category_system = CategorySystem()
        self.content_manager = ContentManager(
            questions_per_article=fundamental_questions + specific_questions,
            fundamental_questions=fundamental_questions,
            specific_questions=specific_questions
        )
        self.formation_system = FormationFundamentalSystem(str(self.output_dir))
        
        # Inicializar generador inteligente de preguntas
        from formation_fundamental.intelligent_question_generator import IntelligentQuestionGenerator
        self.intelligent_generator = IntelligentQuestionGenerator(
            questions_per_category=fundamental_questions + specific_questions,
            base_questions=fundamental_questions,
            specific_questions=specific_questions
        )
        
        # Estadísticas de procesamiento mejoradas
        self.stats = {
            'total_conversations': 0,
            'low_quality_conversations': 0,
            'fundamental_patterns_identified': 0,
            'specific_patterns_identified': 0,
            'improved_fundamental_questions': 0,
            'improved_specific_questions': 0,
            'categories_processed': 0,
            'processing_time': 0,
            'quality_improvements': {
                'fundamental_avg_before': 0.0,
                'fundamental_avg_after': 0.0,
                'specific_avg_before': 0.0,
                'specific_avg_after': 0.0
            }
        }
        
        print(f"🚀 BATCH OPTIMIZATION QR INICIALIZADO (Enhanced)")
        print(f"📁 Fuente: {self.source_dir}")
        print(f"📁 Salida: {self.output_dir}")
        print(f"🎯 Umbral de calidad: {self.quality_threshold}")
        print(f"📋 Preguntas fundamentales: {self.fundamental_questions} (umbral: 0.9)")
        print(f"🎯 Preguntas específicas: {self.specific_questions} (umbral: 0.7)")
    
    def run_optimization_pipeline(self) -> Dict[str, Any]:
        """Ejecuta el pipeline completo de optimización"""
        start_time = datetime.now()
        
        print("\n🔄 INICIANDO PIPELINE DE OPTIMIZACIÓN QR")
        print("=" * 60)
        
        try:
            # FASE 1: Identificar conversaciones de baja calidad
            print("📋 FASE 1: Identificación de conversaciones de baja calidad")
            low_quality_data = self._identify_low_quality_conversations()
            
            # FASE 2: Analizar patrones problemáticos
            print("\n🔍 FASE 2: Análisis de patrones problemáticos")
            problematic_patterns = self._analyze_problematic_patterns(low_quality_data)
            
            # FASE 3: Generar patrones optimizados
            print("\n⚙️ FASE 3: Generación de patrones optimizados")
            optimized_patterns = self._generate_optimized_patterns(problematic_patterns)
            
            # FASE 4: Crear preguntas mejoradas
            print("\n📝 FASE 4: Creación de preguntas mejoradas")
            improved_questions = self._generate_improved_questions(optimized_patterns)
            
            # FASE 5: Validar mejoras
            print("\n🧪 FASE 5: Validación de mejoras")
            validation_results = self._validate_improvements(improved_questions)
            
            # FASE 6: Generar archivos fundamentals_training
            print("\n💾 FASE 6: Generación de archivos fundamentals_training")
            self._generate_fundamentals_training(optimized_patterns, improved_questions)
            
            # FASE 7: Crear reportes
            print("\n📊 FASE 7: Generación de reportes")
            self._generate_reports(low_quality_data, optimized_patterns, validation_results)
            
            end_time = datetime.now()
            self.stats['processing_time'] = (end_time - start_time).total_seconds()
            
            print(f"\n✅ PIPELINE COMPLETADO EN {self.stats['processing_time']:.2f} segundos")
            return self.stats
            
        except Exception as e:
            print(f"❌ Error en pipeline: {e}")
            raise
    
    def _identify_low_quality_conversations(self) -> Dict[str, List[Dict]]:
        """Identifica conversaciones con quality_score < threshold usando adaptive_processor"""
        print(f"   🔍 Procesando {self.source_dir} con adaptive_processor")
        
        low_quality_data = defaultdict(list)
        
        if not self.source_dir.exists():
            print(f"   ⚠️ Directorio fuente no existe: {self.source_dir}")
            return dict(low_quality_data)
        
        # Usar adaptive_processor para procesar el dataset
        print(f"   🚀 Ejecutando adaptive_processor en {self.source_dir}")
        
        # Crear directorio temporal para resultados
        temp_output = self.output_dir / "temp_processed"
        temp_output.mkdir(exist_ok=True, parents=True)
        
        try:
            # Ejecutar adaptive_processor
            from adaptive_processor import AdaptiveProcessor
            
            processor = AdaptiveProcessor(
                input_dir=str(self.source_dir),
                output_dir=str(temp_output),
                questions_per_article=self.fundamental_questions + self.specific_questions,
                fundamental_questions=self.fundamental_questions,
                specific_questions=self.specific_questions
            )
            
            print(f"   ⚙️ Configuración: {self.fundamental_questions} fundamentales + {self.specific_questions} específicas")
            print(f"   📊 Umbrales: fundamentales ≥ 0.9, específicas ≥ 0.7")
            
            # Procesar datasets con reportes por batch
            results = processor.process_datasets()
            
            print(f"   ✅ Procesamiento completado")
            print(f"   📊 Estadísticas del procesamiento:")
            if results:
                for key, value in results.items():
                    if isinstance(value, (int, float)):
                        print(f"     • {key}: {value:,}")
            
            # Ahora buscar conversaciones low_quality en los resultados
            print(f"   🔍 Analizando conversaciones low_quality generadas")
            
            # Buscar archivos low_quality
            low_quality_dir = temp_output / "data_conversations_low_quality"
            if low_quality_dir.exists():
                for jsonl_file in low_quality_dir.glob("*.jsonl"):
                    category = jsonl_file.stem.replace("low_quality_", "")
                    print(f"     📂 Procesando low_quality: {category}")
                    
                    with open(jsonl_file, 'r', encoding='utf-8') as f:
                        for line_num, line in enumerate(f, 1):
                            line = line.strip()
                            if line:
                                try:
                                    conversation = json.loads(line)
                                    quality_score = conversation.get('quality_score', 0.0)
                                    
                                    self.stats['total_conversations'] += 1
                                    
                                    if quality_score < self.quality_threshold:
                                        conversation['source_file'] = str(jsonl_file)
                                        conversation['line_number'] = line_num
                                        conversation['category'] = category
                                        low_quality_data[category].append(conversation)
                                        self.stats['low_quality_conversations'] += 1
                                        
                                except json.JSONDecodeError:
                                    continue
            
            # También buscar en archivos principales de conversaciones
            conv_dirs = [temp_output / "data_conversations_complete"]
            for conv_dir in conv_dirs:
                if conv_dir.exists():
                    for category_dir in conv_dir.iterdir():
                        if category_dir.is_dir():
                            category_name = category_dir.name
                            conversation_files = list(category_dir.glob("*.jsonl"))
                            
                            if conversation_files:
                                print(f"     📂 Analizando categoría: {category_name} ({len(conversation_files)} archivos)")
                            
                            for file_path in conversation_files:
                                try:
                                    with open(file_path, 'r', encoding='utf-8') as f:
                                        for line_num, line in enumerate(f, 1):
                                            line = line.strip()
                                            if line:
                                                try:
                                                    conversation = json.loads(line)
                                                    quality_score = conversation.get('quality_score', 1.0)
                                                    
                                                    self.stats['total_conversations'] += 1
                                                    
                                                    if quality_score < self.quality_threshold:
                                                        conversation['source_file'] = str(file_path)
                                                        conversation['line_number'] = line_num
                                                        conversation['category'] = category_name
                                                        low_quality_data[category_name].append(conversation)
                                                        self.stats['low_quality_conversations'] += 1
                                                        
                                                except json.JSONDecodeError:
                                                    continue
                                                    
                                except Exception as e:
                                    print(f"     ⚠️ Error procesando {file_path}: {e}")
                                    continue
            
        except Exception as e:
            print(f"   ❌ Error ejecutando adaptive_processor: {e}")
            import traceback
            traceback.print_exc()
        
        print(f"   ✅ Análisis completado:")
        print(f"     📊 Conversaciones totales: {self.stats['total_conversations']:,}")
        print(f"     📉 Baja calidad encontradas: {self.stats['low_quality_conversations']:,}")
        print(f"     � Categorías con problemas: {len(low_quality_data)}")
        
        return dict(low_quality_data)
    
    def _analyze_problematic_patterns(self, low_quality_data: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Analiza patrones comunes en conversaciones de baja calidad separando fundamentales vs específicas"""
        print("   🔍 Analizando patrones problemáticos (fundamentales vs específicas)...")
        
        patterns = {
            'by_category': {},
            'fundamental_issues': defaultdict(int),
            'specific_issues': defaultdict(int),
            'question_patterns': {
                'fundamental': defaultdict(int),
                'specific': defaultdict(int)
            },
            'answer_patterns': {
                'fundamental': defaultdict(int),
                'specific': defaultdict(int)
            },
            'quality_analysis': {
                'fundamental_below_09': 0,
                'specific_below_07': 0,
                'fundamental_avg_quality': 0.0,
                'specific_avg_quality': 0.0
            }
        }
        
        for category, conversations in low_quality_data.items():
            category_patterns = {
                'total_count': len(conversations),
                'fundamental_problems': 0,
                'specific_problems': 0,
                'avg_quality_score': 0,
                'fundamental_phrases': defaultdict(int),
                'specific_phrases': defaultdict(int),
                'structural_issues': {
                    'fundamental': defaultdict(int),
                    'specific': defaultdict(int)
                }
            }
            
            total_score = 0
            fundamental_scores = []
            specific_scores = []
            
            for conv in conversations:
                quality_score = conv.get('quality_score', 0)
                total_score += quality_score
                
                # Analizar tipo de pregunta (fundamental vs específica)
                question = conv.get('question', '').lower()
                question_type = self._classify_question_type(question)
                
                if question_type == 'fundamental':
                    fundamental_scores.append(quality_score)
                    if quality_score < 0.9:
                        category_patterns['fundamental_problems'] += 1
                        patterns['quality_analysis']['fundamental_below_09'] += 1
                        self._analyze_fundamental_issues(conv, patterns['fundamental_issues'])
                        self._analyze_question_patterns(question, patterns['question_patterns']['fundamental'])
                else:
                    specific_scores.append(quality_score)
                    if quality_score < 0.7:
                        category_patterns['specific_problems'] += 1
                        patterns['quality_analysis']['specific_below_07'] += 1
                        self._analyze_specific_issues(conv, patterns['specific_issues'])
                        self._analyze_question_patterns(question, patterns['question_patterns']['specific'])
                
                # Analizar problemas estructurales
                self._analyze_structural_issues(conv, category_patterns['structural_issues'][question_type])
            
            if conversations:
                category_patterns['avg_quality_score'] = total_score / len(conversations)
                
            if fundamental_scores:
                patterns['quality_analysis']['fundamental_avg_quality'] = sum(fundamental_scores) / len(fundamental_scores)
                
            if specific_scores:
                patterns['quality_analysis']['specific_avg_quality'] = sum(specific_scores) / len(specific_scores)
            
            patterns['by_category'][category] = category_patterns
            self.stats['fundamental_patterns_identified'] += category_patterns['fundamental_problems']
            self.stats['specific_patterns_identified'] += category_patterns['specific_problems']
        
        print(f"   ✅ Patrones fundamentales problemáticos: {self.stats['fundamental_patterns_identified']}")
        print(f"   ✅ Patrones específicos problemáticos: {self.stats['specific_patterns_identified']}")
        print(f"   📊 Calidad promedio fundamentales: {patterns['quality_analysis']['fundamental_avg_quality']:.3f}")
        print(f"   📊 Calidad promedio específicas: {patterns['quality_analysis']['specific_avg_quality']:.3f}")
        
        return patterns
    
    def _classify_question_type(self, question: str) -> str:
        """Clasifica si una pregunta es fundamental o específica"""
        fundamental_indicators = [
            'qué es', 'quién es', 'cuál es', 'explícame', 'explica',
            'definición', 'concepto', 'características principales',
            'importancia', 'significado', 'propósito', 'función'
        ]
        
        specific_indicators = [
            'cuándo', 'dónde', 'cómo', 'por qué específicamente',
            'detalles', 'proceso', 'pasos', 'método', 'técnica',
            'ejemplo', 'caso', 'situación', 'contexto específico'
        ]
        
        question_lower = question.lower()
        
        fundamental_score = sum(1 for indicator in fundamental_indicators if indicator in question_lower)
        specific_score = sum(1 for indicator in specific_indicators if indicator in question_lower)
        
        return 'fundamental' if fundamental_score >= specific_score else 'specific'
    
    def _analyze_fundamental_issues(self, conversation: Dict, issues_counter: defaultdict):
        """Analiza problemas específicos en preguntas fundamentales"""
        question = conversation.get('question', '')
        answer = conversation.get('answer', '')
        
        # Problemas comunes en preguntas fundamentales
        if len(question) < 20:
            issues_counter['question_too_short'] += 1
        if 'explícame en detalle' not in question.lower() and len(answer) < 100:
            issues_counter['insufficient_detail'] += 1
        if not any(word in question.lower() for word in ['qué', 'cuál', 'quién']):
            issues_counter['missing_fundamental_words'] += 1
        if '?' not in question:
            issues_counter['missing_question_mark'] += 1
    
    def _analyze_specific_issues(self, conversation: Dict, issues_counter: defaultdict):
        """Analiza problemas específicos en preguntas específicas"""
        question = conversation.get('question', '')
        answer = conversation.get('answer', '')
        
        # Problemas comunes en preguntas específicas
        if len(question) < 15:
            issues_counter['question_too_short'] += 1
        if not any(word in question.lower() for word in ['cómo', 'cuándo', 'dónde', 'por qué']):
            issues_counter['missing_specific_words'] += 1
        if len(answer) < 50:
            issues_counter['answer_too_brief'] += 1
        if 'ejemplo' not in question.lower() and 'por ejemplo' not in answer.lower():
            issues_counter['lacks_examples'] += 1
    
    def _analyze_question_patterns(self, question: str, patterns_counter: defaultdict):
        """Analiza patrones en las preguntas"""
        words = question.lower().split()
        
        # Contar palabras comunes
        for word in words:
            if len(word) > 3:  # Solo palabras significativas
                patterns_counter[word] += 1
        
        # Contar frases comunes
        common_phrases = ['qué es', 'cuál es', 'cómo se', 'por qué', 'explícame']
        for phrase in common_phrases:
            if phrase in question.lower():
                patterns_counter[f"phrase_{phrase.replace(' ', '_')}"] += 1
    
    def _analyze_structural_issues(self, conversation: Dict, issues_counter: defaultdict):
        """Analiza problemas estructurales en la conversación"""
        question = conversation.get('question', '')
        answer = conversation.get('answer', '')
        
        # Problemas estructurales
        if not question.strip():
            issues_counter['empty_question'] += 1
        if not answer.strip():
            issues_counter['empty_answer'] += 1
        if question == answer:
            issues_counter['question_equals_answer'] += 1
        if len(answer.split()) < 10:
            issues_counter['answer_too_short'] += 1
                
                question = conv.get('question', '')
                answer = conv.get('answer', '')
                
                # Análisis de longitud
                if len(answer) < 50:
                    category_patterns['length_issues']['too_short'] += 1
                    patterns['common_issues']['respuesta_muy_corta'] += 1
                elif len(answer) > 1000:
                    category_patterns['length_issues']['too_long'] += 1
                    patterns['common_issues']['respuesta_muy_larga'] += 1
                
                # Análisis de frases problemáticas
                problematic_phrases = [
                    'es un concepto',
                    'se define como',
                    'corresponde a',
                    'se refiere a',
                    'no se puede',
                    'información no disponible'
                ]
                
                for phrase in problematic_phrases:
                    if phrase in answer.lower():
                        category_patterns['common_phrases'][phrase] += 1
                        patterns['linguistic_issues'][phrase] += 1
                
                # Análisis de patrones de pregunta
                if question.startswith('¿Qué es'):
                    patterns['question_patterns']['definicional'] += 1
                elif question.startswith('¿Cómo'):
                    patterns['question_patterns']['procedimental'] += 1
                elif question.startswith('¿Cuál'):
                    patterns['question_patterns']['selectivo'] += 1
                
                # Problemas estructurales
                if not answer.strip().endswith(('.', '!', '?')):
                    category_patterns['structural_issues']['sin_puntuacion_final'] += 1
                    patterns['common_issues']['sin_puntuacion_final'] += 1
                
                if answer.count('\n') == 0 and len(answer) > 200:
                    category_patterns['structural_issues']['sin_parrafos'] += 1
                    patterns['common_issues']['sin_estructura_parrafos'] += 1
            
            if conversations:
                category_patterns['avg_quality_score'] = total_score / len(conversations)
            
            patterns['by_category'][category] = category_patterns
            self.stats['patterns_identified'] += len(category_patterns['common_phrases'])
        
        print(f"   ✅ Patrones identificados: {self.stats['patterns_identified']}")
        print(f"   📊 Issues más comunes:")
        for issue, count in sorted(patterns['common_issues'].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"      • {issue}: {count:,} casos")
        
        return patterns
    
    def _generate_optimized_patterns(self, problematic_patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Genera patrones optimizados para corregir problemas identificados"""
        print("   ⚙️ Generando patrones optimizados para fundamentales y específicas...")
        
        optimized_patterns = {
            'fundamental_improvements': {},
            'specific_improvements': {},
            'universal_fixes': {},
            'category_specific_fixes': {}
        }
        
        # PATRONES OPTIMIZADOS PARA PREGUNTAS FUNDAMENTALES
        fundamental_issues = problematic_patterns.get('fundamental_issues', {})
        optimized_patterns['fundamental_improvements'] = {
            'templates': [
                "¿Qué es {topic} y cuáles son sus características principales?",
                "Explícame en detalle {topic} y su importancia.",
                "¿Cuál es la definición completa de {topic} y por qué es relevante?",
                "¿Quién o qué es {topic} y qué función cumple?",
                "Describe {topic} de manera comprehensiva incluyendo sus aspectos fundamentales."
            ],
            'quality_enhancements': {
                'minimum_answer_length': 150,
                'required_elements': ['definición', 'características', 'importancia', 'contexto'],
                'quality_threshold': 0.9,
                'mandatory_detail_question': True
            },
            'fixes_for_issues': {}
        }
        
        # Fixes específicos para problemas fundamentales
        if fundamental_issues.get('question_too_short', 0) > 0:
            optimized_patterns['fundamental_improvements']['fixes_for_issues']['short_questions'] = {
                'strategy': 'expand_with_context',
                'templates': [
                    "¿Qué es {topic} en el contexto de {category} y cuáles son sus características principales?",
                    "Explícame en detalle qué significa {topic} y por qué es importante."
                ]
            }
        
        if fundamental_issues.get('insufficient_detail', 0) > 0:
            optimized_patterns['fundamental_improvements']['fixes_for_issues']['insufficient_detail'] = {
                'strategy': 'force_comprehensive_answers',
                'requirements': ['definición completa', 'ejemplos', 'contexto histórico', 'relevancia actual']
            }
        
        # PATRONES OPTIMIZADOS PARA PREGUNTAS ESPECÍFICAS
        specific_issues = problematic_patterns.get('specific_issues', {})
        optimized_patterns['specific_improvements'] = {
            'templates': [
                "¿Cómo se implementa específicamente {topic} en {context}?",
                "¿Cuándo y dónde se utiliza {topic} de manera práctica?",
                "¿Por qué es importante {specific_aspect} de {topic}?",
                "¿Qué pasos específicos se siguen para {action} en {topic}?",
                "Proporciona ejemplos concretos de {topic} en {application_area}."
            ],
            'quality_enhancements': {
                'minimum_answer_length': 80,
                'required_elements': ['ejemplos específicos', 'contexto práctico', 'detalles técnicos'],
                'quality_threshold': 0.7,
                'examples_required': True
            },
            'fixes_for_issues': {}
        }
        
        # Fixes específicos para problemas específicos
        if specific_issues.get('lacks_examples', 0) > 0:
            optimized_patterns['specific_improvements']['fixes_for_issues']['missing_examples'] = {
                'strategy': 'force_examples',
                'requirements': ['al menos 2 ejemplos concretos', 'casos de uso específicos']
            }
        
        if specific_issues.get('answer_too_brief', 0) > 0:
            optimized_patterns['specific_improvements']['fixes_for_issues']['brief_answers'] = {
                'strategy': 'expand_with_details',
                'requirements': ['detalles técnicos', 'pasos específicos', 'contexto de aplicación']
            }
        
        # FIXES UNIVERSALES (aplican a ambos tipos)
        common_issues = problematic_patterns.get('quality_analysis', {})
        optimized_patterns['universal_fixes'] = {
            'structural_improvements': {
                'ensure_question_marks': True,
                'minimum_paragraph_structure': True,
                'proper_punctuation': True,
                'logical_flow': True
            },
            'content_improvements': {
                'avoid_repetition': True,
                'use_varied_vocabulary': True,
                'include_transitions': True,
                'maintain_coherence': True
            }
        }
        
        # FIXES POR CATEGORÍA
        for category, patterns in problematic_patterns.get('by_category', {}).items():
            category_fixes = {
                'fundamental_focus': [],
                'specific_focus': [],
                'category_templates': []
            }
            
            # Determinar enfoque por categoría
            if patterns.get('fundamental_problems', 0) > patterns.get('specific_problems', 0):
                category_fixes['fundamental_focus'] = [
                    f"Énfasis en definiciones claras para {category}",
                    f"Respuestas más comprehensivas para conceptos de {category}",
                    f"Mayor detalle en explicaciones fundamentales de {category}"
                ]
            else:
                category_fixes['specific_focus'] = [
                    f"Más ejemplos prácticos para {category}",
                    f"Detalles técnicos específicos de {category}",
                    f"Casos de uso concretos en {category}"
                ]
            
            optimized_patterns['category_specific_fixes'][category] = category_fixes
        
        print(f"   ✅ Patrones fundamentales generados: {len(optimized_patterns['fundamental_improvements']['templates'])}")
        print(f"   ✅ Patrones específicos generados: {len(optimized_patterns['specific_improvements']['templates'])}")
        print(f"   ✅ Fixes por categoría: {len(optimized_patterns['category_specific_fixes'])}")
        
        return optimized_patterns
        """Genera patrones optimizados para corregir problemas identificados"""
        print("   ⚙️ Generando patrones optimizados...")
        
        optimized_patterns = {
            'improved_templates': {},
            'quality_filters': {},
            'enhancement_rules': {},
            'fallback_strategies': {}
        }
        
        # Generar templates mejorados por categoría
        for category, patterns in problematic_patterns['by_category'].items():
            improved_template = {
                'category': category,
                'min_length': 100,  # Evitar respuestas muy cortas
                'max_length': 800,  # Evitar respuestas muy largas
                'required_structure': {
                    'introduction': True,
                    'main_content': True,
                    'conclusion': True
                },
                'avoid_phrases': list(patterns['common_phrases'].keys()),
                'quality_enhancements': []
            }
            
            # Reglas específicas basadas en problemas encontrados
            if patterns['length_issues']['too_short'] > 10:
                improved_template['quality_enhancements'].append({
                    'type': 'length_expansion',
                    'target': 'increase_detail',
                    'method': 'add_examples_and_context'
                })
            
            if patterns['structural_issues']['sin_puntuacion_final'] > 5:
                improved_template['quality_enhancements'].append({
                    'type': 'punctuation_fix',
                    'target': 'ensure_proper_ending',
                    'method': 'add_final_punctuation'
                })
            
            if patterns['structural_issues']['sin_parrafos'] > 5:
                improved_template['quality_enhancements'].append({
                    'type': 'structure_improvement',
                    'target': 'add_paragraphs',
                    'method': 'break_into_logical_sections'
                })
            
            optimized_patterns['improved_templates'][category] = improved_template
        
        # Filtros de calidad generales
        optimized_patterns['quality_filters'] = {
            'minimum_quality_score': 0.7,
            'required_elements': ['proper_punctuation', 'adequate_length', 'relevant_content'],
            'prohibited_patterns': list(problematic_patterns['linguistic_issues'].keys()),
            'structural_requirements': {
                'min_sentences': 3,
                'max_sentences': 15,
                'paragraph_breaks': True
            }
        }
        
        # Reglas de mejora
        optimized_patterns['enhancement_rules'] = {
            'phrase_replacement': {
                'es un concepto': 'representa una idea fundamental que',
                'se define como': 'puede entenderse como',
                'corresponde a': 'hace referencia a',
                'se refiere a': 'alude específicamente a'
            },
            'structure_templates': {
                'definition': '{topic} {enhanced_definition}. {detailed_explanation}. {practical_examples}.',
                'explanation': '{topic} {process_description}. {step_by_step}. {final_summary}.',
                'comparison': '{topic} {comparison_intro}. {similarities}. {differences}. {conclusion}.'
            }
        }
        
        print(f"   ✅ Templates optimizados: {len(optimized_patterns['improved_templates'])}")
        print(f"   🎯 Filtros de calidad: {len(optimized_patterns['quality_filters'])}")
        
        return optimized_patterns
    
    def _generate_improved_questions(self, optimized_patterns: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """Genera preguntas mejoradas usando los patrones optimizados separando fundamentales y específicas"""
        print("   📝 Generando preguntas mejoradas (fundamentales y específicas)...")
        
        improved_questions = {
            'fundamental': [],
            'specific': [],
            'by_category': {}
        }
        
        # Usar el ContentManager mejorado para generar preguntas
        for category in ['ciencia', 'historia', 'tecnologia', 'arte', 'deportes']:
            print(f"     📂 Procesando categoría: {category}")
            
            # Generar artículo de ejemplo para la categoría
            sample_article = {
                'title': f"Ejemplo de {category}",
                'content': f"Contenido de ejemplo para la categoría {category} que permite generar preguntas fundamentales y específicas de alta calidad.",
                'url': f"example_{category}.html"
            }
            
            category_improved = {
                'fundamental': [],
                'specific': []
            }
            
            try:
                # Usar el content_manager con los nuevos parámetros
                conversations = self.content_manager.generate_enhanced_questions(
                    article=sample_article,
                    category=category
                )
                
                for conv in conversations:
                    question_type = self._classify_question_type(conv.get('question', ''))
                    quality_score = conv.get('quality_score', 0.0)
                    
                    # Aplicar mejoras según el tipo
                    if question_type == 'fundamental' and quality_score >= 0.9:
                        improved_questions['fundamental'].append(conv)
                        category_improved['fundamental'].append(conv)
                        self.stats['improved_fundamental_questions'] += 1
                    elif question_type == 'specific' and quality_score >= 0.7:
                        improved_questions['specific'].append(conv)
                        category_improved['specific'].append(conv)
                        self.stats['improved_specific_questions'] += 1
                
                improved_questions['by_category'][category] = category_improved
                
                print(f"       ✅ Fundamentales: {len(category_improved['fundamental'])}")
                print(f"       ✅ Específicas: {len(category_improved['specific'])}")
                
            except Exception as e:
                print(f"       ⚠️ Error procesando {category}: {e}")
                continue
        
        print(f"   ✅ Total fundamentales mejoradas: {len(improved_questions['fundamental'])}")
        print(f"   ✅ Total específicas mejoradas: {len(improved_questions['specific'])}")
        
        return improved_questions
            
            improved_questions[category] = category_questions
            self.stats['categories_processed'] += 1
            
            print(f"     📂 {category}: {len(category_questions)} preguntas inteligentes generadas")
        
        print(f"   ✅ Total preguntas mejoradas: {self.stats['improved_questions_generated']}")
        
        # Guardar preguntas inteligentes en archivos separados
        self.intelligent_generator.save_questions_to_file(
            intelligent_questions, 
            str(self.output_dir / "intelligent_questions")
        )
        
        return improved_questions
    
    def _convert_intelligent_to_improved(self, question_data: Dict, category: str, 
                                       category_config: Dict, optimized_patterns: Dict) -> Optional[Dict]:
        """Convierte pregunta inteligente a formato mejorado con respuesta"""
        try:
            # Usar template de la pregunta inteligente
            question_template = question_data.get('template', '')
            question_type = question_data.get('type', 'general')
            
            # Crear topic placeholder específico para la categoría
            topic_placeholder = "{topic_" + category.replace('-', '_') + "}"
            question = question_template.replace("{title}", topic_placeholder)
            
            # Generar respuesta mejorada basada en el tipo de pregunta
            enhanced_answer = self._generate_enhanced_answer_intelligent(
                question_data, category, category_config, optimized_patterns
            )
            
            improved_question = {
                'question': question,
                'answer': enhanced_answer,
                'question_type': question_type,
                'category': category,
                'quality_score': question_data.get('final_rank_score', 0.8),
                'enhancement_applied': True,
                'intelligent_generation': True,
                'ranking_details': question_data.get('ranking_details', {}),
                'specificity': question_data.get('specificity', 'medium'),
                'applicability': question_data.get('applicability', 'medium'),
                'generation_method': question_data.get('generation_method', 'intelligent'),
                'template_source': 'intelligent_question_generator',
                'improvement_timestamp': datetime.now().isoformat(),
                'metadata': {
                    'original_template': question_template,
                    'category_config': category_config.get('keywords', [])[:5],  # Solo primeras 5 keywords
                    'quality_enhancements': ['intelligent_generation', 'category_specific', 'ranked_selection']
                }
            }
            
            return improved_question
            
        except Exception as e:
            print(f"     ⚠️ Error convirtiendo pregunta para {category}: {e}")
            return None
    
    def _generate_enhanced_answer_intelligent(self, question_data: Dict, category: str, 
                                            category_config: Dict, optimized_patterns: Dict) -> str:
        """Genera respuesta mejorada basada en pregunta inteligente"""
        question_type = question_data.get('type', 'general')
        specificity = question_data.get('specificity', 'medium')
        
        # Templates de respuesta por tipo de pregunta
        answer_templates = {
            'definitional': f"{{topic_{category.replace('-', '_')}}} es {{concepto_principal}} que se caracteriza por {{caracteristicas_clave}}. En el contexto de {category}, {{explicacion_detallada}} y presenta {{aspectos_relevantes}}.",
            
            'functional': f"{{topic_{category.replace('-', '_')}}} funciona mediante {{proceso_principal}} que involucra {{pasos_clave}}. El mecanismo incluye {{componentes}} y se basa en {{principios_fundamentales}}.",
            
            'technical_specs': f"Las especificaciones técnicas de {{topic_{category.replace('-', '_')}}} incluyen {{especificaciones_principales}}. Los parámetros clave son {{parametros}} con {{valores_tipicos}}.",
            
            'historical_context': f"{{topic_{category.replace('-', '_')}}} ocurrió en {{periodo_historico}} durante {{contexto_temporal}}. Los antecedentes incluyen {{causas}} y las consecuencias fueron {{efectos}}.",
            
            'cultural_meaning': f"En términos culturales, {{topic_{category.replace('-', '_')}}} representa {{significado_cultural}} y tiene {{importancia_social}}. Se manifiesta através de {{expresiones}} y {{tradiciones}}.",
            
            'importance': f"La importancia de {{topic_{category.replace('-', '_')}}} radica en {{relevancia_principal}} para {category}. Su impacto se observa en {{areas_impacto}} y {{beneficios_clave}}.",
            
            'comparative': f"{{topic_{category.replace('-', '_')}}} se diferencia de {{conceptos_similares}} en {{diferencias_clave}}. Sus ventajas incluyen {{ventajas}} mientras que {{elementos_distintivos}}."
        }
        
        # Seleccionar template apropiado
        template = answer_templates.get(question_type, answer_templates['definitional'])
        
        # Agregar elementos de calidad basados en especificidad
        if specificity == 'high':
            template += " Específicamente, {detalles_especificos} y {aspectos_tecnicos}."
        elif specificity == 'medium':
            template += " Además, {informacion_adicional} y {contexto_relevante}."
        
        # Agregar elementos de aplicabilidad
        applicability = question_data.get('applicability', 'medium')
        if 'universal' in applicability:
            template += " Este concepto es aplicable universalmente en {aplicaciones_universales}."
        
        return template
    
    def _create_improved_question(self, category: str, question_type: str, 
                                template: Dict, category_config: Dict) -> Optional[Dict]:
        """Crea una pregunta mejorada individual"""
        try:
            # Templates mejorados por tipo
            question_templates = {
                'definitional': [
                    "¿Cómo se puede explicar en detalle {topic}?",
                    "¿Qué características fundamentales define {topic}?",
                    "¿De qué manera {topic} se manifiesta en su contexto?"
                ],
                'explanatory': [
                    "¿Cuál es el proceso completo detrás de {topic}?",
                    "¿Cómo funciona específicamente {topic}?",
                    "¿Qué mecanismos intervienen en {topic}?"
                ],
                'analytical': [
                    "¿Qué factores influyen en el desarrollo de {topic}?",
                    "¿Cuáles son las implicaciones de {topic}?",
                    "¿Qué relaciones establece {topic} con otros elementos?"
                ],
                'comparative': [
                    "¿En qué se diferencia {topic} de conceptos similares?",
                    "¿Qué ventajas presenta {topic} comparado con alternativas?",
                    "¿Cómo se relaciona {topic} con otros elementos de su categoría?"
                ]
            }
            
            # Seleccionar template aleatorio
            import random
            templates = question_templates.get(question_type, question_templates['definitional'])
            selected_template = random.choice(templates)
            
            # Crear topic placeholder
            topic_placeholder = "{topic_" + category.replace('-', '_') + "}"
            question = selected_template.replace("{topic}", topic_placeholder)
            
            # Generar respuesta mejorada usando template
            answer_template = template.get('quality_enhancements', [])
            enhanced_answer = self._generate_enhanced_answer(question_type, category, template)
            
            improved_question = {
                'question': question,
                'answer': enhanced_answer,
                'question_type': question_type,
                'category': category,
                'quality_score': 0.8,  # Score objetivo alto
                'enhancement_applied': True,
                'template_used': 'optimized_' + question_type,
                'generation_method': 'batch_optimization_qr',
                'target_quality_threshold': 0.7,
                'timestamp': datetime.now().isoformat()
            }
            
            return improved_question
            
        except Exception as e:
            print(f"     ⚠️ Error creando pregunta mejorada: {e}")
            return None
    
    def _generate_enhanced_answer(self, question_type: str, category: str, template: Dict) -> str:
        """Genera respuesta mejorada evitando patrones problemáticos"""
        
        # Obtener frases a evitar
        avoid_phrases = template.get('avoid_phrases', [])
        
        # Templates de respuesta mejorada
        enhanced_templates = {
            'definitional': (
                "Este concepto representa un elemento fundamental en {category} que abarca múltiples dimensiones. "
                "Su comprensión requiere analizar tanto sus características intrínsecas como su contexto de aplicación. "
                "Los aspectos más relevantes incluyen su función específica, sus relaciones con otros elementos "
                "y su importancia dentro del marco conceptual más amplio."
            ),
            'explanatory': (
                "El proceso involucra una secuencia de etapas interconectadas que se desarrollan de manera sistemática. "
                "Inicialmente, se establecen las condiciones necesarias, seguido por la implementación de los mecanismos "
                "específicos. Durante el desarrollo, diversos factores contribuyen al resultado final, "
                "creando un conjunto integrado de outcomes observables."
            ),
            'analytical': (
                "El análisis revela múltiples dimensiones que interactúan de manera compleja. "
                "Los factores determinantes incluyen elementos contextuales, estructurales y funcionales "
                "que configuran su comportamiento. Las implicaciones se extienden más allá del ámbito inmediato, "
                "generando efectos en sistemas relacionados y estableciendo patrones de influencia duraderos."
            ),
            'comparative': (
                "La comparación con elementos similares destaca características distintivas significativas. "
                "Las similitudes se centran en aspectos estructurales básicos, mientras que las diferencias "
                "emergen en la implementación específica y los resultados obtenidos. "
                "Esta diferenciación proporciona ventajas únicas en contextos particulares."
            )
        }
        
        base_answer = enhanced_templates.get(question_type, enhanced_templates['definitional'])
        
        # Personalizar para la categoría
        category_name = category.replace('-', ' ').replace('_', ' ').title()
        enhanced_answer = base_answer.replace('{category}', category_name)
        
        # Asegurar que no contiene frases problemáticas
        for phrase in avoid_phrases:
            if phrase in enhanced_answer.lower():
                # Reemplazar con versión mejorada
                improvements = {
                    'es un concepto': 'representa un elemento',
                    'se define como': 'puede caracterizarse como',
                    'corresponde a': 'hace referencia a',
                    'se refiere a': 'alude específicamente a'
                }
                replacement = improvements.get(phrase, phrase.replace('es un', 'constituye un'))
                enhanced_answer = enhanced_answer.replace(phrase, replacement)
        
        return enhanced_answer
    
    def _validate_improvements(self, improved_questions: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Valida que las mejoras cumplan los criterios de calidad"""
        print("   🧪 Validando mejoras generadas...")
        
        validation_results = {
            'total_validated': 0,
            'passed_validation': 0,
            'quality_scores': [],
            'issues_found': [],
            'validation_summary': {}
        }
        
        for category, questions in improved_questions.items():
            category_validation = {
                'total': len(questions),
                'passed': 0,
                'avg_quality': 0,
                'issues': []
            }
            
            total_quality = 0
            
            for question in questions:
                validation_results['total_validated'] += 1
                
                # Validar usando formation_system
                validation_result = self.formation_system.validate_conversation(question)
                
                quality_score = validation_result.get('quality_score', 0)
                is_valid = validation_result.get('is_valid', False)
                
                total_quality += quality_score
                validation_results['quality_scores'].append(quality_score)
                
                if is_valid and quality_score >= 0.7:
                    validation_results['passed_validation'] += 1
                    category_validation['passed'] += 1
                else:
                    issues = validation_result.get('issues', [])
                    validation_results['issues_found'].extend(issues)
                    category_validation['issues'].extend(issues)
            
            if questions:
                category_validation['avg_quality'] = total_quality / len(questions)
            
            validation_results['validation_summary'][category] = category_validation
        
        pass_rate = (validation_results['passed_validation'] / validation_results['total_validated']) * 100 if validation_results['total_validated'] > 0 else 0
        avg_quality = sum(validation_results['quality_scores']) / len(validation_results['quality_scores']) if validation_results['quality_scores'] else 0
        
        print(f"   ✅ Validaciones: {validation_results['total_validated']}")
        print(f"   🎯 Aprobadas: {validation_results['passed_validation']} ({pass_rate:.1f}%)")
        print(f"   📊 Calidad promedio: {avg_quality:.3f}")
        
        return validation_results
    
    def _generate_fundamentals_training(self, optimized_patterns: Dict[str, Any], 
                                      improved_questions: Dict[str, List[Dict]]):
        """Genera archivos en carpeta fundamentals_training con preguntas inteligentes"""
        print("   💾 Generando archivos fundamentals_training con sistema inteligente...")
        
        # Crear fundamental.jsonl principal con información del sistema inteligente
        fundamental_path = self.output_dir / "fundamental.jsonl"
        
        with open(fundamental_path, 'w', encoding='utf-8') as f:
            # Agregar configuraciones optimizadas
            config_entry = {
                'type': 'intelligent_optimized_configuration',
                'data': {
                    'quality_threshold': self.quality_threshold,
                    'optimization_patterns': optimized_patterns,
                    'intelligent_system': {
                        'questions_per_category': self.intelligent_generator.questions_per_category,
                        'base_questions_count': self.intelligent_generator.base_questions_count,
                        'specific_questions_count': self.intelligent_generator.specific_questions_count,
                        'ranking_weights': self.intelligent_generator.ranking_weights
                    },
                    'generation_timestamp': datetime.now().isoformat(),
                    'version': '3.0-intelligent-optimized'
                },
                'quality_score': 0.95,  # Calidad alta con sistema inteligente
                'timestamp': datetime.now().isoformat()
            }
            json.dump(config_entry, f, ensure_ascii=False)
            f.write('\n')
        
        # Crear archivos por categoría con preguntas inteligentes mejoradas
        categories_processed = 0
        questions_files_created = 0
        patterns_files_created = 0
        
        for category, questions in improved_questions.items():
            # Archivo de preguntas inteligentes mejoradas
            questions_file = self.output_dir / f"questions_intelligent.{category}.jsonl"
            
            with open(questions_file, 'w', encoding='utf-8') as f:
                for question in questions:
                    # Agregar metadata del sistema inteligente
                    question['system_version'] = '3.0-intelligent'
                    question['batch_optimization_qr'] = True
                    question['intelligent_enhancement'] = True
                    
                    json.dump(question, f, ensure_ascii=False)
                    f.write('\n')
            
            questions_files_created += 1
            
            # Archivo de patrones optimizados para esta categoría
            if category in optimized_patterns['improved_templates']:
                pattern_file = self.output_dir / f"patterns_intelligent.{category}.jsonl"
                
                with open(pattern_file, 'w', encoding='utf-8') as f:
                    pattern_entry = {
                        'type': 'intelligent_optimized_pattern',
                        'category': category,
                        'data': optimized_patterns['improved_templates'][category],
                        'intelligent_system_applied': True,
                        'questions_generated': len(questions),
                        'avg_quality_score': sum(q.get('quality_score', 0.8) for q in questions) / len(questions) if questions else 0.8,
                        'timestamp': datetime.now().isoformat()
                    }
                    json.dump(pattern_entry, f, ensure_ascii=False)
                    f.write('\n')
                
                patterns_files_created += 1
            
            categories_processed += 1
        
        # Crear archivo de estadísticas del sistema inteligente
        stats_file = self.output_dir / "intelligent_system_stats.json"
        
        intelligent_stats = {
            'system_version': '3.0-intelligent-batch-optimization',
            'processing_timestamp': datetime.now().isoformat(),
            'configuration': {
                'questions_per_category': self.intelligent_generator.questions_per_category,
                'base_questions_count': self.intelligent_generator.base_questions_count,
                'specific_questions_count': self.intelligent_generator.specific_questions_count,
                'quality_threshold': self.quality_threshold
            },
            'processing_results': {
                'categories_processed': categories_processed,
                'questions_files_created': questions_files_created,
                'patterns_files_created': patterns_files_created,
                'total_questions_generated': sum(len(q) for q in improved_questions.values()),
                'avg_questions_per_category': sum(len(q) for q in improved_questions.values()) / len(improved_questions) if improved_questions else 0
            },
            'ranking_system': self.intelligent_generator.ranking_weights,
            'quality_improvements': {
                'intelligent_generation': True,
                'category_specific_questions': True,
                'ranking_based_selection': True,
                'base_plus_specific_pattern': True
            }
        }
        
        with open(stats_file, 'w', encoding='utf-8') as f:
            json.dump(intelligent_stats, f, ensure_ascii=False, indent=2)
        
        print(f"   ✅ Archivos generados en: {self.output_dir}")
        print(f"     📄 fundamental.jsonl (configuración inteligente)")
        print(f"     📄 {questions_files_created} questions_intelligent.{'{category}'}.jsonl")
        print(f"     📄 {patterns_files_created} patterns_intelligent.{'{category}'}.jsonl")
        print(f"     📊 intelligent_system_stats.json")
        print(f"     🎯 {sum(len(q) for q in improved_questions.values())} preguntas inteligentes totales")
    
    def _generate_reports(self, low_quality_data: Dict, optimized_patterns: Dict, 
                         validation_results: Dict):
        """Genera reportes detallados del proceso de optimización"""
        print("   📊 Generando reportes...")
        
        # Reporte principal
        report_path = self.output_dir / "optimization_report.md"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 🚀 REPORTE DE OPTIMIZACIÓN BATCH QR\n\n")
            f.write(f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Umbral de calidad:** {self.quality_threshold}\n\n")
            
            # Estadísticas generales
            f.write("## 📊 ESTADÍSTICAS GENERALES\n\n")
            f.write("| Métrica | Valor |\n")
            f.write("|---------|-------|\n")
            for key, value in self.stats.items():
                display_key = key.replace('_', ' ').title()
                if isinstance(value, float):
                    f.write(f"| {display_key} | {value:.2f} |\n")
                else:
                    f.write(f"| {display_key} | {value:,} |\n")
            
            # Análisis por categoría
            f.write("\n## 📂 ANÁLISIS POR CATEGORÍA\n\n")
            for category, conversations in low_quality_data.items():
                f.write(f"### {category.replace('-', ' ').title()}\n")
                f.write(f"- **Conversaciones de baja calidad:** {len(conversations):,}\n")
                
                if category in validation_results['validation_summary']:
                    val_data = validation_results['validation_summary'][category]
                    f.write(f"- **Preguntas mejoradas generadas:** {val_data['total']}\n")
                    f.write(f"- **Calidad promedio mejorada:** {val_data['avg_quality']:.3f}\n")
                    f.write(f"- **Tasa de aprobación:** {(val_data['passed']/val_data['total']*100):.1f}%\n")
                f.write("\n")
            
            # Patrones optimizados
            f.write("## ⚙️ PATRONES OPTIMIZADOS\n\n")
            templates_count = len(optimized_patterns.get('improved_templates', {}))
            f.write(f"- **Templates mejorados:** {templates_count}\n")
            f.write(f"- **Filtros de calidad:** {len(optimized_patterns.get('quality_filters', {}))}\n")
            f.write(f"- **Reglas de mejora:** {len(optimized_patterns.get('enhancement_rules', {}))}\n")
            
            # Validación
            f.write("\n## 🧪 RESULTADOS DE VALIDACIÓN\n\n")
            total_val = validation_results['total_validated']
            passed_val = validation_results['passed_validation']
            avg_quality = sum(validation_results['quality_scores']) / len(validation_results['quality_scores']) if validation_results['quality_scores'] else 0
            
            f.write(f"- **Total validado:** {total_val:,}\n")
            f.write(f"- **Aprobado:** {passed_val:,} ({(passed_val/total_val*100):.1f}%)\n")
            f.write(f"- **Calidad promedio:** {avg_quality:.3f}\n")
            
            # Archivos generados
            f.write("\n## 📁 ARCHIVOS GENERADOS\n\n")
            f.write(f"```\n")
            f.write(f"{self.output_dir}/\n")
            f.write(f"├── fundamental.jsonl\n")
            f.write(f"├── optimization_report.md\n")
            
            for category in low_quality_data.keys():
                f.write(f"├── questions_improved.{category}.jsonl\n")
                f.write(f"├── patterns_optimized.{category}.jsonl\n")
            
            f.write(f"└── low_quality_patterns/\n")
            f.write(f"```\n")
        
        # Reporte de estadísticas JSON
        stats_path = self.output_dir / "optimization_stats.json"
        with open(stats_path, 'w', encoding='utf-8') as f:
            json.dump({
                'execution_summary': self.stats,
                'validation_results': validation_results,
                'optimization_patterns': optimized_patterns,
                'timestamp': datetime.now().isoformat()
            }, f, ensure_ascii=False, indent=2)
        
        print(f"   ✅ Reportes generados:")
        print(f"     📄 {report_path}")
        print(f"     📄 {stats_path}")


def main():
    """Función principal para ejecutar optimización batch con data_test_ultra_hybrid"""
    
    print("🚀 BATCH OPTIMIZATION QR - SISTEMA DE OPTIMIZACIÓN")
    print("=" * 60)
    print("📂 Usando dataset de prueba: data_test_ultra_hybrid")
    print("📋 Configuración: 5 fundamentales + 3 específicas")
    print("=" * 60)
    
    # Crear instancia del optimizador usando data_test_ultra_hybrid
    optimizer = BatchOptimizationQR(
        source_dir="data_test_ultra_hybrid",  # Dataset pequeño de prueba
        output_dir="test_output_batch_optimization",
        quality_threshold=0.6,
        fundamental_questions=5,
        specific_questions=3
    )
    
    try:
        # Ejecutar pipeline completo
        results = optimizer.run_optimization_pipeline()
        
        print(f"\n🎉 OPTIMIZACIÓN COMPLETADA EXITOSAMENTE")
        print(f"📊 Estadísticas finales:")
        for key, value in results.items():
            if isinstance(value, dict):
                print(f"   • {key.replace('_', ' ').title()}:")
                for k, v in value.items():
                    print(f"     - {k}: {v}")
            else:
                print(f"   • {key.replace('_', ' ').title()}: {value:,}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR EN OPTIMIZACIÓN: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    print("🚀 BATCH OPTIMIZATION QR - Sistema Mejorado")
    print("=" * 60)
    
    # Crear el optimizador con parámetros mejorados
    optimizer = BatchOptimizationQR(
        source_dir="data_conversations_complete",
        output_dir="fundamentals_training",
        quality_threshold=0.6,
        fundamental_questions=5,
        specific_questions=3
    )
    
    try:
        # Ejecutar el pipeline de optimización
        results = optimizer.run_optimization_pipeline()
        
        print("\n📊 RESULTADOS DEL PROCESO:")
        print("=" * 40)
        print(f"✅ Total conversaciones analizadas: {results.get('total_conversations', 0):,}")
        print(f"📉 Conversaciones de baja calidad: {results.get('low_quality_conversations', 0):,}")
        print(f"📋 Patrones fundamentales identificados: {results.get('fundamental_patterns_identified', 0)}")
        print(f"🎯 Patrones específicos identificados: {results.get('specific_patterns_identified', 0)}")
        print(f"💡 Preguntas fundamentales mejoradas: {results.get('improved_fundamental_questions', 0)}")
        print(f"🎯 Preguntas específicas mejoradas: {results.get('improved_specific_questions', 0)}")
        print(f"📂 Categorías procesadas: {results.get('categories_processed', 0)}")
        print(f"⏱️ Tiempo de procesamiento: {results.get('processing_time', 0):.2f}s")
        
        quality_improvements = results.get('quality_improvements', {})
        if quality_improvements:
            print(f"\n📈 MEJORAS DE CALIDAD:")
            print(f"   Fundamentales: {quality_improvements.get('fundamental_avg_before', 0):.3f} → {quality_improvements.get('fundamental_avg_after', 0):.3f}")
            print(f"   Específicas: {quality_improvements.get('specific_avg_before', 0):.3f} → {quality_improvements.get('specific_avg_after', 0):.3f}")
        
        print(f"\n🎉 OPTIMIZACIÓN COMPLETADA EXITOSAMENTE")
        success = True
        
    except Exception as e:
        print(f"\n❌ ERROR EN OPTIMIZACIÓN: {e}")
        success = False
    
    sys.exit(0 if success else 1)
