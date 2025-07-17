#!/usr/bin/env python3
"""
🚀 BATCH OPTIMIZATION QR - Sistema de Optimización de Preguntas-Respuestas
==========================================================================
Sistema optimizado para procesar datasets usando adaptive_processor e identificar 
conversaciones de baja calidad para generar patrones optimizados.

CONFIGURACIÓN OPTIMIZADA:
- Dataset fuente: data_test_ultra_hybrid (1 archivo de prueba)
- Fundamentales: 5 (umbral: 0.9)
- Específicas: 3 (umbral: 0.7)
- Logs reducidos y reportes por batch
- Reportes después de procesar cada archivo fuente
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional
from collections import defaultdict

# Configurar path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))


class BatchOptimizationQR:
    """Sistema simplificado para optimización de preguntas-respuestas de baja calidad"""
    
    def __init__(self, 
                 source_dir: str = "data_simple_conversations3",
                 output_dir: str = "fundamental_training",
                 quality_threshold: float = 0.6,
                 fundamental_questions: int = 5,
                 specific_questions: int = 3):
        
        self.source_dir = Path(source_dir)
        self.output_dir = Path(output_dir)
        self.quality_threshold = quality_threshold
        self.fundamental_questions = fundamental_questions
        self.specific_questions = specific_questions
        
        # Crear directorios de salida
        self.output_dir.mkdir(exist_ok=True, parents=True)
        
        # Estadísticas de procesamiento
        self.stats = {
            'total_articles_processed': 0,
            'total_conversations_generated': 0,
            'low_quality_conversations': 0,
            'fundamental_questions_generated': 0,
            'specific_questions_generated': 0,
            'fundamental_low_quality': 0,
            'specific_low_quality': 0,
            'processing_time': 0,
            'files_processed': 0,
            'batch_reports_generated': 0
        }
        
        print(f"🚀 BATCH OPTIMIZATION QR INICIALIZADO")
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
            # FASE 1: Procesar dataset con adaptive_processor
            print("📋 FASE 1: Procesamiento con adaptive_processor")
            processing_results = self._process_with_adaptive_processor()
            
            # FASE 2: Analizar resultados y generar reportes por batch
            print("\n🔍 FASE 2: Análisis de resultados y reportes por batch")
            analysis_results = self._analyze_processing_results(processing_results)
            
            # FASE 3: Generar reporte final optimizado
            print("\n📊 FASE 3: Generación de reporte final")
            self._generate_final_report(analysis_results)
            
            end_time = datetime.now()
            self.stats['processing_time'] = (end_time - start_time).total_seconds()
            
            print(f"\n✅ PIPELINE COMPLETADO EN {self.stats['processing_time']:.2f} segundos")
            return self.stats
            
        except Exception as e:
            print(f"\n❌ ERROR EN PIPELINE: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _process_with_adaptive_processor(self) -> Dict[str, Any]:
        """Analiza las conversaciones ya generadas en lugar de procesar desde cero"""
        print(f"   � Analizando conversaciones existentes en {self.source_dir}")
        
        # Usar el directorio fuente directamente (conversaciones ya generadas)
        processing_results = {
            'temp_output_dir': self.source_dir,
            'success': False,
            'error': None,
            'adaptive_processor_stats': {}
        }
        
        try:
            if not self.source_dir.exists():
                raise FileNotFoundError(f"Directorio de conversaciones no existe: {self.source_dir}")
            
            # Contar archivos y estadísticas básicas
            conversation_files = list(self.source_dir.rglob('*.jsonl'))
            
            print(f"   📁 Archivos de conversaciones encontrados: {len(conversation_files)}")
            
            # Estadísticas básicas
            total_conversations = 0
            categories_found = set()
            
            for conv_file in conversation_files:
                try:
                    with open(conv_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                try:
                                    conv = json.loads(line)
                                    total_conversations += 1
                                    if 'category' in conv:
                                        categories_found.add(conv['category'])
                                except json.JSONDecodeError:
                                    continue
                except Exception as e:
                    print(f"     ⚠️ Error leyendo {conv_file}: {e}")
                    continue
            
            processing_results['adaptive_processor_stats'] = {
                'total_conversations': total_conversations,
                'conversation_files': len(conversation_files),
                'categories_found': len(categories_found),
                'categories_list': sorted(list(categories_found))
            }
            
            processing_results['success'] = True
            
            print(f"   ✅ Análisis completado: {total_conversations:,} conversaciones en {len(categories_found)} categorías")
                
            # Generar reporte del procesamiento
            self._generate_processing_batch_report(processing_results)
            
        except Exception as e:
            processing_results['error'] = str(e)
            print(f"   ❌ Error analizando conversaciones: {e}")
            import traceback
            traceback.print_exc()
        
        return processing_results
    
    def _generate_processing_batch_report(self, processing_results: Dict[str, Any]):
        """Genera reporte después de procesar el archivo fuente"""
        print(f"   📊 Generando reporte del batch de procesamiento...")
        
        report_file = self.output_dir / f"batch_report_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# 📊 REPORTE DE BATCH - PROCESAMIENTO\n\n")
            f.write(f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Dataset procesado:** {self.source_dir}\n")
            f.write(f"**Configuración:** {self.fundamental_questions} fundamentales + {self.specific_questions} específicas\n\n")
            
            f.write("## 🚀 RESULTADOS DEL PROCESAMIENTO\n\n")
            if processing_results['success']:
                f.write("✅ **Estado:** COMPLETADO EXITOSAMENTE\n\n")
                
                adaptive_stats = processing_results.get('adaptive_processor_stats', {})
                if adaptive_stats:
                    f.write("### 📈 Estadísticas del Adaptive Processor\n\n")
                    for key, value in adaptive_stats.items():
                        if isinstance(value, (int, float)):
                            f.write(f"- **{key.replace('_', ' ').title()}:** {value:,}\n")
                        else:
                            f.write(f"- **{key.replace('_', ' ').title()}:** {value}\n")
                else:
                    f.write("- **Procesamiento completado** sin estadísticas detalladas\n")
                    
            else:
                f.write("❌ **Estado:** ERROR\n\n")
                f.write(f"**Error:** {processing_results.get('error', 'Unknown')}\n\n")
            
            f.write("## 📁 ARCHIVOS GENERADOS\n\n")
            temp_output = processing_results.get('temp_output_dir')
            if temp_output and temp_output.exists():
                for item in temp_output.rglob('*'):
                    if item.is_file():
                        relative_path = item.relative_to(temp_output)
                        size = item.stat().st_size
                        f.write(f"- `{relative_path}` ({size:,} bytes)\n")
            
        self.stats['batch_reports_generated'] += 1
        print(f"   ✅ Reporte de batch generado: {report_file.name}")
    
    def _analyze_processing_results(self, processing_results: Dict[str, Any]) -> Dict[str, Any]:
        """Analiza los resultados del procesamiento"""
        print(f"   🔍 Analizando resultados del procesamiento...")
        
        analysis_results = {
            'conversations_analyzed': 0,
            'low_quality_found': 0,
            'fundamental_analysis': {'total': 0, 'low_quality': 0, 'avg_quality': 0.0},
            'specific_analysis': {'total': 0, 'low_quality': 0, 'avg_quality': 0.0},
            'categories_found': set(),
            'quality_distribution': defaultdict(int)
        }
        
        temp_output = processing_results.get('temp_output_dir')
        if not temp_output or not temp_output.exists():
            print(f"   ⚠️ No se encontró directorio de resultados")
            return analysis_results
        
        # Usar estadísticas del análisis previo si están disponibles
        adaptive_stats = processing_results.get('adaptive_processor_stats', {})
        if adaptive_stats:
            self.stats['total_conversations_generated'] = adaptive_stats.get('total_conversations', 0)
            analysis_results['conversations_analyzed'] = self.stats['total_conversations_generated']
            analysis_results['categories_found'] = set(adaptive_stats.get('categories_list', []))
            
            print(f"     📈 {self.stats['total_conversations_generated']} conversaciones encontradas")
            print(f"     📈 {len(analysis_results['categories_found'])} categorías encontradas")
        
        # Analizar archivos de conversaciones existentes
        conv_dirs = [
            temp_output / "categorias",
            temp_output / "consciencia_completa"
        ]
        
        # Si no existen esos subdirectorios, analizar directamente el directorio fuente
        if not any(d.exists() for d in conv_dirs):
            conv_dirs = [temp_output]
        
        for conv_dir in conv_dirs:
            if conv_dir.exists():
                print(f"     📂 Analizando: {conv_dir.name}")
                self._analyze_conversation_directory(conv_dir, analysis_results)
        
        # Generar patrones de entrenamiento fundamental
        self._generate_fundamental_patterns(analysis_results)
        
        # Generar reporte del análisis
        self._generate_analysis_batch_report(analysis_results)
        
        return analysis_results
    
    def _analyze_conversation_directory(self, conv_dir: Path, analysis_results: Dict[str, Any]):
        """Analiza un directorio de conversaciones"""
        
        for item in conv_dir.rglob('*.jsonl'):
            try:
                with open(item, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                conversation = json.loads(line)
                                self._analyze_single_conversation(conversation, analysis_results)
                            except json.JSONDecodeError:
                                continue
                                
            except Exception as e:
                print(f"     ⚠️ Error analizando {item}: {e}")
                continue
    
    def _analyze_single_conversation(self, conversation: Dict, analysis_results: Dict[str, Any]):
        """Analiza una conversación individual"""
        quality_score = conversation.get('quality_score', 0.0)
        question_type = conversation.get('question_type', 'unknown')
        category = conversation.get('category', 'unknown')
        
        analysis_results['conversations_analyzed'] += 1
        analysis_results['categories_found'].add(category)
        
        # Distribución de calidad (bins de 0.1)
        quality_bin = int(quality_score * 10) / 10
        analysis_results['quality_distribution'][quality_bin] += 1
        
        # Análisis por tipo de pregunta
        if 'fundamental' in question_type.lower():
            analysis_results['fundamental_analysis']['total'] += 1
            if quality_score < 0.9:
                analysis_results['fundamental_analysis']['low_quality'] += 1
                self.stats['fundamental_low_quality'] += 1
            self.stats['fundamental_questions_generated'] += 1
            
        elif 'specific' in question_type.lower():
            analysis_results['specific_analysis']['total'] += 1
            if quality_score < 0.7:
                analysis_results['specific_analysis']['low_quality'] += 1
                self.stats['specific_low_quality'] += 1
            self.stats['specific_questions_generated'] += 1
        
        # Conteo general de baja calidad
        if quality_score < self.quality_threshold:
            analysis_results['low_quality_found'] += 1
            self.stats['low_quality_conversations'] += 1
        
        self.stats['total_conversations_generated'] += 1
    
    def _generate_fundamental_patterns(self, analysis_results: Dict[str, Any]):
        """Genera patrones de entrenamiento fundamental basados en el análisis"""
        print(f"   🧠 Generando patrones de entrenamiento fundamental...")
        
        # Crear directorio de patrones fundamentales
        patterns_dir = self.output_dir / "patterns"
        patterns_dir.mkdir(exist_ok=True, parents=True)
        
        # Recopilar muestras por categoría y calidad
        category_samples = defaultdict(lambda: {'high_quality': [], 'low_quality': []})
        question_types = defaultdict(lambda: {'fundamental': [], 'specific': []})
        
        # Analizar todas las conversaciones para extraer patrones
        temp_output = self.source_dir
        for conv_file in temp_output.rglob('*.jsonl'):
            try:
                with open(conv_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                conv = json.loads(line)
                                self._extract_conversation_patterns(conv, category_samples, question_types)
                            except json.JSONDecodeError:
                                continue
            except Exception as e:
                continue
        
        # Generar archivos de patrones
        self._save_category_patterns(patterns_dir, category_samples)
        self._save_question_type_patterns(patterns_dir, question_types)
        self._save_training_templates(patterns_dir, analysis_results)
        
        print(f"   ✅ Patrones fundamentales generados en: {patterns_dir}")
    
    def _extract_conversation_patterns(self, conv: Dict, category_samples: Dict, question_types: Dict):
        """Extrae patrones de una conversación individual"""
        category = conv.get('category', 'unknown')
        quality_score = conv.get('quality_score', 0.0)
        question_type = conv.get('question_type', 'general')
        
        # Clasificar por calidad
        quality_bucket = 'high_quality' if quality_score >= 0.8 else 'low_quality'
        category_samples[category][quality_bucket].append({
            'conversation': conv.get('conversation', []),
            'quality_score': quality_score,
            'article_title': conv.get('article_title', ''),
            'metadata': conv.get('metadata', {})
        })
        
        # Clasificar por tipo de pregunta
        if 'fundamental' in question_type.lower():
            question_types[category]['fundamental'].append(conv)
        else:
            question_types[category]['specific'].append(conv)
    
    def _save_category_patterns(self, patterns_dir: Path, category_samples: Dict):
        """Guarda patrones por categoría"""
        category_patterns_file = patterns_dir / "category_patterns.json"
        
        # Crear resumen de patrones por categoría
        category_summary = {}
        for category, samples in category_samples.items():
            high_quality_count = len(samples['high_quality'])
            low_quality_count = len(samples['low_quality'])
            total = high_quality_count + low_quality_count
            
            if total > 0:
                category_summary[category] = {
                    'total_conversations': total,
                    'high_quality_count': high_quality_count,
                    'low_quality_count': low_quality_count,
                    'quality_ratio': high_quality_count / total if total > 0 else 0,
                    'sample_high_quality': samples['high_quality'][:3],  # Top 3 ejemplos
                    'sample_low_quality': samples['low_quality'][:3]     # Top 3 problemas
                }
        
        with open(category_patterns_file, 'w', encoding='utf-8') as f:
            json.dump(category_summary, f, indent=2, ensure_ascii=False)
        
        print(f"     ✅ Patrones de categoría guardados: {category_patterns_file.name}")
    
    def _save_question_type_patterns(self, patterns_dir: Path, question_types: Dict):
        """Guarda patrones por tipo de pregunta"""
        
        # Patrones fundamentales
        fundamental_patterns_file = patterns_dir / "fundamental_patterns.json"
        fundamental_summary = {}
        
        for category, types in question_types.items():
            fundamentals = types['fundamental']
            if fundamentals:
                # Analizar patrones de preguntas fundamentales
                questions = []
                answers = []
                qualities = []
                
                for conv in fundamentals:
                    conv_data = conv.get('conversation', [])
                    quality = conv.get('quality_score', 0.0)
                    
                    for msg in conv_data:
                        if msg.get('role') == 'user':
                            questions.append(msg.get('content', ''))
                        elif msg.get('role') == 'assistant':
                            answers.append(msg.get('content', ''))
                    
                    qualities.append(quality)
                
                if questions:
                    fundamental_summary[category] = {
                        'question_patterns': questions[:5],  # Top 5 preguntas
                        'answer_patterns': answers[:5],      # Top 5 respuestas
                        'avg_quality': sum(qualities) / len(qualities) if qualities else 0,
                        'total_fundamentals': len(fundamentals),
                        'quality_distribution': {
                            'excellent': sum(1 for q in qualities if q >= 0.9),
                            'good': sum(1 for q in qualities if 0.8 <= q < 0.9),
                            'fair': sum(1 for q in qualities if 0.7 <= q < 0.8),
                            'poor': sum(1 for q in qualities if q < 0.7)
                        }
                    }
        
        with open(fundamental_patterns_file, 'w', encoding='utf-8') as f:
            json.dump(fundamental_summary, f, indent=2, ensure_ascii=False)
        
        print(f"     ✅ Patrones fundamentales guardados: {fundamental_patterns_file.name}")
    
    def _save_training_templates(self, patterns_dir: Path, analysis_results: Dict):
        """Guarda templates optimizados para entrenamiento"""
        
        templates_file = patterns_dir / "training_templates.json"
        
        # Crear templates basados en análisis
        templates = {
            'metadata': {
                'generated_date': datetime.now().isoformat(),
                'total_conversations_analyzed': analysis_results.get('conversations_analyzed', 0),
                'categories_found': len(analysis_results.get('categories_found', [])),
                'quality_threshold': self.quality_threshold
            },
            'fundamental_template': {
                'question_starters': [
                    "Explícame en detalle todo sobre {article_title}",
                    "¿Qué aspectos fundamentales debo conocer sobre {article_title}?",
                    "Proporciona una explicación completa de {article_title}",
                    "Describe los elementos esenciales de {article_title}",
                    "¿Cuáles son las características principales de {article_title}?"
                ],
                'quality_requirements': {
                    'minimum_score': 0.9,
                    'comprehensive_coverage': True,
                    'factual_accuracy': True,
                    'clear_structure': True
                }
            },
            'specific_template': {
                'question_patterns': {
                    'historia-tiempo': [
                        "¿Cuándo ocurrió {event}?",
                        "¿Qué acontecimientos históricos están relacionados con {article_title}?",
                        "¿Cuál fue el contexto histórico de {article_title}?"
                    ],
                    'espacio-geografia': [
                        "¿Dónde se encuentra {location}?",
                        "¿Cuáles son las características geográficas de {article_title}?",
                        "¿Qué ubicación específica tiene {article_title}?"
                    ],
                    'naturaleza-universo': [
                        "¿Cómo funciona {natural_phenomenon}?",
                        "¿Qué características biológicas tiene {species}?",
                        "¿Cuál es el proceso natural de {article_title}?"
                    ]
                },
                'quality_requirements': {
                    'minimum_score': 0.7,
                    'context_relevance': True,
                    'specific_information': True
                }
            },
            'optimization_recommendations': {
                'fundamental_improvements': [
                    "Usar introducción contextual antes de detalles técnicos",
                    "Incluir definiciones de términos especializados",
                    "Estructurar respuesta en secciones claras",
                    "Proporcionar ejemplos concretos cuando sea posible"
                ],
                'specific_improvements': [
                    "Enfocar en aspectos únicos de la categoría",
                    "Incluir datos específicos y medibles",
                    "Relacionar con conceptos similares en la categoría",
                    "Proporcionar contexto relevante específico"
                ]
            }
        }
        
        with open(templates_file, 'w', encoding='utf-8') as f:
            json.dump(templates, f, indent=2, ensure_ascii=False)
        
        print(f"     ✅ Templates de entrenamiento guardados: {templates_file.name}")

    def _generate_analysis_batch_report(self, analysis_results: Dict[str, Any]):
        """Genera reporte del análisis por batch"""
        print(f"   📊 Generando reporte del batch de análisis...")
        
        report_file = self.output_dir / f"batch_report_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        # Calcular promedios de calidad
        fundamental_total = analysis_results['fundamental_analysis']['total']
        specific_total = analysis_results['specific_analysis']['total']
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# 📊 REPORTE DE BATCH - ANÁLISIS DE CALIDAD\n\n")
            f.write(f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Configuración:** {self.fundamental_questions}F + {self.specific_questions}E\n\n")
            
            f.write("## 📈 ESTADÍSTICAS GENERALES\n\n")
            f.write(f"- **Conversaciones analizadas:** {analysis_results['conversations_analyzed']:,}\n")
            f.write(f"- **Baja calidad encontradas:** {analysis_results['low_quality_found']:,}\n")
            f.write(f"- **Categorías encontradas:** {len(analysis_results['categories_found'])}\n")
            f.write(f"- **Tasa de baja calidad:** {(analysis_results['low_quality_found']/max(1,analysis_results['conversations_analyzed'])*100):.1f}%\n\n")
            
            f.write("## 🎯 ANÁLISIS POR TIPO DE PREGUNTA\n\n")
            f.write("### 📋 PREGUNTAS FUNDAMENTALES (Umbral: 0.9)\n")
            f.write(f"- **Total generadas:** {fundamental_total:,}\n")
            f.write(f"- **Bajo umbral 0.9:** {analysis_results['fundamental_analysis']['low_quality']:,}\n")
            if fundamental_total > 0:
                f.write(f"- **Tasa de éxito:** {((fundamental_total - analysis_results['fundamental_analysis']['low_quality'])/fundamental_total*100):.1f}%\n")
            f.write("\n")
            
            f.write("### 🎯 PREGUNTAS ESPECÍFICAS (Umbral: 0.7)\n")
            f.write(f"- **Total generadas:** {specific_total:,}\n")
            f.write(f"- **Bajo umbral 0.7:** {analysis_results['specific_analysis']['low_quality']:,}\n")
            if specific_total > 0:
                f.write(f"- **Tasa de éxito:** {((specific_total - analysis_results['specific_analysis']['low_quality'])/specific_total*100):.1f}%\n")
            f.write("\n")
            
            f.write("## 📊 DISTRIBUCIÓN DE CALIDAD\n\n")
            f.write("| Rango de Calidad | Cantidad |\n")
            f.write("|------------------|----------|\n")
            for quality_bin in sorted(analysis_results['quality_distribution'].keys()):
                count = analysis_results['quality_distribution'][quality_bin]
                f.write(f"| {quality_bin:.1f} - {quality_bin + 0.1:.1f} | {count:,} |\n")
            
            f.write("\n## 📂 CATEGORÍAS ENCONTRADAS\n\n")
            for category in sorted(analysis_results['categories_found']):
                f.write(f"- {category}\n")
        
        self.stats['batch_reports_generated'] += 1
        print(f"   ✅ Reporte de análisis generado: {report_file.name}")
    
    def _generate_final_report(self, analysis_results: Dict[str, Any]):
        """Genera el reporte final consolidado"""
        print(f"   📊 Generando reporte final consolidado...")
        
        report_file = self.output_dir / f"final_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("# 🚀 REPORTE FINAL - BATCH OPTIMIZATION QR\n\n")
            f.write(f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Dataset procesado:** {self.source_dir}\n")
            f.write(f"**Tiempo de procesamiento:** {self.stats['processing_time']:.2f} segundos\n\n")
            
            f.write("## 📊 RESUMEN EJECUTIVO\n\n")
            f.write(f"- **Configuración:** {self.fundamental_questions} fundamentales + {self.specific_questions} específicas\n")
            f.write(f"- **Conversaciones generadas:** {self.stats['total_conversations_generated']:,}\n")
            f.write(f"- **Fundamentales generadas:** {self.stats['fundamental_questions_generated']:,}\n")
            f.write(f"- **Específicas generadas:** {self.stats['specific_questions_generated']:,}\n")
            f.write(f"- **Reportes de batch generados:** {self.stats['batch_reports_generated']}\n\n")
            
            f.write("## 🎯 ANÁLISIS DE CALIDAD\n\n")
            f.write(f"- **Umbral general de baja calidad:** {self.quality_threshold}\n")
            f.write(f"- **Conversaciones de baja calidad:** {self.stats['low_quality_conversations']:,}\n")
            f.write(f"- **Fundamentales bajo umbral 0.9:** {self.stats['fundamental_low_quality']:,}\n")
            f.write(f"- **Específicas bajo umbral 0.7:** {self.stats['specific_low_quality']:,}\n\n")
            
            # Tasas de éxito
            fund_success_rate = 0
            if self.stats['fundamental_questions_generated'] > 0:
                fund_success_rate = ((self.stats['fundamental_questions_generated'] - self.stats['fundamental_low_quality']) / self.stats['fundamental_questions_generated']) * 100
            
            spec_success_rate = 0
            if self.stats['specific_questions_generated'] > 0:
                spec_success_rate = ((self.stats['specific_questions_generated'] - self.stats['specific_low_quality']) / self.stats['specific_questions_generated']) * 100
            
            f.write("## 📈 TASAS DE ÉXITO\n\n")
            f.write(f"- **Fundamentales (≥ 0.9):** {fund_success_rate:.1f}%\n")
            f.write(f"- **Específicas (≥ 0.7):** {spec_success_rate:.1f}%\n\n")
            
            f.write("## 📁 ARCHIVOS Y REPORTES GENERADOS\n\n")
            f.write(f"```\n")
            f.write(f"{self.output_dir}/\n")
            f.write(f"├── adaptive_processor_results/\n")
            f.write(f"├── batch_report_processing_*.md\n")
            f.write(f"├── batch_report_analysis_*.md\n")
            f.write(f"└── final_report_*.md\n")
            f.write(f"```\n\n")
            
            f.write("## 🚀 RECOMENDACIONES\n\n")
            if fund_success_rate < 80:
                f.write("- ⚠️ **Fundamentales:** Considerar ajustar templates para mejorar calidad\n")
            if spec_success_rate < 80:
                f.write("- ⚠️ **Específicas:** Considerar ajustar contexto específico para mejorar relevancia\n")
            if self.stats['low_quality_conversations'] > self.stats['total_conversations_generated'] * 0.3:
                f.write("- ⚠️ **General:** Alto porcentaje de baja calidad, revisar configuración\n")
            
            f.write("\n---\n")
            f.write("*Reporte generado por Batch Optimization QR*\n")
        
        print(f"   ✅ Reporte final generado: {report_file.name}")


def main():
    """Función principal para ejecutar optimización batch con conversaciones generadas"""
    
    print("🚀 BATCH OPTIMIZATION QR - SISTEMA DE OPTIMIZACIÓN")
    print("=" * 70)
    print("📂 Analizando conversaciones existentes: data_simple_conversations3")
    print("📋 Configuración: 5 fundamentales + 3 específicas")
    print("📊 Generando patrones para: fundamental_training")
    print("=" * 70)
    
    # Crear instancia del optimizador usando conversaciones existentes
    optimizer = BatchOptimizationQR(
        source_dir="data_simple_conversations3",  # Conversaciones ya generadas
        output_dir="fundamental_training",        # Salida para patrones de entrenamiento
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
    success = main()
    sys.exit(0 if success else 1)
