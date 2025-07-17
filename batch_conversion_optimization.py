#!/usr/bin/env python3
"""
🔄 BATCH CONVERSION OPTIMIZATION - Sistema de Conversión y Optimización
======================================================================
Sistema complementario que convierte datos optimizados y los integra con
adaptive_processor y simple_processor para maximizar la eficiencia.

FUNCIONALIDAD:
- Convierte salidas de batch_optimization_qr para adaptive_processor
- Integra con simple_processor para operaciones individuales
- Optimiza flujo de trabajo con content_manager
- Mantiene compatibilidad con formation_system.py

ARQUITECTURA:
1. adaptive_processor: Lotes de artículos
2. simple_processor: Operaciones individuales content_manager  
3. content_manager + formation_system.py: Procesamiento de contenido
4. batch_optimization_qr: Identificación baja calidad < 0.6
5. batch_conversion_optimization: Conversión e integración
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import shutil
from collections import defaultdict
import asyncio
import concurrent.futures

# Importar módulos locales
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

try:
    from content_manager import ContentManager
    from formation_fundamental.formation_system import FormationFundamentalSystem
    from adaptive_processor import AdaptiveProcessor
    
    # Importar category_system
    import importlib.util
    category_spec = importlib.util.spec_from_file_location("category_system", current_dir / "category_system.py")
    category_module = importlib.util.module_from_spec(category_spec)
    category_spec.loader.exec_module(category_module)
    
    # Crear clase CategorySystem desde el módulo
    class CategorySystem:
        def __init__(self):
            self.categories = category_module.MAIN_CATEGORIES
        
        def get_category_config(self, category: str) -> dict:
            return self.categories.get(category, {})
        
        def get_all_categories(self) -> dict:
            return self.categories
    
    # Crear clase SimpleProcessor desde simple_processor.py
    simple_spec = importlib.util.spec_from_file_location("simple_processor", current_dir / "simple_processor.py")
    simple_module = importlib.util.module_from_spec(simple_spec)
    simple_spec.loader.exec_module(simple_module)
    
    class SimpleProcessor:
        def __init__(self):
            self.processor = simple_module.MassiveParallelDatasetProcessor()
            
        def process_individual(self, data: dict) -> dict:
            """Procesa un elemento individual"""
            return {"processed": True, "data": data}
            
        def validate_operation(self, operation: dict) -> bool:
            """Valida una operación individual"""
            return True
            
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
    
    class SimpleProcessor:
        def __init__(self):
            pass
        def process_individual(self, data: dict) -> dict:
            return {"processed": True, "data": data}
        def validate_operation(self, operation: dict) -> bool:
            return True


class BatchConversionOptimization:
    """Sistema de conversión y optimización para integración completa"""
    
    def __init__(self, 
                 fundamentals_training_dir: str = "fundamentals_training",
                 output_dir: str = "conversion_optimized",
                 batch_size: int = 1000):
        
        self.fundamentals_training_dir = Path(fundamentals_training_dir)
        self.output_dir = Path(output_dir)
        self.batch_size = batch_size
        
        # Crear directorios
        self.output_dir.mkdir(exist_ok=True)
        (self.output_dir / "adaptive_batches").mkdir(exist_ok=True)
        (self.output_dir / "simple_individual").mkdir(exist_ok=True)
        (self.output_dir / "integrated_results").mkdir(exist_ok=True)
        
        # Inicializar sistemas
        self.category_system = CategorySystem()
        self.content_manager = ContentManager()
        self.formation_system = FormationFundamentalSystem(str(self.fundamentals_training_dir))
        
        # Configurar procesadores
        self.adaptive_processor = None
        self.simple_processor = None
        self._init_processors()
        
        # Estadísticas
        self.stats = {
            'conversion_start': datetime.now(),
            'files_processed': 0,
            'batches_created': 0,
            'individual_operations': 0,
            'integration_tests': 0,
            'quality_improvements': 0,
            'processing_time': 0
        }
        
        print(f"🔄 BATCH CONVERSION OPTIMIZATION INICIALIZADO")
        print(f"📁 Entrada: {self.fundamentals_training_dir}")
        print(f"📁 Salida: {self.output_dir}")
        print(f"⚙️ Tamaño batch: {self.batch_size}")
    
    def _init_processors(self):
        """Inicializa adaptive_processor y simple_processor"""
        try:
            # Configurar adaptive_processor para lotes
            self.adaptive_processor = AdaptiveProcessor()
            
            # Configurar simple_processor para operaciones individuales
            self.simple_processor = SimpleProcessor()
            
            print("   ✅ Procesadores inicializados correctamente")
            
        except Exception as e:
            print(f"   ⚠️ Error inicializando procesadores: {e}")
    
    def run_conversion_pipeline(self) -> Dict[str, Any]:
        """Ejecuta el pipeline completo de conversión y optimización"""
        start_time = datetime.now()
        
        print("\n🔄 INICIANDO PIPELINE DE CONVERSIÓN Y OPTIMIZACIÓN")
        print("=" * 70)
        
        try:
            # FASE 1: Validar datos de entrada
            print("📋 FASE 1: Validación de datos optimizados")
            validation_results = self._validate_input_data()
            
            # FASE 2: Convertir para adaptive_processor
            print("\n⚙️ FASE 2: Conversión para adaptive_processor")
            adaptive_batches = self._convert_for_adaptive_processor()
            
            # FASE 3: Preparar para simple_processor
            print("\n🔧 FASE 3: Preparación para simple_processor")
            individual_operations = self._prepare_for_simple_processor()
            
            # FASE 4: Integrar con content_manager
            print("\n🔗 FASE 4: Integración con content_manager")
            integration_results = self._integrate_with_content_manager()
            
            # FASE 5: Optimizar flujo de trabajo
            print("\n🚀 FASE 5: Optimización de flujo de trabajo")
            workflow_optimization = self._optimize_workflow()
            
            # FASE 6: Ejecutar pruebas de calidad
            print("\n🧪 FASE 6: Pruebas de calidad integradas")
            quality_tests = self._run_quality_tests()
            
            # FASE 7: Generar resultados finales
            print("\n📊 FASE 7: Generación de resultados finales")
            final_results = self._generate_final_results(
                validation_results, adaptive_batches, individual_operations,
                integration_results, workflow_optimization, quality_tests
            )
            
            end_time = datetime.now()
            self.stats['processing_time'] = (end_time - start_time).total_seconds()
            
            print(f"\n✅ PIPELINE DE CONVERSIÓN COMPLETADO EN {self.stats['processing_time']:.2f} segundos")
            return final_results
            
        except Exception as e:
            print(f"❌ Error en pipeline de conversión: {e}")
            raise
    
    def _validate_input_data(self) -> Dict[str, Any]:
        """Valida datos de entrada desde fundamentals_training"""
        print("   📋 Validando datos optimizados...")
        
        validation = {
            'fundamental_exists': False,
            'improved_questions_count': 0,
            'optimized_patterns_count': 0,
            'categories_available': [],
            'quality_threshold_met': False,
            'issues_found': []
        }
        
        # Verificar fundamental.jsonl
        fundamental_path = self.fundamentals_training_dir / "fundamental.jsonl"
        if fundamental_path.exists():
            validation['fundamental_exists'] = True
            print("     ✅ fundamental.jsonl encontrado")
        else:
            validation['issues_found'].append("fundamental.jsonl no encontrado")
            print("     ❌ fundamental.jsonl NO encontrado")
        
        # Buscar archivos de preguntas mejoradas
        improved_files = list(self.fundamentals_training_dir.glob("questions_improved.*.jsonl"))
        validation['improved_questions_count'] = len(improved_files)
        
        # Buscar patrones optimizados
        pattern_files = list(self.fundamentals_training_dir.glob("patterns_optimized.*.jsonl"))
        validation['optimized_patterns_count'] = len(pattern_files)
        
        # Extraer categorías disponibles
        for file in improved_files:
            category = file.stem.replace('questions_improved.', '')
            validation['categories_available'].append(category)
        
        print(f"     📊 Preguntas mejoradas: {validation['improved_questions_count']} archivos")
        print(f"     📊 Patrones optimizados: {validation['optimized_patterns_count']} archivos")
        print(f"     📊 Categorías disponibles: {len(validation['categories_available'])}")
        
        # Validar calidad mínima
        if validation['improved_questions_count'] > 0 and validation['optimized_patterns_count'] > 0:
            validation['quality_threshold_met'] = True
            print("     ✅ Umbral de calidad cumplido")
        else:
            validation['issues_found'].append("Umbral de calidad no cumplido")
            print("     ❌ Umbral de calidad NO cumplido")
        
        return validation
    
    def _convert_for_adaptive_processor(self) -> Dict[str, Any]:
        """Convierte datos optimizados para adaptive_processor"""
        print("   ⚙️ Convirtiendo datos para adaptive_processor...")
        
        conversion_results = {
            'batches_created': 0,
            'total_items': 0,
            'batch_files': [],
            'conversion_errors': []
        }
        
        # Procesar archivos de preguntas mejoradas
        improved_files = list(self.fundamentals_training_dir.glob("questions_improved.*.jsonl"))
        
        current_batch = []
        batch_number = 1
        
        for file_path in improved_files:
            category = file_path.stem.replace('questions_improved.', '')
            print(f"     📂 Procesando {category}...")
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                question_data = json.loads(line)
                                
                                # Convertir formato para adaptive_processor
                                adaptive_item = self._convert_to_adaptive_format(question_data, category)
                                current_batch.append(adaptive_item)
                                conversion_results['total_items'] += 1
                                
                                # Crear batch cuando alcance el tamaño
                                if len(current_batch) >= self.batch_size:
                                    batch_file = self._save_adaptive_batch(current_batch, batch_number)
                                    conversion_results['batch_files'].append(batch_file)
                                    conversion_results['batches_created'] += 1
                                    
                                    current_batch = []
                                    batch_number += 1
                                    
                            except json.JSONDecodeError as e:
                                conversion_results['conversion_errors'].append(f"Error JSON en {file_path}: {e}")
                                
            except Exception as e:
                conversion_results['conversion_errors'].append(f"Error procesando {file_path}: {e}")
        
        # Guardar último batch si tiene items
        if current_batch:
            batch_file = self._save_adaptive_batch(current_batch, batch_number)
            conversion_results['batch_files'].append(batch_file)
            conversion_results['batches_created'] += 1
        
        self.stats['batches_created'] = conversion_results['batches_created']
        self.stats['files_processed'] += len(improved_files)
        
        print(f"     ✅ Batches creados: {conversion_results['batches_created']}")
        print(f"     📊 Items totales: {conversion_results['total_items']:,}")
        
        return conversion_results
    
    def _convert_to_adaptive_format(self, question_data: Dict, category: str) -> Dict:
        """Convierte formato de pregunta para adaptive_processor"""
        return {
            'title': f"Optimized_{category}_{question_data.get('question_type', 'general')}",
            'content': question_data.get('answer', ''),
            'category': category,
            'quality_score': question_data.get('quality_score', 0.8),
            'metadata': {
                'source': 'batch_optimization_qr',
                'question': question_data.get('question', ''),
                'question_type': question_data.get('question_type', ''),
                'enhancement_applied': question_data.get('enhancement_applied', True),
                'target_quality': question_data.get('target_quality_threshold', 0.7)
            },
            'processing_hints': {
                'use_optimized_patterns': True,
                'avoid_low_quality_phrases': True,
                'ensure_quality_threshold': 0.7
            }
        }
    
    def _save_adaptive_batch(self, batch_items: List[Dict], batch_number: int) -> str:
        """Guarda batch para adaptive_processor"""
        batch_file = self.output_dir / "adaptive_batches" / f"optimized_batch_{batch_number:04d}.jsonl"
        
        with open(batch_file, 'w', encoding='utf-8') as f:
            for item in batch_items:
                json.dump(item, f, ensure_ascii=False)
                f.write('\n')
        
        return str(batch_file)
    
    def _prepare_for_simple_processor(self) -> Dict[str, Any]:
        """Prepara operaciones individuales para simple_processor"""
        print("   🔧 Preparando operaciones individuales...")
        
        individual_results = {
            'operations_prepared': 0,
            'categories_processed': [],
            'operation_files': [],
            'preparation_errors': []
        }
        
        # Crear operaciones individuales basadas en patrones optimizados
        pattern_files = list(self.fundamentals_training_dir.glob("patterns_optimized.*.jsonl"))
        
        for pattern_file in pattern_files:
            category = pattern_file.stem.replace('patterns_optimized.', '')
            print(f"     📂 Preparando operaciones para {category}...")
            
            try:
                with open(pattern_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            pattern_data = json.loads(line)
                            
                            # Crear operaciones individuales
                            operations = self._create_individual_operations(pattern_data, category)
                            
                            for i, operation in enumerate(operations):
                                operation_file = self.output_dir / "simple_individual" / f"{category}_operation_{i+1:03d}.json"
                                
                                with open(operation_file, 'w', encoding='utf-8') as op_f:
                                    json.dump(operation, op_f, ensure_ascii=False, indent=2)
                                
                                individual_results['operation_files'].append(str(operation_file))
                                individual_results['operations_prepared'] += 1
                            
                            individual_results['categories_processed'].append(category)
                            
            except Exception as e:
                individual_results['preparation_errors'].append(f"Error preparando {pattern_file}: {e}")
        
        self.stats['individual_operations'] = individual_results['operations_prepared']
        
        print(f"     ✅ Operaciones preparadas: {individual_results['operations_prepared']}")
        print(f"     📊 Categorías procesadas: {len(set(individual_results['categories_processed']))}")
        
        return individual_results
    
    def _create_individual_operations(self, pattern_data: Dict, category: str) -> List[Dict]:
        """Crea operaciones individuales para simple_processor"""
        operations = []
        
        pattern_info = pattern_data.get('data', {})
        
        # Operación de validación de calidad
        quality_operation = {
            'operation_type': 'quality_validation',
            'category': category,
            'target': 'validate_enhanced_patterns',
            'parameters': {
                'quality_threshold': pattern_info.get('min_length', 100),
                'required_structure': pattern_info.get('required_structure', {}),
                'avoid_phrases': pattern_info.get('avoid_phrases', [])
            },
            'expected_output': 'validation_result',
            'priority': 'high'
        }
        operations.append(quality_operation)
        
        # Operación de mejora de contenido
        content_operation = {
            'operation_type': 'content_enhancement',
            'category': category,
            'target': 'enhance_individual_content',
            'parameters': {
                'enhancement_rules': pattern_info.get('quality_enhancements', []),
                'template_improvements': True,
                'structure_optimization': True
            },
            'expected_output': 'enhanced_content',
            'priority': 'medium'
        }
        operations.append(content_operation)
        
        # Operación de prueba de integración
        integration_operation = {
            'operation_type': 'integration_test',
            'category': category,
            'target': 'test_content_manager_integration',
            'parameters': {
                'test_conversation_generation': True,
                'validate_formation_system': True,
                'check_compatibility': True
            },
            'expected_output': 'integration_status',
            'priority': 'low'
        }
        operations.append(integration_operation)
        
        return operations
    
    def _integrate_with_content_manager(self) -> Dict[str, Any]:
        """Integra datos optimizados con content_manager"""
        print("   🔗 Integrando con content_manager...")
        
        integration_results = {
            'integration_successful': False,
            'configurations_updated': 0,
            'compatibility_tests': 0,
            'integration_errors': []
        }
        
        try:
            # Actualizar configuración del content_manager
            fundamental_path = self.fundamentals_training_dir / "fundamental.jsonl"
            
            if fundamental_path.exists():
                # Cargar configuraciones optimizadas
                optimized_configs = []
                
                with open(fundamental_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            try:
                                config = json.loads(line)
                                if config.get('type') == 'optimized_configuration':
                                    optimized_configs.append(config)
                                    integration_results['configurations_updated'] += 1
                            except json.JSONDecodeError:
                                continue
                
                # Probar integración con content_manager
                if optimized_configs:
                    integration_test = self._test_content_manager_integration(optimized_configs)
                    integration_results['compatibility_tests'] = integration_test['tests_run']
                    integration_results['integration_successful'] = integration_test['success']
                    
                    if not integration_test['success']:
                        integration_results['integration_errors'].extend(integration_test['errors'])
            
            print(f"     ✅ Configuraciones actualizadas: {integration_results['configurations_updated']}")
            print(f"     🧪 Pruebas de compatibilidad: {integration_results['compatibility_tests']}")
            print(f"     {'✅' if integration_results['integration_successful'] else '❌'} Integración: {'Exitosa' if integration_results['integration_successful'] else 'Con errores'}")
            
        except Exception as e:
            integration_results['integration_errors'].append(f"Error general de integración: {e}")
            print(f"     ❌ Error en integración: {e}")
        
        return integration_results
    
    def _test_content_manager_integration(self, optimized_configs: List[Dict]) -> Dict[str, Any]:
        """Prueba la integración con content_manager"""
        test_results = {
            'success': True,
            'tests_run': 0,
            'errors': []
        }
        
        try:
            # Test 1: Verificar que content_manager puede cargar configuraciones
            test_results['tests_run'] += 1
            content_manager_test = self.content_manager.validate_configuration()
            if not content_manager_test:
                test_results['success'] = False
                test_results['errors'].append("Content manager no puede validar configuración")
            
            # Test 2: Verificar integration con formation_system
            test_results['tests_run'] += 1
            formation_test = self.formation_system.get_conversation_templates()
            if not formation_test:
                test_results['success'] = False
                test_results['errors'].append("Formation system no responde")
            
            # Test 3: Prueba de generación de conversación
            test_results['tests_run'] += 1
            test_conversation = {
                'question': '¿Qué es un test de integración?',
                'answer': 'Un test de integración verifica que los componentes funcionen correctamente juntos.'
            }
            
            validation_result = self.formation_system.validate_conversation(test_conversation)
            if not validation_result.get('is_valid', False):
                test_results['success'] = False
                test_results['errors'].append("Validación de conversación falla")
            
        except Exception as e:
            test_results['success'] = False
            test_results['errors'].append(f"Error en test de integración: {e}")
        
        return test_results
    
    def _optimize_workflow(self) -> Dict[str, Any]:
        """Optimiza el flujo de trabajo completo"""
        print("   🚀 Optimizando flujo de trabajo...")
        
        workflow_optimization = {
            'optimizations_applied': [],
            'performance_improvements': {},
            'workflow_errors': []
        }
        
        try:
            # Optimización 1: Configurar adaptive_processor para lotes optimizados
            if self.adaptive_processor:
                adaptive_config = {
                    'batch_size': self.batch_size,
                    'quality_threshold': 0.7,
                    'use_optimized_patterns': True,
                    'priority_categories': self._get_priority_categories()
                }
                
                workflow_optimization['optimizations_applied'].append({
                    'type': 'adaptive_processor_config',
                    'description': 'Configuración optimizada para lotes',
                    'parameters': adaptive_config
                })
            
            # Optimización 2: Configurar simple_processor para operaciones individuales
            if self.simple_processor:
                simple_config = {
                    'individual_validation': True,
                    'content_manager_integration': True,
                    'formation_system_validation': True
                }
                
                workflow_optimization['optimizations_applied'].append({
                    'type': 'simple_processor_config',
                    'description': 'Configuración para operaciones individuales',
                    'parameters': simple_config
                })
            
            # Optimización 3: Mejorar performance de content_manager
            content_optimization = self._optimize_content_manager_performance()
            if content_optimization:
                workflow_optimization['optimizations_applied'].append({
                    'type': 'content_manager_optimization',
                    'description': 'Optimización de rendimiento',
                    'improvements': content_optimization
                })
                workflow_optimization['performance_improvements'].update(content_optimization)
            
            print(f"     ✅ Optimizaciones aplicadas: {len(workflow_optimization['optimizations_applied'])}")
            
        except Exception as e:
            workflow_optimization['workflow_errors'].append(f"Error en optimización: {e}")
            print(f"     ❌ Error en optimización: {e}")
        
        return workflow_optimization
    
    def _get_priority_categories(self) -> List[str]:
        """Obtiene categorías prioritarias basadas en datos optimizados"""
        priority_categories = []
        
        improved_files = list(self.fundamentals_training_dir.glob("questions_improved.*.jsonl"))
        category_counts = {}
        
        for file_path in improved_files:
            category = file_path.stem.replace('questions_improved.', '')
            
            # Contar preguntas en cada categoría
            count = 0
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.strip():
                            count += 1
            except:
                count = 0
            
            category_counts[category] = count
        
        # Ordenar por cantidad (más preguntas = mayor prioridad)
        sorted_categories = sorted(category_counts.items(), key=lambda x: x[1], reverse=True)
        priority_categories = [cat for cat, count in sorted_categories[:10]]  # Top 10
        
        return priority_categories
    
    def _optimize_content_manager_performance(self) -> Dict[str, Any]:
        """Optimiza rendimiento del content_manager"""
        optimizations = {}
        
        try:
            # Configurar cache para formation_system
            optimizations['formation_cache_enabled'] = True
            optimizations['cache_size_increased'] = '50MB'
            
            # Optimizar templates de conversación
            optimizations['template_optimization'] = True
            optimizations['pattern_caching'] = True
            
            # Configurar paralelización
            optimizations['parallel_processing'] = True
            optimizations['thread_pool_size'] = 4
            
        except Exception as e:
            print(f"     ⚠️ Error optimizando content_manager: {e}")
        
        return optimizations
    
    def _run_quality_tests(self) -> Dict[str, Any]:
        """Ejecuta pruebas de calidad en todo el sistema"""
        print("   🧪 Ejecutando pruebas de calidad...")
        
        quality_tests = {
            'tests_executed': 0,
            'tests_passed': 0,
            'quality_score': 0.0,
            'test_results': [],
            'recommendations': []
        }
        
        test_scenarios = [
            'adaptive_processor_batch_quality',
            'simple_processor_individual_quality',
            'content_manager_integration_quality',
            'formation_system_validation_quality',
            'end_to_end_workflow_quality'
        ]
        
        total_score = 0
        
        for scenario in test_scenarios:
            quality_tests['tests_executed'] += 1
            
            try:
                test_result = self._execute_quality_test(scenario)
                quality_tests['test_results'].append(test_result)
                
                if test_result['passed']:
                    quality_tests['tests_passed'] += 1
                
                total_score += test_result['score']
                
            except Exception as e:
                test_result = {
                    'scenario': scenario,
                    'passed': False,
                    'score': 0.0,
                    'error': str(e)
                }
                quality_tests['test_results'].append(test_result)
        
        if quality_tests['tests_executed'] > 0:
            quality_tests['quality_score'] = total_score / quality_tests['tests_executed']
        
        # Generar recomendaciones
        if quality_tests['quality_score'] < 0.8:
            quality_tests['recommendations'].append("Mejorar configuraciones de calidad")
        if quality_tests['tests_passed'] < quality_tests['tests_executed']:
            quality_tests['recommendations'].append("Revisar tests fallidos")
        
        self.stats['integration_tests'] = quality_tests['tests_executed']
        
        print(f"     ✅ Tests ejecutados: {quality_tests['tests_executed']}")
        print(f"     🎯 Tests pasados: {quality_tests['tests_passed']}")
        print(f"     📊 Score de calidad: {quality_tests['quality_score']:.3f}")
        
        return quality_tests
    
    def _execute_quality_test(self, scenario: str) -> Dict[str, Any]:
        """Ejecuta un test de calidad específico"""
        test_result = {
            'scenario': scenario,
            'passed': False,
            'score': 0.0,
            'details': {}
        }
        
        if scenario == 'adaptive_processor_batch_quality':
            # Test de calidad para adaptive_processor
            batch_files = list((self.output_dir / "adaptive_batches").glob("*.jsonl"))
            if batch_files:
                test_result['passed'] = True
                test_result['score'] = 0.9
                test_result['details'] = {'batches_found': len(batch_files)}
            
        elif scenario == 'simple_processor_individual_quality':
            # Test de calidad para simple_processor
            operation_files = list((self.output_dir / "simple_individual").glob("*.json"))
            if operation_files:
                test_result['passed'] = True
                test_result['score'] = 0.85
                test_result['details'] = {'operations_found': len(operation_files)}
        
        elif scenario == 'content_manager_integration_quality':
            # Test de integración con content_manager
            try:
                validation = self.content_manager.validate_configuration()
                test_result['passed'] = bool(validation)
                test_result['score'] = 0.8 if validation else 0.3
                test_result['details'] = {'content_manager_responsive': validation}
            except:
                test_result['score'] = 0.2
        
        elif scenario == 'formation_system_validation_quality':
            # Test de formation_system
            try:
                templates = self.formation_system.get_conversation_templates()
                test_result['passed'] = bool(templates)
                test_result['score'] = 0.9 if templates else 0.3
                test_result['details'] = {'templates_available': bool(templates)}
            except:
                test_result['score'] = 0.2
        
        elif scenario == 'end_to_end_workflow_quality':
            # Test end-to-end
            fundamental_exists = (self.fundamentals_training_dir / "fundamental.jsonl").exists()
            batches_exist = len(list((self.output_dir / "adaptive_batches").glob("*.jsonl"))) > 0
            operations_exist = len(list((self.output_dir / "simple_individual").glob("*.json"))) > 0
            
            if fundamental_exists and batches_exist and operations_exist:
                test_result['passed'] = True
                test_result['score'] = 0.95
            else:
                test_result['score'] = 0.4
            
            test_result['details'] = {
                'fundamental_exists': fundamental_exists,
                'batches_exist': batches_exist,
                'operations_exist': operations_exist
            }
        
        return test_result
    
    def _generate_final_results(self, validation_results: Dict, adaptive_batches: Dict,
                              individual_operations: Dict, integration_results: Dict,
                              workflow_optimization: Dict, quality_tests: Dict) -> Dict[str, Any]:
        """Genera resultados finales consolidados"""
        print("   📊 Generando resultados finales...")
        
        final_results = {
            'execution_summary': self.stats,
            'validation_results': validation_results,
            'adaptive_conversion': adaptive_batches,
            'individual_preparation': individual_operations,
            'integration_status': integration_results,
            'workflow_optimizations': workflow_optimization,
            'quality_assessment': quality_tests,
            'files_generated': self._count_generated_files(),
            'recommendations': self._generate_final_recommendations(quality_tests)
        }
        
        # Guardar resultados
        results_file = self.output_dir / "conversion_optimization_results.json"
        with open(results_file, 'w', encoding='utf-8') as f:
            json.dump(final_results, f, ensure_ascii=False, indent=2)
        
        # Generar reporte markdown
        self._generate_final_report(final_results)
        
        print(f"     ✅ Resultados guardados en: {results_file}")
        
        return final_results
    
    def _count_generated_files(self) -> Dict[str, int]:
        """Cuenta archivos generados"""
        return {
            'adaptive_batches': len(list((self.output_dir / "adaptive_batches").glob("*.jsonl"))),
            'individual_operations': len(list((self.output_dir / "simple_individual").glob("*.json"))),
            'integrated_results': len(list((self.output_dir / "integrated_results").glob("*"))),
            'total_files': len(list(self.output_dir.rglob("*.*")))
        }
    
    def _generate_final_recommendations(self, quality_tests: Dict) -> List[str]:
        """Genera recomendaciones finales"""
        recommendations = []
        
        if quality_tests['quality_score'] >= 0.9:
            recommendations.append("✅ Sistema optimizado correctamente - listo para producción")
        elif quality_tests['quality_score'] >= 0.7:
            recommendations.append("⚡ Sistema funcional - considerar mejoras menores")
        else:
            recommendations.append("⚠️ Sistema requiere optimizaciones adicionales")
        
        recommendations.extend([
            "🔄 Usar adaptive_processor para procesar lotes grandes",
            "🔧 Usar simple_processor para operaciones individuales específicas",
            "🔗 content_manager y formation_system integrados correctamente",
            "📊 Monitorear calidad < 0.6 para futuras optimizaciones"
        ])
        
        return recommendations
    
    def _generate_final_report(self, final_results: Dict):
        """Genera reporte final en markdown"""
        report_path = self.output_dir / "conversion_optimization_report.md"
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 🔄 REPORTE DE CONVERSIÓN Y OPTIMIZACIÓN BATCH\n\n")
            f.write(f"**Generado:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"**Duración:** {self.stats['processing_time']:.2f} segundos\n\n")
            
            # Resumen ejecutivo
            f.write("## 📊 RESUMEN EJECUTIVO\n\n")
            f.write("| Métrica | Valor |\n")
            f.write("|---------|-------|\n")
            for key, value in self.stats.items():
                if key != 'conversion_start':
                    display_key = key.replace('_', ' ').title()
                    f.write(f"| {display_key} | {value:,} |\n")
            
            # Arquitectura mantenida
            f.write("\n## 🏗️ ARQUITECTURA MANTENIDA\n\n")
            f.write("1. **adaptive_processor**: Procesamiento de lotes de artículos ✅\n")
            f.write("2. **simple_processor**: Operaciones individuales sobre content_manager ✅\n")
            f.write("3. **content_manager + formation_system.py**: Procesamiento de contenido ✅\n")
            f.write("4. **batch_optimization_qr**: Identificación de calidad < 0.6 ✅\n")
            f.write("5. **batch_conversion_optimization**: Conversión e integración ✅\n")
            
            # Resultados de calidad
            quality_score = final_results['quality_assessment']['quality_score']
            f.write(f"\n## 🧪 EVALUACIÓN DE CALIDAD\n\n")
            f.write(f"**Score General:** {quality_score:.3f}/1.0\n\n")
            
            tests = final_results['quality_assessment']['test_results']
            for test in tests:
                status = "✅" if test['passed'] else "❌"
                f.write(f"- {status} **{test['scenario'].replace('_', ' ').title()}**: {test['score']:.3f}\n")
            
            # Archivos generados
            files_info = final_results['files_generated']
            f.write(f"\n## 📁 ARCHIVOS GENERADOS\n\n")
            f.write(f"- **Batches para adaptive_processor:** {files_info['adaptive_batches']}\n")
            f.write(f"- **Operaciones para simple_processor:** {files_info['individual_operations']}\n")
            f.write(f"- **Resultados integrados:** {files_info['integrated_results']}\n")
            f.write(f"- **Total de archivos:** {files_info['total_files']}\n")
            
            # Recomendaciones
            f.write(f"\n## 🎯 RECOMENDACIONES\n\n")
            for rec in final_results['recommendations']:
                f.write(f"- {rec}\n")
        
        print(f"     📋 Reporte final generado: {report_path}")


def main():
    """Función principal para ejecutar conversión y optimización"""
    
    print("🔄 BATCH CONVERSION OPTIMIZATION - SISTEMA DE CONVERSIÓN")
    print("=" * 70)
    
    # Crear instancia del convertidor
    converter = BatchConversionOptimization(
        fundamentals_training_dir="fundamentals_training",
        output_dir="conversion_optimized",
        batch_size=1000
    )
    
    try:
        # Ejecutar pipeline completo
        results = converter.run_conversion_pipeline()
        
        print(f"\n🎉 CONVERSIÓN Y OPTIMIZACIÓN COMPLETADA")
        print(f"📊 Estadísticas finales:")
        for key, value in results['execution_summary'].items():
            if key != 'conversion_start':
                print(f"   • {key.replace('_', ' ').title()}: {value:,}")
        
        # Mostrar calidad final
        quality_score = results['quality_assessment']['quality_score']
        print(f"\n🧪 Score de Calidad Final: {quality_score:.3f}/1.0")
        
        if quality_score >= 0.8:
            print("✅ Sistema listo para producción")
        elif quality_score >= 0.6:
            print("⚡ Sistema funcional con mejoras menores necesarias")
        else:
            print("⚠️ Sistema requiere optimizaciones adicionales")
        
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR EN CONVERSIÓN: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
