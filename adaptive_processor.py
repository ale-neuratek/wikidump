#!/usr/bin/env python3
"""
🧠 ADAPTIVE DATASET PROCESSOR - Coordinador de procesamiento optimizado
======================================================================
Responsabilidades:
- Estimación y análisis del dataset
- Configuración adaptativa de hardware
- Optimización de parámetros según tamaño
- Coordinación del procesamiento
- Logging detallado con timestamps
- Generación de categoría consciencia con reconocimiento temporal
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime
from hardware_configs import get_hardware_config, optimize_for_queue_issues, diagnose_dataset_configuration

class AdaptiveProcessor:
    """Procesador adaptativo que optimiza automáticamente según el dataset"""
    
    def __init__(self):
        self.start_time = time.time()
        self.log_file = "adaptive_processing.log"
        self.log_interval = 1 * 60  # 1 minuto para más seguimiento
        self.last_log_time = time.time()
        
        # Limpiar log anterior
        if Path(self.log_file).exists():
            Path(self.log_file).unlink()
        
    def log(self, message: str, force: bool = False):
        """Log con timestamp si ha pasado el intervalo o es forzado"""
        current_time = time.time()
        
        if force or (current_time - self.last_log_time) >= self.log_interval:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            elapsed = (current_time - self.start_time) / 3600  # horas
            
            log_entry = f"[{timestamp}] T+{elapsed:.1f}h: {message}"
            
            # Log a archivo
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry + '\n')
            
            # Log a consola
            print(log_entry)
            
            if not force:
                self.last_log_time = current_time

    def estimate_dataset_size(self, input_dir: str) -> dict:
        """Estima el tamaño y características del dataset"""
        input_path = Path(input_dir)
        files = list(input_path.glob("*.jsonl"))
        
        if not files:
            return {'total_articles': 0, 'total_files': 0, 'total_size_gb': 0}
        
        self.log(f"📂 ANÁLISIS DEL DATASET INICIADO", force=True)
        self.log(f"   📁 Directorio: {input_dir}", force=True)
        self.log(f"   📄 Archivos encontrados: {len(files)}", force=True)
        
        total_articles = 0
        total_size_bytes = 0
        sample_files = min(3, len(files))
        
        for i, file_path in enumerate(files[:sample_files]):
            try:
                file_size = file_path.stat().st_size
                total_size_bytes += file_size
                
                sample_count = 0
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f):
                        if line.strip():
                            sample_count += 1
                        if line_num >= 999:  # Muestra de 1000 líneas
                            break
                
                # Extrapolar para el archivo completo
                if sample_count > 0:
                    lines_per_byte = sample_count / (file_size * min(1000, line_num + 1) / 1000)
                    estimated_in_file = int(file_size * lines_per_byte)
                    total_articles += estimated_in_file
                    
                    file_size_mb = file_size / (1024 * 1024)
                    self.log(f"   📄 {file_path.name}: ~{estimated_in_file:,} artículos ({file_size_mb:.1f}MB)", force=True)
            
            except Exception as e:
                self.log(f"   ⚠️ Error estimando {file_path.name}: {e}", force=True)
                # Estimación por defecto
                file_size_mb = file_path.stat().st_size / (1024 * 1024)
                estimated_in_file = int(file_size_mb * 400)  # ~400 artículos por MB
                total_articles += estimated_in_file
        
        # Extrapolar para todos los archivos
        if sample_files > 0:
            avg_articles_per_file = total_articles / sample_files
            avg_size_per_file = total_size_bytes / sample_files
            
            total_articles = int(avg_articles_per_file * len(files))
            total_size_bytes = int(avg_size_per_file * len(files))
        
        total_size_gb = total_size_bytes / (1024 ** 3)
        
        self.log(f"📊 ESTIMACIÓN COMPLETADA:", force=True)
        self.log(f"   🎯 Total estimado: {total_articles:,} artículos", force=True)
        self.log(f"   📏 Tamaño total: {total_size_gb:.2f} GB", force=True)
        self.log(f"   📁 Archivos totales: {len(files)}", force=True)
        
        return {
            'total_articles': total_articles,
            'total_files': len(files),
            'total_size_gb': total_size_gb
        }

    def get_optimal_config(self, dataset_info: dict) -> dict:
        """Determina la configuración óptima según el dataset"""
        total_articles = dataset_info['total_articles']
        total_size_gb = dataset_info['total_size_gb']
        
        self.log(f"⚙️ DETERMINANDO CONFIGURACIÓN ÓPTIMA", force=True)
        
        # Configuración base usando hardware_configs
        hardware_config = get_hardware_config()
        
        # Crear configuración base con workers limitados según el tamaño
        if total_articles > 100000:  # Datasets muy grandes
            base_workers = min(hardware_config['MAX_WORKERS'], 12)
            timeout_multiplier = 2.0
            self.log(f"   🎯 Configuración para dataset GRANDE seleccionada", force=True)
        elif total_articles > 50000:  # Datasets medianos
            base_workers = min(hardware_config['MAX_WORKERS'], 8)
            timeout_multiplier = 1.5
            self.log(f"   🎯 Configuración para dataset MEDIANO seleccionada", force=True)
        else:  # Datasets pequeños
            base_workers = min(hardware_config['MAX_WORKERS'], 6)
            timeout_multiplier = 1.0
            self.log(f"   🎯 Configuración para dataset PEQUEÑO seleccionada", force=True)
        
        # Crear configuración inicial
        config = {
            'workers': base_workers,
            'batch_size': hardware_config['BATCH_SIZE'],
            'timeout': int(hardware_config['WORKER_TIMEOUT'] * 1000 * timeout_multiplier),  # milisegundos
            'queue_size': hardware_config['QUEUE_SIZE'],
            'memory_buffer_gb': hardware_config['MEMORY_BUFFER_GB']
        }
        
        # Aplicar optimizaciones para colas si es necesario
        if total_articles > 100000:
            config = optimize_for_queue_issues(hardware_config, total_articles)
            # Extraer valores para usar en el procesador
            workers = min(config['MAX_WORKERS'], base_workers)
            batch_size = config['BATCH_SIZE']
            timeout = int(config.get('WORKER_TIMEOUT', 1.0) * 1000 * timeout_multiplier)
        else:
            # Usar configuración simple para datasets pequeños
            workers = base_workers
            batch_size = min(config['batch_size'], 100 if total_size_gb > 2.0 else 200)
            timeout = config['timeout']
            self.log(f"   🎯 Configuración para dataset PEQUEÑO seleccionada", force=True)
        
        # Ajustar batch_size según el tamaño
        if total_size_gb > 5.0:
            batch_size = min(batch_size, 50)
        elif total_size_gb > 2.0:
            batch_size = min(batch_size, 100)
        
        self.log(f"   👥 Workers: {workers}", force=True)
        self.log(f"   📦 Batch size: {batch_size}", force=True)
        self.log(f"   ⏱️ Timeout: {timeout}s", force=True)
        
        return {
            'workers': workers,
            'batch_size': batch_size,
            'timeout': timeout
        }

    def process_dataset(self, input_dir: str, output_dir: str) -> dict:
        """Procesa el dataset con configuración adaptativa"""
        
        # 1. Análisis del dataset
        dataset_info = self.estimate_dataset_size(input_dir)
        if dataset_info['total_articles'] == 0:
            self.log("❌ No se encontraron artículos para procesar", force=True)
            return {'success': False, 'error': 'No articles found'}
        
        # 2. Configuración óptima
        config = self.get_optimal_config(dataset_info)
        
        # 3. Diagnóstico inicial
        self.log("🔍 EJECUTANDO DIAGNÓSTICO INICIAL", force=True)
        diagnosis = diagnose_dataset_configuration(dataset_info['total_articles'])
        
        for issue in diagnosis.get('potential_issues', []):
            self.log(f"   ⚠️ {issue}", force=True)
        
        for rec in diagnosis.get('recommendations', []):
            self.log(f"   💡 {rec}", force=True)
        
        # 4. Procesamiento principal
        self.log("🚀 INICIANDO PROCESAMIENTO PRINCIPAL", force=True)
        
        try:
            from simple_processor import MassiveParallelDatasetProcessor
            
            # Crear configuración adaptativa para el procesador
            adaptive_config = {
                'CATEGORY_WORKERS': config['workers'] // 3,
                'CONVERSATION_WORKERS': config['workers'] // 3,
                'OUTPUT_WORKERS': config['workers'] // 3,
                'BATCH_SIZE': config['batch_size'],
                'QUEUE_SIZE': 1000,
                'CONVERSATIONS_PER_OUTPUT_FILE': 50000,
                'AUTO_FLUSH_THRESHOLD': 25000,
                'MAX_QUEUE_RETRIES': 30,
                'QUEUE_TIMEOUT': 5.0,
                'LOG_FILE': 'processing.log'
            }
            
            processor = MassiveParallelDatasetProcessor(
                input_dir=input_dir,
                output_dir=output_dir,
                adaptive_config=adaptive_config
            )
            
            success = processor.process_all_files()
            
            if success:
                # Obtener estadísticas del procesador
                stats = processor.stats
                categories_found = list(processor.discovered_categories) if hasattr(processor, 'discovered_categories') else []
                
                result = {
                    'success': True,
                    'articles_processed': stats.get('articles_processed', 0),
                    'conversations_generated': stats.get('conversations_generated', 0),
                    'categories_found': categories_found,
                    'total_time': stats.get('total_time', 0)
                }
            else:
                result = {'success': False, 'error': 'Processing failed'}
            
            if result['success']:
                self.log("✅ PROCESAMIENTO COMPLETADO EXITOSAMENTE", force=True)
                self.log(f"   📊 Artículos procesados: {result.get('articles_processed', 0):,}", force=True)
                self.log(f"   💬 Conversaciones generadas: {result.get('conversations_generated', 0):,}", force=True)
                self.log(f"   ⏱️ Tiempo total: {result.get('total_time', 0):.1f}s", force=True)
                
                # 5. Generar categoría consciencia si hay categorías disponibles
                categories_found = result.get('categories_found', [])
                total_articles = result.get('articles_processed', 0)
                
                self.log(f"🏷️ CATEGORÍAS ENCONTRADAS: {len(categories_found)} categorías", force=True)
                if categories_found:
                    self.log(f"   📝 Categorías: {', '.join(categories_found[:15])}", force=True)
                    self.generate_consciencia_category(categories_found, output_dir, total_articles)
                else:
                    self.log("   ⚠️ No se encontraron categorías para generar consciencia", force=True)
                
                self.log("🎯 PROCESAMIENTO COMPLETO FINALIZADO EXITOSAMENTE", force=True)
                
            else:
                self.log(f"❌ PROCESAMIENTO FALLÓ: {result.get('error', 'Unknown error')}", force=True)
            
            return result
            
        except Exception as e:
            error_msg = f"Error durante el procesamiento: {str(e)}"
            self.log(f"❌ ERROR CRÍTICO: {error_msg}", force=True)
            import traceback
            self.log(f"📋 TRACEBACK: {traceback.format_exc()}", force=True)
            return {'success': False, 'error': error_msg}

    def generate_consciencia_category(self, categories_found: list, output_dir: str, total_articles: int = 0):
        """Genera la categoría consciencia usando ContentManager con mejoras temporales"""
        
        self.log(f"🧠 GENERANDO CATEGORÍA CONSCIENCIA", force=True)
        self.log(f"   🏷️ Categorías encontradas: {len(categories_found)}", force=True)
        self.log(f"   📊 Total de artículos procesados: {total_articles:,}", force=True)
        
        try:
            # Importar content_manager y delegarle la responsabilidad
            from content_manager import ContentManager
            content_manager = ContentManager()
            
            # Generar categoría consciencia con identificación temporal mejorada
            result = content_manager.generate_consciencia_category(categories_found, output_dir, total_articles)
            
            self.log(f"🧠 Categoría consciencia completada:", force=True)
            self.log(f"   📝 {result['total_conversations']} conversaciones generadas", force=True)
            self.log(f"   📁 {result['total_files']} archivos JSONL creados", force=True)
            self.log(f"   🏷️ {result['categories_described']} categorías descritas", force=True)
            self.log(f"   🕒 Con identificación temporal mejorada (es/fue)", force=True)
            
        except Exception as e:
            self.log(f"❌ ERROR generando consciencia: {str(e)}", force=True)
            import traceback
            self.log(f"📋 TRACEBACK: {traceback.format_exc()}", force=True)

def main():
    """Función principal para usar el procesador adaptativo"""
    if len(sys.argv) != 3:
        print("Uso: python adaptive_processor.py <directorio_input> <directorio_output>")
        print("Ejemplo: python adaptive_processor.py data_test_small wiki_conversations_adaptive")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    
    print("🧠 ADAPTIVE DATASET PROCESSOR")
    print("=" * 50)
    print(f"📁 Input: {input_dir}")
    print(f"📁 Output: {output_dir}")
    print("=" * 50)
    
    processor = AdaptiveProcessor()
    result = processor.process_dataset(input_dir, output_dir)
    
    if result['success']:
        print("\n🎉 PROCESAMIENTO COMPLETADO EXITOSAMENTE")
        exit_code = 0
    else:
        print(f"\n❌ PROCESAMIENTO FALLÓ: {result.get('error', 'Unknown error')}")
        exit_code = 1
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()