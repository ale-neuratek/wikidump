#!/usr/bin/env python3
"""
🧠 ADAPTIVE DATASET PROCESSOR - Coordinador de procesamiento optimizado
======================================================================
Responsabilidades:
- Estimación y análisis del dataset
- Configuración adaptativa de hardware
- Optimización de parámetros según tamaño
- Coordinación del procesamiento
- Logging detallado con timestamps cada 5-10 minutos
- Generación de categoría "consciencia" al final (NO conscious.txt)
- Todos los artículos procesados como conversaciones via content_manager
"""

import sys
import os
import json
import time
from pathlib import Path
from datetime import datetime
from hardware_configs import get_hardware_config

class AdaptiveProcessor:
    """Procesador adaptativo que optimiza automáticamente según el dataset"""
    
    def __init__(self):
        self.start_time = time.time()
        self.log_file = "adaptive_processing.log"
        self.log_interval = 5 * 60  # 5 minutos
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
        if len(files) > sample_files:
            # Calcular tamaño total de todos los archivos
            total_size_bytes = sum(f.stat().st_size for f in files)
            avg_per_file = total_articles / sample_files
            total_articles = int(avg_per_file * len(files))
        
        total_size_gb = total_size_bytes / (1024**3)
        
        dataset_info = {
            'total_articles': total_articles,
            'total_files': len(files),
            'total_size_gb': total_size_gb,
            'total_size_bytes': total_size_bytes
        }
        
        self.log(f"📊 ANÁLISIS COMPLETO: {total_articles:,} artículos en {len(files)} archivos ({total_size_gb:.1f}GB)", force=True)
        return dataset_info
    
    def get_optimized_config(self, dataset_info: dict) -> dict:
        """Obtiene configuración optimizada según el dataset"""
        total_articles = dataset_info['total_articles']
        
        # Obtener configuración base del hardware
        base_config = get_hardware_config(dataset_size_articles=total_articles)
        
        # Configuraciones específicas para el procesamiento
        optimized_config = {
            **base_config,
            
            # Configuración de logging
            'LOG_INTERVAL_MINUTES': 5,
            'LOG_FILE': self.log_file,
            
            # Configuración de categoría consciencia
            'INCLUDE_CONSCIENCIA_CATEGORY': True,
            'CONSCIENCIA_CONVERSATIONS_PER_CATEGORY': 2,  # 2 conversaciones por categoría encontrada
            
            # Configuración de procesamiento
            'PROCESS_ALL_ARTICLES': True,
            'CONVERSATIONS_PER_ARTICLE': 1,  # 1 conversación principal por artículo
            
            # Configuración de archivos de salida
            'CONVERSATIONS_PER_OUTPUT_FILE': 50000,  # 50K conversaciones por archivo
            'REMOVE_CONSCIOUS_TXT': True,  # NO generar conscious.txt
            
            # Información del dataset para el procesador
            'DATASET_INFO': dataset_info
        }
        
        self.log(f"⚙️ CONFIGURACIÓN OPTIMIZADA:", force=True)
        self.log(f"   🔄 Workers: {optimized_config.get('MAX_WORKERS', 'N/A')}", force=True)
        self.log(f"   📦 Batch size: {optimized_config.get('BATCH_SIZE', 'N/A'):,}", force=True)
        self.log(f"   🗂️ Queue size: {optimized_config.get('QUEUE_SIZE', 'N/A'):,}", force=True)
        self.log(f"   💾 Memory buffer: {optimized_config.get('MEMORY_BUFFER_GB', 'N/A')}GB", force=True)
        
        return optimized_config

    def run_processing(self, input_dir: str, output_dir: str) -> int:
        """Ejecuta el procesamiento completo con configuración optimizada"""
        
        self.log(f"🚀 PROCESAMIENTO ADAPTATIVO INICIADO", force=True)
        self.log(f"   📂 Input: {input_dir}", force=True)
        self.log(f"   📁 Output: {output_dir}", force=True)
        
        # Estimar dataset
        dataset_info = self.estimate_dataset_size(input_dir)
        if dataset_info['total_articles'] == 0:
            self.log(f"❌ No se encontraron artículos en {input_dir}", force=True)
            return 1
        
        # Obtener configuración optimizada
        config = self.get_optimized_config(dataset_info)
        
        # Importar el procesador simplificado
        from simple_processor import MassiveParallelDatasetProcessor
        
        try:
            # Log detalles del inicio
            input_files = list(Path(input_dir).glob("*.jsonl"))
            self.log(f"📋 INICIANDO PROCESAMIENTO DE {len(input_files)} ARCHIVOS", force=True)
            self.log(f"   📦 Lotes configurados: {config.get('BATCH_SIZE', 'N/A'):,} artículos por lote", force=True)
            self.log(f"   ⚙️ Workers: {config.get('MAX_WORKERS', 'N/A')}", force=True)
            self.log(f"   🚫 NO generar conscious.txt - solo categoría consciencia", force=True)
            
            # Crear procesador con configuración optimizada
            processor = MassiveParallelDatasetProcessor(input_dir, output_dir, adaptive_config=config)
            
            # Ejecutar procesamiento principal con logging periódico
            self.log(f"🏁 INICIANDO PROCESAMIENTO PRINCIPAL", force=True)
            success = processor.process_all_files()
            
            if success:
                self.log(f"✅ PROCESAMIENTO PRINCIPAL COMPLETADO", force=True)
                
                # Obtener categorías encontradas del procesador
                categories_found = []
                if hasattr(processor, 'categories_stats'):
                    categories_found = list(processor.categories_stats.keys())
                elif hasattr(processor, 'content_manager'):
                    categories_found = processor.content_manager.get_categories()
                else:
                    categories_found = ['arte', 'geografia', 'historia', 'ciencias', 'biologia', 'tecnologia', 'deportes', 'general']
                
                self.log(f"🏷️ CATEGORÍAS ENCONTRADAS: {len(categories_found)} categorías", force=True)
                self.log(f"   📝 Categorías: {', '.join(categories_found[:10])}", force=True)
                
                # Generar categoría consciencia al final usando content_manager
                self.generate_consciencia_category(categories_found, output_dir)
                
                self.log(f"🎯 PROCESAMIENTO COMPLETO FINALIZADO EXITOSAMENTE", force=True)
                return 0
            else:
                self.log(f"❌ PROCESAMIENTO PRINCIPAL FALLÓ", force=True)
                return 1
                
        except Exception as e:
            self.log(f"💥 ERROR DURANTE PROCESAMIENTO: {e}", force=True)
            import traceback
            traceback.print_exc()
            return 1

    def generate_consciencia_category(self, categories_found: list, output_dir: str):
        """Genera la categoría consciencia al final con conversaciones sobre categorías encontradas"""
        
        self.log(f"🧠 GENERANDO CATEGORÍA CONSCIENCIA", force=True)
        self.log(f"   🏷️ Categorías encontradas: {len(categories_found)}", force=True)
        
        # Importar content_manager para generar conversaciones
        from content_manager import ContentManager
        content_manager = ContentManager()
        
        # Crear directorio consciencia
        consciencia_dir = Path(output_dir) / "consciencia"
        consciencia_dir.mkdir(parents=True, exist_ok=True)
        
        # Crear conversaciones sobre conocimiento y categorías
        consciencia_conversations = []
        
        # 1. Conversación principal sobre el conocimiento disponible
        main_article = {
            'title': 'Sistema de Conocimiento',
            'content': f'''Este sistema contiene conocimiento organizado en {len(categories_found)} categorías principales. 
            El conocimiento está estructurado para permitir consultas específicas sobre diferentes temas. 
            Las categorías disponibles incluyen: {", ".join(categories_found[:10])}. 
            Cada categoría contiene artículos especializados con información detallada y conversaciones generadas 
            para facilitar el aprendizaje interactivo. El sistema está diseñado para proporcionar respuestas 
            contextuales y permitir exploración profunda de cualquier tema de interés.'''
        }
        
        main_result = content_manager.process_article(main_article)
        if main_result:
            for conv in main_result['conversations']:
                consciencia_conversations.append({
                    'question': conv['question'],
                    'answer': conv['answer'],
                    'category': 'consciencia',
                    'subcategory': 'conocimiento_general',
                    'conversation_type': 'descripcion_sistema'
                })
        
        # 2. Conversaciones sobre capacidades del sistema
        capabilities_article = {
            'title': 'Capacidades del Sistema de Conocimiento',
            'content': f'''Las capacidades de este sistema incluyen: análisis de {len(categories_found)} categorías temáticas, 
            generación de conversaciones contextuales, categorización inteligente de contenidos, 
            respuestas especializadas por área de conocimiento, y exploración interactiva de temas. 
            El sistema puede responder preguntas específicas sobre cualquier categoría, 
            proporcionar definiciones, explicaciones históricas, datos geográficos, información científica, 
            y análisis temáticos profundos. La información está constantemente organizada y accesible.'''
        }
        
        capabilities_result = content_manager.process_article(capabilities_article)
        if capabilities_result:
            for conv in capabilities_result['conversations']:
                consciencia_conversations.append({
                    'question': conv['question'],
                    'answer': conv['answer'],
                    'category': 'consciencia', 
                    'subcategory': 'capacidades_sistema',
                    'conversation_type': 'descripcion_capacidades'
                })
        
        # 3. Conversaciones específicas sobre cada categoría encontrada (máximo 2 por categoría)
        for i, category in enumerate(categories_found[:15]):  # Limitar a 15 categorías principales
            category_article = {
                'title': f'Categoría {category.title()}',
                'content': f'''La categoría {category} contiene información especializada sobre este tema. 
                Esta área del conocimiento incluye definiciones, conceptos clave, ejemplos relevantes, 
                y análisis detallados. Los usuarios pueden hacer preguntas específicas sobre {category} 
                y recibir respuestas contextuales basadas en el contenido disponible. 
                Esta categoría forma parte del sistema de conocimiento integral y está conectada 
                con otras áreas temáticas para proporcionar una comprensión completa.'''
            }
            
            category_result = content_manager.process_article(category_article)
            if category_result:
                # Solo tomar las primeras 2 conversaciones
                for conv in category_result['conversations'][:2]:
                    consciencia_conversations.append({
                        'question': conv['question'],
                        'answer': conv['answer'],
                        'category': 'consciencia',
                        'subcategory': f'categoria_{category}',
                        'conversation_type': 'descripcion_categoria'
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
                    # Formato de conversación estándar usando content_manager
                    conversation_record = {
                        'conversations': [
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
            'description': 'Conversaciones sobre el conocimiento disponible, capacidades del sistema, y descripciones de categorías',
            'generation_date': datetime.now().isoformat(),
            'conversation_types': ['descripcion_sistema', 'descripcion_capacidades', 'descripcion_categoria'],
            'note': 'Esta categoría NO reemplaza conscious.txt - es una categoría de conversaciones como las demás'
        }
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        self.log(f"🧠 Categoría consciencia completada:", force=True)
        self.log(f"   📝 {len(consciencia_conversations)} conversaciones generadas", force=True)
        self.log(f"   📁 {file_counter} archivos JSONL creados", force=True)
        self.log(f"   🏷️ {len(categories_found)} categorías descritas", force=True)

def main():
    """Función principal del procesador adaptativo"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Adaptive Dataset Processor")
    parser.add_argument("--input", default="data_ultra_hybrid", help="Directorio de entrada")
    parser.add_argument("--output", default="consciencia_completa", help="Directorio de salida")
    
    args = parser.parse_args()
    
    # Crear procesador adaptativo
    processor = AdaptiveProcessor()
    
    processor.log("🧠 ADAPTIVE DATASET PROCESSOR INICIADO", force=True)
    processor.log("=" * 50, force=True)
    
    # Verificar entrada
    if not Path(args.input).exists():
        processor.log(f"❌ Directorio de entrada no encontrado: {args.input}", force=True)
        return 1
    
    try:
        # Ejecutar procesamiento completo
        result = processor.run_processing(args.input, args.output)
        
        elapsed = (time.time() - processor.start_time) / 3600
        processor.log(f"⏱️ PROCESAMIENTO TOTAL: {elapsed:.2f} horas", force=True)
        
        return result
        
    except Exception as e:
        processor.log(f"❌ Error: {e}", force=True)
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
