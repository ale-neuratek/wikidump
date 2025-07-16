#!/usr/bin/env python3
"""
🚀 PROCESADOR MASIVO OPTIMIZADO - Simple y directo
=================================================
Solo procesamiento - toda la optimización la maneja adaptive_processor.py
"""

import os
import sys
import json
import time
import queue
import signal
import hashlib
import traceback
import threading
import gc
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Iterator
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# Importar módulos locales
from content_manager import ContentManager

class ProcessingLogger:
    """Logger especializado con timestamps para seguimiento del procesamiento"""
    
    def __init__(self, log_file: str = "processing.log"):
        self.log_file = Path(log_file)
        self.last_log_time = time.time()
        self.start_time = time.time()
        self.log_interval = 2 * 60  # 2 minutos para más seguimiento
        self.last_progress_time = time.time()
        self.progress_interval = 1 * 60  # Log de progreso cada 1 minuto
        
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
    
    def log_progress(self, message: str, force: bool = False):
        """Log de progreso más frecuente"""
        current_time = time.time()
        
        if force or (current_time - self.last_progress_time) >= self.progress_interval:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            elapsed = (current_time - self.start_time) / 3600  # horas
            
            log_entry = f"[{timestamp}] T+{elapsed:.1f}h: 📈 {message}"
            
            # Log a archivo
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry + '\n')
            
            # Log a consola
            print(log_entry)
            
            if not force:
                self.last_progress_time = current_time
    
    def log_batch_progress(self, batch_num: int, total_articles: int, total_conversations: int, queue_status: str, force: bool = False):
        """Log específico para progreso de batches"""
        self.log(f"Batch #{batch_num:,} | Artículos: {total_articles:,} | Conversaciones: {total_conversations:,} | Colas: {queue_status}", force=force)
    
    def log_file_progress(self, file_num: int, total_files: int, file_name: str, articles_in_file: int, force: bool = False):
        """Log específico para progreso de archivos"""
        self.log(f"Archivo {file_num}/{total_files}: {file_name} ({articles_in_file:,} artículos)", force=force)

class MassiveParallelDatasetProcessor:
    """Procesador masivo paralelo simplificado - solo procesamiento"""
    
    def __init__(self, input_dir: str, output_dir: str = "consciencia_completa", adaptive_config: Dict = None):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Usar configuración adaptativa proporcionada
        self.config = adaptive_config or {
            'CATEGORY_WORKERS': 8,
            'CONVERSATION_WORKERS': 16,
            'OUTPUT_WORKERS': 8,
            'BATCH_SIZE': 50000,
            'QUEUE_SIZE': 1000,
            'CONVERSATIONS_PER_OUTPUT_FILE': 50000,
            'AUTO_FLUSH_THRESHOLD': 25000,
            'MAX_QUEUE_RETRIES': 30,
            'QUEUE_TIMEOUT': 5.0,
            'LOG_FILE': 'processing.log'
        }
        
        # Inicializar logger desde la configuración
        self.logger = ProcessingLogger(self.config.get('LOG_FILE', 'processing.log'))
        self.logger.log("🧠 PROCESADOR MASIVO INICIADO", force=True)
        
        # Inicializar gestor de contenido
        self.content_manager = ContentManager()
        
        # Crear estructura de carpetas
        self.categories_dir = self.output_dir / "categorias"
        self.stats_dir = self.output_dir / "estadisticas"
        self.categories_dir.mkdir(exist_ok=True)
        self.stats_dir.mkdir(exist_ok=True)
        
        # Descubrir archivos de entrada
        self.hybrid_files = self._discover_hybrid_files()
        
        # Workers y colas desde configuración adaptativa
        self.category_workers = self.config.get('CATEGORY_WORKERS', 8)
        self.conversation_workers = self.config.get('CONVERSATION_WORKERS', 16)
        self.output_workers = self.config.get('OUTPUT_WORKERS', 8)
        
        # Thread pools
        self.category_pool = ThreadPoolExecutor(max_workers=self.category_workers, thread_name_prefix="category")
        self.conversation_pool = ThreadPoolExecutor(max_workers=self.conversation_workers, thread_name_prefix="conversation")
        self.output_pool = ThreadPoolExecutor(max_workers=self.output_workers, thread_name_prefix="output")
        
        # Colas
        queue_size = self.config.get('QUEUE_SIZE', 1000)
        self.article_queue = queue.Queue(maxsize=queue_size)
        
        # Categorías dinámicas (se descubren durante procesamiento)
        self.discovered_categories = set()
        self.category_queues = {}
        self.category_dirs = {}
        self.category_buffers = defaultdict(lambda: defaultdict(list))
        self.category_counters = defaultdict(int)
        self.file_counters = defaultdict(int)
        
        # Estado
        self.running = True
        self.stats = {
            'articles_processed': 0,
            'articles_categorized': 0,
            'conversations_generated': 0,
            'files_written': 0,
            'processing_errors': 0,
            'start_time': time.time()
        }
        
        self.logger.log(f"   📂 Input: {self.input_dir}", force=True)
        self.logger.log(f"   📁 Output: {self.output_dir}", force=True)
        self.logger.log(f"   📄 Archivos encontrados: {len(self.hybrid_files)}", force=True)
        self.logger.log(f"   🔄 Workers: Cat({self.category_workers}) Conv({self.conversation_workers}) Out({self.output_workers})", force=True)
    
    def _discover_hybrid_files(self) -> List[Path]:
        """Descubre archivos de entrada"""
        hybrid_files = []
        
        if self.input_dir.exists():
            files = list(self.input_dir.glob("*.jsonl"))
            hybrid_files.extend(files)
        
        # Buscar también en directorio actual como fallback
        if not hybrid_files:
            files = list(Path(".").glob("articles_hybrid_*.jsonl"))
            hybrid_files.extend(files)
        
        unique_files = list(set(hybrid_files))
        unique_files.sort()
        
        return unique_files
    
    def _ensure_category_setup(self, category: str):
        """Asegura que una categoría esté configurada (colas, directorios, etc.)"""
        if category not in self.discovered_categories:
            # Crear directorio
            category_dir = self.categories_dir / category
            category_dir.mkdir(exist_ok=True)
            self.category_dirs[category] = category_dir
            
            # Crear cola
            queue_size = self.config.get('QUEUE_SIZE', 1000)
            self.category_queues[category] = queue.Queue(maxsize=queue_size)
            
            # Registrar categoría
            self.discovered_categories.add(category)
            
            self.logger.log(f"✨ Nueva categoría descubierta: {category}")
    
    def start_workers(self):
        """Inicia pools de workers especializados"""
        self.logger.log(f"🚀 Iniciando workers especializados...", force=True)
        
        # Workers de categorización
        for i in range(self.category_workers):
            self.category_pool.submit(self._category_worker, i)
        
        # Workers de procesamiento de conversaciones
        for i in range(self.conversation_workers):
            self.conversation_pool.submit(self._conversation_worker_universal, i)
        
        # Workers de escritura
        for i in range(self.output_workers):
            self.output_pool.submit(self._output_worker_universal, i)
        
        total_workers = self.category_workers + self.conversation_workers + self.output_workers
        self.logger.log(f"✅ {total_workers} workers activos", force=True)
    
    def _category_worker(self, worker_id: int):
        """Worker de categorización que distribuye artículos"""
        articles_this_worker = 0
        last_log_time = time.time()
        
        while self.running:
            try:
                article_batch = self.article_queue.get(timeout=0.1)
                if article_batch is None:
                    break
                
                # Procesar batch de artículos
                for article in article_batch:
                    if not self.running:
                        break
                        
                    # Procesar artículo usando ContentManager
                    result = self.content_manager.process_article(article)
                    
                    if result:
                        category = result['category']
                        
                        # Asegurar que la categoría esté configurada
                        self._ensure_category_setup(category)
                        
                        # Preparar artículo procesado
                        enhanced_article = {
                            **article,
                            'category': category,
                            'subcategory': result['subcategory'],
                            'confidence': result['confidence'],
                            'conversations': result['conversations']
                        }
                        
                        # Enviar a cola de categoría
                        try:
                            self.category_queues[category].put_nowait(enhanced_article)
                            self.stats['articles_categorized'] += 1
                        except queue.Full:
                            # Si la cola está llena, usar categoría general
                            self._ensure_category_setup('general')
                            try:
                                enhanced_article['category'] = 'general'
                                self.category_queues['general'].put_nowait(enhanced_article)
                            except queue.Full:
                                pass  # Skip si todas las colas están llenas
                    
                    self.stats['articles_processed'] += 1
                    articles_this_worker += 1
                
                self.article_queue.task_done()
                
                # Log progreso cada 60 segundos por worker
                current_time = time.time()
                if current_time - last_log_time >= 60:
                    self.logger.log_progress(f"Worker-{worker_id}: {articles_this_worker:,} artículos procesados, {len(self.discovered_categories)} categorías")
                    last_log_time = current_time
                
            except queue.Empty:
                if not self.running:
                    break
                continue
            except Exception as e:
                if self.running:
                    self.logger.log(f"⚠️ Error categoría worker {worker_id}: {e}")
                if not self.running:
                    break
    
    def _conversation_worker_universal(self, worker_id: int):
        """Worker universal de conversaciones"""
        local_buffer = defaultdict(list)
        conversations_this_worker = 0
        last_log_time = time.time()
        
        while self.running:
            try:
                articles_processed = 0
                
                # Procesar desde todas las colas de categorías
                for category in list(self.discovered_categories):
                    if category not in self.category_queues:
                        continue
                        
                    try:
                        # Obtener múltiples artículos por iteración
                        batch_articles = []
                        for _ in range(min(5, self.category_queues[category].qsize())):
                            try:
                                article = self.category_queues[category].get_nowait()
                                if article is not None:
                                    batch_articles.append((category, article))
                                    self.category_queues[category].task_done()
                            except queue.Empty:
                                break
                        
                        # Procesar batch
                        for cat, article in batch_articles:
                            if not self.running:
                                break
                                
                            # Usar conversaciones ya generadas por ContentManager
                            conversations = article.get('conversations', [])
                            
                            if conversations:
                                # Añadir metadatos a conversaciones
                                for conv in conversations:
                                    conv['source_article'] = article.get('title', '')
                                    conv['worker_id'] = worker_id
                                
                                local_buffer[cat].extend(conversations)
                                articles_processed += 1
                                conversations_this_worker += len(conversations)
                                
                                # Flush local cuando alcance threshold
                                if len(local_buffer[cat]) >= self.config.get('AUTO_FLUSH_THRESHOLD', 25000):
                                    self.category_buffers[cat][worker_id].extend(local_buffer[cat])
                                    self.category_counters[cat] += len(local_buffer[cat])
                                    local_buffer[cat].clear()
                                    
                                    # Auto-flush si el buffer principal está lleno
                                    if len(self.category_buffers[cat][worker_id]) >= self.config.get('AUTO_FLUSH_THRESHOLD', 25000):
                                        self._flush_category_worker_buffer(cat, worker_id)
                            
                            self.stats['conversations_generated'] += len(conversations)
                        
                        if batch_articles:
                            break  # Pasar a siguiente worker si procesó algo
                            
                    except Exception as e:
                        if self.running:
                            self.logger.log(f"⚠️ Error batch processing {category}: {e}")
                        continue
                
                # Log progreso cada 90 segundos por worker de conversaciones
                current_time = time.time()
                if current_time - last_log_time >= 90:
                    buffer_sizes = {cat: len(buf) for cat, buf in local_buffer.items() if buf}
                    self.logger.log_progress(f"ConvWorker-{worker_id}: {conversations_this_worker:,} conversaciones, buffers: {buffer_sizes}")
                    last_log_time = current_time
                
                # Si no procesó artículos, flush buffers locales
                if articles_processed == 0:
                    for cat, buffer in local_buffer.items():
                        if buffer:
                            self.category_buffers[cat][worker_id].extend(buffer)
                            self.category_counters[cat] += len(buffer)
                            buffer.clear()
                    
                    if not self.running:
                        break
                    time.sleep(0.01)  # Pausa corta
                    
            except Exception as e:
                if self.running:
                    self.logger.log(f"⚠️ Error conversación worker {worker_id}: {e}")
                if not self.running:
                    break
        
        # Log final del worker
        self.logger.log(f"✅ ConvWorker-{worker_id} finalizado: {conversations_this_worker:,} conversaciones procesadas", force=True)
        
        # Flush final de buffers locales
        for cat, buffer in local_buffer.items():
            if buffer:
                self.category_buffers[cat][worker_id].extend(buffer)
                self.category_counters[cat] += len(buffer)
    
    def _output_worker_universal(self, worker_id: int):
        """Worker universal de escritura"""
        while self.running:
            try:
                # Verificar buffers de todas las categorías para este worker
                for category in list(self.discovered_categories):
                    if len(self.category_buffers[category][worker_id]) >= self.config.get('AUTO_FLUSH_THRESHOLD', 25000):
                        self._flush_category_worker_buffer(category, worker_id)
                
                time.sleep(0.05)  # Pausa para eficiencia
                
            except Exception as e:
                if self.running:
                    self.logger.log(f"⚠️ Error output worker {worker_id}: {e}")
                if not self.running:
                    break
    
    def _flush_category_worker_buffer(self, category: str, worker_id: int):
        """Escribe buffer específico de un worker a archivo JSONL"""
        if not self.category_buffers[category][worker_id]:
            return
        
        try:
            # Crear archivo específico por categoría y worker
            output_file = self.category_dirs[category] / f"conversaciones_{category}_{worker_id}_{self.file_counters[category]:04d}.jsonl"
            
            # Escribir conversaciones
            with open(output_file, 'w', encoding='utf-8', buffering=8*1024*1024) as f:  # 8MB buffer
                for conversation in self.category_buffers[category][worker_id]:
                    # Crear registro de conversación completo
                    conversation_record = {
                        'id': f"{category}_{worker_id}_{self.file_counters[category]}_{len(self.category_buffers[category][worker_id])}",
                        'category': category,
                        'subcategory': conversation.get('subcategory', 'general'),
                        'article_title': conversation.get('source_article', ''),
                        'conversation': [
                            {'role': 'user', 'content': conversation['question']},
                            {'role': 'assistant', 'content': conversation['answer']}
                        ],
                        'metadata': {
                            'source_article': conversation.get('source_article', ''),
                            'category': category,
                            'subcategory': conversation.get('subcategory', 'general'),
                            'conversation_type': conversation.get('conversation_type', 'general'),
                            'generation_date': datetime.now().isoformat(),
                            'worker_id': worker_id
                        }
                    }
                    f.write(json.dumps(conversation_record, ensure_ascii=False) + '\n')
            
            conversations_written = len(self.category_buffers[category][worker_id])
            self.category_buffers[category][worker_id].clear()
            self.file_counters[category] += 1
            
            self.stats['files_written'] += 1
            
            # Log más frecuente para escrituras
            self.logger.log_progress(f"💾 {category}-W{worker_id}: {conversations_written:,} conversaciones → {output_file.name}")
            
        except Exception as e:
            self.logger.log(f"❌ Error flush {category}-{worker_id}: {e}")
    
    def load_hybrid_files_batch(self, batch_size: int = None) -> Iterator[List[Dict]]:
        """Carga artículos de archivos en batches"""
        if batch_size is None:
            batch_size = self.config.get('BATCH_SIZE', 50000)
        
        total_articles_processed = 0
        current_batch = []
        
        self.logger.log(f"📚 INICIANDO CARGA: {len(self.hybrid_files)} archivos, batch_size={batch_size:,}", force=True)
        
        for file_idx, hybrid_file in enumerate(self.hybrid_files, 1):
            try:
                file_size_mb = hybrid_file.stat().st_size / 1024 / 1024
                file_articles = 0
                
                self.logger.log_file_progress(file_idx, len(self.hybrid_files), hybrid_file.name, 0, force=True)
                
                with open(hybrid_file, 'r', encoding='utf-8', buffering=32*1024*1024) as f:  # 32MB buffer
                    for line_num, line in enumerate(f, 1):
                        if not line.strip():
                            continue
                        
                        try:
                            article = json.loads(line)
                            if (article.get('title') and 
                                article.get('content') and 
                                len(article.get('content', '')) > 100):
                                current_batch.append(article)
                                file_articles += 1
                                total_articles_processed += 1
                                
                                # Enviar batch cuando esté lleno
                                if len(current_batch) >= batch_size:
                                    yield current_batch
                                    current_batch = []
                                
                        except json.JSONDecodeError:
                            continue
                        except Exception:
                            continue
                        
                        # Log progreso cada 25K líneas (más frecuente)
                        if line_num % 25000 == 0:
                            self.logger.log_progress(f"📖 {hybrid_file.name}: {line_num:,} líneas, {file_articles:,} artículos válidos")
                
                self.logger.log(f"✅ {hybrid_file.name}: {file_articles:,} artículos válidos ({file_size_mb:.1f}MB)")
                
            except Exception as e:
                self.logger.log(f"⚠️ Error procesando {hybrid_file.name}: {e}")
                continue
        
        # Enviar último batch si tiene contenido
        if current_batch:
            yield current_batch
        
        self.logger.log(f"✅ CARGA COMPLETADA: {total_articles_processed:,} artículos de {len(self.hybrid_files)} archivos", force=True)
    
    def add_article_batch(self, articles: List[Dict]) -> bool:
        """Añade batch de artículos al pipeline"""
        try:
            self.article_queue.put(articles, timeout=0.1)
            return True
        except queue.Full:
            return False
    
    def process_all_files(self):
        """Procesa todos los archivos con enfoque simplificado"""
        if not self.hybrid_files:
            self.logger.log("❌ No se encontraron archivos para procesar", force=True)
            return False
        
        self.logger.log("🚀 INICIANDO PROCESAMIENTO MASIVO", force=True)
        
        # Iniciar workers
        self.start_workers()
        
        # Iniciar monitor de estado
        self.start_status_monitor()
        
        # Configurar manejo de señales
        def signal_handler(signum, frame):
            self.logger.log(f"⚠️ Señal {signum} recibida, terminando...", force=True)
            self.running = False
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Procesar archivos en batches
        start_time = time.time()
        total_batches = 0
        last_status_time = time.time()
        
        try:
            for batch in self.load_hybrid_files_batch():
                # Enviar batch a workers con configuración anti-bloqueo
                attempts = 0
                max_retries = self.config.get('MAX_QUEUE_RETRIES', 30)
                queue_timeout = self.config.get('QUEUE_TIMEOUT', 5.0)
                
                while not self.add_article_batch(batch):
                    attempts += 1
                    if attempts > max_retries:
                        self.logger.log(f"⚠️ Cola llena después de {attempts} intentos, continuando...")
                        break
                    time.sleep(queue_timeout / max_retries)
                
                total_batches += 1
                
                # Log progreso cada 5 batches O cada 2 minutos
                current_time = time.time()
                if total_batches % 5 == 0 or (current_time - last_status_time) >= 120:
                    elapsed = time.time() - start_time
                    rate = self.stats['articles_processed'] / elapsed if elapsed > 0 else 0
                    
                    # Estado de colas
                    total_queue_size = sum(q.qsize() for q in self.category_queues.values()) if self.category_queues else 0
                    article_queue_size = self.article_queue.qsize()
                    queue_status = f"Art({article_queue_size}) Cat({total_queue_size})"
                    
                    # Información detallada de categorías
                    top_categories = sorted(self.category_counters.items(), key=lambda x: x[1], reverse=True)[:5]
                    category_info = ", ".join([f"{cat}:{count:,}" for cat, count in top_categories])
                    
                    self.logger.log_progress(
                        f"Batch #{total_batches:,} | Art: {self.stats['articles_processed']:,} | Conv: {self.stats['conversations_generated']:,} | "
                        f"Rate: {rate:.0f}/s | Colas: {queue_status} | Top cats: {category_info}",
                        force=True
                    )
                    last_status_time = current_time
                
                # Log cada 25 batches con información más detallada
                if total_batches % 25 == 0:
                    self.logger.log(f"🚀 CHECKPOINT: {total_batches:,} batches procesados, {len(self.discovered_categories)} categorías descubiertas", force=True)
            
            self.logger.log(f"🎉 Todos los batches procesados: {total_batches:,} batches totales", force=True)
            
        except Exception as e:
            self.logger.log(f"❌ Error durante procesamiento: {e}", force=True)
            traceback.print_exc()
        
        # Finalizar procesamiento
        self.finalize_processing()
        
        return True
    
    def finalize_processing(self):
        """Finaliza el procesamiento"""
        self.logger.log("🎉 INICIANDO FINALIZACIÓN", force=True)
        
        # Parar workers
        self.stop_workers()
        
        # Flush todos los buffers
        self._flush_all_buffers_final()
        
        # Generar estadísticas finales
        self.generate_final_statistics()
        
        self.logger.log("✅ FINALIZACIÓN COMPLETADA", force=True)
    
    def stop_workers(self):
        """Detiene workers de forma controlada"""
        self.logger.log("🛑 DETENIENDO WORKERS...", force=True)
        self.running = False
        
        # Enviar señales de parada
        for _ in range(self.category_workers + 10):
            try:
                self.article_queue.put_nowait(None)
            except:
                pass
        
        for category_queue in self.category_queues.values():
            for _ in range(10):
                try:
                    category_queue.put_nowait(None)
                except:
                    pass
        
        # Shutdown pools
        pools = [
            ("Category", self.category_pool),
            ("Conversation", self.conversation_pool), 
            ("Output", self.output_pool)
        ]
        
        for name, pool in pools:
            try:
                pool.shutdown(wait=True, timeout=10)
                self.logger.log(f"✅ {name} pool detenido")
            except Exception as e:
                self.logger.log(f"⚠️ {name} pool error: {e}")
        
        self.logger.log("✅ Workers detenidos", force=True)
    
    def _flush_all_buffers_final(self):
        """Flush final de todos los buffers"""
        self.logger.log("💾 FLUSH FINAL DE BUFFERS...", force=True)
        
        total_flushed = 0
        for category in self.discovered_categories:
            for worker_id in self.category_buffers[category].keys():
                if self.category_buffers[category][worker_id]:
                    self._flush_category_worker_buffer(category, worker_id)
                    total_flushed += len(self.category_buffers[category][worker_id])
        
        self.logger.log(f"✅ Flush final completado: {total_flushed:,} conversaciones", force=True)
    
    def generate_final_statistics(self):
        """Genera estadísticas finales"""
        self.logger.log("📊 GENERANDO ESTADÍSTICAS FINALES...", force=True)
        
        processing_time = time.time() - self.stats['start_time']
        
        # Estadísticas de procesamiento
        summary = {
            'processing_summary': {
                'total_articles': self.stats['articles_processed'],
                'total_conversations': self.stats['conversations_generated'],
                'total_categories': len(self.discovered_categories),
                'files_written': self.stats['files_written'],
                'processing_errors': self.stats['processing_errors'],
                'processing_time_seconds': processing_time,
                'processing_time_hours': processing_time / 3600,
                'articles_per_second': self.stats['articles_processed'] / processing_time if processing_time > 0 else 0,
            },
            'categories_discovered': list(self.discovered_categories),
            'category_distribution': dict(self.category_counters),
            'generation_date': datetime.now().isoformat()
        }
        
        # Guardar estadísticas
        summary_file = self.stats_dir / "resumen_procesamiento.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        # Mostrar resumen final
        self.logger.log("🎉 PROCESAMIENTO COMPLETADO", force=True)
        self.logger.log(f"   ⏱️ Tiempo total: {processing_time/3600:.1f} horas", force=True)
        self.logger.log(f"   📚 Artículos procesados: {self.stats['articles_processed']:,}", force=True)
        self.logger.log(f"   💬 Conversaciones generadas: {self.stats['conversations_generated']:,}", force=True)
        self.logger.log(f"   🗂️ Categorías encontradas: {len(self.discovered_categories)}", force=True)
        self.logger.log(f"   📁 Archivos generados: {self.stats['files_written']:,}", force=True)
        self.logger.log(f"   🚀 Velocidad: {self.stats['articles_processed']/(processing_time/3600):.0f} artículos/hora", force=True)
        
        self.logger.log(f"📂 Categorías descubiertas: {', '.join(sorted(self.discovered_categories))}", force=True)
    
    def get_categories(self) -> list:
        """Obtiene la lista de categorías encontradas durante el procesamiento"""
        # Combinar categorías descubiertas con las del content_manager
        discovered = list(self.discovered_categories)
        content_manager_cats = self.content_manager.get_categories()
        
        # Unir y filtrar duplicados
        all_categories = list(set(discovered + content_manager_cats))
        return sorted(all_categories)
    
    @property
    def categories_stats(self) -> dict:
        """Estadísticas por categoría encontrada"""
        stats = {}
        for category in self.discovered_categories:
            stats[category] = self.category_counters.get(category, 0)
        return stats

    def start_status_monitor(self):
        """Inicia un monitor de estado que reporta cada 1 minuto"""
        def status_monitor():
            while self.running:
                try:
                    time.sleep(60)  # 1 minuto para más seguimiento
                    if not self.running:
                        break
                    
                    elapsed = time.time() - self.stats['start_time']
                    rate = self.stats['articles_processed'] / elapsed if elapsed > 0 else 0
                    
                    # Estado general
                    self.logger.log(f"🔄 STATUS: {self.stats['articles_processed']:,} artículos | "
                                  f"{self.stats['conversations_generated']:,} conversaciones | "
                                  f"{len(self.discovered_categories)} categorías | "
                                  f"{rate:.0f} art/s | "
                                  f"{elapsed/3600:.1f}h elapsed", force=True)
                    
                    # Estado de workers y colas
                    total_queue_size = sum(q.qsize() for q in self.category_queues.values()) if self.category_queues else 0
                    self.logger.log(f"📊 COLAS: Artículos({self.article_queue.qsize()}) | "
                                  f"Categorías total({total_queue_size}) | "
                                  f"Archivos escritos({self.stats['files_written']})", force=True)
                    
                    # Top 5 categorías
                    if self.category_counters:
                        top_cats = sorted(self.category_counters.items(), key=lambda x: x[1], reverse=True)[:5]
                        cats_str = ", ".join([f"{cat}:{count:,}" for cat, count in top_cats])
                        self.logger.log(f"🏷️ TOP CATEGORÍAS: {cats_str}", force=True)
                    
                except Exception as e:
                    if self.running:
                        self.logger.log(f"⚠️ Error en status monitor: {e}")
        
        # Ejecutar monitor en thread separado
        import threading
        monitor_thread = threading.Thread(target=status_monitor, daemon=True)
        monitor_thread.start()
        self.logger.log("📡 Monitor de estado iniciado (reporte cada 1 minuto)", force=True)
        return monitor_thread

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Massive Parallel Dataset Processor")
    parser.add_argument("--input", default="data_ultra_hybrid", help="Directorio de entrada")
    parser.add_argument("--output", default="consciencia_completa", help="Directorio de salida")
    
    args = parser.parse_args()
    
    try:
        processor = MassiveParallelDatasetProcessor(args.input, args.output)
        success = processor.process_all_files()
        return 0 if success else 1
    except Exception as e:
        print(f"❌ Error: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
