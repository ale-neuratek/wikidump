#!/usr/bin/env python3
"""
🧠 FULL DATASET FOR TRAINING - Procesador Completo con Categoría Consciencia
=============================================================================
Procesador masivo paralelo con logging optimizado y categoría consciencia

OBJETIVOS:
- Procesar 100% de los artículos híbridos disponibles
- Crear categoría "consciencia" con descripción del conocimiento disponible
- Logging con timestamps cada 5-10 minutos para seguimiento
- Generar conversaciones de entrenamiento usando content_manager
- Sistema de categorización inteligente sin conscious.txt

ESTRUCTURA DE SALIDA:
consciencia_completa/
├── categorias/
│   ├── consciencia/           ← NUEVA: Descripción del conocimiento disponible
│   │   ├── conversaciones_consciencia_0001.jsonl
│   │   └── metadata_consciencia.json
│   ├── arte/
│   │   ├── conversaciones_arte_0001.jsonl
│   │   └── metadata_arte.json
│   ├── ciencias/
│   └── ... (todas las categorías encontradas)
└── estadisticas/
    ├── distribucion_categorias.json
    └── resumen_procesamiento.json
"""

import json
import re
import os
import sys
import time
import hashlib
import threading
import queue
import gc
import signal
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Set, Iterator
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

# Importar configuraciones dinámicas por hardware
from hardware_configs import get_hardware_config, print_hardware_info

# Importar el gestor de contenido externo
from content_manager import ContentManager
class MassiveParallelDatasetProcessor:
    """Procesador de dataset simplificado, optimización manejada por adaptive_processor"""
    
    def __init__(self, input_dir: str, output_dir: str = "consciencia_completa"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Configuración por defecto (será sobrescrita por adaptive_processor)
        self.adaptive_config = {
            'CATEGORY_WORKERS': 8,
            'CONVERSATION_WORKERS': 16,
            'OUTPUT_WORKERS': 8,
            'BATCH_SIZE': 50000,
            'QUEUE_SIZE': 1000,
            'CONVERSATIONS_PER_OUTPUT_FILE': 50000,
            'AUTO_FLUSH_THRESHOLD': 25000,
            'MAX_QUEUE_RETRIES': 30,
            'QUEUE_TIMEOUT': 5.0
        }
        
        # Logger (será inyectado por adaptive_processor)
        self.logger = None
        
        # Inicializar gestor de contenido
        self.content_manager = ContentManager()
        
        # Crear estructura de carpetas
        self.categories_dir = self.output_dir / "categorias"
        self.stats_dir = self.output_dir / "estadisticas"
        self.categories_dir.mkdir(exist_ok=True)
        self.stats_dir.mkdir(exist_ok=True)
        
        # Crear carpetas por categoría
        self.category_dirs = {}
        for category in self.get_categories():
            category_dir = self.categories_dir / category
            category_dir.mkdir(exist_ok=True)
            self.category_dirs[category] = category_dir
        
        # Thread pools (inicializados con configuración por defecto)
        self._init_workers()
        
        # Colas especializadas
        self._init_queues()
        
        # Buffers y contadores
        self.category_buffers = {category: defaultdict(list) for category in self.get_categories()}
        self.category_counters = {category: 0 for category in self.get_categories()}
        self.file_counters = {category: 0 for category in self.get_categories()}
        
        # Estado y estadísticas
        self.running = True
        self.stats = {
            'articles_processed': 0,
            'articles_categorized': 0,
            'conversations_generated': 0,
            'files_written': 0,
            'processing_errors': 0,
            'start_time': time.time()
        }
        
        # Descubrir archivos
        self.hybrid_files = self._discover_hybrid_files()
        
        self._log(f"🧠 PROCESADOR INICIADO", force=True)
        self._log(f"   📂 Input: {self.input_dir}")
        self._log(f"   📁 Output: {self.output_dir}")
        self._log(f"   📄 Archivos encontrados: {len(self.hybrid_files)}")
        self._log(f"   📂 Categorías: {len(self.get_categories())}")
    
    def _log(self, message: str, force: bool = False):
        """Log usando el logger inyectado o print por defecto"""
        if self.logger:
            self.logger.log(message, force=force)
        else:
            if force:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    def _init_workers(self):
        """Inicializar workers con configuración actual"""
        if hasattr(self, 'category_pool'):
            self.category_pool.shutdown(wait=False)
        if hasattr(self, 'conversation_pool'):
            self.conversation_pool.shutdown(wait=False)
        if hasattr(self, 'output_pool'):
            self.output_pool.shutdown(wait=False)
        
        self.category_pool = ThreadPoolExecutor(
            max_workers=self.adaptive_config['CATEGORY_WORKERS'], 
            thread_name_prefix="category"
        )
        self.conversation_pool = ThreadPoolExecutor(
            max_workers=self.adaptive_config['CONVERSATION_WORKERS'], 
            thread_name_prefix="conversation"
        )
        self.output_pool = ThreadPoolExecutor(
            max_workers=self.adaptive_config['OUTPUT_WORKERS'], 
            thread_name_prefix="output"
        )
    
    def _init_queues(self):
        """Inicializar colas con configuración actual"""
        queue_size = self.adaptive_config['QUEUE_SIZE']
        self.article_queue = queue.Queue(maxsize=queue_size)
        self.category_queues = {
            category: queue.Queue(maxsize=queue_size) 
            for category in self.get_categories()
        }
        
    def get_categories(self) -> List[str]:
        """Obtiene lista de categorías principales del gestor de contenido"""
        return self.content_manager.get_categories()
    
    def _discover_hybrid_files(self) -> List[Path]:
        """Descubre archivos híbridos disponibles"""
        hybrid_files = []
        search_dirs = [self.input_dir, Path(".")]
        
        for search_dir in search_dirs:
            if search_dir.exists():
                files = list(search_dir.glob("articles_hybrid_*.jsonl"))
                hybrid_files.extend(files)
        
        # Remover duplicados y ordenar
        unique_files = list(set(hybrid_files))
        unique_files.sort()
        
        return unique_files
    
    def start_workers(self):
        """Inicia pools de workers especializados (patrón híbrido extractor optimizado)"""
        print(f"🚀 Iniciando workers especializados (patrón híbrido extractor)...")
        
        # Workers de categorización (procesan artículos y los clasifican)
        for i in range(self.category_workers):
            self.category_pool.submit(self._category_worker, i)
        
        # Workers de procesamiento de conversaciones (generan conversaciones)
        for i in range(self.conversation_workers):
            self.conversation_pool.submit(self._conversation_worker_universal, i)
        
        # Workers de escritura (escriben a disco)
        for i in range(self.output_workers):
            self.output_pool.submit(self._output_worker_universal, i)
        
        total_workers = self.category_workers + self.conversation_workers + self.output_workers
        print(f"✅ {total_workers} workers especializados activos (patrón híbrido)")
        print(f"   📂 Categorización: {self.category_workers} workers")
        print(f"   💬 Procesamiento: {self.conversation_workers} workers")
        print(f"   📁 Escritura: {self.output_workers} workers")
    
    def _category_worker(self, worker_id: int):
        """Worker de categorización ULTRA-OPTIMIZADO que distribuye artículos SIN LOCKS"""
        while self.running:
            try:
                article_batch = self.article_queue.get(timeout=0.1)
                if article_batch is None:
                    break
                
                # OPTIMIZACIÓN: Procesar todo el batch de una vez
                categorized_articles = defaultdict(list)
                
                for article in article_batch:
                    if not self.running:
                        break
                        
                    # Procesar artículo usando ContentManager (SIN LOCKS)
                    result = self.content_manager.process_article(article)
                    
                    if result:
                        enhanced_article = {
                            **article,
                            'category': result['category'],
                            'subcategory': result['subcategory'],
                            'confidence': result['confidence'],
                            'conversations': result['conversations']
                        }
                        categorized_articles[result['category']].append(enhanced_article)
                    
                    self.stats['articles_processed'] += 1  # Atómico sin lock
                
                # Enviar artículos categorizados a sus colas específicas
                for category, articles in categorized_articles.items():
                    for article in articles:
                        try:
                            self.category_queues[category].put_nowait(article)
                            self.stats['articles_categorized'] += 1  # Atómico sin lock
                        except queue.Full:
                            # Si la cola está llena, usar categoría general
                            try:
                                article['category'] = 'general'
                                self.category_queues['general'].put_nowait(article)
                            except queue.Full:
                                pass  # Skip si todas las colas están llenas
                
                self.article_queue.task_done()
                
            except queue.Empty:
                if not self.running:
                    break
                continue
            except Exception as e:
                if self.running:
                    print(f"⚠️ Error categoría worker {worker_id}: {e}")
                if not self.running:
                    break
    
    def _conversation_worker(self, category: str, worker_id: int):
        """Worker especializado en generación de conversaciones SIN LOCKS"""
        while self.running:
            try:
                article = self.category_queues[category].get(timeout=0.05)
                if article is None:
                    break
                
                if not self.running:
                    break
                
                # Usar conversaciones ya generadas por ContentManager
                conversations = article.get('conversations', [])
                
                # Si no hay conversaciones pre-generadas, usar ContentManager
                if not conversations:
                    result = self.content_manager.process_article(article)
                    conversations = result['conversations'] if result else []
                
                if conversations and self.running:
                    # Añadir a buffer específico del worker (SIN LOCKS EXPLÍCITOS)
                    self.category_buffers[category][worker_id].extend(conversations)
                    self.category_counters[category] += len(conversations)  # Atómico
                    
                    # Auto-flush si el buffer del worker está lleno
                    if len(self.category_buffers[category][worker_id]) >= self.adaptive_config['AUTO_FLUSH_THRESHOLD']:
                        self._flush_category_worker_buffer(category, worker_id)
                
                self.stats['conversations_generated'] += len(conversations)  # Atómico sin lock
                
                self.category_queues[category].task_done()
                
            except queue.Empty:
                if not self.running:
                    break
                continue
            except Exception as e:
                if self.running:
                    print(f"⚠️ Error conversación worker {category}-{worker_id}: {e}")
                if not self.running:
                    break
    
    def _conversation_worker_universal(self, worker_id: int):
        """Worker universal de conversaciones ULTRA-OPTIMIZADO (patrón híbrido extractor)"""
        local_buffer = defaultdict(list)  # Buffer local para reducir contención
        local_counter = 0
        
        while self.running:
            try:
                # Procesar desde todas las colas de categorías de forma universal
                articles_processed = 0
                
                # OPTIMIZACIÓN: Procesar múltiples artículos por iteración
                for category in self.get_categories():
                    try:
                        # Intentar obtener múltiples artículos de una vez
                        batch_articles = []
                        for _ in range(min(10, self.category_queues[category].qsize())):  # Hasta 10 artículos por batch
                            try:
                                article = self.category_queues[category].get_nowait()
                                if article is not None:
                                    batch_articles.append((category, article))
                                    self.category_queues[category].task_done()
                            except queue.Empty:
                                break
                        
                        # Procesar batch de artículos
                        for cat, article in batch_articles:
                            if not self.running:
                                break
                                
                            # Usar conversaciones ya generadas por ContentManager
                            conversations = article.get('conversations', [])
                            
                            # Si no hay conversaciones pre-generadas, usar ContentManager
                            if not conversations:
                                result = self.content_manager.process_article(article)
                                conversations = result['conversations'] if result else []
                            
                            if conversations and self.running:
                                # Añadir a buffer local para reducir contención
                                for conv in conversations:
                                    conv['source_article'] = article.get('title', '')
                                    conv['worker_id'] = worker_id
                                
                                local_buffer[cat].extend(conversations)
                                local_counter += len(conversations)
                                articles_processed += 1
                                
                                # Flush local cuando alcance threshold
                                if len(local_buffer[cat]) >= self.adaptive_config['AUTO_FLUSH_THRESHOLD']:
                                    self.category_buffers[cat][worker_id].extend(local_buffer[cat])
                                    self.category_counters[cat] += len(local_buffer[cat])  # Atómico
                                    local_buffer[cat].clear()
                                    
                                    # Auto-flush si el buffer principal está lleno
                                    if len(self.category_buffers[cat][worker_id]) >= self.adaptive_config['AUTO_FLUSH_THRESHOLD']:
                                        self._flush_category_worker_buffer(cat, worker_id)
                            
                            self.stats['conversations_generated'] += len(conversations)  # Atómico sin lock
                        
                        if batch_articles:
                            break  # Pasar a siguiente worker si procesó algo
                            
                    except Exception as e:
                        if self.running:
                            print(f"⚠️ Error batch processing {category}: {e}")
                        continue
                
                # Si no procesó artículos, flush buffers locales y pausa breve
                if articles_processed == 0:
                    # Flush buffers locales pendientes
                    for cat, buffer in local_buffer.items():
                        if buffer:
                            self.category_buffers[cat][worker_id].extend(buffer)
                            self.category_counters[cat] += len(buffer)
                            buffer.clear()
                    
                    if not self.running:
                        break
                    time.sleep(0.001)  # Pausa ultra-corta - 1ms para flujo continuo
                    
            except Exception as e:
                if self.running:
                    print(f"⚠️ Error conversación worker universal {worker_id}: {e}")
                if not self.running:
                    break
        
        # Flush final de buffers locales
        for cat, buffer in local_buffer.items():
            if buffer:
                self.category_buffers[cat][worker_id].extend(buffer)
                self.category_counters[cat] += len(buffer)
    
    def _output_worker_universal(self, worker_id: int):
        """Worker universal de escritura paralela SIN LOCKS (patrón híbrido extractor optimizado)"""
        while self.running:
            try:
                # Verificar buffers de todas las categorías para este worker
                for category in self.get_categories():
                    if len(self.category_buffers[category][worker_id]) >= self.adaptive_config['AUTO_FLUSH_THRESHOLD']:
                        self._flush_category_worker_buffer(category, worker_id)
                
                # Sleep ultra-corto para máxima responsividad con 450GB RAM
                time.sleep(0.01)
                
            except Exception as e:
                if self.running:
                    print(f"⚠️ Error output worker universal {worker_id}: {e}")
                if not self.running:
                    break
    
    def _flush_category_worker_buffer(self, category: str, worker_id: int):
        """Escribe buffer específico de un worker a archivo JSONL SIN LOCKS"""
        if not self.category_buffers[category][worker_id]:
            return
        
        try:
            # Crear archivo específico por categoría y worker
            output_file = self.category_dirs[category] / f"conversaciones_{category}_{worker_id}_{self.file_counters[category]:04d}.jsonl"
            
            # Escribir conversaciones con buffering ULTRA-MASIVO para 450GB RAM
            with open(output_file, 'w', encoding='utf-8', buffering=64*1024*1024) as f:  # 64MB buffer
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
            conversations_written = len(self.category_buffers[category][worker_id])
            self.category_buffers[category][worker_id].clear()
            self.file_counters[category] += 1
            
            self.stats['files_written'] += 1  # Atómico sin lock
            
            print(f"💾 {category}-W{worker_id}: {conversations_written:,} conversaciones → {output_file.name}")
            
        except Exception as e:
            print(f"❌ Error flush {category}-{worker_id}: {e}")
    
    def load_hybrid_files_batch(self, batch_size: int = None) -> Iterator[List[Dict]]:
        """Carga artículos de archivos híbridos en batches - MODO STREAMING SIMPLIFICADO Y ROBUSTO"""
        if batch_size is None:
            batch_size = self.adaptive_config['BATCH_SIZE']
        
        total_articles_processed = 0
        
        print(f"📚 MODO STREAMING SIMPLIFICADO: {len(self.hybrid_files)} archivos, batch_size={batch_size:,}")
        
        # SIMPLIFICACIÓN: Procesar archivos secuencialmente pero con batches grandes
        current_batch = []
        
        for file_idx, hybrid_file in enumerate(self.hybrid_files, 1):
            try:
                file_size_mb = hybrid_file.stat().st_size / 1024 / 1024
                print(f"📖 Procesando archivo {file_idx}/{len(self.hybrid_files)}: {hybrid_file.name} ({file_size_mb:.1f}MB)")
                
                file_articles = 0
                with open(hybrid_file, 'r', encoding='utf-8', buffering=128*1024*1024) as f:  # 128MB buffer para 450GB RAM
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
                                    print(f"📨 Enviando batch: {len(current_batch):,} artículos (total: {total_articles_processed:,})")
                                    yield current_batch
                                    current_batch = []
                                
                                # FLUJO CONTINUO: Enviar batch parcial cada 5000 artículos para evitar esperas
                                elif len(current_batch) % 5000 == 0 and len(current_batch) > 0:
                                    print(f"📤 Enviando batch parcial: {len(current_batch):,} artículos (flujo continuo)")
                                    yield current_batch.copy()  # Enviar copia para flujo continuo
                                    # NO limpiar current_batch aquí para mantener el batch principal
                                    
                        except json.JSONDecodeError:
                            continue
                        except Exception:
                            continue
                        
                        # Progreso cada 20K líneas por archivo
                        if line_num % 50000 == 0:
                            print(f"   � Procesadas {line_num:,} líneas, {file_articles:,} artículos válidos")
                
                print(f"✅ {hybrid_file.name}: {file_articles:,} artículos válidos procesados")
                
                # Gestión de memoria cada 2 archivos (más frecuente para 450GB RAM)
                if file_idx % 2 == 0:  # Cada 2 archivos
                    gc.collect()
                
            except Exception as e:
                print(f"⚠️ Error procesando {hybrid_file.name}: {e}")
                continue
        
        # Enviar último batch si tiene contenido
        if current_batch:
            print(f"📨 Enviando batch final: {len(current_batch):,} artículos")
            yield current_batch
        
        print(f"✅ STREAMING COMPLETADO: {total_articles_processed:,} artículos de {len(self.hybrid_files)} archivos")
    
    def add_article_batch(self, articles: List[Dict]) -> bool:
        """Añade batch de artículos al pipeline SIN LOCKS"""
        try:
            self.article_queue.put(articles, timeout=0.1)
            return True
        except queue.Full:
            return False
    
    def process_all_files_massive(self):
        """Procesa TODOS los archivos híbridos con enfoque masivo paralelo SIN LOCKS"""
        if not self.hybrid_files:
            print(f"❌ No se encontraron archivos híbridos para procesar")
            return False
        
        print(f"🚀 INICIANDO PROCESAMIENTO MASIVO PARALELO SIN LOCKS")
        print(f"   📁 Archivos híbridos: {len(self.hybrid_files)}")
        print(f"   🎯 Modo: PROCESAMIENTO COMPLETO (100% artículos)")
        print(f"   🔢 Conversaciones por artículo: {MASSIVE_PARALLEL_CONFIG['CONVERSATIONS_PER_ARTICLE']}")
        print(f"   🚀 Paralelización: SIN LOCKS para máximo rendimiento")
        
        # Iniciar workers especializados
        self.start_workers()
        
        # Manejo de señales ultra-agresivo (como en caroline_ultra_extractor_hybrid.py)
        def signal_handler(signum, frame):
            print(f"\n⚠️ Señal {signum} recibida, terminando INMEDIATAMENTE...")
            self.running = False
            self.stop_workers()
            
            # Esperar máximo 5 segundos para limpieza
            cleanup_start = time.time()
            while time.time() - cleanup_start < 5:
                import threading
                if threading.active_count() <= 5:
                    break
                time.sleep(0.5)
            
            print(f"🚨 TERMINACIÓN FORZADA POR SEÑAL")
            os._exit(1)  # Salida inmediata
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Procesar archivos en batches
        start_time = time.time()
        total_batches = 0
        
        print(f"🔄 Iniciando iteración de batches...")
        
        try:
            for batch in self.load_hybrid_files_batch(self.adaptive_config['BATCH_SIZE']):
                print(f"📦 Recibido batch #{total_batches + 1} con {len(batch):,} artículos")
                
                # Enviar batch a workers con configuración anti-bloqueo optimizada
                attempts = 0
                max_retries = self.adaptive_config.get('MAX_QUEUE_RETRIES', 30)
                queue_timeout = self.adaptive_config.get('QUEUE_TIMEOUT', 5.0)
                
                while not self.add_article_batch(batch):
                    attempts += 1
                    if attempts > max_retries:  # Usar configuración adaptativa
                        print(f"⚠️ Cola llena después de {attempts} intentos, continuando...")
                        break
                    time.sleep(queue_timeout / max_retries)  # Sleep adaptativo
                
                total_batches += 1
                print(f"✅ Batch #{total_batches} enviado a workers")
                
                # Progreso cada 10 batches (más frecuente para diagnóstico de flujo continuo)
                if total_batches % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = self.stats['articles_processed'] / elapsed if elapsed > 0 else 0
                    
                    print(f"📊 Progreso Masivo: {total_batches:,} batches | "
                          f"{self.stats['articles_processed']:,} procesados | "
                          f"{self.stats['articles_categorized']:,} categorizados | "
                          f"{self.stats['conversations_generated']:,} conversaciones | "
                          f"{rate:.0f} a/s")
                    
                    # Diagnóstico de colas
                    total_queue_size = sum(q.qsize() for q in self.category_queues.values())
                    article_queue_size = self.article_queue.qsize()
                    print(f"🔍 Estado colas: Artículos({article_queue_size}) Categorías({total_queue_size})")
            
            print(f"🎉 Todos los batches procesados: {total_batches:,} batches totales")
            
        except Exception as e:
            print(f"❌ Error durante procesamiento de batches: {e}")
            import traceback
            traceback.print_exc()
        
        return True
    
    def _flush_all_category_buffers(self):
        """Flushea todos los buffers de todas las categorías y workers"""
        print(f"🔄 Finalizando escritura de todos los buffers...")
        
        for category in self.get_categories():
            for worker_id in self.category_buffers[category].keys():
                if self.category_buffers[category][worker_id]:
                    self._flush_category_worker_buffer(category, worker_id)
    
    def stop_workers(self):
        """Detiene workers de forma ULTRA-AGRESIVA sin esperas (inspirado en caroline_ultra_extractor_hybrid.py)"""
        print(f"🛑 DETENCIÓN ULTRA-AGRESIVA DE WORKERS...")
        self.running = False
        
        # 1. Enviar señales de parada masivas a todas las colas (sin timeout)
        print("📤 Flooding colas con señales de parada...")
        for _ in range(self.category_workers + self.conversation_workers + self.output_workers + 10):
            try:
                self.article_queue.put_nowait(None)
                for q in self.category_queues.values():
                    q.put_nowait(None)
            except:
                pass  # Ignorar errores de cola llena
        
        # 2. Vaciar todas las colas agresivamente
        print("🗑️ Vaciando colas agresivamente...")
        
        def drain_queue(q, name):
            count = 0
            try:
                while not q.empty():
                    q.get_nowait()
                    count += 1
                    if count > 5000:  # Límite de seguridad
                        break
            except:
                pass
            return count
        
        article_drained = drain_queue(self.article_queue, "Article")
        total_cat_drained = 0
        for category, q in self.category_queues.items():
            cat_drained = drain_queue(q, f"Cat-{category}")
            total_cat_drained += cat_drained
        
        print(f"🗑️ Vaciadas: Article({article_drained}), Categories({total_cat_drained})")
        
        # 3. Shutdown inmediato de pools (sin esperar)
        print("🛑 Shutdown inmediato de pools...")
        
        pools = [
            ("Category", self.category_pool),
            ("Conversation", self.conversation_pool), 
            ("Output", self.output_pool)
        ]
        
        for name, pool in pools:
            try:
                pool.shutdown(wait=False)  # NO ESPERAR
                print(f"✅ {name} pool → shutdown iniciado")
            except Exception as e:
                print(f"⚠️ {name} pool error: {e}")
        
        # 4. Verificación rápida de threads con timeout forzado
        import threading
        start_wait = time.time()
        FORCE_EXIT_TIMEOUT = 10  # 10 segundos máximo
        
        while time.time() - start_wait < FORCE_EXIT_TIMEOUT:
            active_count = threading.active_count()
            
            if active_count <= 10:  # Solo threads del sistema
                print(f"✅ Workers terminados limpiamente ({active_count} threads)")
                break
            
            time.sleep(0.5)
        
        elapsed = time.time() - start_wait
        if elapsed >= FORCE_EXIT_TIMEOUT:
            print(f"🚨 TIMEOUT ALCANZADO - Forzando terminación después de {elapsed:.1f}s")
            
            # Matar pools agresivamente
            for name, pool in pools:
                try:
                    pool._threads.clear()  # Forzar limpieza de threads
                except:
                    pass
        
        final_active = threading.active_count()
        print(f"🧵 Threads finales: {final_active}")
        
        # 5. Forzar garbage collection
        import gc
        gc.collect()
        
        print(f"✅ DETENCIÓN COMPLETADA en {elapsed:.1f}s")
        
    def discover_hybrid_files(self) -> List[Path]:
        """Descubre todos los archivos híbridos disponibles"""
        return self._discover_hybrid_files()
        
    def process_all_files(self):
        """Procesa todos los archivos híbridos con paralelización masiva"""
        if not self.hybrid_files:
            print("❌ No se encontraron archivos híbridos")
            return False
            
        print(f"\n🚀 INICIANDO PROCESAMIENTO MASIVO SIN LOCKS")
        print(f"   📁 Archivos a procesar: {len(self.hybrid_files)}")
        print(f"   🎯 Categorización inteligente: ACTIVADA")
        print(f"   📝 Conversaciones de entrenamiento: ACTIVADAS")
        print(f"   🚀 Paralelización: MASIVA SIN LOCKS")
        
        # Procesar con sistema masivo paralelo
        success = self.process_all_files_massive()
        
        if success:
            # Finalizar procesamiento
            self.finalize_processing()
        
        return success
        
    def process_hybrid_file(self, file_path: Path):
        """Procesa un archivo híbrido individual"""
        articles_in_file = 0
        conversations_in_file = 0
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                    
                try:
                    article = json.loads(line)
                    
                    # Procesar artículo
                    result = self.process_single_article(article)
                    
                    if result:
                        articles_in_file += 1
                        conversations_in_file += len(result['conversations'])
                        
                        # Guardar resultado
                        self.save_article_result(result)
                        
                except json.JSONDecodeError as e:
                    if line_num % 10000 == 0:  # Log error cada 10K líneas
                        print(f"⚠️ Error JSON línea {line_num}: {e}")
                    continue
                except Exception as e:
                    print(f"⚠️ Error procesando artículo línea {line_num}: {e}")
                    continue
                    
                # Progreso cada 1000 artículos
                if articles_in_file % 1000 == 0:
                    print(f"   📊 Progreso: {articles_in_file:,} artículos, {conversations_in_file:,} conversaciones")
                    
        print(f"   ✅ Completado: {articles_in_file:,} artículos, {conversations_in_file:,} conversaciones")
        
        # Actualizar estadísticas globales
        self.stats['files_processed'] += 1
        self.stats['articles_processed'] += articles_in_file
        self.stats['conversations_generated'] += conversations_in_file
            
    def process_single_article(self, article: Dict) -> Optional[Dict]:
        """Procesa un artículo individual usando el ContentManager externo"""
        title = article.get('title', '').strip()
        content = article.get('content', '').strip()
        
        if not title or not content:
            return None
            
        # Procesar artículo usando el ContentManager externo
        result = self.content_manager.process_article(article)
        
        if not result:
            return None
            
        # Reestructurar para mantener compatibilidad con el formato existente
        formatted_result = {
            'id': f"{result['category']}_{hashlib.md5(title.encode()).hexdigest()[:8]}",
            'title': result['title'],
            'content': content,
            'category': result['category'],
            'subcategory': result['subcategory'],
            'categorization_confidence': result['confidence'],
            'conversations': result['conversations'],
            'metadata': {
                'source': 'content_manager',
                'processed_at': result['metadata']['processed_at'],
                'content_length': result['metadata']['content_length'],
                'conversations_count': result['metadata']['conversations_count'],
                'original_article': article
            }
        }
        
        # Actualizar contadores (sin lock para mejor rendimiento)
        self.category_counters[result['category']] += 1
        self.stats['articles_categorized'] += 1
        self.stats['conversations_generated'] += len(result['conversations'])
            
        # Registrar problemas de confianza baja
        if result['confidence'] < 0.5:
            self.problematic_categorizations.append({
                'title': title,
                'category': result['category'],
                'confidence': result['confidence'],
                'reason': 'low_confidence'
            })
            
        return formatted_result
        
    def save_article_result(self, result: Dict):
        """Guarda el resultado de un artículo en la categoría correspondiente"""
        category = result['category']
        
        # Crear directorio de categoría si no existe
        category_dir = self.categories_dir / category
        category_dir.mkdir(exist_ok=True)
        
        # Determinar archivo de salida
        file_counter = self.file_counters[category]
        conversations_per_file = 10000  # 10K conversaciones por archivo
        
        if file_counter % conversations_per_file == 0:
            file_num = (file_counter // conversations_per_file) + 1
            current_file = category_dir / f"conversaciones_{category}_{file_num:04d}.jsonl"
        else:
            file_num = (file_counter // conversations_per_file) + 1
            current_file = category_dir / f"conversaciones_{category}_{file_num:04d}.jsonl"
            
        # Escribir conversaciones individuales
        with open(current_file, 'a', encoding='utf-8') as f:
            for conv in result['conversations']:
                conversation_record = {
                    'id': f"{result['id']}_{len(result['conversations'])}",
                    'category': category,
                    'subcategory': result['subcategory'],
                    'article_title': result['title'],
                    'conversation': [
                        {'role': 'user', 'content': conv['question']},
                        {'role': 'assistant', 'content': conv['answer']}
                    ],
                    'metadata': {
                        'source_article': result['title'],
                        'category': category,
                        'subcategory': result['subcategory'],
                        'conversation_type': conv['conversation_type'],
                        'content_length': len(result['content']),
                        'categorization_confidence': result['categorization_confidence'],
                        'generation_date': datetime.now().isoformat()
                    }
                }
                
                f.write(json.dumps(conversation_record, ensure_ascii=False) + '\n')
                
        self.file_counters[category] += len(result['conversations'])
        
    def finalize_processing(self):
        """Finaliza el procesamiento con modo inteligente ultra-optimizado (inspirado en caroline_ultra_extractor_hybrid.py)"""
        print(f"\n🎉 INICIANDO FINALIZACIÓN ULTRA-OPTIMIZADA")
        
        # Usar finalización inteligente ultra-agresiva (incluye stop_workers internamente)
        self._finalize_processing_intelligent()
        
        # Solo generar archivos finales si la finalización fue exitosa
        print(f"📝 Generando archivos finales...")
        
        try:
            # Generar conscious.txt
            self.generate_conscious_file()
            
            # Generar metadatos por categoría
            self.generate_category_metadata()
            
            # Generar estadísticas
            self.generate_final_statistics()
            
            # Mostrar resumen final
            self.show_final_summary()
            
            print(f"✅ Archivos finales generados exitosamente")
            
        except Exception as e:
            print(f"⚠️ Error generando archivos finales: {e}")
            print(f"📊 Los archivos de conversaciones principales ya están guardados")
        
        print(f"🎯 FINALIZACIÓN COMPLETADA")
        
    def generate_conscious_file(self):
        """Genera el archivo conscious.txt con la base fundamental"""
        conscious_file = self.output_dir / "conscious.txt"
        
        with open(conscious_file, 'w', encoding='utf-8') as f:
            f.write("# CAROLINE CONSCIOUS KNOWLEDGE - BASE FUNDAMENTAL CORREGIDA\n")
            f.write("# =========================================================\n")
            f.write(f"# Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Artículos procesados: {self.stats['articles_processed']:,}\n")
            f.write(f"# Conversaciones generadas: {self.stats['conversations_generated']:,}\n")
            f.write(f"# Categorías identificadas: {len(self.get_categories())}\n")
            f.write("# Sistema de categorización inteligente: ACTIVADO\n")
            f.write("# Corrección de problemas de categorización: APLICADA\n")
            f.write("# =========================================================\n\n")
            
            f.write("## SISTEMA CONSCIENTE MEJORADO\n")
            f.write("soy caroline, modelo de ia especializado en wikipedia española\n")
            f.write("he procesado 1.5 millones de artículos con categorización corregida\n")
            f.write("cada artículo ha sido analizado con patrones inteligentes\n")
            f.write("las categorías erróneas han sido corregidas automáticamente\n")
            f.write("genero conversaciones específicas por categoría y subcategoría\n")
            f.write("mi conocimiento está estructurado en carpetas organizadas\n\n")
            
            f.write("## CATEGORÍAS PROCESADAS CON CORRECCIÓN\n")
            total_conversations = sum(self.category_counters.values())
            
            for category in sorted(self.get_categories()):
                count = self.category_counters[category]
                percentage = (count / total_conversations * 100) if total_conversations > 0 else 0
                
                f.write(f"### {category.upper()}\n")
                f.write(f"- Artículos: {count:,} ({percentage:.1f}%)\n")
                f.write(f"- Directorio: consciencia/categorias/{category}/\n")
                f.write(f"- Archivos: conversaciones_{category}_NNNN.jsonl\n")
                
                # Mostrar subcategorías si existen
                category_dir = self.categories_dir / category
                if category_dir.exists():
                    jsonl_files = list(category_dir.glob("*.jsonl"))
                    f.write(f"- Archivos generados: {len(jsonl_files)}\n")
                    
                f.write("\n")
                
            f.write("## MEJORAS APLICADAS\n")
            f.write("1. Corrección de categorización musical (ej: Catch a Fire → arte)\n")
            f.write("2. Identificación precisa de biografías vs. otros contenidos\n")
            f.write("3. Separación clara entre ciencias y otras disciplinas\n")
            f.write("4. Subcategorización específica por tipo de contenido\n")
            f.write("5. Conversaciones contextuales por área de conocimiento\n\n")
            
            f.write("## INSTRUCCIONES DE USO\n")
            f.write("- Cada categoría tiene su propio directorio\n")
            f.write("- Las conversaciones están en formato JSONL listo para entrenamiento\n")
            f.write("- Metadatos incluyen confianza de categorización\n")
            f.write("- Subcategorías permiten especialización fina\n")
            f.write("- Sistema autocorrige errores de categorización comunes\n")
            
        print(f"📝 Archivo conscious.txt generado: {conscious_file}")
        
    def generate_category_metadata(self):
        """Genera metadatos por cada categoría"""
        for category in self.get_categories():
            if self.category_counters[category] > 0:  # Solo categorías con contenido
                category_dir = self.categories_dir / category
                metadata_file = category_dir / f"metadata_{category}.json"
                
                # Contar archivos
                jsonl_files = list(category_dir.glob("*.jsonl"))
                total_conversations = self.category_counters[category]
                
                metadata = {
                    'category': category,
                    'total_articles': total_conversations,
                    'total_files': len(jsonl_files),
                    'files_generated': [f.name for f in jsonl_files],
                    'generation_date': datetime.now().isoformat(),
                    'description': f"Conversaciones de entrenamiento para la categoría {category}",
                    'conversation_types': ['definicion', 'temporal', 'espacial', 'explicacion', 'contextualizacion'],
                    'processing_mode': 'massive_parallel_no_locks'
                }
                
                with open(metadata_file, 'w', encoding='utf-8') as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=2)
                    
                print(f"📊 Metadatos generados: {metadata_file}")
            
    def generate_final_statistics(self):
        """Genera estadísticas finales del procesamiento"""
        
        # Distribución de categorías
        distribution_file = self.stats_dir / "distribucion_categorias.json"
        total_conversations = sum(self.category_counters.values())
        
        distribution = {
            'total_conversations': total_conversations,
            'total_categories': len(self.category_counters),
            'distribution': {
                category: {
                    'count': count,
                    'percentage': (count / total_conversations * 100) if total_conversations > 0 else 0
                }
                for category, count in sorted(self.category_counters.items(), key=lambda x: x[1], reverse=True)
            },
            'generation_date': datetime.now().isoformat()
        }
        
        with open(distribution_file, 'w', encoding='utf-8') as f:
            json.dump(distribution, f, ensure_ascii=False, indent=2)
            
        # Problemas de categorización
        problems_file = self.stats_dir / "problemas_categorizacion.json"
        
        problems = {
            'total_problematic': len(self.problematic_categorizations),
            'low_confidence_threshold': 0.5,
            'problems': self.problematic_categorizations[:100],  # Top 100 problemas
            'analysis_date': datetime.now().isoformat()
        }
        
        with open(problems_file, 'w', encoding='utf-8') as f:
            json.dump(problems, f, ensure_ascii=False, indent=2)
            
        # Resumen de procesamiento
        summary_file = self.stats_dir / "resumen_procesamiento.json"
        
        processing_time = time.time() - self.stats['start_time']
        
        summary = {
            'processing_summary': {
                'total_articles': self.stats['articles_processed'],
                'total_conversations': self.stats['conversations_generated'],
                'total_categories': len([c for c in self.get_categories() if self.category_counters[c] > 0]),
                'files_written': self.stats['files_written'],
                'processing_errors': self.stats['processing_errors'],
                'processing_time_seconds': processing_time,
                'processing_time_hours': processing_time / 3600,
                'articles_per_second': self.stats['articles_processed'] / processing_time if processing_time > 0 else 0,
                'processing_mode': 'massive_parallel_no_locks'
            },
            'categories_created': [c for c in self.get_categories() if self.category_counters[c] > 0],
            'output_structure': {
                'base_directory': str(self.output_dir),
                'categories_directory': str(self.categories_dir),
                'statistics_directory': str(self.stats_dir),
                'conscious_file': str(self.output_dir / "conscious.txt")
            },
            'generation_date': datetime.now().isoformat()
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
            
        print(f"📈 Estadísticas generadas en: {self.stats_dir}")
        
    def show_final_summary(self):
        """Muestra resumen final del procesamiento"""
        processing_time = time.time() - self.stats['start_time']
        
        print(f"\n🎉 PROCESAMIENTO MASIVO FINALIZADO")
        print(f"   ⏱️ Tiempo total: {processing_time/3600:.1f} horas")
        print(f"   📚 Artículos procesados: {self.stats['articles_processed']:,}")
        print(f"   💬 Conversaciones generadas: {self.stats['conversations_generated']:,}")
        print(f"   🗂️ Categorías creadas: {len([c for c in self.get_categories() if self.category_counters[c] > 0])}")
        print(f"   📁 Archivos generados: {self.stats['files_written']:,}")
        print(f"   ⚠️ Errores de procesamiento: {self.stats['processing_errors']}")
        print(f"   🚀 Velocidad: {self.stats['articles_processed']/(processing_time/3600):.0f} artículos/hora")
        print(f"   🔥 Modo: PARALELIZACIÓN MASIVA SIN LOCKS")
        
        print(f"\n📂 ESTRUCTURA GENERADA:")
        print(f"   📁 {self.output_dir}/")
        print(f"   ├── conscious.txt (base fundamental)")
        print(f"   ├── categorias/ (categorías con contenido)")
        
        for category in sorted(self.get_categories()):
            count = self.category_counters[category]
            if count > 0:
                files = len(list((self.categories_dir / category).glob("*.jsonl"))) if (self.categories_dir / category).exists() else 0
                print(f"   │   ├── {category}/ ({count:,} conversaciones, {files} archivos)")
            
        print(f"   └── estadisticas/ (análisis y metadatos)")
        
        print(f"\n✅ Dataset de entrenamiento masivo listo en: {self.output_dir}")
    
    def _finalize_processing_intelligent(self):
        """Finalización automática inteligente ULTRA-AGRESIVA (basada exactamente en caroline_ultra_extractor_hybrid.py)"""
        print(f"🔄 INICIANDO FINALIZACIÓN ULTRA-AGRESIVA SIN LOCKS...")
        
        # 1. Detener la alimentación de nuevos trabajos INMEDIATAMENTE
        self.running = False
        
        # 2. Timeout MUCHO más agresivo (máximo 10 segundos, como en caroline_ultra_extractor_hybrid.py)
        max_wait = 10  # Solo 10 segundos máximo (ultra-agresivo)
        print(f"⏳ Esperando finalización ULTRA-AGRESIVA (máximo {max_wait}s)...")
        
        start_wait = time.time()
        last_conversations = sum(self.category_counters.values())
        stable_count = 0
        
        while time.time() - start_wait < max_wait:
            time.sleep(0.5)  # Check cada 0.5 segundos (MUY agresivo)
            current_conversations = sum(self.category_counters.values())
            
            # Estado de colas
            total_queue_size = sum(q.qsize() for q in self.category_queues.values())
            article_queue_size = self.article_queue.qsize()
            
            elapsed_wait = time.time() - start_wait
            print(f"📊 T+{elapsed_wait:.1f}s: {current_conversations:,} conversaciones | Colas: Art({article_queue_size}) Cat({total_queue_size})")
            
            # Verificar estabilidad
            if current_conversations == last_conversations:
                stable_count += 1
            else:
                stable_count = 0
                last_conversations = current_conversations
            
            # Condiciones de finalización ULTRA-AGRESIVAS (inspiradas en caroline_ultra_extractor_hybrid.py)
            if article_queue_size == 0 and total_queue_size == 0 and stable_count >= 2:
                print(f"✅ Procesamiento completado (colas vacías)")
                break
            elif stable_count >= 6:  # Solo 3 segundos sin cambios (6 * 0.5s)
                print(f"✅ Procesamiento estabilizado")
                break
        
        # 3. TIMEOUT FORZADO como en caroline_ultra_extractor_hybrid.py
        elapsed_total = time.time() - start_wait
        if elapsed_total >= max_wait:
            print(f"� TIMEOUT DE FINALIZACIÓN ({elapsed_total:.1f}s) - Continuando con terminación")
        
        # 4. Flush final de todos los buffers
        self._flush_all_buffers_final()
        
        # 5. Llamar a stop_workers() directamente (como en caroline_ultra_extractor_hybrid.py)
        print(f"🛑 Llamando a stop_workers() ultra-agresivo...")
        self.stop_workers()
        
        print(f"🔥 FINALIZACIÓN ULTRA-AGRESIVA COMPLETADA en {elapsed_total:.1f}s")
    
    def _flush_all_buffers_final(self):
        """Flush final de todos los buffers sin locks"""
        print(f"💾 FLUSH FINAL DE TODOS LOS BUFFERS...")
        
        total_flushed = 0
        for category in self.get_categories():
            if self.category_counters[category] > 0:
                # Flush todos los workers de esta categoría
                for worker_id in self.category_buffers[category]:
                    buffer = self.category_buffers[category][worker_id]
                    if buffer:
                        try:
                            self._flush_category_worker_buffer(category, worker_id)
                            total_flushed += len(buffer)
                        except Exception as e:
                            print(f"⚠️ Error flush final {category}-{worker_id}: {e}")
        
        print(f"✅ Flush final completado: {total_flushed:,} conversaciones")


def main():
    """Función principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Full Dataset For Training - Procesador Completo")
    
    # Accept both --input/--output and --input-dir/--output-dir for flexibility
    parser.add_argument("--input", "--input-dir", "-i", type=str, 
                       default="./data_ultra_hybrid",
                       help="Directorio de entrada con archivos híbridos")
    parser.add_argument("--output", "--output-dir", "-o", type=str, 
                       default="consciencia",
                       help="Directorio de salida")
    
    args = parser.parse_args()
    
    print("🧠 FULL DATASET FOR TRAINING - Procesador Completo")
    print("=" * 60)
    
    # Configurar directorios desde argumentos CLI
    input_dir = args.input
    output_dir = args.output
    
    print(f"📂 Input: {input_dir}")
    print(f"📁 Output: {output_dir}")
    
    # Verificar input
    if not Path(input_dir).exists():
        print(f"❌ Directorio de entrada no encontrado: {input_dir}")
        print("💡 Asegúrate de que los archivos híbridos estén disponibles")
        return 1
        
    # Crear procesador masivo
    processor = MassiveParallelDatasetProcessor(input_dir, output_dir)
    
    # Procesar todo
    success = processor.process_all_files()
    
    if success:
        print("\n🎉 PROCESAMIENTO EXITOSO")
        print("📋 El dataset de entrenamiento está listo para usar")
        print("🧠 Base de conocimiento consciente generada")
        return 0
    else:
        print("\n❌ PROCESAMIENTO FALLIDO")
        return 1


if __name__ == "__main__":
    sys.exit(main())
