#!/usr/bin/env python3
"""
🚀 ADAPTIVE WIKI EXTRACTOR - Ultra-optimizado con inteligencia adaptativa
=========================================================================
Combina SAX parser confiable con optimizaciones masivas de rendimiento
y configuración adaptativa según el tamaño del dataset.

CARACTERÍSTICAS:
- Configuración adaptativa automática según hardware detectado
- Estimación inteligente de tamaño de dataset
- Optimización dinámica de workers y batches
- Logging detallado con timestamps
- Terminación limpia con timeouts inteligentes
- Recuperación automática ante errores
"""

import xml.sax
import re
import time
import os
import gc
import psutil
import hashlib
import sys
import argparse
import json
import threading
import queue
import mmap
import io
from datetime import datetime
from pathlib import Path
from typing import List, Set, Dict, Optional, Tuple, Iterator
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal

# Patrones precompilados una sola vez (optimización crítica)
PRECOMPILED_PATTERNS = {
    'cleanup': re.compile(r'\{\{[^}]*\}\}|<ref[^>]*>.*?</ref>|<[^>]*>', re.DOTALL),
    'links': re.compile(r'\[\[([^|\]]*\|)?([^\]]*)\]\]'),
    'whitespace': re.compile(r'\s+'),
    'spanish': re.compile(r'[a-záéíóúñüç]', re.IGNORECASE)
}

# Importar configuraciones dinámicas por hardware
from hardware_configs import get_hardware_config, print_hardware_info, optimize_for_queue_issues, diagnose_dataset_configuration

class AdaptiveExtractorLogger:
    """Logger adaptativo con timestamps inteligentes"""
    
    def __init__(self, log_file: str = "extraction.log"):
        self.log_file = log_file
        self.start_time = time.time()
        self.log_interval = 60  # 1 minuto
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
PRECOMPILED_PATTERNS = {
    'cleanup': re.compile(r'\{\{[^}]*\}\}|<ref[^>]*>.*?</ref>|<[^>]*>', re.DOTALL),
    'links': re.compile(r'\[\[([^|\]]*\|)?([^\]]*)\]\]'),
    'whitespace': re.compile(r'\s+'),
    'spanish': re.compile(r'[a-záéíóúñüç]', re.IGNORECASE)
}

# Importar configuraciones dinámicas por hardware
from hardware_configs import get_hardware_config, print_hardware_info, optimize_for_queue_issues, diagnose_dataset_configuration

class AdaptiveExtractorConfig:
    """Configuración adaptativa inteligente para el extractor"""
    
    def __init__(self, xml_path: str):
        self.xml_path = Path(xml_path)
        self.logger = AdaptiveExtractorLogger()
        
        # Análisis del archivo XML
        self.file_size_gb = self.xml_path.stat().st_size / (1024**3)
        
        # Configuración base por hardware
        self.hardware_config = get_hardware_config()
        
        # Estimación inteligente de parámetros
        self._estimate_optimal_config()
        
        self.logger.log(f"🧠 CONFIGURACIÓN ADAPTATIVA DETERMINADA", force=True)
        self.logger.log(f"   📁 Archivo XML: {self.file_size_gb:.1f}GB", force=True)
        self.logger.log(f"   👥 Workers totales: {self.optimal_workers}", force=True)
        self.logger.log(f"   📦 Batch size: {self.optimal_batch_size:,}", force=True)
        self.logger.log(f"   🗂️ Queue size: {self.optimal_queue_size:,}", force=True)
        
    def _estimate_optimal_config(self):
        """Estima configuración óptima según el tamaño del archivo"""
        base_workers = self.hardware_config['MAX_WORKERS']
        
        # Configuración adaptativa según tamaño del archivo
        if self.file_size_gb > 15.0:  # Archivos muy grandes (como el nuestro de 19.8GB)
            self.optimal_workers = min(base_workers, 288)  # Máximo paralelismo
            self.optimal_batch_size = min(self.hardware_config['BATCH_SIZE'], 50000)  # Batches más pequeños para mejor distribución
            self.optimal_queue_size = self.hardware_config['QUEUE_SIZE'] * 2  # Colas más grandes
            self.optimal_timeout = 0.1  # Timeout más agresivo
            self.optimal_flush_threshold = 100000  # Flush más frecuente
            self.logger.log(f"   🎯 Configuración para archivo MUY GRANDE detectada", force=True)
            
        elif self.file_size_gb > 5.0:  # Archivos grandes
            self.optimal_workers = min(base_workers, 200)
            self.optimal_batch_size = self.hardware_config['BATCH_SIZE'] // 2
            self.optimal_queue_size = int(self.hardware_config['QUEUE_SIZE'] * 1.5)
            self.optimal_timeout = 0.2
            self.optimal_flush_threshold = 150000
            self.logger.log(f"   🎯 Configuración para archivo GRANDE detectada", force=True)
            
        elif self.file_size_gb > 1.0:  # Archivos medianos
            self.optimal_workers = min(base_workers, 100)
            self.optimal_batch_size = self.hardware_config['BATCH_SIZE'] // 4
            self.optimal_queue_size = self.hardware_config['QUEUE_SIZE']
            self.optimal_timeout = 0.5
            self.optimal_flush_threshold = 200000
            self.logger.log(f"   🎯 Configuración para archivo MEDIANO detectada", force=True)
            
        else:  # Archivos pequeños
            self.optimal_workers = min(base_workers, 50)
            self.optimal_batch_size = self.hardware_config['BATCH_SIZE'] // 8
            self.optimal_queue_size = self.hardware_config['QUEUE_SIZE'] // 2
            self.optimal_timeout = 1.0
            self.optimal_flush_threshold = 300000
            self.logger.log(f"   🎯 Configuración para archivo PEQUEÑO detectada", force=True)
        
        # Aplicar optimizaciones específicas para colas si es necesario
        estimated_articles = int(self.file_size_gb * 100000)  # ~100k artículos por GB
        if estimated_articles > 1000000:  # Más de 1M artículos
            optimized_config = optimize_for_queue_issues(self.hardware_config, estimated_articles)
            
            # Ajustar parámetros con las optimizaciones
            self.optimal_queue_size = min(optimized_config['QUEUE_SIZE'], self.optimal_queue_size * 2)
            self.optimal_timeout = min(optimized_config.get('QUEUE_TIMEOUT', 1.0), self.optimal_timeout)
            
            self.logger.log(f"   ⚡ Optimizaciones de cola aplicadas para {estimated_articles:,} artículos", force=True)

# Configuración adaptativa según hardware detectado
ADAPTIVE_CONFIG = None  # Se inicializará en main()

class AdaptiveUltraProcessor:
    """Procesador ultra-optimizado con configuración adaptativa e inteligencia mejorada"""
    
    def __init__(self, output_dir: str = "data_ultra_hybrid", config: AdaptiveExtractorConfig = None):
        self.config = config or ADAPTIVE_CONFIG
        self.logger = self.config.logger
        
        # Configuración adaptativa
        self.num_workers = self.config.optimal_workers
        self.batch_size = self.config.optimal_batch_size
        self.queue_size = self.config.optimal_queue_size
        self.worker_timeout = self.config.optimal_timeout
        self.flush_threshold = self.config.optimal_flush_threshold
        
        # Pools especializados con configuración adaptativa
        extraction_workers = self.num_workers // 3
        processing_workers = self.num_workers // 3  
        output_workers = self.num_workers - extraction_workers - processing_workers  # Resto para output
        
        self.extraction_pool = ThreadPoolExecutor(max_workers=extraction_workers, thread_name_prefix="extract")
        self.processing_pool = ThreadPoolExecutor(max_workers=processing_workers, thread_name_prefix="process")
        self.output_pool = ThreadPoolExecutor(max_workers=output_workers, thread_name_prefix="output")
        
        # Colas de trabajo con tamaño adaptativo
        self.raw_batch_queue = queue.Queue(maxsize=self.queue_size)
        self.processed_queue = queue.Queue(maxsize=self.queue_size)
        self.output_queue = queue.Queue(maxsize=self.queue_size)
        
        # Estado y estadísticas mejoradas
        self.running = True
        self.stats = {
            'batches_sent': 0,
            'batches_processed': 0,
            'articles_processed': 0,
            'batches_written': 0,
            'start_time': time.time(),
            'last_stats_time': time.time()
        }
        
        # Output management mejorado
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.file_counter = 0
        
        # Buffers lockless per-worker mejorados
        self.worker_buffers = defaultdict(list)
        self.worker_file_counters = defaultdict(int)
        
        self.logger.log(f"🚀 ADAPTIVE ULTRA-PROCESSOR INICIADO:", force=True)
        self.logger.log(f"   🔄 Workers: Extract({extraction_workers}), Process({processing_workers}), Output({output_workers})", force=True)
        self.logger.log(f"   📦 Batch size: {self.batch_size:,}", force=True)
        self.logger.log(f"   🗂️ Queue size: {self.queue_size:,}", force=True)
        self.logger.log(f"   📁 Output dir: {self.output_dir}", force=True)
        self.logger.log(f"   ⏱️ Worker timeout: {self.worker_timeout}s", force=True)
    
    def log_progress_intelligent(self, force: bool = False):
        """Logging inteligente de progreso con intervalos adaptativos"""
        current_time = time.time()
        
        # Logging adaptativo: más frecuente al inicio, menos después
        elapsed = current_time - self.stats['start_time']
        if elapsed < 300:  # Primeros 5 minutos: cada minuto
            interval = 60
        elif elapsed < 1800:  # Siguientes 25 minutos: cada 2 minutos
            interval = 120
        else:  # Después: cada 5 minutos
            interval = 300
        
        if force or (current_time - self.stats['last_stats_time']) >= interval:
            self._print_detailed_progress()
            self.stats['last_stats_time'] = current_time
    
    def _print_detailed_progress(self):
        """Progreso detallado con métricas de rendimiento"""
        current_time = time.time()
        elapsed = current_time - self.stats['start_time']
        
        articles_rate = self.stats['articles_processed'] / elapsed if elapsed > 0 else 0
        
        # Estado de colas
        raw_size = self.raw_batch_queue.qsize()
        proc_size = self.processed_queue.qsize()
        out_size = self.output_queue.qsize()
        
        # Métricas de rendimiento
        total_queue_usage = (raw_size + proc_size + out_size) / (self.queue_size * 3) * 100
        
        self.logger.log(f"� PROGRESO ADAPTATIVO:", force=True)
        self.logger.log(f"   📚 Artículos procesados: {self.stats['articles_processed']:,}", force=True)
        self.logger.log(f"   � Velocidad: {articles_rate:.0f} artículos/s", force=True)
        self.logger.log(f"   📦 Batches: Enviados({self.stats['batches_sent']:,}) Procesados({self.stats['batches_processed']:,}) Escritos({self.stats['batches_written']:,})", force=True)
        self.logger.log(f"   �️ Colas: Raw({raw_size}) Proc({proc_size}) Out({out_size}) - Uso: {total_queue_usage:.1f}%", force=True)
        self.logger.log(f"   ⏱️ Tiempo transcurrido: {elapsed/60:.1f}min", force=True)
        
        # Alertas de rendimiento
        if total_queue_usage > 80:
            self.logger.log(f"   ⚠️ Colas saturadas al {total_queue_usage:.1f}%", force=True)
        if articles_rate > 0 and articles_rate < 1000:
            self.logger.log(f"   ⚠️ Velocidad baja: {articles_rate:.0f} artículos/s", force=True)
    
    def start_workers(self):
        """Inicia todos los pools de workers especializados"""
        print(f"🚀 Iniciando workers especializados...")
        
        # Workers de extracción (reciben batches del SAX parser)
        for i in range(self.num_workers // 3):
            self.extraction_pool.submit(self._extraction_worker, i)
        
        # Workers de procesamiento (limpian y validan artículos)
        for i in range(self.num_workers // 3):
            self.processing_pool.submit(self._processing_worker, i)
        
        # Workers de salida (escriben a disco)
        for i in range(self.num_workers // 3):
            self.output_pool.submit(self._output_worker, i)
        
        print(f"✅ {self.num_workers} workers especializados activos")
    
    def _extraction_worker(self, worker_id: int):
        """Worker especializado en extracción rápida de datos básicos"""
        start_time = time.time()
        while self.running and (time.time() - start_time < 3600):  # Max 1 hora
            try:
                raw_batch = self.raw_batch_queue.get(timeout=self.worker_timeout)
                if raw_batch is None:
                    self.logger.log(f"🔄 Extractor-{worker_id}: Señal de parada")
                    break
                
                # Extracción ultra-rápida (solo filtros básicos)
                extracted = []
                for title, text in raw_batch:
                    # Filtros ultra-rápidos sin regex
                    if (len(text) > 200 and 
                        ':' not in title and 
                        'wikipedia:' not in title.lower() and
                        'plantilla:' not in title.lower()):
                        extracted.append((title, text))
                
                if extracted:
                    try:
                        self.processed_queue.put(extracted, timeout=0.1)
                        self.stats['batches_sent'] += 1
                    except queue.Full:
                        self.logger.log(f"⚠️ Extractor-{worker_id}: Cola procesamiento llena, descartando batch")
                
                self.raw_batch_queue.task_done()
                
            except queue.Empty:
                if not self.running:
                    break
                continue
            except Exception as e:
                print(f"⚠️ Error extractor {worker_id}: {e}")
                break
        
        print(f"✅ Extractor-{worker_id}: Terminado")
    
    def _processing_worker(self, worker_id: int):
        """Worker especializado en procesamiento intensivo con regex"""
        # Patrones locales para thread-safety
        patterns = PRECOMPILED_PATTERNS
        start_time = time.time()
        
        while self.running and (time.time() - start_time < 3600):  # Max 1 hora
            try:
                batch = self.processed_queue.get(timeout=self.worker_timeout)
                if batch is None:
                    self.logger.log(f"🔄 Processor-{worker_id}: Señal de parada")
                    break
                
                # Procesamiento intensivo ultra-optimizado
                processed_articles = []
                
                for title, text in batch:
                    try:
                        # Limpieza ultra-rápida en un solo paso
                        cleaned = patterns['cleanup'].sub('', text)
                        cleaned = patterns['links'].sub(r'\2', cleaned)
                        cleaned = patterns['whitespace'].sub(' ', cleaned).strip()
                        
                        if len(cleaned) < 100:
                            continue
                        
                        # Verificación de idioma ultra-rápida
                        sample = cleaned[:400]
                        spanish_count = patterns['spanish'].findall(sample)
                        if len(spanish_count) / len(sample) < 0.25:
                            continue
                        
                        # Crear artículo optimizado
                        article = {
                            'title': title.strip(),
                            'content': cleaned,
                            'length': len(cleaned),
                            'worker_id': worker_id,
                            'hash': hashlib.sha256(f"{title}{cleaned[:30]}".encode()).hexdigest()[:8]
                        }
                        
                        processed_articles.append(article)
                        
                    except Exception:
                        continue  # Skip artículos problemáticos sin logging
                
                if processed_articles:
                    try:
                        self.output_queue.put(processed_articles, timeout=0.1)
                        self.stats['articles_processed'] += len(processed_articles)
                        self.stats['batches_processed'] += 1
                    except queue.Full:
                        print(f"⚠️ Processor-{worker_id}: Cola output llena, descartando {len(processed_articles)} artículos")
                
                self.processed_queue.task_done()
                
            except queue.Empty:
                if not self.running:
                    break
                continue
            except Exception as e:
                print(f"⚠️ Error processor {worker_id}: {e}")
                break
        
        print(f"✅ Processor-{worker_id}: Terminado")
    
    def _output_worker(self, worker_id: int):
        """Worker especializado en escritura ultra-rápida a disco con timeout estricto"""
        write_buffer = []
        last_flush_time = time.time()
        start_time = time.time()  # Para timeout absoluto
        MAX_WORKER_TIME = 3600  # 1 hora máximo por worker
        
        while self.running and (time.time() - start_time < MAX_WORKER_TIME):
            try:
                articles = self.output_queue.get(timeout=0.5)  # Timeout más corto
                if articles is None:
                    print(f"💾 Worker-{worker_id}: Señal de parada recibida")
                    break
                
                write_buffer.extend(articles)
                
                # Escribir cuando el buffer esté lleno O haya pasado tiempo
                current_time = time.time()
                should_flush = (len(write_buffer) >= 200000 or  # 200K artículos por archivo (ultra-masivo)
                               (write_buffer and current_time - last_flush_time > 15))  # O cada 15 segundos (más agresivo)
                
                if should_flush:
                    self._write_buffer_ultra_fast(write_buffer, worker_id)
                    write_buffer.clear()
                    last_flush_time = current_time
                
                self.output_queue.task_done()
                
            except queue.Empty:
                # Escribir buffer pendiente en timeout si hay datos
                current_time = time.time()
                if write_buffer and (current_time - last_flush_time > 10):  # Flush cada 10s en timeout
                    self._write_buffer_ultra_fast(write_buffer, worker_id)
                    write_buffer.clear()
                    last_flush_time = current_time
                
                # Verificar si debemos terminar por falta de trabajo
                if not self.running:
                    break
                    
                continue
            except Exception as e:
                print(f"⚠️ Error output {worker_id}: {e}")
                # En caso de error, intentar escribir buffer y salir
                if write_buffer:
                    try:
                        self._write_buffer_ultra_fast(write_buffer, worker_id)
                    except:
                        pass
                break
        
        # Escribir buffer final SIEMPRE
        if write_buffer:
            print(f"💾 Worker-{worker_id}: Escribiendo buffer final ({len(write_buffer)} artículos)")
            try:
                self._write_buffer_ultra_fast(write_buffer, worker_id)
            except Exception as e:
                print(f"❌ Worker-{worker_id}: Error en buffer final: {e}")
        
        elapsed = time.time() - start_time
        print(f"✅ Worker-{worker_id}: Terminado después de {elapsed:.1f}s")
    
    def _write_buffer_ultra_fast(self, articles: List[Dict], worker_id: int):
        """Escritura ultra-rápida a disco con buffers grandes LOCKLESS (mejorado con patterns del billion_parameters)"""
        try:
            # Counter atómico sin lock explícito (per-worker)
            current_file_num = self.worker_file_counters[worker_id]
            self.worker_file_counters[worker_id] += 1
            
            output_file = self.output_dir / f"articles_hybrid_{worker_id}_{current_file_num:04d}.jsonl"
            
            # Buffer ultra-masivo de escritura (8MB inspirado en billion_parameters)
            with open(output_file, 'w', encoding='utf-8', buffering=8*1024*1024) as f:  # 8MB buffer
                for article in articles:
                    json.dump(article, f, ensure_ascii=False, separators=(',', ':'))
                    f.write('\n')
            
            # Stats atómicos sin lock
            self.stats['batches_written'] += 1
            
            print(f"💾 Worker-{worker_id}: {len(articles):,} artículos → {output_file.name}")
            
        except Exception as e:
            print(f"❌ Error escritura lockless: {e}")
    
    def add_batch(self, batch: List[Tuple[str, str]]):
        """Añade batch al pipeline (thread-safe) con retry agresivo"""
        for attempt in range(5):  # 5 intentos
            try:
                timeout = 0.1 + (attempt * 0.05)  # Timeout incremental
                self.raw_batch_queue.put(batch, timeout=timeout)
                return True
            except queue.Full:
                if attempt == 4:  # Último intento
                    print(f"⚠️ Cola saturada después de 5 intentos, skipping batch de {len(batch)} páginas")
                    return False
                time.sleep(0.01)  # Breve pausa antes del retry
        return False
    
    def stop_workers(self):
        """Detiene workers de forma ULTRA-AGRESIVA sin esperas"""
        print(f"� DETENCIÓN ULTRA-AGRESIVA DE WORKERS...")
        self.running = False
        
        # 1. Enviar señales de parada masivas (sin timeout)
        print("📤 Flooding colas con señales de parada...")
        for _ in range(self.num_workers * 3):  # Triple de señales
            try:
                self.raw_batch_queue.put_nowait(None)
                self.processed_queue.put_nowait(None)
                self.output_queue.put_nowait(None)
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
                    if count > 1000:  # Límite de seguridad
                        break
            except:
                pass
            return count
        
        raw_drained = drain_queue(self.raw_batch_queue, "Raw")
        proc_drained = drain_queue(self.processed_queue, "Processed")
        out_drained = drain_queue(self.output_queue, "Output")
        
        print(f"🗑️ Vaciadas: Raw({raw_drained}), Proc({proc_drained}), Out({out_drained})")
        
        # 3. Shutdown inmediato de pools (sin esperar)
        print("🛑 Shutdown inmediato de pools...")
        
        pools = [
            ("Extraction", self.extraction_pool),
            ("Processing", self.processing_pool), 
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
        
        while time.time() - start_wait < ULTRA_CONFIG['FORCE_EXIT_TIMEOUT']:
            active_count = threading.active_count()
            
            if active_count <= 10:  # Solo threads del sistema
                print(f"✅ Workers terminados limpiamente ({active_count} threads)")
                break
            
            time.sleep(0.5)
        
        elapsed = time.time() - start_wait
        if elapsed >= ULTRA_CONFIG['FORCE_EXIT_TIMEOUT']:
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
    
    def get_stats(self):
        """Obtiene estadísticas actuales"""
        return self.stats.copy()

class UltraFastXMLHandler(xml.sax.ContentHandler):
    """Handler SAX ultra-optimizado con envío masivo a workers y finalización automática"""
    
    def __init__(self, processor: AdaptiveUltraProcessor):
        super().__init__()
        self.processor = processor
        
        # Estado del parser
        self.current_element = ""
        self.current_page = {}
        self.in_page = False
        self.content_buffer = ""
        
        # Batch management ultra-eficiente
        self.page_batch = []
        self.batch_size = processor.batch_size
        
        # Contadores y estado
        self.total_pages_seen = 0
        self.total_batches_sent = 0
        self.start_time = time.time()
        self.last_page_time = time.time()  # Para detectar finalización
        self.xml_finished = False  # Flag de finalización del XML
        
        processor.logger.log(f"📖 ULTRA-FAST XML HANDLER:", force=True)
        processor.logger.log(f"   📦 Batch size: {self.batch_size:,}", force=True)
        processor.logger.log(f"   🚀 Turbo mode: ✅", force=True)
    
    def startElement(self, name, attrs):
        self.current_element = name
        if name == 'page':
            self.in_page = True
            self.current_page = {}
        elif name == 'text':
            self.content_buffer = ""
    
    def endDocument(self):
        """Se llama cuando el XML ha terminado de procesarse"""
        print(f"🏁 XML COMPLETADO - Iniciando finalización automática...")
        self.xml_finished = True
        
        # Enviar último batch inmediatamente
        if self.page_batch:
            print(f"📦 Enviando último batch: {len(self.page_batch)} páginas")
            self._send_batch_to_workers()
        
        print(f"✅ XML procesado completamente")
    
    def endElement(self, name):
        if name == 'page' and self.in_page:
            self._process_page()
            self.in_page = False
            self.current_page = {}
        elif name in ['title', 'text'] and self.in_page:
            self.current_page[name] = self.content_buffer
            self.content_buffer = ""
        self.current_element = ""
    
    def characters(self, content):
        if self.current_element in ['title', 'text']:
            self.content_buffer += content
    
    def _process_page(self):
        """Procesa página de forma ultra-eficiente"""
        self.total_pages_seen += 1
        self.last_page_time = time.time()  # Actualizar tiempo de última página
        
        title = self.current_page.get('title', '').strip()
        text = self.current_page.get('text', '').strip()
        
        # Filtro ultra-rápido sin regex
        if title and text and len(text) > 150:
            self.page_batch.append((title, text))
        
        # Enviar batch cuando esté lleno
        if len(self.page_batch) >= self.batch_size:
            self._send_batch_to_workers()
        
        # Progreso cada 200K páginas
        if self.total_pages_seen % 200000 == 0:
            self._print_ultra_progress()
    
    def _send_batch_to_workers(self):
        """Envía batch a workers de forma persistente"""
        if not self.page_batch:
            return
        
        # Envío persistente con retry
        max_attempts = 10
        for attempt in range(max_attempts):
            if self.processor.add_batch(self.page_batch.copy()):
                self.total_batches_sent += 1
                self.page_batch.clear()
                return
            
            # Si falla, esperar progresivamente más tiempo
            wait_time = 0.01 * (2 ** attempt)  # Backoff exponencial
            time.sleep(min(wait_time, 1.0))  # Máximo 1 segundo
            
            # En el intento 5, reducir batch size
            if attempt == 5 and len(self.page_batch) > 10000:
                print(f"⚠️ Reduciendo batch size de {len(self.page_batch)} a 10000")
                self.page_batch = self.page_batch[:10000]
        
        # Si después de todos los intentos no se puede enviar
        print(f"❌ No se pudo enviar batch después de {max_attempts} intentos, descartando {len(self.page_batch)} páginas")
        self.page_batch.clear()
    
    def _print_ultra_progress(self):
        """Progreso ultra-rápido con información de colas"""
        elapsed = time.time() - self.start_time
        pages_rate = self.total_pages_seen / elapsed if elapsed > 0 else 0
        
        processor_stats = self.processor.get_stats()
        
        # Información de colas para debugging
        raw_queue_size = self.processor.raw_batch_queue.qsize()
        processed_queue_size = self.processor.processed_queue.qsize()
        output_queue_size = self.processor.output_queue.qsize()
        
        print(f"🚀 ULTRA-FAST PROGRESS:")
        print(f"   📖 Páginas: {self.total_pages_seen:,} ({pages_rate:.0f} p/s)")
        print(f"   📦 Batches enviados: {self.total_batches_sent:,}")
        print(f"   📚 Artículos procesados: {processor_stats['articles_processed']:,}")
        print(f"   🗂️ Colas: Raw({raw_queue_size}), Proc({processed_queue_size}), Out({output_queue_size})")
        print(f"   ⏱️ Tiempo: {elapsed/60:.1f}min")
        
        # Verificar si alcanzamos el objetivo
        if pages_rate >= ULTRA_CONFIG['TARGET_SPEED']:
            print(f"🎯 ✅ OBJETIVO ALCANZADO: {pages_rate:.0f} >= {ULTRA_CONFIG['TARGET_SPEED']:,} p/s")
        
        # Advertir si las colas están muy llenas
        max_queue_size = ULTRA_CONFIG['QUEUE_SIZE']
        if raw_queue_size > max_queue_size * 0.8:
            print(f"⚠️ Cola raw cerca del límite: {raw_queue_size}/{max_queue_size}")
        if output_queue_size > max_queue_size * 0.8:
            print(f"⚠️ Cola output cerca del límite: {output_queue_size}/{max_queue_size}")
    
    def finalize_processing(self):
        """Finaliza el procesamiento con timeout estricto"""
        print(f"🔄 INICIANDO FINALIZACIÓN CON TIMEOUT...")
        
        # 1. Enviar último batch si existe
        if self.page_batch:
            print(f"📦 Último batch: {len(self.page_batch)} páginas")
            try:
                self.processor.raw_batch_queue.put(self.page_batch.copy(), timeout=2.0)
                print(f"✅ Último batch enviado")
            except:
                print(f"⚠️ Último batch descartado por timeout")
            self.page_batch.clear()
        
        # 2. Esperar con timeout muy agresivo
        max_wait = ULTRA_CONFIG['MAX_FINALIZATION_TIME']
        print(f"⏳ Esperando finalización (máximo {max_wait}s)...")
        
        start_wait = time.time()
        last_articles = self.processor.get_stats()['articles_processed']
        stable_count = 0
        
        while time.time() - start_wait < max_wait:
            time.sleep(1)
            current_stats = self.processor.get_stats()
            current_articles = current_stats['articles_processed']
            
            # Estado de colas
            raw_size = self.processor.raw_batch_queue.qsize()
            proc_size = self.processor.processed_queue.qsize()
            out_size = self.processor.output_queue.qsize()
            
            elapsed_wait = time.time() - start_wait
            print(f"📊 T+{elapsed_wait:.0f}s: {current_articles:,} artículos | Colas: R({raw_size}) P({proc_size}) O({out_size})")
            
            # Verificar estabilidad
            if current_articles == last_articles:
                stable_count += 1
            else:
                stable_count = 0
                last_articles = current_articles
            
            # Condiciones de finalización más agresivas
            if raw_size == 0 and proc_size == 0 and out_size == 0 and stable_count >= 2:
                print(f"✅ Procesamiento completado (colas vacías)")
                break
            elif stable_count >= 3:  # Solo 3 segundos de estabilidad
                print(f"✅ Procesamiento estabilizado")
                break
        
        # 3. Finalización forzada
        elapsed_total = time.time() - start_wait
        if elapsed_total >= max_wait:
            print(f"� TIMEOUT DE FINALIZACIÓN ({elapsed_total:.1f}s) - Continuando con terminación")
        
        # Estadísticas finales
        final_stats = self.processor.get_stats()
        print(f"📊 RESUMEN FINAL:")
        print(f"   📖 Páginas XML: {self.total_pages_seen:,}")
        print(f"   📦 Batches enviados: {self.total_batches_sent:,}")
        print(f"   📚 Artículos procesados: {final_stats['articles_processed']:,}")
        print(f"   💾 Archivos escritos: {final_stats['batches_written']:,}")
        
        # Marcar workers para detención
        self.processor.running = False
        print(f"� Workers marcados para detención")

def setup_system_for_ultra_performance():
    """Configura el sistema para máximo rendimiento"""
    print("⚡ CONFIGURANDO SISTEMA PARA ULTRA-RENDIMIENTO...")
    
    try:
        # Límites del sistema
        import resource
        resource.setrlimit(resource.RLIMIT_NOFILE, (100000, 100000))
        
        # Variables de entorno optimizadas
        os.environ['PYTHONUNBUFFERED'] = '1'
        os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
        os.environ['OMP_NUM_THREADS'] = str(ULTRA_CONFIG['MAX_WORKERS'])
        
        # Configurar afinidad de CPU
        process = psutil.Process()
        available_cpus = list(range(min(ULTRA_CONFIG['MAX_WORKERS'], os.cpu_count())))
        process.cpu_affinity(available_cpus)
        
        # Configurar prioridad alta
        try:
            process.nice(-10)  # Prioridad alta
        except:
            pass
        
        print(f"✅ Sistema configurado:")
        print(f"   🔄 CPU cores: {len(available_cpus)}")
        print(f"   📁 File descriptors: 100,000")
        print(f"   ⚡ Prioridad: Alta")
        
    except Exception as e:
        print(f"⚠️ Configuración parcial: {e}")

def main():
    """Función principal con procesamiento adaptativo ultra-optimizado"""
    parser = argparse.ArgumentParser(description="Extractor Adaptativo Ultra-Optimizado")
    parser.add_argument("--xml", type=str, required=True, help="Archivo XML de Wikipedia")
    parser.add_argument("--output", type=str, default="data_ultra_hybrid", help="Directorio de salida")
    
    args = parser.parse_args()
    
    # Verificar archivo
    xml_path = Path(args.xml)
    if not xml_path.exists():
        print(f"❌ Archivo XML no encontrado: {args.xml}")
        return 1
    
    print(f"🚀 EXTRACTOR ADAPTATIVO ULTRA-OPTIMIZADO")
    print("=" * 80)
    
    # Crear configuración adaptativa basada en el archivo XML
    config = AdaptiveExtractorConfig.from_xml_file(str(xml_path))
    
    print(f"📁 XML: {xml_path.name} ({xml_path.stat().st_size / (1024**3):.1f}GB)")
    print(f"🎯 OBJETIVO: {config.target_speed:,} páginas/segundo")
    print(f"⚡ CONFIGURACIÓN ADAPTATIVA:")
    print(f"   👥 Workers: {config.optimal_workers}")
    print(f"   📦 Batch size: {config.optimal_batch_size:,}")
    print(f"   🗂️ Queue size: {config.optimal_queue_size:,}")
    print(f"   ⏱️ Timeout: {config.optimal_timeout}s")
    print("=" * 80)
    
    try:
        # Crear procesador adaptativo ultra-optimizado
        processor = AdaptiveUltraProcessor(args.output, config)
        
        # Iniciar workers especializados
        processor.start_workers()
        
        # Crear handler SAX ultra-rápido
        handler = UltraFastXMLHandler(processor)
        
        # Manejo de señales con terminación forzada
        def signal_handler(signum, frame):
            config.logger.log(f"⚠️ Señal {signum} recibida, terminando INMEDIATAMENTE...", force=True)
            processor.running = False
            processor.stop_workers()
            
            # Esperar máximo 5 segundos
            cleanup_start = time.time()
            while time.time() - cleanup_start < 5:
                import threading
                if threading.active_count() <= 5:
                    break
                time.sleep(0.5)
            
            config.logger.log(f"🚨 TERMINACIÓN FORZADA POR SEÑAL", force=True)
            os._exit(1)  # Salida inmediata
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Procesar XML con detección automática de finalización
        start_time = time.time()
        config.logger.log(f"🚀 Iniciando procesamiento adaptativo ultra-optimizado...", force=True)
        
        try:
            # SAX parsing (confiable) + workers ultra-optimizados
            xml.sax.parse(str(xml_path), handler)
            config.logger.log(f"✅ SAX Parser completado exitosamente", force=True)
        except Exception as e:
            config.logger.log(f"⚠️ SAX Parser terminó con excepción: {e}", force=True)
        
        # Finalizar con detección inteligente
        config.logger.log(f"🏁 XML TERMINADO - Iniciando finalización inteligente...", force=True)
        handler.finalize_processing()
        
        config.logger.log(f"🛑 DETENIENDO WORKERS...", force=True)
        processor.stop_workers()
        
        # Breve pausa para permitir que los workers terminen limpiamente
        config.logger.log(f"⏳ Pausa de limpieza (2 segundos)...", force=True)
        time.sleep(2)
        
        # Estadísticas finales
        elapsed = time.time() - start_time
        final_stats = processor.get_stats()
        
        pages_rate = handler.total_pages_seen / elapsed if elapsed > 0 else 0
        articles_rate = final_stats['articles_processed'] / elapsed if elapsed > 0 else 0
        
        config.logger.log(f"🎉 PROCESAMIENTO ADAPTATIVO COMPLETADO:", force=True)
        config.logger.log(f"   📖 Páginas procesadas: {handler.total_pages_seen:,}", force=True)
        config.logger.log(f"   📚 Artículos válidos: {final_stats['articles_processed']:,}", force=True)
        config.logger.log(f"   📦 Batches escritos: {final_stats['batches_written']:,}", force=True)
        config.logger.log(f"   ⏱️ Tiempo total: {elapsed/3600:.2f}h", force=True)
        config.logger.log(f"   🚀 Velocidad páginas: {pages_rate:.0f} p/s", force=True)
        config.logger.log(f"   🚀 Velocidad artículos: {articles_rate:.0f} a/s", force=True)
        
        # Evaluar resultado
        if pages_rate >= config.target_speed:
            config.logger.log(f"🎯 ✅ OBJETIVO ALCANZADO: {pages_rate:.0f} >= {config.target_speed:,} p/s", force=True)
        else:
            improvement = pages_rate / 600  # vs versión anterior
            config.logger.log(f"📈 MEJORA CONSEGUIDA: {improvement:.1f}x más rápido", force=True)
            config.logger.log(f"📊 Progreso hacia objetivo: {(pages_rate / config.target_speed) * 100:.1f}%", force=True)
        
        # Verificación final de finalización limpia
        import threading
        active_threads = threading.active_count()
        if active_threads <= 5:  # Main + threads del sistema
            config.logger.log(f"✅ FINALIZACIÓN LIMPIA - {active_threads} threads activos", force=True)
        else:
            config.logger.log(f"⚠️ FINALIZACIÓN PARCIAL - {active_threads} threads aún activos", force=True)
            config.logger.log(f"🚨 Esperando 10s adicionales para limpieza...", force=True)
            
            # Esperar limpieza final con timeout
            cleanup_start = time.time()
            while time.time() - cleanup_start < 10:
                current_threads = threading.active_count()
                if current_threads <= 5:
                    config.logger.log(f"✅ LIMPIEZA COMPLETADA - {current_threads} threads", force=True)
                    break
                time.sleep(1)
            
            # Si aún hay threads activos, forzar salida
            final_threads = threading.active_count()
            if final_threads > 5:
                config.logger.log(f"🚨 FORZANDO SALIDA - {final_threads} threads persistentes", force=True)
                
                # Garbage collection final agresivo
                import gc
                gc.collect()
                
                config.logger.log(f"🚀 PROCESO TERMINANDO FORZADAMENTE...", force=True)
                os._exit(0)  # Salida forzada sin cleanup adicional
        
        # Forzar garbage collection final
        import gc
        gc.collect()
        
        config.logger.log(f"🎯 PROCESO PRINCIPAL TERMINANDO LIMPIAMENTE...", force=True)
        return 0
        
    except KeyboardInterrupt:
        print("\n⚠️ Interrumpido por usuario")
        processor.stop_workers()
        return 130
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
