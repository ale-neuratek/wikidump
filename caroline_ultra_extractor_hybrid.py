#!/usr/bin/env python3
"""
🚀 Caroline Ultra Extractor - VERSIÓN HÍBRIDA ULTRA-OPTIMIZADA
Combina SAX parser confiable con optimizaciones masivas de rendimiento
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

# Configuración ultra-optimizada para GH200 con 500GB RAM (mejorada con patterns lockless)
ULTRA_CONFIG = {
    'MAX_WORKERS': min(256, os.cpu_count() * 4),  # Incrementado para máximo rendimiento
    'BATCH_SIZE': 100000,  # Batches ultra-masivos para aprovechar RAM
    'QUEUE_SIZE': 1000,  # Colas ultra-masivas para evitar bloqueos
    'MEMORY_BUFFER_GB': 300,  # 300GB de buffer para GH200
    'TARGET_SPEED': 100000,  # 100K páginas/segundo objetivo ultra-agresivo
    'PROCESSING_THREADS': 3,  # Threads especializados por tipo
    'AUTO_FLUSH_THRESHOLD': 200000,  # Flush cada 200K artículos (ultra-masivo)
    'TURBO_MODE': True,
    'ULTRA_AGGRESSIVE_MODE': True,  # Modo ultra-agresivo sin esperas
    'LOCKLESS_STATS': True,  # Estadísticas atómicas sin locks
    'STREAMING_BUFFERS': True,  # Buffers en modo streaming continuo
}

class UltraOptimizedProcessor:
    """Procesador ultra-optimizado con specialización de workers"""
    
    def __init__(self):
        self.num_workers = ULTRA_CONFIG['MAX_WORKERS']
        self.batch_size = ULTRA_CONFIG['BATCH_SIZE']
        
        # Pools especializados para diferentes etapas
        self.extraction_pool = ThreadPoolExecutor(max_workers=self.num_workers // 3, thread_name_prefix="extract")
        self.processing_pool = ThreadPoolExecutor(max_workers=self.num_workers // 3, thread_name_prefix="process")
        self.output_pool = ThreadPoolExecutor(max_workers=self.num_workers // 3, thread_name_prefix="output")
        
        # Colas de trabajo ultra-eficientes
        self.raw_batch_queue = queue.Queue(maxsize=ULTRA_CONFIG['QUEUE_SIZE'])
        self.processed_queue = queue.Queue(maxsize=ULTRA_CONFIG['QUEUE_SIZE'])
        self.output_queue = queue.Queue(maxsize=ULTRA_CONFIG['QUEUE_SIZE'])
        
        # Estado y estadísticas
        self.running = True
        self.stats = {
            'batches_sent': 0,
            'batches_processed': 0,
            'articles_processed': 0,
            'batches_written': 0
        }
        
        # Output management (mejorado con counter atómico lockless)
        self.output_dir = Path("data_ultra_hybrid")
        self.output_dir.mkdir(exist_ok=True)
        self.file_counter = 0  # Counter atómico simple (sin lock explícito)
        
        # Per-worker buffers lockless (inspirado en caroline_ultra_billion_parameters_500m)
        self.worker_buffers = defaultdict(list)  # Buffers por worker ID sin locks
        self.worker_file_counters = defaultdict(int)  # Contadores por worker
        
        print(f"🚀 ULTRA-OPTIMIZED PROCESSOR:")
        print(f"   🔄 Workers: Extract({self.num_workers//3}), Process({self.num_workers//3}), Output({self.num_workers//3})")
        print(f"   📦 Batch size: {self.batch_size:,}")
        print(f"   💾 Memory buffer: {ULTRA_CONFIG['MEMORY_BUFFER_GB']}GB")
    
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
        while self.running:
            try:
                raw_batch = self.raw_batch_queue.get(timeout=1)
                if raw_batch is None:
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
                    self.processed_queue.put(extracted)
                    self.stats['batches_sent'] += 1
                
                self.raw_batch_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"⚠️ Error extractor {worker_id}: {e}")
    
    def _processing_worker(self, worker_id: int):
        """Worker especializado en procesamiento intensivo con regex"""
        # Patrones locales para thread-safety
        patterns = PRECOMPILED_PATTERNS
        
        while self.running:
            try:
                batch = self.processed_queue.get(timeout=1)
                if batch is None:
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
                    self.output_queue.put(processed_articles)
                    self.stats['articles_processed'] += len(processed_articles)
                    self.stats['batches_processed'] += 1
                
                self.processed_queue.task_done()
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"⚠️ Error processor {worker_id}: {e}")
    
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
        
        # 4. Verificación rápida de threads
        import threading
        active_count = threading.active_count()
        print(f"🧵 Threads activos: {active_count}")
        
        # 5. Forzar garbage collection
        import gc
        gc.collect()
        
        print(f"✅ DETENCIÓN ULTRA-AGRESIVA COMPLETADA")
        print(f"🚨 NOTA: Algunos threads pueden seguir activos pero el proceso principal continuará")
    
    def get_stats(self):
        """Obtiene estadísticas actuales"""
        return self.stats.copy()

class UltraFastXMLHandler(xml.sax.ContentHandler):
    """Handler SAX ultra-optimizado con envío masivo a workers y finalización automática"""
    
    def __init__(self, processor: UltraOptimizedProcessor):
        super().__init__()
        self.processor = processor
        
        # Estado del parser
        self.current_element = ""
        self.current_page = {}
        self.in_page = False
        self.content_buffer = ""
        
        # Batch management ultra-eficiente
        self.page_batch = []
        self.batch_size = ULTRA_CONFIG['BATCH_SIZE']
        
        # Contadores y estado
        self.total_pages_seen = 0
        self.total_batches_sent = 0
        self.start_time = time.time()
        self.last_page_time = time.time()  # Para detectar finalización
        self.xml_finished = False  # Flag de finalización del XML
        
        print(f"📖 ULTRA-FAST XML HANDLER:")
        print(f"   📦 Batch size: {self.batch_size:,}")
        print(f"   🚀 Turbo mode: ✅")
    
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
        """Finaliza el procesamiento con detección automática y timeout inteligente"""
        print(f"🔄 INICIANDO FINALIZACIÓN INTELIGENTE...")
        
        # 1. Verificar si XML terminó correctamente
        if self.xml_finished:
            print(f"✅ XML completado correctamente")
        else:
            print(f"⚠️ XML no completado - posible interrupción")
            
            # Enviar último batch si existe
            if self.page_batch:
                print(f"📦 Último batch: {len(self.page_batch)} páginas")
                try:
                    self.processor.raw_batch_queue.put(self.page_batch.copy(), timeout=1.0)
                    print(f"✅ Último batch enviado")
                except:
                    print(f"⚠️ Último batch descartado por timeout")
                self.page_batch.clear()
        
        # 2. Esperar con timeout adaptativo basado en el volumen
        expected_time = max(5, min(30, self.total_batches_sent // 10))  # 5-30 segundos según volumen
        print(f"⏳ Esperando finalización (máximo {expected_time} segundos basado en volumen)...")
        
        start_wait = time.time()
        last_articles = self.processor.get_stats()['articles_processed']
        stable_count = 0  # Contador de estabilidad
        
        while time.time() - start_wait < expected_time:
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
            
            # Condiciones de finalización
            if raw_size == 0 and proc_size == 0 and out_size == 0:
                if stable_count >= 2:  # 2 segundos estable con colas vacías
                    print(f"✅ Procesamiento completado (colas vacías + estable)")
                    break
            elif stable_count >= 5:  # 5 segundos sin cambios
                print(f"✅ Procesamiento estabilizado (sin nuevos artículos)")
                break
        
        # 3. Finalización
        elapsed_total = time.time() - start_wait
        print(f"🛑 Finalización después de {elapsed_total:.1f}s")
        
        # Estadísticas finales
        final_stats = self.processor.get_stats()
        print(f"📊 RESUMEN FINAL:")
        print(f"   📖 Páginas XML: {self.total_pages_seen:,}")
        print(f"   📦 Batches enviados: {self.total_batches_sent:,}")
        print(f"   📚 Artículos procesados: {final_stats['articles_processed']:,}")
        print(f"   💾 Archivos escritos: {final_stats['batches_written']:,}")
        
        # Forzar detención inmediata
        self.processor.running = False
        print(f"🚨 Workers marcados para detención")

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
    """Función principal híbrida ultra-optimizada"""
    parser = argparse.ArgumentParser(description="Caroline Ultra Extractor - Versión Híbrida")
    parser.add_argument("--xml", type=str, required=True, help="Archivo XML de Wikipedia")
    parser.add_argument("--output", type=str, default="data_ultra_hybrid", help="Directorio de salida")
    
    args = parser.parse_args()
    
    # Configurar sistema
    setup_system_for_ultra_performance()
    
    # Verificar archivo
    xml_path = Path(args.xml)
    if not xml_path.exists():
        print(f"❌ Archivo XML no encontrado: {args.xml}")
        return 1
    
    print(f"🚀 CAROLINE ULTRA EXTRACTOR - VERSIÓN HÍBRIDA")
    print(f"🎯 OBJETIVO: {ULTRA_CONFIG['TARGET_SPEED']:,} páginas/segundo")
    print(f"📁 XML: {xml_path.name} ({xml_path.stat().st_size / (1024**3):.1f}GB)")
    print(f"⚡ TURBO MODE: {'✅' if ULTRA_CONFIG['TURBO_MODE'] else '❌'}")
    
    try:
        # Crear procesador ultra-optimizado
        processor = UltraOptimizedProcessor()
        
        # Iniciar workers especializados
        processor.start_workers()
        
        # Crear handler SAX ultra-rápido
        handler = UltraFastXMLHandler(processor)
        
        # Manejo de señales
        def signal_handler(signum, frame):
            print(f"\n⚠️ Señal recibida, finalizando...")
            processor.stop_workers()
            sys.exit(0)
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        # Procesar XML con detección automática de finalización
        start_time = time.time()
        print(f"🚀 Iniciando procesamiento híbrido ultra-optimizado...")
        
        try:
            # SAX parsing (confiable) + workers ultra-optimizados
            xml.sax.parse(str(xml_path), handler)
            print(f"✅ SAX Parser completado exitosamente")
        except Exception as e:
            print(f"⚠️ SAX Parser terminó con excepción: {e}")
        
        # Finalizar con detección inteligente
        print(f"🏁 XML TERMINADO - Iniciando finalización inteligente...")
        handler.finalize_processing()
        
        print(f"🛑 DETENIENDO WORKERS...")
        processor.stop_workers()
        
        # Breve pausa para permitir que los workers terminen limpiamente
        print(f"⏳ Pausa de limpieza (2 segundos)...")
        time.sleep(2)
        
        # Estadísticas finales
        elapsed = time.time() - start_time
        final_stats = processor.get_stats()
        
        pages_rate = handler.total_pages_seen / elapsed if elapsed > 0 else 0
        articles_rate = final_stats['articles_processed'] / elapsed if elapsed > 0 else 0
        
        print(f"\n🎉 PROCESAMIENTO HÍBRIDO COMPLETADO:")
        print(f"   📖 Páginas procesadas: {handler.total_pages_seen:,}")
        print(f"   📚 Artículos válidos: {final_stats['articles_processed']:,}")
        print(f"   📦 Batches escritos: {final_stats['batches_written']:,}")
        print(f"   ⏱️ Tiempo total: {elapsed/3600:.2f}h")
        print(f"   🚀 Velocidad páginas: {pages_rate:.0f} p/s")
        print(f"   🚀 Velocidad artículos: {articles_rate:.0f} a/s")
        
        # Evaluar resultado
        if pages_rate >= ULTRA_CONFIG['TARGET_SPEED']:
            print(f"🎯 ✅ OBJETIVO ALCANZADO: {pages_rate:.0f} >= {ULTRA_CONFIG['TARGET_SPEED']:,} p/s")
        else:
            improvement = pages_rate / 600  # vs versión anterior
            print(f"📈 MEJORA CONSEGUIDA: {improvement:.1f}x más rápido")
            print(f"📊 Progreso hacia objetivo: {(pages_rate / ULTRA_CONFIG['TARGET_SPEED']) * 100:.1f}%")
        
        # Verificación final de finalización limpia
        import threading
        active_threads = threading.active_count()
        if active_threads <= 5:  # Main + threads del sistema
            print(f"✅ FINALIZACIÓN LIMPIA - {active_threads} threads activos")
        else:
            print(f"⚠️ FINALIZACIÓN PARCIAL - {active_threads} threads aún activos")
            print(f"🚨 El proceso debería terminar automáticamente en unos segundos")
        
        # Forzar garbage collection final
        import gc
        gc.collect()
        
        print(f"🎯 PROCESO PRINCIPAL TERMINANDO...")
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
