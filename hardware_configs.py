#!/usr/bin/env python3
"""
🚀 HARDWARE CONFIGURATIONS - Configuraciones Optimizadas por Hardware
====================================================================
Configuraciones específicas para máxima eficiencia en diferentes equipos

EQUIPOS OBJETIVO:
- GH200: 500GB RAM, 72 cores ARM Neoverse
- 8xH100: ~750GB RAM, múltiples GPUs NVIDIA H100
"""

import os
import psutil
from typing import Dict, Any

def detect_hardware() -> str:
    """Detecta automáticamente el tipo de hardware"""
    total_ram_gb = psutil.virtual_memory().total / (1024**3)
    cpu_count = os.cpu_count()
    
    if total_ram_gb >= 450:  # GH200
        return "GH200"
    elif total_ram_gb >= 700:  # 8xH100 setup
        return "8xH100"
    else:
        return "STANDARD"

def get_hardware_config(hardware_type: str = None, dataset_size_articles: int = None) -> Dict[str, Any]:
    """Obtiene configuración optimizada para el hardware detectado/especificado y tamaño de dataset"""
    
    if hardware_type is None:
        hardware_type = detect_hardware()
    
    configs = {
        "GH200": {
            # ========= CONFIGURACIÓN GH200 (500GB RAM, 72 cores ARM) =========
            'MAX_WORKERS': 288,  # 4x cores para máximo paralelismo ARM
            'BATCH_SIZE': 200000,  # Batches ultra-masivos aprovechando 500GB RAM
            'QUEUE_SIZE': 2000,  # Colas masivas para evitar bottlenecks
            'MEMORY_BUFFER_GB': 400,  # 80% de RAM para buffers (400/500GB)
            'TARGET_SPEED': 150000,  # 150K páginas/segundo objetivo agresivo
            'PROCESSING_THREADS': 4,  # Más threads especializados
            'AUTO_FLUSH_THRESHOLD': 400000,  # Flush cada 400K artículos
            'TURBO_MODE': True,
            'ULTRA_AGGRESSIVE_MODE': True,
            'LOCKLESS_STATS': True,
            'STREAMING_BUFFERS': True,
            'FORCE_EXIT_TIMEOUT': 15,
            'WORKER_TIMEOUT': 0.3,  # Timeout más agresivo
            'MAX_FINALIZATION_TIME': 20,
            
            # ========= CONFIGURACIÓN SEGUNDA ETAPA GH200 =========
            'CATEGORY_WORKERS': 96,  # 1/3 de workers para categorización
            'CONVERSATION_WORKERS': 96,  # 1/3 para generación de conversaciones
            'OUTPUT_WORKERS': 96,  # 1/3 para escritura optimizada
            'DATASET_QUEUE_SIZE': 5000,  # Colas masivas para dataset
            'CONVERSATION_BATCH_SIZE': 50000,  # Batches masivos de conversaciones
            'MEMORY_EFFICIENT_MODE': True,
            'ADAPTIVE_BATCHING': True,
        },
        
        "8xH100": {
            # ========= CONFIGURACIÓN 8xH100 (750GB+ RAM, múltiples GPUs) =========
            'MAX_WORKERS': 512,  # Máximo paralelismo para 8 GPUs
            'BATCH_SIZE': 300000,  # Batches ultra-masivos con 750GB+ RAM
            'QUEUE_SIZE': 4000,  # Colas aún más masivas
            'MEMORY_BUFFER_GB': 600,  # 600GB de buffer
            'TARGET_SPEED': 250000,  # 250K páginas/segundo objetivo extremo
            'PROCESSING_THREADS': 8,  # Threads especializados por GPU
            'AUTO_FLUSH_THRESHOLD': 500000,  # Flush cada 500K artículos
            'TURBO_MODE': True,
            'ULTRA_AGGRESSIVE_MODE': True,
            'LOCKLESS_STATS': True,
            'STREAMING_BUFFERS': True,
            'FORCE_EXIT_TIMEOUT': 20,
            'WORKER_TIMEOUT': 0.2,  # Timeout ultra-agresivo
            'MAX_FINALIZATION_TIME': 30,
            
            # ========= CONFIGURACIÓN SEGUNDA ETAPA 8xH100 =========
            'CATEGORY_WORKERS': 170,  # ~1/3 de workers
            'CONVERSATION_WORKERS': 170,  # ~1/3 de workers
            'OUTPUT_WORKERS': 172,  # ~1/3 de workers (suma 512)
            'DATASET_QUEUE_SIZE': 8000,  # Colas ultra-masivas
            'CONVERSATION_BATCH_SIZE': 100000,  # Batches extremos
            'MEMORY_EFFICIENT_MODE': True,
            'ADAPTIVE_BATCHING': True,
            'GPU_ACCELERATION': True,  # Activar aceleración GPU si disponible
            'MULTI_GPU_PARALLEL': True,  # Paralelización entre GPUs
        },
        
        "STANDARD": {
            # ========= CONFIGURACIÓN ESTÁNDAR (fallback) =========
            'MAX_WORKERS': min(64, os.cpu_count() * 2),
            'BATCH_SIZE': 50000,
            'QUEUE_SIZE': 500,
            'MEMORY_BUFFER_GB': min(32, psutil.virtual_memory().total // (1024**3) // 2),
            'TARGET_SPEED': 50000,
            'PROCESSING_THREADS': 2,
            'AUTO_FLUSH_THRESHOLD': 100000,
            'TURBO_MODE': True,
            'ULTRA_AGGRESSIVE_MODE': False,
            'LOCKLESS_STATS': True,
            'STREAMING_BUFFERS': False,
            'FORCE_EXIT_TIMEOUT': 10,
            'WORKER_TIMEOUT': 1.0,
            'MAX_FINALIZATION_TIME': 15,
            
            # Segunda etapa estándar
            'CATEGORY_WORKERS': 16,
            'CONVERSATION_WORKERS': 16,
            'OUTPUT_WORKERS': 16,
            'DATASET_QUEUE_SIZE': 1000,
            'CONVERSATION_BATCH_SIZE': 10000,
            'MEMORY_EFFICIENT_MODE': False,
            'ADAPTIVE_BATCHING': False,
        }
    }
    
    base_config = configs.get(hardware_type, configs["STANDARD"])
    
    # ADAPTACIÓN AUTOMÁTICA SEGÚN TAMAÑO DEL DATASET
    if dataset_size_articles is not None:
        print(f"🎯 ADAPTANDO CONFIGURACIÓN PARA {dataset_size_articles:,} ARTÍCULOS")
        
        if dataset_size_articles < 10000:
            # Dataset muy pequeño - configuración mínima
            scale_factor = 0.02
            print(f"📉 Dataset muy pequeño - Reduciendo workers 50x")
        elif dataset_size_articles < 50000:
            # Dataset pequeño - configuración reducida
            scale_factor = 0.05
            print(f"📉 Dataset pequeño - Reduciendo workers 20x")
        elif dataset_size_articles < 200000:
            # Dataset mediano - configuración moderada
            scale_factor = 0.1
            print(f"📉 Dataset mediano - Reduciendo workers 10x")
        elif dataset_size_articles < 500000:
            # Dataset grande pero no masivo
            scale_factor = 0.3
            print(f"📊 Dataset grande - Reduciendo workers 3x")
        elif dataset_size_articles < 1000000:
            # Dataset muy grande
            scale_factor = 0.6
            print(f"📈 Dataset muy grande - Configuración casi completa")
        else:
            # Dataset masivo - configuración completa
            scale_factor = 1.0
            print(f"🚀 Dataset masivo - Configuración completa")
        
        # Aplicar factor de escala a workers
        base_config = base_config.copy()
        base_config['MAX_WORKERS'] = max(4, int(base_config['MAX_WORKERS'] * scale_factor))
        base_config['CATEGORY_WORKERS'] = max(2, int(base_config.get('CATEGORY_WORKERS', 16) * scale_factor))
        base_config['CONVERSATION_WORKERS'] = max(2, int(base_config.get('CONVERSATION_WORKERS', 16) * scale_factor))
        base_config['OUTPUT_WORKERS'] = max(2, int(base_config.get('OUTPUT_WORKERS', 16) * scale_factor))
        
        # Adaptar tamaños de cola y batch
        if scale_factor < 0.1:
            base_config['QUEUE_SIZE'] = max(10, base_config['QUEUE_SIZE'] // 20)
            base_config['DATASET_QUEUE_SIZE'] = max(50, base_config.get('DATASET_QUEUE_SIZE', 1000) // 20)
            base_config['BATCH_SIZE'] = max(100, base_config['BATCH_SIZE'] // 100)
            base_config['CONVERSATION_BATCH_SIZE'] = max(1000, base_config.get('CONVERSATION_BATCH_SIZE', 10000) // 10)
        elif scale_factor < 0.3:
            base_config['QUEUE_SIZE'] = max(50, base_config['QUEUE_SIZE'] // 10)
            base_config['DATASET_QUEUE_SIZE'] = max(100, base_config.get('DATASET_QUEUE_SIZE', 1000) // 10)
            base_config['BATCH_SIZE'] = max(1000, base_config['BATCH_SIZE'] // 20)
            base_config['CONVERSATION_BATCH_SIZE'] = max(5000, base_config.get('CONVERSATION_BATCH_SIZE', 10000) // 2)
        
        print(f"📊 CONFIGURACIÓN ADAPTADA:")
        print(f"   🔄 Workers: {base_config['MAX_WORKERS']} (Cat: {base_config.get('CATEGORY_WORKERS', 'N/A')}, Conv: {base_config.get('CONVERSATION_WORKERS', 'N/A')}, Out: {base_config.get('OUTPUT_WORKERS', 'N/A')})")
        print(f"   📦 Batch size: {base_config['BATCH_SIZE']:,}")
        print(f"   🗂️ Queue size: {base_config['QUEUE_SIZE']:,}")
    
    return base_config

def print_hardware_info():
    """Imprime información del hardware detectado"""
    hardware_type = detect_hardware()
    config = get_hardware_config(hardware_type)
    
    total_ram = psutil.virtual_memory().total / (1024**3)
    cpu_count = os.cpu_count()
    
    print(f"🖥️  HARDWARE DETECTADO: {hardware_type}")
    print(f"💾 RAM Total: {total_ram:.1f}GB")
    print(f"🔄 CPU Cores: {cpu_count}")
    print(f"⚡ Workers Configurados: {config['MAX_WORKERS']}")
    print(f"📦 Batch Size: {config['BATCH_SIZE']:,}")
    print(f"🎯 Target Speed: {config['TARGET_SPEED']:,} páginas/segundo")
    print(f"💾 Memory Buffer: {config['MEMORY_BUFFER_GB']}GB")
    
if __name__ == "__main__":
    print_hardware_info()
