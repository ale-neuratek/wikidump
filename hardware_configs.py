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
            
            # Configuraciones específicas para manejo de colas
            'MAX_QUEUE_RETRIES': 100,  # Base para datasets normales
            'QUEUE_TIMEOUT': 2.0,  # Timeout base para reintentos
            
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
            
            # Configuraciones específicas para manejo de colas
            'MAX_QUEUE_RETRIES': 200,  # Base más alta para hardware premium
            'QUEUE_TIMEOUT': 1.0,  # Timeout base para reintentos
            
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
            
            # Configuraciones específicas para manejo de colas
            'MAX_QUEUE_RETRIES': 30,  # Base conservadora para hardware estándar
            'QUEUE_TIMEOUT': 5.0,  # Timeout más permisivo
            
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
            queue_multiplier = 0.1
            batch_multiplier = 0.01
            print(f"📉 Dataset muy pequeño - Configuración mínima")
        elif dataset_size_articles < 50000:
            # Dataset pequeño - configuración reducida
            scale_factor = 0.05
            queue_multiplier = 0.2
            batch_multiplier = 0.05
            print(f"📉 Dataset pequeño - Configuración reducida")
        elif dataset_size_articles < 200000:
            # Dataset mediano - configuración moderada
            scale_factor = 0.1
            queue_multiplier = 0.4
            batch_multiplier = 0.1
            print(f"📉 Dataset mediano - Configuración moderada")
        elif dataset_size_articles < 500000:
            # Dataset grande pero no masivo
            scale_factor = 0.3
            queue_multiplier = 0.7
            batch_multiplier = 0.3
            print(f"📊 Dataset grande - Configuración parcial")
        elif dataset_size_articles < 1000000:
            # Dataset muy grande
            scale_factor = 0.6
            queue_multiplier = 0.9
            batch_multiplier = 0.6
            print(f"📈 Dataset muy grande - Configuración casi completa")
        elif dataset_size_articles < 2000000:
            # Dataset masivo - configuración completa + optimizaciones anti-bloqueo
            scale_factor = 1.0
            queue_multiplier = 1.5  # 50% más colas para datasets masivos
            batch_multiplier = 0.8  # Batches ligeramente más pequeños para flujo constante
            print(f"🚀 Dataset masivo (1-2M) - Configuración optimizada anti-bloqueo")
        else:
            # Dataset ultra-masivo - configuración extrema
            scale_factor = 1.0
            queue_multiplier = 2.0  # Colas dobles para datasets ultra-masivos
            batch_multiplier = 0.6  # Batches más pequeños para máximo throughput
            print(f"🔥 Dataset ultra-masivo (>2M) - Configuración extrema")
        
        # Aplicar factor de escala a workers
        base_config = base_config.copy()
        base_config['MAX_WORKERS'] = max(4, int(base_config['MAX_WORKERS'] * scale_factor))
        base_config['CATEGORY_WORKERS'] = max(2, int(base_config.get('CATEGORY_WORKERS', 16) * scale_factor))
        base_config['CONVERSATION_WORKERS'] = max(2, int(base_config.get('CONVERSATION_WORKERS', 16) * scale_factor))
        base_config['OUTPUT_WORKERS'] = max(2, int(base_config.get('OUTPUT_WORKERS', 16) * scale_factor))
        
        # Adaptar tamaños de cola con multiplicadores específicos
        base_config['QUEUE_SIZE'] = max(10, int(base_config['QUEUE_SIZE'] * queue_multiplier))
        base_config['DATASET_QUEUE_SIZE'] = max(50, int(base_config.get('DATASET_QUEUE_SIZE', 1000) * queue_multiplier))
        
        # Adaptar batch sizes
        base_config['BATCH_SIZE'] = max(100, int(base_config['BATCH_SIZE'] * batch_multiplier))
        base_config['CONVERSATION_BATCH_SIZE'] = max(1000, int(base_config.get('CONVERSATION_BATCH_SIZE', 10000) * batch_multiplier))
        
        # Configuraciones específicas para datasets masivos (>1.3M artículos)
        if dataset_size_articles > 1300000:
            print(f"🔧 APLICANDO OPTIMIZACIONES ANTI-BLOQUEO PARA DATASETS >1.3M")
            
            # Aumentar significativamente los límites de reintentos para colas
            base_config['MAX_QUEUE_RETRIES'] = min(500, dataset_size_articles // 5000)  # Escalar con dataset
            base_config['QUEUE_TIMEOUT'] = 0.1  # Timeout más agresivo
            
            # Configuración dinámica de flush más frecuente
            base_config['AUTO_FLUSH_THRESHOLD'] = min(base_config.get('AUTO_FLUSH_THRESHOLD', 100000), 
                                                     dataset_size_articles // 10)  # Flush cada 10% del dataset
            
            # Buffer de memoria expandido para datasets masivos
            if hardware_type in ["GH200", "8xH100"]:
                base_config['MEMORY_BUFFER_GB'] = min(base_config['MEMORY_BUFFER_GB'] * 1.2, 
                                                     psutil.virtual_memory().total / (1024**3) * 0.85)
            
            # Configuraciones de timeout más permisivas para datasets masivos
            base_config['WORKER_TIMEOUT'] = max(base_config.get('WORKER_TIMEOUT', 1.0), 0.5)
            base_config['FORCE_EXIT_TIMEOUT'] = max(base_config.get('FORCE_EXIT_TIMEOUT', 10), 30)
            base_config['MAX_FINALIZATION_TIME'] = max(base_config.get('MAX_FINALIZATION_TIME', 15), 60)
            
            print(f"   ⚡ Max queue retries: {base_config['MAX_QUEUE_RETRIES']}")
            print(f"   ⏱️ Queue timeout: {base_config['QUEUE_TIMEOUT']}s")
            print(f"   💾 Auto flush threshold: {base_config['AUTO_FLUSH_THRESHOLD']:,}")
        
        print(f"📊 CONFIGURACIÓN ADAPTADA:")
        print(f"   🔄 Workers: {base_config['MAX_WORKERS']} (Cat: {base_config.get('CATEGORY_WORKERS', 'N/A')}, Conv: {base_config.get('CONVERSATION_WORKERS', 'N/A')}, Out: {base_config.get('OUTPUT_WORKERS', 'N/A')})")
        print(f"   📦 Batch size: {base_config['BATCH_SIZE']:,}")
        print(f"   🗂️ Queue size: {base_config['QUEUE_SIZE']:,} (Dataset: {base_config.get('DATASET_QUEUE_SIZE', 'N/A'):,})")
    
    return base_config

def print_hardware_info(dataset_size: int = None):
    """Imprime información del hardware detectado y configuración optimizada"""
    hardware_type = detect_hardware()
    config = get_hardware_config(hardware_type, dataset_size)
    
    total_ram = psutil.virtual_memory().total / (1024**3)
    cpu_count = os.cpu_count()
    
    print(f"\n🖥️  INFORMACIÓN DE HARDWARE Y CONFIGURACIÓN")
    print(f"{'='*60}")
    print(f"🖥️  Hardware detectado: {hardware_type}")
    print(f"💾 RAM Total: {total_ram:.1f}GB")
    print(f"🔄 CPU Cores: {cpu_count}")
    print(f"{'='*60}")
    print(f"⚡ Workers configurados: {config['MAX_WORKERS']}")
    print(f"📦 Batch size: {config['BATCH_SIZE']:,}")
    print(f"🗂️ Queue size: {config['QUEUE_SIZE']:,}")
    print(f"🎯 Target speed: {config['TARGET_SPEED']:,} páginas/segundo")
    print(f"💾 Memory buffer: {config['MEMORY_BUFFER_GB']}GB")
    
    if dataset_size:
        print(f"\n📊 CONFIGURACIÓN ESPECÍFICA PARA {dataset_size:,} ARTÍCULOS:")
        print(f"   ⚡ Queue retries: {config.get('MAX_QUEUE_RETRIES', 30)}")
        print(f"   ⏱️ Queue timeout: {config.get('QUEUE_TIMEOUT', 1.0)}s")
        print(f"   💾 Auto flush threshold: {config.get('AUTO_FLUSH_THRESHOLD', 100000):,}")
        
        if dataset_size > 1300000:
            print(f"   🔥 OPTIMIZACIONES ANTI-BLOQUEO ACTIVADAS")
    
    print(f"{'='*60}")

def test_configuration_for_dataset_size(dataset_size: int):
    """Función de prueba para verificar configuraciones para diferentes tamaños de dataset"""
    print(f"\n🧪 PRUEBA DE CONFIGURACIÓN PARA {dataset_size:,} ARTÍCULOS")
    
    config = diagnose_dataset_configuration(dataset_size)
    optimized = optimize_for_queue_issues(config, dataset_size)
    
    print(f"\n🔬 COMPARACIÓN ANTES/DESPUÉS:")
    print(f"   Queue retries: {config.get('MAX_QUEUE_RETRIES', 30)} → {optimized['MAX_QUEUE_RETRIES']}")
    print(f"   Queue timeout: {config.get('QUEUE_TIMEOUT', 1.0):.2f}s → {optimized['QUEUE_TIMEOUT']:.2f}s")
    print(f"   Queue size: {config['QUEUE_SIZE']:,} → {optimized['QUEUE_SIZE']:,}")
    
    return optimized

def diagnose_dataset_configuration(dataset_size_articles: int, hardware_type: str = None) -> Dict[str, Any]:
    """Diagnóstica y recomienda configuración óptima para un dataset específico"""
    
    if hardware_type is None:
        hardware_type = detect_hardware()
    
    config = get_hardware_config(hardware_type, dataset_size_articles)
    
    print(f"\n🔍 DIAGNÓSTICO DE CONFIGURACIÓN")
    print(f"{'='*60}")
    print(f"📊 Dataset: {dataset_size_articles:,} artículos")
    print(f"🖥️  Hardware: {hardware_type}")
    print(f"{'='*60}")
    
    # Análisis de riesgo de cuellos de botella
    if dataset_size_articles > 1300000:
        risk_level = "🔥 ALTO"
        recommendations = [
            f"✅ Queue retries aumentados a {config['MAX_QUEUE_RETRIES']} (era 30-100)",
            f"✅ Queue timeout optimizado a {config['QUEUE_TIMEOUT']}s (era 2-5s)",
            f"✅ Colas expandidas a {config['QUEUE_SIZE']:,} (dataset: {config['DATASET_QUEUE_SIZE']:,})",
            f"✅ Auto-flush cada {config['AUTO_FLUSH_THRESHOLD']:,} artículos",
            f"✅ Timeouts extendidos para finalización ({config['MAX_FINALIZATION_TIME']}s)"
        ]
    elif dataset_size_articles > 1000000:
        risk_level = "⚠️ MEDIO"
        recommendations = [
            f"✅ Configuración casi completa aplicada",
            f"✅ Monitoreo recomendado de colas durante procesamiento",
            f"✅ Workers balanceados para {config['MAX_WORKERS']} threads"
        ]
    else:
        risk_level = "✅ BAJO"
        recommendations = [
            f"✅ Configuración estándar suficiente",
            f"✅ Sin optimizaciones especiales necesarias"
        ]
    
    print(f"🚨 Riesgo de cuellos de botella: {risk_level}")
    print(f"\n📋 RECOMENDACIONES:")
    for rec in recommendations:
        print(f"   {rec}")
    
    # Estimación de rendimiento
    estimated_time = dataset_size_articles / config['TARGET_SPEED']
    estimated_hours = estimated_time / 3600
    
    print(f"\n⏱️  ESTIMACIÓN DE RENDIMIENTO:")
    print(f"   🎯 Target speed: {config['TARGET_SPEED']:,} artículos/segundo")
    print(f"   ⌛ Tiempo estimado: {estimated_hours:.1f} horas")
    print(f"   💾 Memoria asignada: {config['MEMORY_BUFFER_GB']}GB")
    
    # Configuración de workers detallada
    print(f"\n🔄 CONFIGURACIÓN DE WORKERS:")
    print(f"   📝 Procesamiento: {config['MAX_WORKERS']} workers")
    print(f"   🏷️  Categorización: {config.get('CATEGORY_WORKERS', 'N/A')} workers")
    print(f"   💬 Conversaciones: {config.get('CONVERSATION_WORKERS', 'N/A')} workers")
    print(f"   💾 Salida: {config.get('OUTPUT_WORKERS', 'N/A')} workers")
    
    return config

def optimize_for_queue_issues(current_config: Dict[str, Any], dataset_size: int) -> Dict[str, Any]:
    """Optimiza configuración específicamente para evitar problemas de colas"""
    
    optimized = current_config.copy()
    
    if dataset_size > 1300000:
        print(f"\n🔧 APLICANDO OPTIMIZACIONES ESPECÍFICAS PARA COLAS")
        
        # Factor de escalado basado en tamaño del dataset
        scale_factor = min(5.0, dataset_size / 1300000)
        
        # Aumentar reintentos de cola exponencialmente
        original_retries = optimized.get('MAX_QUEUE_RETRIES', 30)
        optimized['MAX_QUEUE_RETRIES'] = min(1000, int(original_retries * scale_factor))
        
        # Reducir timeout para intentos más frecuentes
        optimized['QUEUE_TIMEOUT'] = max(0.05, optimized.get('QUEUE_TIMEOUT', 1.0) / scale_factor)
        
        # Expandir colas proporcionalmente
        optimized['QUEUE_SIZE'] = int(optimized['QUEUE_SIZE'] * min(3.0, scale_factor))
        optimized['DATASET_QUEUE_SIZE'] = int(optimized.get('DATASET_QUEUE_SIZE', 1000) * min(3.0, scale_factor))
        
        # Reducir batch size para flujo más constante
        optimized['BATCH_SIZE'] = max(1000, int(optimized['BATCH_SIZE'] * 0.7))
        optimized['CONVERSATION_BATCH_SIZE'] = max(5000, int(optimized.get('CONVERSATION_BATCH_SIZE', 10000) * 0.8))
        
        # Flush más frecuente para datasets masivos
        optimized['AUTO_FLUSH_THRESHOLD'] = min(optimized.get('AUTO_FLUSH_THRESHOLD', 100000), 
                                               dataset_size // 20)  # Flush cada 5% del dataset
        
        print(f"   ⚡ Queue retries: {original_retries} → {optimized['MAX_QUEUE_RETRIES']}")
        print(f"   ⏱️ Queue timeout: {current_config.get('QUEUE_TIMEOUT', 1.0):.2f}s → {optimized['QUEUE_TIMEOUT']:.2f}s")
        print(f"   📦 Batch size: {current_config['BATCH_SIZE']:,} → {optimized['BATCH_SIZE']:,}")
        print(f"   🗂️ Queue size: {current_config['QUEUE_SIZE']:,} → {optimized['QUEUE_SIZE']:,}")
        print(f"   💾 Auto flush: {current_config.get('AUTO_FLUSH_THRESHOLD', 100000):,} → {optimized['AUTO_FLUSH_THRESHOLD']:,}")
    
    return optimized

if __name__ == "__main__":
    print_hardware_info()
    
    # Pruebas de configuración para diferentes tamaños de dataset
    print(f"\n🧪 PRUEBAS DE CONFIGURACIÓN PARA DIFERENTES TAMAÑOS:")
    test_sizes = [50000, 500000, 1000000, 1500000, 2500000, 5000000]
    
    for size in test_sizes:
        test_configuration_for_dataset_size(size)
        print(f"\n{'-'*60}")
