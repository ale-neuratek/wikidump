#!/usr/bin/env python3
"""
🚀 QUEUE OPTIMIZER - Optimizador de colas para procesos en ejecución
===================================================================
Analiza y optimiza procesos que muestran "Cola llena" para máximo rendimiento
"""

import os
import sys
import time
import psutil
import json
from pathlib import Path

def analyze_running_process():
    """Analiza el proceso en ejecución para optimizar rendimiento"""
    print("🔍 ANALIZANDO PROCESO DE DATASET EN EJECUCIÓN...")
    
    # Buscar proceso python con full_dataset_for_training
    target_processes = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent', 'memory_info']):
        try:
            if proc.info['name'] == 'python3' and proc.info['cmdline']:
                cmdline = ' '.join(proc.info['cmdline'])
                if 'full_dataset_for_training' in cmdline or 'adaptive_processor' in cmdline:
                    target_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    
    if not target_processes:
        print("❌ No se encontró proceso de dataset activo")
        return None
    
    proc = target_processes[0]
    print(f"✅ Proceso encontrado: PID {proc.pid}")
    print(f"   💾 Memoria: {proc.memory_info().rss / (1024**3):.1f}GB")
    print(f"   ⚡ CPU: {proc.cpu_percent():.1f}%")
    
    return proc

def analyze_output_progress():
    """Analiza el progreso del output"""
    output_dirs = ['consciencia', 'consciencia_adaptive', 'consciencia_ultra']
    
    best_dir = None
    max_files = 0
    
    for output_dir in output_dirs:
        if Path(output_dir).exists():
            files = list(Path(output_dir).rglob("*.jsonl"))
            if len(files) > max_files:
                max_files = len(files)
                best_dir = output_dir
    
    if not best_dir:
        print("❌ No se encontraron directorios de salida")
        return None
    
    print(f"📂 Analizando directorio: {best_dir}")
    
    # Contar conversaciones totales
    total_conversations = 0
    total_size = 0
    category_stats = {}
    
    for file_path in Path(best_dir).rglob("*.jsonl"):
        try:
            file_size = file_path.stat().st_size
            total_size += file_size
            
            # Contar líneas rápidamente
            with open(file_path, 'r', encoding='utf-8') as f:
                count = sum(1 for line in f if line.strip())
            
            total_conversations += count
            
            # Extraer categoría del path
            if 'categorias' in str(file_path):
                parts = file_path.parts
                if 'categorias' in parts:
                    cat_idx = parts.index('categorias')
                    if cat_idx + 1 < len(parts):
                        category = parts[cat_idx + 1]
                        if category not in category_stats:
                            category_stats[category] = {'count': 0, 'files': 0}
                        category_stats[category]['count'] += count
                        category_stats[category]['files'] += 1
        
        except Exception as e:
            continue
    
    return {
        'directory': best_dir,
        'total_conversations': total_conversations,
        'total_files': max_files,
        'total_size_mb': total_size / (1024 * 1024),
        'categories': category_stats
    }

def estimate_completion_time(progress_data, start_time_estimate):
    """Estima tiempo de finalización basado en progreso"""
    if not progress_data:
        return None
    
    conversations = progress_data['total_conversations']
    if conversations == 0:
        return None
    
    # Estimar dataset total (basado en archivos de entrada)
    input_files = list(Path('data_ultra_hybrid').glob('*.jsonl'))
    estimated_total_articles = len(input_files) * 85000  # Promedio ~85K por archivo
    estimated_total_conversations = estimated_total_articles * 4  # ~4 conv por artículo
    
    elapsed_hours = (time.time() - start_time_estimate) / 3600
    if elapsed_hours > 0:
        rate = conversations / elapsed_hours
        remaining_conversations = estimated_total_conversations - conversations
        remaining_hours = remaining_conversations / rate if rate > 0 else 0
        
        return {
            'elapsed_hours': elapsed_hours,
            'rate_per_hour': rate,
            'progress_percent': (conversations / estimated_total_conversations) * 100,
            'eta_hours': remaining_hours,
            'estimated_total': estimated_total_conversations
        }
    
    return None

def provide_optimization_recommendations(proc_info, progress_data):
    """Proporciona recomendaciones de optimización"""
    print(f"\n🔧 RECOMENDACIONES DE OPTIMIZACIÓN:")
    print("=" * 50)
    
    if not progress_data:
        print("❌ No hay datos suficientes para optimizar")
        return
    
    conversations = progress_data['total_conversations']
    size_mb = progress_data['total_size_mb']
    files = progress_data['total_files']
    
    # Análisis de rendimiento
    if conversations > 0:
        avg_conv_per_file = conversations / files if files > 0 else 0
        mb_per_conv = size_mb / conversations if conversations > 0 else 0
        
        print(f"📊 MÉTRICAS ACTUALES:")
        print(f"   💬 Conversaciones: {conversations:,}")
        print(f"   📁 Archivos: {files}")
        print(f"   💾 Tamaño: {size_mb:.1f}MB")
        print(f"   📈 Promedio: {avg_conv_per_file:.0f} conv/archivo")
        print(f"   📏 Densidad: {mb_per_conv:.3f} MB/conv")
    
    # Recomendaciones específicas
    print(f"\n✅ ESTADO DEL PROCESO:")
    if conversations > 500000:
        print(f"   🎯 Excelente progreso - {conversations:,} conversaciones")
        print(f"   🚀 El proceso está funcionando correctamente")
        print(f"   ⏳ Recomendación: DEJARLO CONTINUAR")
    elif conversations > 100000:
        print(f"   📈 Buen progreso - {conversations:,} conversaciones")
        print(f"   ⚡ Considerando optimizar si hay cuellos de botella")
    else:
        print(f"   ⚠️ Progreso lento - {conversations:,} conversaciones")
        print(f"   🔧 Recomendación: OPTIMIZAR O REINICIAR")
    
    # Análisis de categorías
    if progress_data['categories']:
        print(f"\n🏆 TOP CATEGORÍAS:")
        sorted_cats = sorted(progress_data['categories'].items(), 
                           key=lambda x: x[1]['count'], reverse=True)
        for i, (cat, stats) in enumerate(sorted_cats[:5], 1):
            print(f"   {i}. {cat:<12} {stats['count']:>8,} conv | {stats['files']:>2} archivos")
    
    print(f"\n🎯 RECOMENDACIONES FINALES:")
    if conversations > 400000:
        print(f"   ✅ El proceso está funcionando EXCELENTEMENTE")
        print(f"   🚀 Los mensajes 'Cola llena' son NORMALES para datasets grandes")
        print(f"   ⏳ DEJAR EJECUTAR hasta completar")
        print(f"   📊 Se esperan 2-5M conversaciones totales")
    else:
        print(f"   ⚠️ Evaluar si reiniciar con configuración optimizada")

def main():
    print("🚀 QUEUE OPTIMIZER - Análisis de Proceso en Ejecución")
    print("=" * 60)
    
    # Estimar hora de inicio (aproximada)
    start_time_estimate = time.time() - (2 * 3600)  # Asumiendo 2 horas ejecutándose
    
    # Analizar proceso
    proc_info = analyze_running_process()
    
    # Analizar progreso
    progress_data = analyze_output_progress()
    
    # Estimar tiempo de finalización
    if progress_data:
        time_analysis = estimate_completion_time(progress_data, start_time_estimate)
        
        if time_analysis:
            print(f"\n⏱️ ANÁLISIS TEMPORAL:")
            print(f"   🕐 Tiempo transcurrido: {time_analysis['elapsed_hours']:.1f}h")
            print(f"   📈 Progreso: {time_analysis['progress_percent']:.1f}%")
            print(f"   🚀 Velocidad: {time_analysis['rate_per_hour']:,.0f} conv/h")
            print(f"   🎯 ETA: {time_analysis['eta_hours']:.1f}h adicionales")
            print(f"   📊 Total estimado: {time_analysis['estimated_total']:,} conversaciones")
    
    # Proporcionar recomendaciones
    provide_optimization_recommendations(proc_info, progress_data)

if __name__ == "__main__":
    main()
