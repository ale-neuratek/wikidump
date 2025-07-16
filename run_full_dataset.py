#!/usr/bin/env python3
"""
🚀 PROCESADOR MASIVO DE DATASET COMPLETO
=======================================
Ejecuta el pipeline completo con toda la configuración optimizada para datasets masivos
"""

import os
import sys
import time
import subprocess
from pathlib import Path

def run_full_dataset_processing():
    """Ejecuta el procesamiento del dataset completo con configuración optimizada"""
    
    print("🚀 PROCESAMIENTO MASIVO DEL DATASET COMPLETO")
    print("=" * 60)
    
    # Directorios
    input_dir = "/home/ubuntu/wikidump_workspace/wikidump/data_ultra_hybrid"
    output_dir = "/home/ubuntu/wikidump_workspace/wikidump/consciencia_completa"
    
    # Verificar entrada
    if not Path(input_dir).exists():
        print(f"❌ Directorio de entrada no encontrado: {input_dir}")
        return 1
    
    # Contar archivos de entrada
    input_files = list(Path(input_dir).glob("*.jsonl"))
    if not input_files:
        print(f"❌ No se encontraron archivos .jsonl en {input_dir}")
        return 1
    
    print(f"📂 Directorio de entrada: {input_dir}")
    print(f"📁 Directorio de salida: {output_dir}")
    print(f"📄 Archivos de entrada: {len(input_files)} archivos")
    
    # Estimar tamaño total del dataset
    total_size_mb = sum(f.stat().st_size for f in input_files) / (1024 * 1024)
    estimated_articles = int(total_size_mb * 400)  # ~400 artículos por MB
    
    print(f"📊 Tamaño total: {total_size_mb:.1f}MB")
    print(f"📊 Artículos estimados: {estimated_articles:,}")
    
    # Confirmación del usuario
    print(f"\n⚠️ PROCESAMIENTO MASIVO:")
    print(f"   📊 Se procesarán ~{estimated_articles:,} artículos")
    print(f"   ⏱️ Tiempo estimado: 2-3 horas")
    print(f"   💾 Espacio requerido: ~10-15GB")
    
    confirm = input(f"\n🚀 ¿Proceder con el procesamiento masivo? (s/N): ").strip().lower()
    if confirm not in ['s', 'si', 'y', 'yes']:
        print("❌ Procesamiento cancelado por el usuario")
        return 0
    
    # Limpiar directorio de salida anterior
    if Path(output_dir).exists():
        print(f"🗑️ Limpiando directorio de salida anterior...")
        import shutil
        shutil.rmtree(output_dir)
    
    # Ejecutar adaptive_processor
    print(f"\n🚀 INICIANDO PROCESAMIENTO MASIVO...")
    print(f"⏰ Inicio: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    start_time = time.time()
    
    cmd = [
        "python3", 
        "adaptive_processor.py",
        "--input", input_dir,
        "--output", output_dir
    ]
    
    print(f"🔧 Comando: {' '.join(cmd)}")
    
    try:
        # Ejecutar el comando con salida en tiempo real
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Mostrar output en tiempo real
        for line in process.stdout:
            print(line.rstrip())
        
        # Esperar a que termine
        return_code = process.wait()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print(f"\n⏰ Fin: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"⏱️ Duración total: {duration/3600:.2f} horas ({duration:.0f}s)")
        
        if return_code == 0:
            print(f"\n🎉 PROCESAMIENTO COMPLETADO EXITOSAMENTE")
            
            # Verificar resultados
            verify_results(output_dir)
            
            return 0
        else:
            print(f"\n❌ PROCESAMIENTO FALLÓ (código: {return_code})")
            return return_code
            
    except KeyboardInterrupt:
        print(f"\n🛑 PROCESAMIENTO INTERRUMPIDO POR EL USUARIO")
        process.terminate()
        return 1
    except Exception as e:
        print(f"\n💥 ERROR DURANTE EL PROCESAMIENTO: {e}")
        return 1

def verify_results(output_dir: str):
    """Verifica los resultados del procesamiento"""
    
    print(f"\n🔍 VERIFICANDO RESULTADOS...")
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"❌ Directorio de salida no existe: {output_dir}")
        return
    
    # Contar categorías
    categorias_dir = output_path / "categorias"
    if categorias_dir.exists():
        categories = [d for d in categorias_dir.iterdir() if d.is_dir()]
        print(f"📁 Categorías creadas: {len(categories)}")
        
        # Contar archivos y conversaciones por categoría
        total_files = 0
        total_conversations = 0
        
        for cat_dir in categories:
            jsonl_files = list(cat_dir.glob("*.jsonl"))
            total_files += len(jsonl_files)
            
            cat_conversations = 0
            for jsonl_file in jsonl_files:
                try:
                    with open(jsonl_file, 'r') as f:
                        cat_conversations += sum(1 for line in f if line.strip())
                except:
                    pass
            
            total_conversations += cat_conversations
            print(f"   📂 {cat_dir.name}: {len(jsonl_files)} archivos, {cat_conversations:,} conversaciones")
        
        print(f"📄 Total archivos JSONL: {total_files}")
        print(f"💬 Total conversaciones: {total_conversations:,}")
    
    # Verificar estadísticas
    stats_dir = output_path / "estadisticas"
    if stats_dir.exists():
        print(f"📊 Directorio de estadísticas: ✅")
    
    # Verificar archivo conscious.txt
    conscious_file = output_path / "conscious.txt"
    if conscious_file.exists():
        size_mb = conscious_file.stat().st_size / (1024 * 1024)
        print(f"📝 Archivo conscious.txt: {size_mb:.1f}MB")
    
    print(f"✅ Verificación de resultados completada")

def main():
    """Función principal"""
    try:
        return run_full_dataset_processing()
    except Exception as e:
        print(f"💥 Error fatal: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
