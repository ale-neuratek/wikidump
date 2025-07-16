#!/usr/bin/env python3
"""
Monitor en tiempo real para la validación del pipeline
"""

import time
import os
import psutil
from pathlib import Path

def monitor_validation():
    base_dir = Path("/home/ubuntu/wikidump_workspace/wikidump")
    output_dir = base_dir / "consciencia_validation"
    
    print("🔍 MONITOR DE VALIDACIÓN EN TIEMPO REAL")
    print("=" * 50)
    
    start_time = time.time()
    last_files = 0
    last_size = 0
    
    while True:
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Verificar si existe el directorio de salida
        if output_dir.exists():
            # Contar archivos generados
            files = list(output_dir.rglob("*.jsonl"))
            total_files = len(files)
            
            # Calcular tamaño total
            total_size = sum(f.stat().st_size for f in files if f.exists())
            total_size_mb = total_size / (1024 * 1024)
            
            # Verificar procesos Python activos
            python_procs = []
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if proc.info['name'] == 'python3' and proc.info['cmdline']:
                        cmdline = ' '.join(proc.info['cmdline'])
                        if 'adaptive_processor' in cmdline or 'caroline' in cmdline or 'full_dataset' in cmdline:
                            python_procs.append(proc.info['pid'])
                except:
                    pass
            
            # Mostrar progreso
            files_delta = total_files - last_files
            size_delta = total_size_mb - last_size
            
            print(f"\r⏱️ T+{elapsed:5.0f}s | 📄 {total_files:3d} archivos (+{files_delta:2d}) | "
                  f"💾 {total_size_mb:6.1f}MB (+{size_delta:5.1f}) | "
                  f"🔄 {len(python_procs)} procesos", end="", flush=True)
            
            last_files = total_files
            last_size = total_size_mb
            
            # Si no hay procesos activos y han pasado 10 segundos, salir
            if len(python_procs) == 0 and elapsed > 10:
                print(f"\n✅ Validación completada en {elapsed:.0f}s")
                break
        else:
            print(f"\r⏱️ T+{elapsed:5.0f}s | 🔄 Esperando inicio del pipeline...", end="", flush=True)
        
        time.sleep(2)

if __name__ == "__main__":
    try:
        monitor_validation()
    except KeyboardInterrupt:
        print("\n🛑 Monitor interrumpido")
