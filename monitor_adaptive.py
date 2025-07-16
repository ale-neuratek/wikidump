#!/usr/bin/env python3
"""
Monitor de progreso en tiempo real para el procesamiento adaptativo
"""

import time
import os
from pathlib import Path

def monitor_processing():
    base_dir = Path("/home/ubuntu/wikidump_workspace/wikidump")
    output_dir = base_dir / "consciencia_completa"
    log_file = base_dir / "adaptive_processing.log"
    
    print("🔍 MONITOR DE PROCESAMIENTO ADAPTATIVO")
    print("=" * 50)
    
    start_time = time.time()
    
    while True:
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Verificar log
        log_lines = []
        if log_file.exists():
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    log_lines = f.readlines()
                
                # Mostrar últimas 3 líneas del log
                if log_lines:
                    print(f"\n📋 ÚLTIMAS ENTRADAS DEL LOG:")
                    for line in log_lines[-3:]:
                        print(f"   {line.strip()}")
            except:
                pass
        
        # Verificar archivos generados
        total_files = 0
        total_size_mb = 0
        categories_found = []
        
        if output_dir.exists():
            categorias_dir = output_dir / "categorias"
            if categorias_dir.exists():
                for cat_dir in categorias_dir.iterdir():
                    if cat_dir.is_dir():
                        categories_found.append(cat_dir.name)
                        jsonl_files = list(cat_dir.glob("*.jsonl"))
                        total_files += len(jsonl_files)
                        
                        for f in jsonl_files:
                            try:
                                total_size_mb += f.stat().st_size / (1024 * 1024)
                            except:
                                pass
        
        # Mostrar estado actual
        print(f"\r⏱️ T+{elapsed/60:5.1f}m | 📁 {len(categories_found)} cats | 📄 {total_files} archivos | 💾 {total_size_mb:.1f}MB", end="", flush=True)
        
        if categories_found:
            print(f"\n📂 Categorías: {', '.join(sorted(categories_found))}")
        
        time.sleep(10)  # Actualizar cada 10 segundos

if __name__ == "__main__":
    try:
        monitor_processing()
    except KeyboardInterrupt:
        print("\n🛑 Monitor interrumpido")
