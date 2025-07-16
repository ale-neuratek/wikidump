#!/usr/bin/env python3
"""
📊 OPTIMIZED MONITOR - Monitor para proceso optimizado
==================================================
"""

import time
import os
from pathlib import Path

def monitor_optimized_process():
    """Monitorea el proceso optimizado"""
    start_time = time.time()
    report_count = 0
    
    while True:
        report_count += 1
        elapsed = time.time() - start_time
        
        print(f"\n📊 REPORTE OPTIMIZADO #{report_count} - {time.strftime('%H:%M:%S')}")
        print("=" * 60)
        print(f"⏱️ Tiempo desde reinicio: {elapsed/60:.1f} minutos")
        
        # Verificar directorio de salida
        output_dir = Path("consciencia_optimized")
        if output_dir.exists():
            # Contar archivos
            jsonl_files = list(output_dir.rglob("*.jsonl"))
            total_size = sum(f.stat().st_size for f in jsonl_files) / (1024*1024)
            
            print(f"📁 Archivos generados: {len(jsonl_files)}")
            print(f"💾 Tamaño total: {total_size:.1f}MB")
            
            if jsonl_files:
                # Contar conversaciones
                total_conversations = 0
                for file_path in jsonl_files:
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            count = sum(1 for line in f if line.strip())
                        total_conversations += count
                    except:
                        continue
                
                print(f"💬 Conversaciones: {total_conversations:,}")
                
                if elapsed > 60:  # Después de 1 minuto
                    rate = total_conversations / (elapsed / 3600)  # conv/hora
                    print(f"🚀 Velocidad: {rate:,.0f} conv/h")
                    
                    if rate > 0:
                        # Estimar ETA para 2M conversaciones (objetivo conservador)
                        target = 2000000
                        remaining = target - total_conversations
                        eta_hours = remaining / rate if remaining > 0 else 0
                        print(f"🎯 ETA (2M conv): {eta_hours:.1f}h")
            else:
                print("⏳ Sin archivos aún - procesando...")
        
        else:
            print("📂 Directorio de salida no creado aún")
        
        # Verificar proceso
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'cpu_percent']):
                if 'adaptive_processor.py' in ' '.join(proc.info['cmdline'] or []):
                    cpu = proc.cpu_percent()
                    print(f"⚡ CPU: {cpu:.1f}%")
                    break
        except:
            print("⚠️ No se pudo verificar CPU")
        
        # Dormir 2 minutos entre reportes
        time.sleep(120)

if __name__ == "__main__":
    print("📊 MONITOR DE PROCESO OPTIMIZADO")
    print("Presiona Ctrl+C para detener")
    try:
        monitor_optimized_process()
    except KeyboardInterrupt:
        print("\n✅ Monitor detenido")
