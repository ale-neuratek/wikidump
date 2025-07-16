#!/usr/bin/env python3
"""
🔍 DIAGNÓSTICO DE LOGGING EN TIEMPO REAL
========================================
Ejecuta el procesamiento con logging ultra-frecuente para diagnóstico
"""

import sys
import time
import subprocess
from pathlib import Path

def run_with_realtime_logging():
    """Ejecuta el procesamiento con logs en tiempo real"""
    
    print("🔍 DIAGNÓSTICO DE LOGGING EN TIEMPO REAL")
    print("=" * 50)
    
    # Configuración
    input_dir = "data_ultra_hybrid"
    output_dir = "consciencia_realtime_test"
    
    # Verificar entrada
    input_path = Path(input_dir)
    if not input_path.exists():
        print(f"❌ Directorio de entrada no existe: {input_dir}")
        return False
    
    files = list(input_path.glob("*.jsonl"))
    print(f"✅ Archivos de entrada: {len(files)}")
    
    # Limpiar salida
    output_path = Path(output_dir)
    if output_path.exists():
        import shutil
        shutil.rmtree(output_path)
    
    print(f"🚀 Iniciando procesamiento con logging en tiempo real...")
    print(f"📂 Input: {input_dir}")
    print(f"📁 Output: {output_dir}")
    print("-" * 50)
    
    try:
        # Ejecutar con output en tiempo real
        process = subprocess.Popen([
            sys.executable, "adaptive_processor.py",
            "--input", input_dir,
            "--output", output_dir
        ], 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT, 
        text=True, 
        bufsize=1,  # Line buffered
        universal_newlines=True
        )
        
        start_time = time.time()
        lines_received = 0
        last_status_time = time.time()
        
        print("📝 LOGS EN TIEMPO REAL:")
        print("=" * 50)
        
        while True:
            # Leer línea por línea
            line = process.stdout.readline()
            
            if line:
                # Mostrar inmediatamente
                print(f"LOG: {line.strip()}")
                lines_received += 1
                
                # Status cada 30 segundos
                current_time = time.time()
                if current_time - last_status_time >= 30:
                    elapsed = current_time - start_time
                    print(f"\n🔄 STATUS DIAGNÓSTICO: {elapsed:.1f}s corriendo, {lines_received} logs recibidos")
                    print("-" * 50)
                    last_status_time = current_time
            
            # Verificar si terminó
            if process.poll() is not None:
                # Leer cualquier output restante
                remaining = process.stdout.read()
                if remaining:
                    print(f"LOG: {remaining.strip()}")
                break
            
            # Si no hay línea, esperar un poco
            if not line:
                time.sleep(0.1)
        
        elapsed = time.time() - start_time
        return_code = process.returncode
        
        print(f"\n🎯 DIAGNÓSTICO COMPLETADO:")
        print(f"   ⏱️ Duración: {elapsed:.1f} segundos")
        print(f"   📝 Logs recibidos: {lines_received}")
        print(f"   🔚 Código de salida: {return_code}")
        
        # Verificar archivo de log
        log_file = Path("adaptive_processing.log")
        if log_file.exists():
            with open(log_file) as f:
                log_content = f.read()
            log_lines = len(log_content.strip().split('\n'))
            print(f"   📄 Líneas en archivo log: {log_lines}")
        else:
            print(f"   ❌ No se encontró archivo de log")
        
        return return_code == 0
        
    except KeyboardInterrupt:
        print(f"\n⚠️ Interrumpido por usuario")
        if process:
            process.terminate()
        return False
    except Exception as e:
        print(f"💥 Error: {e}")
        return False

if __name__ == "__main__":
    print("PRESIONA Ctrl+C para detener el diagnóstico")
    success = run_with_realtime_logging()
    print(f"\n{'✅ DIAGNÓSTICO EXITOSO' if success else '❌ DIAGNÓSTICO FALLÓ'}")
