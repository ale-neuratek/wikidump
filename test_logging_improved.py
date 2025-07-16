#!/usr/bin/env python3
"""
🧪 PRUEBA RÁPIDA DE LOGGING
===========================
Prueba el pipeline con algunos archivos para verificar que el logging funcione correctamente
"""

import sys
import time
import subprocess
from pathlib import Path

def run_quick_logging_test():
    """Ejecuta una prueba rápida para verificar el logging"""
    
    print("🧪 PRUEBA RÁPIDA DE LOGGING")
    print("=" * 40)
    
    # Configuración de la prueba
    input_dir = "data_ultra_hybrid"
    output_dir = "consciencia_log_test"
    
    # 1. Verificar que existen archivos de entrada
    input_path = Path(input_dir)
    if not input_path.exists():
        print(f"❌ Directorio de entrada no existe: {input_dir}")
        return False
    
    files = list(input_path.glob("*.jsonl"))
    if not files:
        print(f"❌ No hay archivos JSONL en: {input_dir}")
        return False
    
    print(f"✅ Archivos de entrada encontrados: {len(files)}")
    
    # 2. Limpiar directorio de salida si existe
    output_path = Path(output_dir)
    if output_path.exists():
        import shutil
        shutil.rmtree(output_path)
        print(f"🧹 Directorio de salida limpiado: {output_dir}")
    
    # 3. Ejecutar adaptive_processor con timeout de 5 minutos
    print(f"🚀 Ejecutando procesamiento con logging mejorado...")
    
    start_time = time.time()
    
    try:
        # Ejecutar el procesador
        process = subprocess.Popen([
            sys.executable, "adaptive_processor.py",
            "--input", input_dir,
            "--output", output_dir
        ], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        
        # Mostrar output en tiempo real y monitorear por 5 minutos
        timeout = 300  # 5 minutos
        logs_received = []
        
        while True:
            elapsed = time.time() - start_time
            if elapsed > timeout:
                print(f"⏰ Terminando prueba después de {timeout/60:.1f} minutos")
                process.terminate()
                break
            
            # Leer output
            output = process.stdout.readline()
            if output:
                print(f"📝 {output.strip()}")
                logs_received.append(output.strip())
            
            # Verificar si el proceso terminó
            if process.poll() is not None:
                print(f"✅ Proceso terminado naturalmente")
                break
            
            time.sleep(0.1)
        
        elapsed = time.time() - start_time
        print(f"⏱️ Tiempo de prueba: {elapsed:.1f} segundos")
        
        # 4. Analizar logs
        print(f"\n📊 ANÁLISIS DE LOGS:")
        print("-" * 20)
        
        # Verificar que se recibieron logs
        if logs_received:
            print(f"✅ Se recibieron {len(logs_received)} mensajes de log")
            
            # Verificar frecuencia de logging
            timestamp_logs = [log for log in logs_received if "[20" in log and "T+" in log]
            if len(timestamp_logs) >= 3:
                print(f"✅ Logging con timestamps funcionando: {len(timestamp_logs)} mensajes")
            else:
                print(f"⚠️ Pocos logs con timestamp: {len(timestamp_logs)} mensajes")
            
            # Verificar logs de progreso
            progress_logs = [log for log in logs_received if "📈" in log or "STATUS" in log]
            if progress_logs:
                print(f"✅ Logs de progreso funcionando: {len(progress_logs)} mensajes")
            else:
                print(f"⚠️ No se encontraron logs de progreso")
            
            # Mostrar últimos logs
            print(f"\n📄 ÚLTIMOS 5 LOGS:")
            for log in logs_received[-5:]:
                print(f"   {log}")
                
        else:
            print(f"❌ No se recibieron logs")
        
        # 5. Verificar archivo de log
        log_file = Path("adaptive_processing.log")
        if log_file.exists():
            print(f"\n✅ Archivo de log existe: {log_file}")
            with open(log_file) as f:
                log_content = f.read()
            
            lines = log_content.strip().split('\n')
            print(f"   📄 Líneas en log: {len(lines)}")
            
            if len(lines) >= 5:
                print(f"✅ Log file tiene contenido suficiente")
            else:
                print(f"⚠️ Log file tiene poco contenido")
        else:
            print(f"❌ Archivo de log no existe")
        
        # 6. Verificar que se están procesando artículos
        processing_logs = [log for log in logs_received if "artículos" in log.lower() and any(char.isdigit() for char in log)]
        if processing_logs:
            print(f"✅ Se detectó procesamiento de artículos: {len(processing_logs)} logs")
        else:
            print(f"⚠️ No se detectó procesamiento de artículos")
        
        print(f"\n🎯 RESUMEN DE LA PRUEBA:")
        print(f"   ⏱️ Duración: {elapsed:.1f} segundos")
        print(f"   📝 Logs recibidos: {len(logs_received)}")
        print(f"   📈 Logs de progreso: {len(progress_logs)}")
        print(f"   📊 Logs de procesamiento: {len(processing_logs)}")
        
        return len(logs_received) > 0 and len(progress_logs) > 0
        
    except Exception as e:
        print(f"💥 Error ejecutando prueba: {e}")
        return False

if __name__ == "__main__":
    success = run_quick_logging_test()
    print(f"\n{'✅ PRUEBA EXITOSA' if success else '❌ PRUEBA FALLÓ'}")
    sys.exit(0 if success else 1)
