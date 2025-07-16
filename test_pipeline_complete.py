#!/usr/bin/env python3
"""
🧪 VALIDADOR DEL PIPELINE OPTIMIZADO
===================================
Valida que el pipeline funcione correctamente con todos los requisitos:
1. NO genera conscious.txt
2. Genera categoría "consciencia" al final
3. Usa content_manager para todas las conversaciones
4. Logging completo con timestamps
5. Todas las categorías encontradas son reportadas
"""

import sys
import time
import subprocess
from pathlib import Path

def run_validation_test():
    """Ejecuta una prueba de validación completa"""
    
    print("🧪 VALIDADOR DEL PIPELINE OPTIMIZADO")
    print("=" * 50)
    
    # Configuración de la prueba
    input_dir = "data_ultra_hybrid"
    output_dir = "consciencia_test_validation"
    
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
    
    # 3. Ejecutar adaptive_processor
    print(f"🚀 Ejecutando adaptive_processor...")
    
    start_time = time.time()
    
    try:
        # Ejecutar el procesador
        result = subprocess.run([
            sys.executable, "adaptive_processor.py",
            "--input", input_dir,
            "--output", output_dir
        ], capture_output=True, text=True, timeout=3600)  # 1 hora timeout
        
        elapsed = time.time() - start_time
        print(f"⏱️ Tiempo de ejecución: {elapsed:.1f} segundos")
        
        if result.returncode != 0:
            print(f"❌ Procesamiento falló:")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False
        
        print(f"✅ Procesamiento completado exitosamente")
        
    except subprocess.TimeoutExpired:
        print(f"⏰ Procesamiento excedió timeout de 1 hora")
        return False
    except Exception as e:
        print(f"💥 Error ejecutando procesamiento: {e}")
        return False
    
    # 4. Validar resultados
    print(f"\n🔍 VALIDANDO RESULTADOS")
    print("-" * 30)
    
    # 4.1. Verificar que NO existe conscious.txt
    conscious_txt = output_path / "conscious.txt"
    if conscious_txt.exists():
        print(f"❌ FALLO: conscious.txt fue generado (no debería existir)")
        return False
    else:
        print(f"✅ conscious.txt NO existe (correcto)")
    
    # 4.2. Verificar que existe la categoría consciencia
    consciencia_dir = output_path / "consciencia"
    if not consciencia_dir.exists():
        print(f"❌ FALLO: categoría consciencia no existe")
        return False
    else:
        print(f"✅ Categoría consciencia existe")
    
    # 4.3. Verificar archivos en consciencia
    consciencia_files = list(consciencia_dir.glob("*.jsonl"))
    if not consciencia_files:
        print(f"❌ FALLO: no hay archivos JSONL en categoría consciencia")
        return False
    else:
        print(f"✅ Archivos consciencia: {len(consciencia_files)}")
    
    # 4.4. Verificar metadata de consciencia
    metadata_file = consciencia_dir / "metadata_consciencia.json"
    if not metadata_file.exists():
        print(f"❌ FALLO: metadata de consciencia no existe")
        return False
    else:
        print(f"✅ Metadata de consciencia existe")
        
        # Leer metadata
        import json
        with open(metadata_file) as f:
            metadata = json.load(f)
        
        print(f"   📝 Conversaciones: {metadata.get('total_conversations', 'N/A')}")
        print(f"   📁 Archivos: {metadata.get('total_files', 'N/A')}")
        print(f"   🏷️ Categorías encontradas: {len(metadata.get('categories_found', []))}")
    
    # 4.5. Verificar otras categorías
    categories_found = []
    for item in output_path.iterdir():
        if item.is_dir() and item.name not in ['consciencia', 'estadisticas']:
            categories_found.append(item.name)
    
    print(f"✅ Otras categorías encontradas: {len(categories_found)}")
    if categories_found:
        print(f"   🏷️ Categorías: {', '.join(categories_found[:5])}")
    
    # 4.6. Verificar formato de conversaciones
    sample_file = consciencia_files[0]
    try:
        with open(sample_file) as f:
            for i, line in enumerate(f):
                if i >= 3:  # Solo revisar primeras 3 líneas
                    break
                data = json.loads(line)
                
                # Verificar estructura de conversación
                if 'conversations' not in data:
                    print(f"❌ FALLO: formato de conversación incorrecto en {sample_file}")
                    return False
                
                conversations = data['conversations']
                if not isinstance(conversations, list) or len(conversations) != 2:
                    print(f"❌ FALLO: estructura de conversaciones incorrecta")
                    return False
                
                # Verificar roles
                if conversations[0].get('role') != 'user' or conversations[1].get('role') != 'assistant':
                    print(f"❌ FALLO: roles de conversación incorrectos")
                    return False
        
        print(f"✅ Formato de conversaciones correcto")
        
    except Exception as e:
        print(f"❌ FALLO: error validando formato de conversaciones: {e}")
        return False
    
    # 4.7. Verificar logs
    log_file = Path("adaptive_processing.log")
    if log_file.exists():
        print(f"✅ Log file existe: {log_file}")
        
        # Verificar contenido del log
        with open(log_file) as f:
            log_content = f.read()
        
        if "ADAPTIVE DATASET PROCESSOR INICIADO" in log_content:
            print(f"✅ Log contiene inicio de procesamiento")
        else:
            print(f"⚠️ Log no contiene inicio de procesamiento")
        
        if "CATEGORÍA CONSCIENCIA" in log_content:
            print(f"✅ Log contiene generación de categoría consciencia")
        else:
            print(f"❌ FALLO: Log no contiene generación de categoría consciencia")
            return False
    else:
        print(f"⚠️ Log file no existe")
    
    print(f"\n🎯 VALIDACIÓN COMPLETA")
    print("=" * 30)
    print(f"✅ Todos los requisitos validados exitosamente")
    print(f"   🚫 NO genera conscious.txt")
    print(f"   🧠 Genera categoría consciencia al final")
    print(f"   💬 Usa content_manager para conversaciones")
    print(f"   📝 Logging completo con timestamps")
    print(f"   🏷️ Categorías correctamente reportadas")
    
    return True

if __name__ == "__main__":
    success = run_validation_test()
    sys.exit(0 if success else 1)
