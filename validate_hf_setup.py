#!/usr/bin/env python3
"""
🧪 VALIDADOR DE HUGGING FACE - Test de configuración
==================================================
Script para validar la configuración de Hugging Face sin subir datos reales.
Útil para verificar tokens, permisos y conectividad antes del procesamiento completo.

USO:
    python3 validate_hf_setup.py [directorio_datos]

FUNCIONES:
- Validar token de Hugging Face
- Verificar permisos de escritura en repositorio
- Probar conectividad y latencia
- Validar estructura de datos local
- Estimar tiempo de subida
"""

import os
import sys
import json
import time
from pathlib import Path
from datetime import datetime
import argparse

def check_dependencies():
    """Verificar que las dependencias estén instaladas"""
    try:
        import huggingface_hub
        from huggingface_hub import HfApi, create_repo
        print("✅ huggingface_hub instalado correctamente")
        return True
    except ImportError:
        print("❌ huggingface_hub no está instalado")
        print("💡 Ejecuta: pip3 install huggingface_hub")
        return False

def validate_token():
    """Validar token de Hugging Face"""
    try:
        from huggingface_hub import HfApi
        
        api = HfApi()
        user_info = api.whoami()
        
        print(f"✅ Token válido para usuario: {user_info['name']}")
        print(f"📧 Email: {user_info.get('email', 'No disponible')}")
        print(f"🏢 Organizaciones: {len(user_info.get('orgs', []))}")
        
        return True, user_info
        
    except Exception as e:
        print(f"❌ Error de autenticación: {e}")
        print("💡 Configurar token con: huggingface-cli login")
        return False, None

def test_repository_access(repo_name, user_info):
    """Probar acceso al repositorio"""
    try:
        from huggingface_hub import HfApi, create_repo
        
        api = HfApi()
        
        # Verificar si el repositorio existe
        try:
            repo_info = api.repo_info(repo_name, repo_type="dataset")
            print(f"✅ Repositorio {repo_name} existe")
            print(f"📊 Archivos en repo: {len(repo_info.siblings) if repo_info.siblings else 0}")
            
            # Verificar permisos de escritura (intentar crear un pequeño archivo)
            test_content = f"Test file - {datetime.now().isoformat()}"
            api.upload_file(
                path_or_fileobj=test_content.encode('utf-8'),
                path_in_repo="test_access.txt",
                repo_id=repo_name,
                repo_type="dataset"
            )
            print("✅ Permisos de escritura confirmados")
            
            # Limpiar archivo de prueba
            api.delete_file("test_access.txt", repo_id=repo_name, repo_type="dataset")
            print("🧹 Archivo de prueba eliminado")
            
            return True
            
        except Exception as e:
            if "does not exist" in str(e).lower():
                # Intentar crear el repositorio
                print(f"📦 Repositorio {repo_name} no existe, intentando crear...")
                create_repo(repo_name, private=True, repo_type="dataset")
                print(f"✅ Repositorio {repo_name} creado exitosamente")
                return True
            else:
                print(f"❌ Error de acceso al repositorio: {e}")
                return False
                
    except Exception as e:
        print(f"❌ Error probando repositorio: {e}")
        return False

def analyze_local_data(data_dir):
    """Analizar datos locales"""
    data_path = Path(data_dir)
    
    if not data_path.exists():
        print(f"❌ Directorio no encontrado: {data_dir}")
        return False, {}
    
    # Buscar archivos JSONL
    jsonl_files = list(data_path.rglob("*.jsonl"))
    
    if not jsonl_files:
        print(f"❌ No se encontraron archivos JSONL en {data_dir}")
        return False, {}
    
    print(f"📁 Analizando {len(jsonl_files)} archivos JSONL...")
    
    total_size = 0
    total_conversations = 0
    categories = set()
    file_stats = []
    
    for jsonl_file in jsonl_files:
        file_size = jsonl_file.stat().st_size
        total_size += file_size
        file_conversations = 0
        
        try:
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            data = json.loads(line)
                            file_conversations += 1
                            total_conversations += 1
                            if 'category' in data:
                                categories.add(data['category'])
                        except json.JSONDecodeError:
                            pass
        except Exception as e:
            print(f"⚠️ Error leyendo {jsonl_file.name}: {e}")
        
        file_stats.append({
            'file': jsonl_file.name,
            'size_mb': file_size / 1024 / 1024,
            'conversations': file_conversations
        })
    
    stats = {
        'total_files': len(jsonl_files),
        'total_size_mb': total_size / 1024 / 1024,
        'total_conversations': total_conversations,
        'unique_categories': len(categories),
        'categories': sorted(list(categories)),
        'file_stats': file_stats
    }
    
    print(f"📊 Estadísticas del dataset:")
    print(f"   • Archivos JSONL: {stats['total_files']}")
    print(f"   • Tamaño total: {stats['total_size_mb']:.1f} MB")
    print(f"   • Conversaciones: {stats['total_conversations']:,}")
    print(f"   • Categorías: {stats['unique_categories']}")
    
    # Mostrar top 10 categorías
    if categories:
        print(f"   • Top categorías: {', '.join(list(categories)[:10])}")
        if len(categories) > 10:
            print(f"     ... y {len(categories) - 10} más")
    
    return True, stats

def estimate_upload_time(stats):
    """Estimar tiempo de subida"""
    total_mb = stats['total_size_mb']
    
    # Estimaciones conservadoras (MB/s)
    speeds = {
        'slow': 1.0,     # Conexión lenta
        'medium': 5.0,   # Conexión media
        'fast': 20.0,    # Conexión rápida
    }
    
    print(f"⏱️ Estimación de tiempo de subida para {total_mb:.1f} MB:")
    for speed_name, speed_mbps in speeds.items():
        seconds = total_mb / speed_mbps
        minutes = seconds / 60
        hours = minutes / 60
        
        if hours >= 1:
            time_str = f"{hours:.1f} horas"
        elif minutes >= 1:
            time_str = f"{minutes:.1f} minutos"
        else:
            time_str = f"{seconds:.0f} segundos"
        
        print(f"   • {speed_name.capitalize()}: {time_str}")

def test_upload_speed():
    """Probar velocidad de subida con archivo pequeño"""
    try:
        from huggingface_hub import HfApi
        
        # Crear archivo de prueba
        test_content = "x" * (1024 * 1024)  # 1 MB
        test_filename = f"speed_test_{int(time.time())}.tmp"
        
        api = HfApi()
        
        start_time = time.time()
        
        # Subir archivo de prueba
        api.upload_file(
            path_or_fileobj=test_content.encode('utf-8'),
            path_in_repo=test_filename,
            repo_id="ale-neuratek/wikidump",  # Usar repo por defecto
            repo_type="dataset"
        )
        
        upload_time = time.time() - start_time
        speed_mbps = 1.0 / upload_time  # 1MB / tiempo
        
        # Limpiar archivo de prueba
        api.delete_file(test_filename, repo_id="ale-neuratek/wikidump", repo_type="dataset")
        
        print(f"🚀 Velocidad de subida medida: {speed_mbps:.1f} MB/s")
        return speed_mbps
        
    except Exception as e:
        print(f"⚠️ No se pudo medir velocidad: {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Validar configuración de Hugging Face")
    parser.add_argument("data_dir", nargs="?", default="wiki-datasets", 
                      help="Directorio con datos a validar (default: wiki-datasets)")
    parser.add_argument("--repo", default="ale-neuratek/wikidump",
                      help="Repositorio de destino (default: ale-neuratek/wikidump)")
    parser.add_argument("--test-speed", action="store_true",
                      help="Probar velocidad de subida")
    
    args = parser.parse_args()
    
    print("🧪 VALIDADOR DE CONFIGURACIÓN HUGGING FACE")
    print("=" * 50)
    print()
    
    # 1. Verificar dependencias
    print("1️⃣ Verificando dependencias...")
    if not check_dependencies():
        sys.exit(1)
    print()
    
    # 2. Validar token
    print("2️⃣ Validando autenticación...")
    token_valid, user_info = validate_token()
    if not token_valid:
        sys.exit(1)
    print()
    
    # 3. Probar acceso al repositorio
    print("3️⃣ Probando acceso al repositorio...")
    repo_access = test_repository_access(args.repo, user_info)
    if not repo_access:
        sys.exit(1)
    print()
    
    # 4. Analizar datos locales
    print("4️⃣ Analizando datos locales...")
    data_valid, stats = analyze_local_data(args.data_dir)
    if not data_valid:
        sys.exit(1)
    print()
    
    # 5. Estimar tiempo de subida
    print("5️⃣ Estimando tiempo de subida...")
    estimate_upload_time(stats)
    print()
    
    # 6. Probar velocidad (opcional)
    if args.test_speed:
        print("6️⃣ Probando velocidad de subida...")
        test_upload_speed()
        print()
    
    # Resumen final
    print("✅ VALIDACIÓN COMPLETADA")
    print("=" * 30)
    print(f"🎯 Repositorio: {args.repo}")
    print(f"📁 Datos: {args.data_dir}")
    print(f"👤 Usuario: {user_info['name'] if user_info else 'N/A'}")
    print(f"📊 Dataset: {stats['total_conversations']:,} conversaciones, {stats['total_size_mb']:.1f} MB")
    print()
    print("🚀 ¡Configuración lista para publicación!")
    print()
    print("📋 Próximos pasos:")
    print("   1. Ejecutar el pipeline completo: ./init.sh")
    print("   2. O publicar datos existentes: ./publish_to_hf.sh")

if __name__ == "__main__":
    main()
