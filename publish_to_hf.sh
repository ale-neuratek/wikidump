#!/bin/bash
"""
🤗 HUGGING FACE PUBLISHER - Publicación independiente de datasets
================================================================
Script independiente para publicar datasets JSONL en Hugging Face
sin necesidad de ejecutar todo el pipeline de procesamiento.

USO:
    ./publish_to_hf.sh [directorio_datos] [nombre_repo]

EJEMPLOS:
    ./publish_to_hf.sh wiki-datasets ale-neuratek/wikidump
    ./publish_to_hf.sh ../wiki_conversations ale-neuratek/wikidump-v2
"""

set -e  # Salir en caso de error

# Valores por defecto
DEFAULT_DATA_DIR="wiki-datasets"
DEFAULT_REPO="ale-neuratek/wikidump"

# Parsear argumentos
DATA_DIR="${1:-$DEFAULT_DATA_DIR}"
REPO_NAME="${2:-$DEFAULT_REPO}"

echo "🤗 PUBLICADOR DE DATASETS EN HUGGING FACE"
echo "=========================================="
echo "📁 Directorio de datos: $DATA_DIR"
echo "📦 Repositorio destino: $REPO_NAME"
echo ""

# Verificar que existe el directorio
if [ ! -d "$DATA_DIR" ]; then
    echo "❌ Error: Directorio no encontrado: $DATA_DIR"
    echo "💡 Uso: $0 [directorio_datos] [nombre_repo]"
    exit 1
fi

# Función para verificar requisitos de Hugging Face
check_huggingface_setup() {
    echo "🔍 Verificando configuración de Hugging Face..."
    
    # Verificar si git-lfs está instalado
    if ! command -v git-lfs &> /dev/null; then
        echo "📦 Instalando Git LFS..."
        curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash
        sudo apt-get install git-lfs
        git lfs install
    fi
    
    # Verificar si huggingface_hub está instalado
    if ! python3 -c "import huggingface_hub" &> /dev/null; then
        echo "📦 Instalando Hugging Face Hub..."
        pip3 install huggingface_hub
    fi
    
    echo "✅ Dependencias de Hugging Face verificadas."
}

# Función para configurar autenticación HF
setup_huggingface_auth() {
    echo ""
    echo "🔐 CONFIGURACIÓN DE HUGGING FACE"
    echo "================================="
    echo ""
    
    # Verificar si ya hay token configurado
    if python3 -c "from huggingface_hub import HfApi; HfApi().whoami()" &> /dev/null; then
        echo "✅ Token de Hugging Face ya configurado."
        return 0
    fi
    
    echo "Para publicar en Hugging Face necesitas:"
    echo "1. Una cuenta en https://huggingface.co"
    echo "2. Un token de acceso con permisos de escritura"
    echo ""
    echo "📝 PASOS PARA OBTENER EL TOKEN:"
    echo "1. Ve a https://huggingface.co/settings/tokens"
    echo "2. Crea un nuevo token con permisos 'Write'"
    echo "3. Copia el token generado"
    echo ""
    
    read -p "¿Tienes un token de Hugging Face? (y/n): " -n 1 -r
    echo
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Sin token de Hugging Face, no se puede continuar."
        exit 1
    fi
    
    read -s -p "🔑 Ingresa tu token de Hugging Face: " HF_TOKEN
    echo
    
    # Crear directorio cache si no existe
    mkdir -p "$HOME/.cache/huggingface"
    
    # Configurar token
    echo "$HF_TOKEN" | python3 -c "
from huggingface_hub import HfApi
import sys
import os
token = sys.stdin.read().strip()
try:
    api = HfApi(token=token)
    user = api.whoami()
    print(f'✅ Token válido para usuario: {user[\"name\"]}')
    # Guardar token
    cache_dir = os.path.expanduser('~/.cache/huggingface')
    os.makedirs(cache_dir, exist_ok=True)
    with open(os.path.join(cache_dir, 'token'), 'w') as f:
        f.write(token)
    exit(0)
except Exception as e:
    print(f'❌ Token inválido: {e}')
    exit(1)
"
    
    return $?
}

# Función para crear/actualizar repositorio HF
publish_to_huggingface() {
    local output_dir="$1"
    local repo_name="$2"
    
    echo "📤 Publicando datasets en Hugging Face..."
    echo "Repositorio: $repo_name"
    echo "Datos desde: $output_dir"
    echo ""
    
    # Verificar que exista el directorio de salida
    if [ ! -d "$output_dir" ]; then
        echo "❌ Error: Directorio de salida no encontrado: $output_dir"
        return 1
    fi
    
    # Contar archivos JSONL
    local jsonl_count=$(find "$output_dir" -name "*.jsonl" | wc -l)
    if [ "$jsonl_count" -eq 0 ]; then
        echo "❌ Error: No se encontraron archivos JSONL en $output_dir"
        return 1
    fi
    
    echo "📊 Archivos encontrados: $jsonl_count archivos JSONL"
    
    # Crear script Python para la publicación
    cat <<'EOF' > upload_to_hf.py
#!/usr/bin/env python3
import os
import sys
from pathlib import Path
from huggingface_hub import HfApi, create_repo
from datetime import datetime
import json

def upload_dataset(output_dir, repo_name):
    try:
        api = HfApi()
        
        # Verificar que el usuario tenga acceso
        user_info = api.whoami()
        print(f"👤 Usuario autenticado: {user_info['name']}")
        
        # Crear repositorio si no existe (privado)
        try:
            create_repo(repo_name, private=True, repo_type="dataset")
            print(f"✅ Repositorio {repo_name} creado (privado)")
        except Exception as e:
            if "already exists" in str(e).lower():
                print(f"ℹ️ Repositorio {repo_name} ya existe")
            else:
                print(f"⚠️ Advertencia al crear repositorio: {e}")
        
        # Buscar todos los archivos JSONL
        output_path = Path(output_dir)
        jsonl_files = list(output_path.rglob("*.jsonl"))
        
        if not jsonl_files:
            print(f"❌ No se encontraron archivos JSONL en {output_dir}")
            return False
        
        print(f"📁 Encontrados {len(jsonl_files)} archivos JSONL")
        
        # Calcular estadísticas del dataset
        total_conversations = 0
        total_size = 0
        categories = set()
        
        print("📊 Analizando contenido del dataset...")
        for jsonl_file in jsonl_files:
            file_size = jsonl_file.stat().st_size
            total_size += file_size
            
            # Analizar contenido del archivo
            try:
                with open(jsonl_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        if line.strip():
                            try:
                                data = json.loads(line)
                                total_conversations += 1
                                if 'category' in data:
                                    categories.add(data['category'])
                            except json.JSONDecodeError:
                                print(f"⚠️ Línea {line_num} en {jsonl_file.name} no es JSON válido")
            except Exception as e:
                print(f"⚠️ Error leyendo {jsonl_file.name}: {e}")
        
        # Crear README.md con metadatos detallados
        readme_content = f"""# Wikipedia Spanish Dataset

## Descripción
Dataset de Wikipedia en español procesado automáticamente para entrenamiento de modelos de lenguaje.
Contiene conversaciones estructuradas extraídas de artículos de Wikipedia y organizadas por categorías temáticas.

## Estadísticas
- **Fecha de generación**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Total de conversaciones**: {total_conversations:,}
- **Archivos JSONL**: {len(jsonl_files)}
- **Categorías únicas**: {len(categories)}
- **Tamaño total**: {total_size / 1024 / 1024:.1f} MB
- **Fuente**: Wikipedia dump eswiki-20250601

## Categorías principales
{chr(10).join(f"- {cat}" for cat in sorted(list(categories)[:20]))}
{"..." if len(categories) > 20 else ""}

## Estructura del dataset
Cada archivo JSONL contiene conversaciones estructuradas organizadas por categorías temáticas.
La estructura garantiza máximo 100 carpetas de categorías (top 90 + 10 genéricas).

## Formato de datos
```json
{{
  "conversation": [
    {{"role": "user", "content": "Pregunta sobre el tema"}},
    {{"role": "assistant", "content": "Respuesta basada en el artículo de Wikipedia"}}
  ],
  "category": "categoria_tematica",
  "source": "wikipedia",
  "title": "Título del artículo de Wikipedia",
  "timestamp": "2025-01-XX"
}}
```

## Uso recomendado
```python
import json
from pathlib import Path

def load_conversations(dataset_dir):
    conversations = []
    for jsonl_file in Path(dataset_dir).rglob("*.jsonl"):
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    conv = json.loads(line)
                    conversations.append(conv)
    return conversations

# Cargar todas las conversaciones
conversations = load_conversations("path/to/dataset")
print(f"Cargadas {{len(conversations)}} conversaciones")
```

## Optimizaciones aplicadas
- **Hardware de alto rendimiento**: Optimizado para GH200, 8xH100
- **Procesamiento masivo**: Manejo eficiente de >1.3M artículos
- **Categorización inteligente**: Limitación automática a 100 categorías
- **Calidad de datos**: Filtrado y validación automática de contenido

## Licencia
Los datos de Wikipedia están bajo [CC BY-SA 3.0](https://creativecommons.org/licenses/by-sa/3.0/).
El código de procesamiento está bajo [MIT License](https://opensource.org/licenses/MIT).

## Generado por
- **Pipeline**: Wikidump Processor v2.0
- **Repositorio**: https://github.com/ale-neuratek/wikidump
- **Autor**: Alejandro Iglesias - Neuratek.cl
"""
        
        # Subir README
        print("📝 Creando README.md...")
        api.upload_file(
            path_or_fileobj=readme_content.encode('utf-8'),
            path_in_repo="README.md",
            repo_id=repo_name,
            repo_type="dataset"
        )
        print("✅ README.md subido")
        
        # Subir archivos JSONL manteniendo estructura de carpetas
        print(f"📤 Subiendo {len(jsonl_files)} archivos...")
        uploaded_count = 0
        
        for jsonl_file in jsonl_files:
            file_size = jsonl_file.stat().st_size
            
            # Calcular ruta relativa para mantener estructura
            rel_path = jsonl_file.relative_to(output_path)
            
            print(f"📤 [{uploaded_count+1}/{len(jsonl_files)}] {rel_path} ({file_size / 1024 / 1024:.1f} MB)")
            
            api.upload_file(
                path_or_fileobj=str(jsonl_file),
                path_in_repo=str(rel_path),
                repo_id=repo_name,
                repo_type="dataset"
            )
            uploaded_count += 1
        
        print(f"✅ Dataset publicado exitosamente!")
        print(f"📊 Resumen:")
        print(f"   • Conversaciones: {total_conversations:,}")
        print(f"   • Archivos: {len(jsonl_files)}")
        print(f"   • Categorías: {len(categories)}")
        print(f"   • Tamaño: {total_size / 1024 / 1024:.1f} MB")
        print(f"🔗 Repositorio: https://huggingface.co/datasets/{repo_name}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error durante la publicación: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python3 upload_to_hf.py <output_dir> <repo_name>")
        sys.exit(1)
    
    output_dir = sys.argv[1]
    repo_name = sys.argv[2]
    
    success = upload_dataset(output_dir, repo_name)
    sys.exit(0 if success else 1)
EOF

    # Ejecutar script de publicación
    python3 upload_to_hf.py "$output_dir" "$repo_name"
    local upload_status=$?
    
    # Limpiar script temporal
    rm -f upload_to_hf.py
    
    if [ $upload_status -eq 0 ]; then
        echo ""
        echo "🎉 ¡Publicación en Hugging Face completada!"
        echo "🔗 Repositorio: https://huggingface.co/datasets/$repo_name"
        echo ""
        echo "📋 Próximos pasos:"
        echo "1. Verificar el dataset en el repositorio"
        echo "2. Ajustar configuración de privacidad si es necesario"
        echo "3. Compartir el enlace con tu equipo"
    else
        echo "❌ Error durante la publicación en Hugging Face"
        return 1
    fi
    
    return $upload_status
}

# ========================
# FLUJO PRINCIPAL
# ========================

echo "🔧 Verificando requisitos..."
check_huggingface_setup

echo ""
echo "🔐 Configurando autenticación..."
setup_huggingface_auth

echo ""
echo "📤 Iniciando publicación..."
publish_to_huggingface "$DATA_DIR" "$REPO_NAME"

echo ""
echo "🏁 ¡Proceso completado!"
