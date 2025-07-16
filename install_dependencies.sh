#!/bin/bash
"""
📦 INSTALADOR DE DEPENDENCIAS - Setup rápido
==========================================
Script para instalar todas las dependencias necesarias para el proyecto
wikidump incluyendo las de Hugging Face.

USO:
    ./install_dependencies.sh [--force]

OPCIONES:
    --force    Reinstalar todas las dependencias aunque ya estén instaladas
"""

set -e

FORCE_INSTALL=false
if [ "$1" = "--force" ]; then
    FORCE_INSTALL=true
fi

echo "📦 INSTALADOR DE DEPENDENCIAS WIKIDUMP"
echo "======================================"
echo ""

# ========================
# 1. ACTUALIZAR SISTEMA
# ========================
echo "🔄 Actualizando sistema..."
sudo apt-get update

# ========================
# 2. INSTALAR PYTHON Y PIP
# ========================
echo "🐍 Instalando Python y pip..."
sudo apt-get install -y python3 python3-pip python3-dev

# Actualizar pip a la última versión
pip3 install --upgrade pip

echo "✅ Python $(python3 --version) instalado"
echo "✅ pip $(pip3 --version) instalado"

# ========================
# 3. INSTALAR GIT LFS
# ========================
echo "📡 Instalando Git LFS..."
if ! command -v git-lfs &> /dev/null || [ "$FORCE_INSTALL" = true ]; then
    curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash
    sudo apt-get install git-lfs
    git lfs install
    echo "✅ Git LFS instalado"
else
    echo "✅ Git LFS ya instalado"
fi

# ========================
# 4. INSTALAR ARIA2C
# ========================
echo "⬇️ Instalando aria2c..."
if ! command -v aria2c &> /dev/null || [ "$FORCE_INSTALL" = true ]; then
    sudo apt-get install -y aria2
    echo "✅ aria2c instalado"
else
    echo "✅ aria2c ya instalado"
fi

# ========================
# 5. DEPENDENCIAS PYTHON PRINCIPALES
# ========================
echo "📚 Instalando dependencias Python principales..."

# Verificar si requirements.txt existe
if [ ! -f "requirements.txt" ]; then
    echo "📝 Creando requirements.txt..."
    cat <<EOF > requirements.txt
psutil>=5.8.0
lxml>=4.6.0
pandas>=1.3.0
numpy>=1.21.0
huggingface_hub>=0.16.0
EOF
fi

# Instalar desde requirements.txt
pip3 install -r requirements.txt

echo "✅ Dependencias principales instaladas"

# ========================
# 6. DEPENDENCIAS HUGGING FACE
# ========================
echo "🤗 Instalando dependencias específicas de Hugging Face..."

HF_PACKAGES=(
    "huggingface_hub>=0.16.0"
    "datasets>=2.0.0"
    "transformers>=4.21.0"
)

for package in "${HF_PACKAGES[@]}"; do
    echo "📦 Instalando $package..."
    pip3 install "$package"
done

echo "✅ Dependencias de Hugging Face instaladas"

# ========================
# 7. VERIFICAR INSTALACIONES
# ========================
echo ""
echo "🧪 Verificando instalaciones..."

# Verificar Python packages
python3 -c "
packages = [
    'psutil', 'lxml', 'pandas', 'numpy', 
    'huggingface_hub', 'datasets'
]

for pkg in packages:
    try:
        module = __import__(pkg)
        version = getattr(module, '__version__', 'unknown')
        print(f'✅ {pkg} v{version}')
    except ImportError:
        print(f'❌ {pkg} NO INSTALADO')
"

# Verificar comandos del sistema
echo ""
echo "🔧 Verificando herramientas del sistema:"

TOOLS=("python3" "pip3" "git" "git-lfs" "aria2c")
for tool in "${TOOLS[@]}"; do
    if command -v "$tool" &> /dev/null; then
        version=$($tool --version 2>&1 | head -n1 | cut -d' ' -f1-3)
        echo "✅ $tool: $version"
    else
        echo "❌ $tool: NO INSTALADO"
    fi
done

# ========================
# 8. CONFIGURAR CACHE HUGGING FACE
# ========================
echo ""
echo "📁 Configurando directorios de cache..."
mkdir -p ~/.cache/huggingface
chmod 755 ~/.cache/huggingface
echo "✅ Directorio cache de Hugging Face creado"

# ========================
# 9. RESUMEN FINAL
# ========================
echo ""
echo "🎉 ¡INSTALACIÓN COMPLETADA!"
echo "=========================="
echo ""
echo "✅ Sistema actualizado"
echo "✅ Python y pip instalados"
echo "✅ Git LFS configurado"
echo "✅ aria2c instalado"
echo "✅ Dependencias Python instaladas"
echo "✅ Hugging Face configurado"
echo ""
echo "📋 Próximos pasos:"
echo "1. Configurar token de Hugging Face: python3 validate_hf_setup.py"
echo "2. Probar configuración: ./demo_hf.sh"
echo "3. Ejecutar pipeline completo: ./init.sh"
echo ""
echo "💡 Para reinstalar todo: ./install_dependencies.sh --force"
