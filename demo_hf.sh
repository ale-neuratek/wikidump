#!/bin/bash
"""
🎯 DEMO RÁPIDO - Publicación en Hugging Face
==========================================
Script de demostración para mostrar el flujo completo de publicación
Usa datos de ejemplo para validar que todo funcione correctamente.

FUNCIONES:
- Crear dataset de ejemplo
- Validar configuración HF
- Probar publicación
- Limpiar datos de prueba
"""

set -e

echo "🎯 DEMO: PUBLICACIÓN HUGGING FACE"
echo "================================="
echo ""

# Configuración
DEMO_DIR="demo_data"
DEMO_REPO="ale-neuratek/wikidump-demo"

# Función para crear datos de ejemplo
create_demo_data() {
    echo "📁 Creando datos de ejemplo..."
    
    mkdir -p "$DEMO_DIR/arte"
    mkdir -p "$DEMO_DIR/geografia" 
    mkdir -p "$DEMO_DIR/historia"
    
    # Crear archivos JSONL de ejemplo
    cat <<'EOF' > "$DEMO_DIR/arte/conversaciones_arte_001.jsonl"
{"conversation": [{"role": "user", "content": "¿Qué es el reggae?"}, {"role": "assistant", "content": "El reggae es un género musical que se originó en Jamaica en la década de 1960."}], "category": "arte", "subcategory": "musica-reggae", "source": "wikipedia", "title": "Reggae"}
{"conversation": [{"role": "user", "content": "¿Quién popularizó el reggae?"}, {"role": "assistant", "content": "Bob Marley fue uno de los principales responsables de popularizar el reggae a nivel mundial."}], "category": "arte", "subcategory": "musica-reggae", "source": "wikipedia", "title": "Bob Marley"}
EOF

    cat <<'EOF' > "$DEMO_DIR/geografia/conversaciones_geografia_001.jsonl"
{"conversation": [{"role": "user", "content": "¿Dónde está Madrid?"}, {"role": "assistant", "content": "Madrid es la capital de España, situada en el centro de la península ibérica."}], "category": "geografia", "subcategory": "ciudades", "source": "wikipedia", "title": "Madrid"}
{"conversation": [{"role": "user", "content": "¿Cuántos habitantes tiene Barcelona?"}, {"role": "assistant", "content": "Barcelona tiene aproximadamente 1.6 millones de habitantes en la ciudad y 5.5 millones en el área metropolitana."}], "category": "geografia", "subcategory": "ciudades", "source": "wikipedia", "title": "Barcelona"}
EOF

    cat <<'EOF' > "$DEMO_DIR/historia/conversaciones_historia_001.jsonl"
{"conversation": [{"role": "user", "content": "¿Cuándo nació Simón Bolívar?"}, {"role": "assistant", "content": "Simón Bolívar nació el 24 de julio de 1783 en Caracas, Venezuela."}], "category": "historia", "subcategory": "biografias", "source": "wikipedia", "title": "Simón Bolívar"}
{"conversation": [{"role": "user", "content": "¿Qué hizo Bolívar?"}, {"role": "assistant", "content": "Simón Bolívar fue un líder militar y político que lideró la independencia de varios países sudamericanos del dominio español."}], "category": "historia", "subcategory": "biografias", "source": "wikipedia", "title": "Simón Bolívar"}
EOF

    echo "✅ Datos de ejemplo creados en $DEMO_DIR"
    echo "   📊 3 categorías, 6 conversaciones de ejemplo"
}

# Función para limpiar datos
cleanup_demo() {
    echo "🧹 Limpiando datos de demostración..."
    rm -rf "$DEMO_DIR"
    echo "✅ Datos de demo eliminados"
}

# Función principal de demo
run_demo() {
    echo "🚀 Iniciando demostración..."
    echo ""
    
    # 1. Crear datos de ejemplo
    create_demo_data
    echo ""
    
    # 2. Validar configuración
    echo "🧪 Validando configuración de Hugging Face..."
    if python3 validate_hf_setup.py "$DEMO_DIR" --repo "$DEMO_REPO"; then
        echo "✅ Configuración válida"
    else
        echo "❌ Error en configuración"
        cleanup_demo
        exit 1
    fi
    echo ""
    
    # 3. Mostrar opciones
    echo "📋 OPCIONES DE DEMO:"
    echo "1. Solo validar (no subir nada)"
    echo "2. Publicar datos de demo en repositorio de prueba"
    echo "3. Probar con repositorio principal"
    echo ""
    
    read -p "Selecciona una opción (1-3): " -n 1 -r
    echo
    echo ""
    
    case $REPLY in
        1)
            echo "✅ Demo de validación completado"
            ;;
        2)
            echo "📤 Publicando en repositorio de demo..."
            if ./publish_to_hf.sh "$DEMO_DIR" "$DEMO_REPO"; then
                echo "✅ Demo publicado exitosamente"
                echo "🔗 Repositorio: https://huggingface.co/datasets/$DEMO_REPO"
            else
                echo "❌ Error en la publicación"
            fi
            ;;
        3)
            echo "📤 Publicando en repositorio principal..."
            if ./publish_to_hf.sh "$DEMO_DIR" "ale-neuratek/wikidump"; then
                echo "✅ Demo publicado en repositorio principal"
                echo "🔗 Repositorio: https://huggingface.co/datasets/ale-neuratek/wikidump"
            else
                echo "❌ Error en la publicación"
            fi
            ;;
        *)
            echo "❌ Opción inválida"
            ;;
    esac
    
    echo ""
    
    # 4. Limpiar
    read -p "¿Eliminar datos de demo? (y/n): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        cleanup_demo
    else
        echo "📁 Datos de demo conservados en $DEMO_DIR"
    fi
}

# Verificar que los scripts existan
if [ ! -f "validate_hf_setup.py" ]; then
    echo "❌ Error: validate_hf_setup.py no encontrado"
    exit 1
fi

if [ ! -f "publish_to_hf.sh" ]; then
    echo "❌ Error: publish_to_hf.sh no encontrado"
    exit 1
fi

# Ejecutar demo
run_demo

echo ""
echo "🎉 ¡Demo completado!"
echo ""
echo "📋 Para uso real:"
echo "   1. Procesar Wikipedia: ./init.sh"
echo "   2. Publicar existente: ./publish_to_hf.sh wiki-datasets"
echo "   3. Solo validar: python3 validate_hf_setup.py"
