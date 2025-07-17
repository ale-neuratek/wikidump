#!/usr/bin/env python3
"""
Script de prueba para verificar el nuevo content_manager refactorizado
"""

import sys
import json
from pathlib import Path

# Añadir el directorio actual al path
sys.path.insert(0, str(Path(__file__).parent))

from content_manager import ContentManager, FormationLoader

def test_formation_loader():
    """Prueba el cargador de configuraciones"""
    print("🔍 Probando FormationLoader...")
    
    loader = FormationLoader("formation")
    
    # Verificar que se cargaron las configuraciones
    categories = loader.get_all_categories()
    print(f"   ✅ Categorías cargadas: {len(categories)}")
    print(f"   📝 Categorías: {', '.join(categories)}")
    
    # Verificar fundamental
    title_patterns = loader.get_fundamental_data('title_patterns')
    print(f"   ✅ Patrones de título cargados: {len(title_patterns) if title_patterns else 0}")
    
    # Verificar una categoría específica
    arte_config = loader.get_category_data('arte', 'category_config')
    print(f"   ✅ Configuración de 'arte' cargada: {'Sí' if arte_config else 'No'}")
    
    return True

def test_content_manager():
    """Prueba el ContentManager refactorizado"""
    print("\n🧠 Probando ContentManager...")
    
    try:
        content_manager = ContentManager()
        print("   ✅ ContentManager inicializado correctamente")
        
        # Prueba con artículo de ejemplo
        test_articles = [
            {
                'title': 'Ludwig van Beethoven',
                'content': 'Ludwig van Beethoven fue un compositor y pianista alemán. Nació en Bonn en 1770 y murió en Viena en 1827. Es considerado uno de los compositores más importantes de la música clásica occidental.'
            },
            {
                'title': 'Madrid',
                'content': 'Madrid es la capital y ciudad más poblada de España. Se encuentra en el centro de la península ibérica, en la Comunidad de Madrid. Es una importante ciudad europea.'
            },
            {
                'title': 'Python',
                'content': 'Python es un lenguaje de programación interpretado cuya filosofía hace hincapié en la legibilidad de su código. Es un lenguaje de programación multiparadigma.'
            }
        ]
        
        for i, article in enumerate(test_articles):
            print(f"\n   🧪 Procesando artículo {i+1}: {article['title']}")
            result = content_manager.process_article(article)
            
            if result:
                print(f"      ✅ Categoría: {result['category']}")
                print(f"      ✅ Subcategoría: {result['subcategory']}")
                print(f"      ✅ Tipo de contenido: {result['content_type']}")
                print(f"      ✅ Conversaciones generadas: {len(result['conversations'])}")
                print(f"      ✅ Confianza: {result['confidence']:.2f}")
                
                # Mostrar primera conversación
                if result['conversations']:
                    first_conv = result['conversations'][0]
                    print(f"      📝 Primera pregunta: {first_conv['question']}")
                    print(f"      📝 Respuesta: {first_conv['answer'][:100]}...")
            else:
                print("      ❌ Error procesando artículo")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return False

def test_batch_processing():
    """Prueba el procesamiento en lotes"""
    print("\n📦 Probando procesamiento en lotes...")
    
    try:
        content_manager = ContentManager()
        
        batch_articles = [
            {'title': 'Real Madrid', 'content': 'El Real Madrid Club de Fútbol es un club de fútbol profesional con sede en Madrid, España.'},
            {'title': 'Canis lupus', 'content': 'Canis lupus es una especie de mamífero placentario del orden de los carnívoros.'},
            {'title': 'Guerra Civil Española', 'content': 'La Guerra Civil Española fue un conflicto bélico que se desarrolló en España entre 1936 y 1939.'}
        ]
        
        results = content_manager.process_article_batch(batch_articles)
        print(f"   ✅ Artículos procesados en lote: {len(results)}/{len(batch_articles)}")
        
        for result in results:
            print(f"      📄 {result['title']} → {result['category']} ({result['content_type']})")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error en lote: {e}")
        return False

def test_consciencia_generation():
    """Prueba la generación de categoría consciencia"""
    print("\n🧠 Probando generación de consciencia...")
    
    try:
        content_manager = ContentManager()
        
        categories_found = ['arte', 'geografia', 'historia', 'ciencias', 'deportes']
        result = content_manager.generate_consciencia_category(
            categories_found=categories_found,
            output_dir="test_output",
            total_articles=1000
        )
        
        print(f"   ✅ Conversaciones consciencia: {result['conversations_generated']}")
        print(f"   ✅ Archivos creados: {result['files_created']}")
        print(f"   ✅ Categorías descritas: {result['categories_described']}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error generando consciencia: {e}")
        return False

def main():
    """Función principal de prueba"""
    print("🚀 INICIANDO PRUEBAS DEL CONTENT MANAGER REFACTORIZADO")
    print("=" * 60)
    
    tests = [
        ("FormationLoader", test_formation_loader),
        ("ContentManager", test_content_manager),
        ("Batch Processing", test_batch_processing),
        ("Consciencia Generation", test_consciencia_generation)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"\n✅ {test_name}: PASSED")
            else:
                print(f"\n❌ {test_name}: FAILED")
        except Exception as e:
            print(f"\n💥 {test_name}: ERROR - {e}")
    
    print("\n" + "=" * 60)
    print(f"🎯 RESULTADOS: {passed}/{total} pruebas pasadas")
    
    if passed == total:
        print("🎉 ¡TODAS LAS PRUEBAS EXITOSAS! El refactor está funcionando correctamente.")
    else:
        print("⚠️  Algunas pruebas fallaron. Revisar la implementación.")
    
    print("\n📁 Archivos de configuración en formation/:")
    formation_path = Path("formation")
    if formation_path.exists():
        for file in formation_path.glob("*.jsonl"):
            print(f"   📄 {file.name}")

if __name__ == "__main__":
    main()
