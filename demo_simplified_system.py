#!/usr/bin/env python3
"""
🧪 DEMO - Sistema Simplificado de Content Manager
================================================
Demostración del nuevo sistema simplificado para procesar artículos.
"""

import json
from pathlib import Path
from content_manager import ContentManager

def demo_simplified_system():
    """Demuestra el sistema simplificado"""
    print("🧪 DEMO: Sistema Simplificado de Content Manager")
    print("=" * 60)
    
    # Crear content manager simplificado
    content_manager = ContentManager(
        fundamental_questions=3,
        specific_questions=2
    )
    
    # Artículo de prueba
    test_article = {
        'title': 'Inteligencia Artificial',
        'content': 'La inteligencia artificial (IA) es la simulación de procesos de inteligencia humana por parte de máquinas, especialmente sistemas informáticos. Estos procesos incluyen el aprendizaje, el razonamiento y la autocorrección. Las aplicaciones particulares de la IA incluyen sistemas expertos, procesamiento de lenguaje natural, reconocimiento de voz y visión artificial.'
    }
    
    print(f"📄 Procesando artículo: {test_article['title']}")
    print(f"📝 Contenido: {test_article['content'][:100]}...")
    print()
    
    # Procesar artículo
    result = content_manager.process_article(test_article)
    
    # Mostrar resultados
    if 'error' in result:
        print(f"❌ Error: {result['error']}")
        return
    
    print(f"📂 Categoría detectada: {result['category']}")
    print(f"💬 Conversaciones generadas: {result['total_conversations']}")
    print(f"⏱️ Tiempo de procesamiento: {result['processing_time']:.3f}s")
    print()
    
    # Mostrar algunas conversaciones
    print("🗣️ CONVERSACIONES GENERADAS:")
    print("-" * 40)
    
    for i, conv in enumerate(result['conversations'][:3], 1):
        print(f"\n{i}. Tipo: {conv['question_type']} ({conv['subtype']})")
        print(f"   Calidad: {conv['quality_score']:.2f}")
        print(f"   P: {conv['question']}")
        print(f"   R: {conv['answer'][:100]}...")
    
    # Mostrar validación
    validation = result['validation']
    print(f"\n📊 VALIDACIÓN:")
    print(f"   ✅ Válidas: {validation['valid_conversations']}/{validation['total_conversations']}")
    print(f"   ⭐ Calidad promedio: {validation['average_quality']:.3f}")
    print(f"   📈 Tasa de éxito: {validation['success_rate']:.1%}")
    
    # Mostrar estadísticas generales
    content_manager.print_statistics()
    
    return result

def demo_batch_processing():
    """Demuestra procesamiento por lotes"""
    print("\n🔄 DEMO: Procesamiento por Lotes")
    print("=" * 60)
    
    # Artículos de prueba
    test_articles = [
        {
            'title': 'Python (lenguaje de programación)',
            'content': 'Python es un lenguaje de programación interpretado cuya filosofía hace hincapié en la legibilidad de su código.'
        },
        {
            'title': 'Madrid',
            'content': 'Madrid es la capital de España y de la Comunidad de Madrid. Es la ciudad más poblada del país.'
        },
        {
            'title': 'Fotosíntesis',
            'content': 'La fotosíntesis es el proceso biológico más importante de la Tierra por la cual las plantas convierten la luz solar en energía química.'
        }
    ]
    
    # Crear content manager
    content_manager = ContentManager(
        fundamental_questions=2,
        specific_questions=1
    )
    
    # Procesar lote
    results = content_manager.process_articles_batch(test_articles)
    
    # Mostrar resumen
    print(f"\n📊 RESUMEN DEL LOTE:")
    total_conversations = sum(r.get('total_conversations', 0) for r in results)
    print(f"   📄 Artículos procesados: {len(results)}")
    print(f"   💬 Conversaciones totales: {total_conversations}")
    
    # Exportar a archivo
    output_file = "demo_conversations.jsonl"
    exported = content_manager.export_conversations(results, output_file)
    print(f"   💾 Exportadas: {exported} conversaciones a {output_file}")
    
    return results

if __name__ == "__main__":
    # Ejecutar demos
    demo_simplified_system()
    demo_batch_processing()
    
    print("\n✅ Demo completado. Sistema simplificado funcionando correctamente.")
