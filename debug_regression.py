#!/usr/bin/env python3
"""
Debug específico para entender por qué "Regresión logística multinomial" -> transporte-movilidad
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

def debug_regression_categorization():
    print("🔍 DEBUG: Regresión logística multinomial")
    
    from formation_fundamental.simple_formation_system import SimpleFormationSystem
    system = SimpleFormationSystem('formation')
    
    title = "Regresión logística multinomial"
    title_lower = title.lower()
    
    print(f"Título: '{title}'")
    print(f"Título lower: '{title_lower}'")
    
    # Test manual de palabras clave
    keywords_to_test = {
        'matematicas-logica': ['regresión', 'logística', 'multinomial', 'estadística', 'matemáticas'],
        'transporte-movilidad': ['transporte', 'movilidad', 'logística', 'vehículo']
    }
    
    print("\n🔍 ANÁLISIS DE PALABRAS CLAVE:")
    for category, keywords in keywords_to_test.items():
        print(f"\n📂 Categoría: {category}")
        score = 0
        for keyword in keywords:
            if keyword.lower() in title_lower:
                score += 1
                print(f"  ✅ '{keyword}' encontrada")
            else:
                print(f"  ❌ '{keyword}' no encontrada")
        print(f"  📊 Score total: {score}")
    
    # Categorización real
    category = system.get_category_for_title(title)
    print(f"\n🎯 Categoría asignada: '{category}'")

if __name__ == "__main__":
    debug_regression_categorization()
