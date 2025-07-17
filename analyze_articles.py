#!/usr/bin/env python3
"""
Analiza una muestra de artículos para mejorar la categorización
"""

import sys
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

def analyze_articles():
    print("🔍 ANALIZANDO MUESTRA DE ARTÍCULOS")
    
    # Cargar muestra de artículos
    input_file = "data_test_ultra_hybrid/articles_hybrid_1_0000.jsonl"
    
    articles = []
    with open(input_file, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            if i >= 20:  # Solo los primeros 20
                break
            if line.strip():
                articles.append(json.loads(line.strip()))
    
    print(f"📚 Analizando {len(articles)} artículos...")
    
    from formation_fundamental.simple_formation_system import SimpleFormationSystem
    system = SimpleFormationSystem('formation')
    
    print("\n📋 ANÁLISIS DE TÍTULOS Y CATEGORIZACIÓN:")
    print("="*80)
    
    category_counts = {}
    
    for i, article in enumerate(articles, 1):
        title = article['title']
        content = article.get('content', '')[:200] + "..."  # Primeros 200 chars
        
        category = system.get_category_for_title(title)
        category_counts[category] = category_counts.get(category, 0) + 1
        
        print(f"{i:2d}. '{title}' -> '{category}'")
        print(f"    Content: {content}")
        print()
    
    print("\n📊 DISTRIBUCIÓN DE CATEGORÍAS:")
    print("="*40)
    for category, count in sorted(category_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / len(articles)) * 100
        print(f"  {category:20s}: {count:2d} ({percentage:5.1f}%)")

if __name__ == "__main__":
    analyze_articles()
