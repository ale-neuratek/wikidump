#!/usr/bin/env python3
"""
🔍 DEBUG CONTENT MANAGER - Script para diagnosticar problemas
=============================================================
"""

import sys
import traceback
import json
from pathlib import Path

def test_basic_imports():
    """Test imports básicos"""
    print("🔍 1. TESTING BASIC IMPORTS")
    try:
        print("   - Importing sys, json, pathlib... ✅")
        
        # Test formation_fundamental imports
        sys.path.append('./formation_fundamental')
        from simple_formation_system import SimpleFormationSystem
        print("   - SimpleFormationSystem imported... ✅")
        
        from intelligent_question_generator import IntelligentQuestionGenerator
        print("   - IntelligentQuestionGenerator imported... ✅")
        
        # Test content_manager import
        from content_manager import SimplifiedContentManager
        print("   - SimplifiedContentManager imported... ✅")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Import error: {e}")
        traceback.print_exc()
        return False

def test_formation_system():
    """Test SimpleFormationSystem"""
    print("\n🔍 2. TESTING FORMATION SYSTEM")
    try:
        sys.path.append('./formation_fundamental')
        from simple_formation_system import SimpleFormationSystem
        
        fs = SimpleFormationSystem('formation')
        print("   - SimpleFormationSystem created... ✅")
        
        # Test get_category_for_title
        test_title = "Test Article"
        category = fs.get_category_for_title(test_title)
        print(f"   - get_category_for_title('{test_title}') = '{category}' ✅")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Formation system error: {e}")
        traceback.print_exc()
        return False

def test_content_manager_creation():
    """Test SimplifiedContentManager creation"""
    print("\n🔍 3. TESTING CONTENT MANAGER CREATION")
    try:
        from content_manager import SimplifiedContentManager
        
        cm = SimplifiedContentManager()
        print("   - SimplifiedContentManager created... ✅")
        
        # Check if it has process_article method
        if hasattr(cm, 'process_article'):
            print("   - process_article method exists... ✅")
        else:
            print("   - ❌ process_article method NOT found")
            return False
            
        return True
        
    except Exception as e:
        print(f"   ❌ Content Manager creation error: {e}")
        traceback.print_exc()
        return False

def test_process_article():
    """Test process_article method"""
    print("\n🔍 4. TESTING PROCESS_ARTICLE METHOD")
    try:
        from content_manager import SimplifiedContentManager
        
        cm = SimplifiedContentManager()
        print("   - SimplifiedContentManager created... ✅")
        
        # Test article
        test_article = {
            'title': 'Artículo de Prueba',
            'content': 'Este es un artículo de prueba con contenido suficiente para ser procesado. Contiene información histórica importante sobre eventos del pasado y personajes relevantes de la historia mundial.'
        }
        
        print(f"   - Test article: '{test_article['title']}'")
        print(f"   - Content length: {len(test_article['content'])} chars")
        
        # Process article
        result = cm.process_article(test_article)
        print(f"   - process_article returned: {type(result)}")
        
        if result:
            print(f"   - Result keys: {list(result.keys())}")
            print(f"   - Category: {result.get('category', 'N/A')}")
            print(f"   - Conversations: {len(result.get('conversations', []))}")
            print("   - process_article SUCCESS... ✅")
        else:
            print("   - ❌ process_article returned None/False")
            return False
            
        return True
        
    except Exception as e:
        print(f"   ❌ Process article error: {e}")
        traceback.print_exc()
        return False

def test_with_real_article():
    """Test with a real article from JSONL"""
    print("\n🔍 5. TESTING WITH REAL ARTICLE")
    try:
        # Load first article from data_test_ultra_hybrid
        jsonl_file = Path("data_test_ultra_hybrid/articles_hybrid_1_0000.jsonl")
        
        if not jsonl_file.exists():
            print(f"   ⚠️ JSONL file not found: {jsonl_file}")
            return False
            
        print(f"   - Reading from: {jsonl_file}")
        
        # Read first valid article
        with open(jsonl_file, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 5:  # Only check first 5 lines
                    break
                    
                if line.strip():
                    try:
                        article = json.loads(line)
                        if article.get('title') and article.get('content'):
                            print(f"   - Found article: '{article['title'][:50]}...'")
                            print(f"   - Content length: {len(article['content'])} chars")
                            
                            # Test with SimplifiedContentManager
                            from content_manager import SimplifiedContentManager
                            cm = SimplifiedContentManager()
                            
                            result = cm.process_article(article)
                            
                            if result:
                                print(f"   - Category: {result.get('category', 'N/A')}")
                                print(f"   - Conversations: {len(result.get('conversations', []))}")
                                print("   - Real article processing SUCCESS... ✅")
                                return True
                            else:
                                print("   - ❌ Real article processing returned None")
                                return False
                                
                    except json.JSONDecodeError:
                        continue
        
        print("   ⚠️ No valid articles found in first 5 lines")
        return False
        
    except Exception as e:
        print(f"   ❌ Real article test error: {e}")
        traceback.print_exc()
        return False

def main():
    """Main debugging function"""
    print("🔍 CONTENT MANAGER DEBUG SESSION")
    print("=" * 50)
    
    # Run tests
    tests = [
        test_basic_imports,
        test_formation_system,
        test_content_manager_creation,
        test_process_article,
        test_with_real_article
    ]
    
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"   💥 Test crashed: {e}")
            traceback.print_exc()
    
    print(f"\n📊 RESULTS: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 ALL TESTS PASSED - Content Manager is working!")
    else:
        print("❌ SOME TESTS FAILED - Debug needed")
    
    return passed == len(tests)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
