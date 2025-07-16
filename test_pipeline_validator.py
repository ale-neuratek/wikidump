#!/usr/bin/env python3
"""
Pipeline Validator - Herramienta para validar el pipeline con un subconjunto controlado
Analiza cuellos de botella, valida contenido y verifica generación completa por categoría
"""

import os
import sys
import time
import json
import subprocess
from pathlib import Path
import shutil
from typing import Dict, List, Tuple

class PipelineValidator:
    def __init__(self, base_dir: str = "/home/ubuntu/wikidump_workspace/wikidump"):
        self.base_dir = Path(base_dir)
        self.data_dir = self.base_dir / "data_ultra_hybrid"
        self.test_data_dir = self.base_dir / "data_test_validation"
        self.output_dir = self.base_dir / "consciencia_validation"
        
    def create_test_dataset(self, num_files: int = 3) -> List[str]:
        """Crear un dataset de prueba con archivos seleccionados"""
        print(f"🔧 Creando dataset de prueba con {num_files} archivos...")
        
        # Limpiar directorio de prueba anterior
        if self.test_data_dir.exists():
            shutil.rmtree(self.test_data_dir)
        self.test_data_dir.mkdir(exist_ok=True)
        
        # Seleccionar archivos más pequeños para prueba rápida
        available_files = list(self.data_dir.glob("articles_hybrid_*.jsonl"))
        available_files.sort()
        
        # Obtener información de tamaño de archivos
        file_sizes = []
        for file_path in available_files[:10]:  # Solo revisar los primeros 10
            try:
                with open(file_path, 'r') as f:
                    lines = sum(1 for _ in f)
                file_sizes.append((file_path, lines))
            except Exception as e:
                print(f"⚠️ Error leyendo {file_path}: {e}")
        
        # Ordenar por tamaño y tomar los más pequeños
        file_sizes.sort(key=lambda x: x[1])
        selected_files = file_sizes[:num_files]
        
        print("📊 Archivos seleccionados para prueba:")
        total_articles = 0
        copied_files = []
        
        for file_path, lines in selected_files:
            dest_path = self.test_data_dir / file_path.name
            shutil.copy2(file_path, dest_path)
            copied_files.append(str(dest_path))
            total_articles += lines
            print(f"   📄 {file_path.name}: {lines:,} artículos")
        
        print(f"✅ Dataset de prueba creado: {total_articles:,} artículos totales")
        return copied_files
    
    def run_pipeline_test(self, files: List[str]) -> Tuple[bool, Dict]:
        """Ejecutar el pipeline con el dataset de prueba"""
        print("\n🚀 Ejecutando pipeline de prueba...")
        
        # Limpiar directorio de salida anterior
        if self.output_dir.exists():
            shutil.rmtree(self.output_dir)
        
        start_time = time.time()
        
        # Ejecutar adaptive_processor con el dataset de prueba
        cmd = [
            "python3", 
            str(self.base_dir / "adaptive_processor.py"),
            "--input", str(self.test_data_dir),
            "--output", str(self.output_dir)
        ]
        
        print(f"🔧 Comando: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.base_dir),
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutos máximo
            )
            
            end_time = time.time()
            duration = end_time - start_time
            
            success = result.returncode == 0
            
            execution_info = {
                "success": success,
                "duration": duration,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode
            }
            
            if success:
                print(f"✅ Pipeline completado en {duration:.1f}s")
            else:
                print(f"❌ Pipeline falló después de {duration:.1f}s")
                print(f"Error: {result.stderr}")
            
            return success, execution_info
            
        except subprocess.TimeoutExpired:
            print("⏰ Pipeline timeout después de 30 minutos")
            return False, {"error": "timeout", "duration": 1800}
        except Exception as e:
            print(f"💥 Error ejecutando pipeline: {e}")
            return False, {"error": str(e), "duration": 0}
    
    def analyze_bottlenecks(self, execution_info: Dict) -> Dict:
        """Analizar cuellos de botella del pipeline"""
        print("\n🔍 Analizando cuellos de botella...")
        
        bottlenecks = {
            "queue_warnings": 0,
            "timeout_issues": 0,
            "memory_issues": 0,
            "performance_metrics": {}
        }
        
        if "stdout" in execution_info:
            output = execution_info["stdout"]
            
            # Contar advertencias de colas llenas
            queue_warnings = output.count("Cola llena después de")
            bottlenecks["queue_warnings"] = queue_warnings
            
            if queue_warnings > 0:
                print(f"⚠️ Detectadas {queue_warnings} advertencias de colas llenas")
            
            # Buscar métricas de rendimiento
            lines = output.split('\n')
            for line in lines:
                if "artículos/hora" in line:
                    try:
                        speed = line.split("artículos/hora")[0].split()[-1]
                        bottlenecks["performance_metrics"]["articles_per_hour"] = int(speed)
                    except:
                        pass
                elif "Conversaciones generadas:" in line:
                    try:
                        conv = line.split("Conversaciones generadas:")[1].strip().split()[0]
                        bottlenecks["performance_metrics"]["conversations"] = int(conv.replace(',', ''))
                    except:
                        pass
        
        return bottlenecks
    
    def validate_content_quality(self) -> Dict:
        """Validar la calidad del contenido generado"""
        print("\n🔍 Validando calidad del contenido...")
        
        validation = {
            "categories_created": 0,
            "files_generated": 0,
            "total_conversations": 0,
            "category_distribution": {},
            "content_samples": {},
            "validation_errors": []
        }
        
        if not self.output_dir.exists():
            validation["validation_errors"].append("Directorio de salida no existe")
            return validation
        
        # Verificar estructura de directorios
        categorias_dir = self.output_dir / "categorias"
        if not categorias_dir.exists():
            validation["validation_errors"].append("Directorio de categorías no existe")
            return validation
        
        # Analizar cada categoría
        for cat_dir in categorias_dir.iterdir():
            if cat_dir.is_dir():
                cat_name = cat_dir.name
                validation["categories_created"] += 1
                
                # Contar archivos y conversaciones en esta categoría
                jsonl_files = list(cat_dir.glob("*.jsonl"))
                validation["files_generated"] += len(jsonl_files)
                
                cat_conversations = 0
                content_samples = []
                
                for jsonl_file in jsonl_files:
                    try:
                        with open(jsonl_file, 'r', encoding='utf-8') as f:
                            lines = 0
                            for line_num, line in enumerate(f):
                                if line.strip():
                                    lines += 1
                                    if line_num < 3:  # Tomar muestras de las primeras líneas
                                        try:
                                            data = json.loads(line)
                                            content_samples.append(data)
                                        except:
                                            pass
                            cat_conversations += lines
                    except Exception as e:
                        validation["validation_errors"].append(f"Error leyendo {jsonl_file}: {e}")
                
                validation["category_distribution"][cat_name] = cat_conversations
                validation["total_conversations"] += cat_conversations
                validation["content_samples"][cat_name] = content_samples[:2]  # 2 muestras por categoría
        
        print(f"✅ Categorías creadas: {validation['categories_created']}")
        print(f"✅ Archivos generados: {validation['files_generated']}")
        print(f"✅ Conversaciones totales: {validation['total_conversations']:,}")
        
        return validation
    
    def generate_validation_report(self, bottlenecks: Dict, validation: Dict, execution_info: Dict):
        """Generar reporte completo de validación"""
        print("\n📊 REPORTE DE VALIDACIÓN DEL PIPELINE")
        print("=" * 60)
        
        # Resumen de ejecución
        print(f"\n🚀 EJECUCIÓN DEL PIPELINE:")
        print(f"   ✅ Estado: {'Exitoso' if execution_info.get('success') else 'Fallido'}")
        print(f"   ⏱️ Duración: {execution_info.get('duration', 0):.1f}s")
        
        # Análisis de cuellos de botella
        print(f"\n🔍 ANÁLISIS DE CUELLOS DE BOTELLA:")
        print(f"   ⚠️ Advertencias de colas: {bottlenecks['queue_warnings']}")
        
        if bottlenecks['queue_warnings'] > 0:
            print(f"   🎯 RECOMENDACIÓN: Incrementar tamaño de colas o reducir batch size")
        
        perf = bottlenecks['performance_metrics']
        if perf:
            print(f"   🚀 Rendimiento:")
            if 'articles_per_hour' in perf:
                print(f"      📈 {perf['articles_per_hour']:,} artículos/hora")
            if 'conversations' in perf:
                print(f"      💬 {perf['conversations']:,} conversaciones generadas")
        
        # Validación de contenido
        print(f"\n✅ VALIDACIÓN DE CONTENIDO:")
        print(f"   📁 Categorías creadas: {validation['categories_created']}")
        print(f"   📄 Archivos generados: {validation['files_generated']}")
        print(f"   💬 Conversaciones totales: {validation['total_conversations']:,}")
        
        if validation['category_distribution']:
            print(f"\n📊 DISTRIBUCIÓN POR CATEGORÍA:")
            sorted_cats = sorted(validation['category_distribution'].items(), 
                               key=lambda x: x[1], reverse=True)
            for cat, count in sorted_cats:
                percentage = (count / validation['total_conversations']) * 100
                print(f"   📂 {cat}: {count:,} ({percentage:.1f}%)")
        
        # Muestras de contenido
        print(f"\n🔍 MUESTRAS DE CONTENIDO:")
        for cat, samples in validation['content_samples'].items():
            if samples:
                print(f"\n   📂 {cat.upper()}:")
                for i, sample in enumerate(samples[:1]):  # Solo una muestra por categoría
                    if 'conversations' in sample:
                        conv = sample['conversations'][0] if sample['conversations'] else {}
                        if 'human' in conv and 'assistant' in conv:
                            human_preview = conv['human'][:100] + "..." if len(conv['human']) > 100 else conv['human']
                            assistant_preview = conv['assistant'][:100] + "..." if len(conv['assistant']) > 100 else conv['assistant']
                            print(f"      🙋 Human: {human_preview}")
                            print(f"      🤖 Assistant: {assistant_preview}")
        
        # Errores encontrados
        if validation['validation_errors']:
            print(f"\n❌ ERRORES DE VALIDACIÓN:")
            for error in validation['validation_errors']:
                print(f"   💥 {error}")
        
        print("\n" + "=" * 60)
        
        # Recomendaciones finales
        print(f"\n🎯 RECOMENDACIONES:")
        
        if bottlenecks['queue_warnings'] > 10:
            print(f"   🔧 CRÍTICO: Demasiadas advertencias de colas ({bottlenecks['queue_warnings']})")
            print(f"      → Incrementar QUEUE_SIZE en hardware_configs.py")
            print(f"      → Reducir BATCH_SIZE para menor presión en colas")
        elif bottlenecks['queue_warnings'] > 0:
            print(f"   ⚠️ MODERADO: Algunas advertencias de colas ({bottlenecks['queue_warnings']})")
            print(f"      → Monitorear en dataset completo")
        else:
            print(f"   ✅ EXCELENTE: Sin problemas de colas detectados")
        
        if validation['categories_created'] >= 10:
            print(f"   ✅ BUENO: Distribución de categorías adecuada ({validation['categories_created']} categorías)")
        else:
            print(f"   ⚠️ Pocas categorías generadas ({validation['categories_created']})")
        
        if validation['total_conversations'] > 1000:
            print(f"   ✅ BUENO: Volumen de conversaciones adecuado ({validation['total_conversations']:,})")
        else:
            print(f"   ⚠️ Bajo volumen de conversaciones ({validation['total_conversations']:,})")
    
    def run_full_validation(self, num_files: int = 3):
        """Ejecutar validación completa del pipeline"""
        print("🔬 INICIANDO VALIDACIÓN COMPLETA DEL PIPELINE")
        print("=" * 60)
        
        # Paso 1: Crear dataset de prueba
        files = self.create_test_dataset(num_files)
        
        # Paso 2: Ejecutar pipeline
        success, execution_info = self.run_pipeline_test(files)
        
        # Paso 3: Analizar cuellos de botella
        bottlenecks = self.analyze_bottlenecks(execution_info)
        
        # Paso 4: Validar contenido
        validation = self.validate_content_quality()
        
        # Paso 5: Generar reporte
        self.generate_validation_report(bottlenecks, validation, execution_info)
        
        return {
            "success": success,
            "execution_info": execution_info,
            "bottlenecks": bottlenecks,
            "validation": validation
        }

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Pipeline Validator")
    parser.add_argument("--files", type=int, default=3, help="Número de archivos para prueba")
    parser.add_argument("--base-dir", default="/home/ubuntu/wikidump_workspace/wikidump", help="Directorio base")
    
    args = parser.parse_args()
    
    validator = PipelineValidator(args.base_dir)
    results = validator.run_full_validation(args.files)
    
    if results["success"]:
        print("\n🎉 VALIDACIÓN COMPLETADA EXITOSAMENTE")
        return 0
    else:
        print("\n❌ VALIDACIÓN FALLÓ")
        return 1

if __name__ == "__main__":
    sys.exit(main())
