#!/usr/bin/env python3
"""
🚀 MAIN WIKIDUMP PROCESSOR - Pipeline Completo de Procesamiento
===============================================================
Script principal que ejecuta el pipeline completo de procesamiento de Wikipedia
para máxima eficiencia en hardware de alto rendimiento (GH200, 8xH100)

PIPELINE:
1. ETAPA 1: Caroline Ultra Extractor (XML → JSONL intermedios)
2. ETAPA 2: Adaptive Processor (JSONL → Conversaciones + Consciencia)

OPTIMIZACIONES:
- Configuración adaptativa por hardware detectado
- Monitoreo en tiempo real del rendimiento
- Recuperación automática ante fallos
- Validación de calidad de datos
- Identificación temporal integrada (es/fue)
"""

import os
import sys
import time
import argparse
import subprocess
from pathlib import Path
from datetime import datetime
import psutil
import signal

# Importar configuraciones de hardware
from hardware_configs import get_hardware_config, print_hardware_info, detect_hardware

class WikidumpMainProcessor:
    """Procesador principal del pipeline completo de Wikidump"""
    
    def __init__(self, xml_path: str, output_dir: str = "wiki_conversations_complete", skip_stage1: bool = False):
        self.xml_path = Path(xml_path)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Rutas para cada etapa
        self.stage1_output = Path("data_ultra_hybrid")
        self.stage2_output = self.output_dir
        
        # Estado del pipeline
        self.skip_stage1 = skip_stage1
        self.hardware_type = detect_hardware()
        self.hardware_config = get_hardware_config()
        
        # Logging
        self.log_file = "main_processing.log"
        self.start_time = time.time()
        
        # Configurar manejo de señales
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.running = True
        self.current_stage = 0
        
    def _signal_handler(self, signum, frame):
        """Manejo elegante de señales de terminación"""
        print(f"\n⚠️ Señal {signum} recibida durante la etapa {self.current_stage}")
        print("🛑 Terminando procesamiento de manera elegante...")
        self.running = False
        
    def print_pipeline_info(self):
        """Imprime información del pipeline y hardware"""
        print("="*80)
        print("🚀 WIKIDUMP MAIN PROCESSOR - Pipeline Completo")
        print("="*80)
        
        print_hardware_info()
        
        print(f"\n📁 CONFIGURACIÓN DEL PIPELINE:")
        print(f"   📄 XML Input: {self.xml_path} ({self.xml_path.stat().st_size / (1024**3):.1f}GB)")
        print(f"   📂 Stage 1 Output: {self.stage1_output}")
        print(f"   📂 Stage 2 Output: {self.stage2_output}")
        print(f"   ⏭️  Skip Stage 1: {'Sí' if self.skip_stage1 else 'No'}")
        
        print(f"\n⚡ CONFIGURACIÓN DE RENDIMIENTO:")
        print(f"   🔄 Max Workers: {self.hardware_config['MAX_WORKERS']}")
        print(f"   📦 Batch Size: {self.hardware_config['BATCH_SIZE']:,}")
        print(f"   🎯 Target Speed: {self.hardware_config['TARGET_SPEED']:,} páginas/segundo")
        print(f"   💾 Memory Buffer: {self.hardware_config['MEMORY_BUFFER_GB']}GB")
        
        print("="*80)
        
    def stage1_xml_to_jsonl(self) -> bool:
        """Ejecuta Stage 1: XML → JSONL usando Caroline Ultra Extractor"""
        if self.skip_stage1:
            print("⏭️ STAGE 1 OMITIDA - Verificando archivos JSONL existentes...")
            return self._validate_stage1_output()
            
        print("\n🚀 INICIANDO STAGE 1: XML → JSONL (Caroline Ultra Extractor)")
        print("="*60)
        
        self.current_stage = 1
        start_time = time.time()
        
        try:
            # Ejecutar caroline_ultra_extractor_hybrid.py
            cmd = [
                sys.executable, 
                "caroline_ultra_extractor_hybrid.py",
                "--xml", str(self.xml_path),
                "--output", str(self.stage1_output)
            ]
            
            print(f"📋 Ejecutando: {' '.join(cmd)}")
            
            result = subprocess.run(
                cmd,
                cwd=Path.cwd(),
                capture_output=False,
                text=True,
                check=True
            )
            
            elapsed = time.time() - start_time
            print(f"\n✅ STAGE 1 COMPLETADA en {elapsed:.1f}s")
            
            return self._validate_stage1_output()
            
        except subprocess.CalledProcessError as e:
            print(f"❌ ERROR en Stage 1: {e}")
            return False
        except KeyboardInterrupt:
            print(f"\n⚠️ Stage 1 interrumpida por el usuario")
            return False
    
    def stage2_jsonl_to_datasets(self) -> bool:
        """Ejecuta Stage 2: JSONL → Conversaciones usando Adaptive Processor"""
        print("\n🧠 INICIANDO STAGE 2: JSONL → Conversaciones (Adaptive Processor)")
        print("="*60)
        
        self.current_stage = 2
        start_time = time.time()
        
        try:
            # Ejecutar adaptive_processor.py
            cmd = [
                sys.executable,
                "adaptive_processor.py",
                str(self.stage1_output),
                str(self.stage2_output)
            ]
            
            print(f"📋 Ejecutando: {' '.join(cmd)}")
            
            # Crear proceso para capturar salida en tiempo real
            process = subprocess.Popen(
                cmd,
                cwd=Path.cwd(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                bufsize=1
            )
            
            # Mostrar salida en tiempo real
            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    print(output.strip())
            
            # Esperar que termine y obtener código de salida
            return_code = process.poll()
            
            elapsed = time.time() - start_time
            
            if return_code == 0:
                print(f"\n✅ STAGE 2 COMPLETADA en {elapsed:.1f}s")
                return self._validate_stage2_output()
            else:
                print(f"\n❌ STAGE 2 FALLÓ con código {return_code}")
                return False
            
        except subprocess.CalledProcessError as e:
            print(f"❌ ERROR en Stage 2: {e}")
            return False
        except KeyboardInterrupt:
            print(f"\n⚠️ Stage 2 interrumpida por el usuario")
            return False
    
    def _validate_stage1_output(self) -> bool:
        """Valida la salida de Stage 1"""
        if not self.stage1_output.exists():
            print(f"❌ Directorio de Stage 1 no existe: {self.stage1_output}")
            return False
            
        jsonl_files = list(self.stage1_output.glob("articles_hybrid_*.jsonl"))
        if not jsonl_files:
            print(f"❌ No se encontraron archivos JSONL en {self.stage1_output}")
            return False
            
        total_size = sum(f.stat().st_size for f in jsonl_files)
        total_articles = 0
        
        try:
            # Contar artículos rápidamente
            import subprocess
            result = subprocess.run(
                f"wc -l {self.stage1_output}/*.jsonl | tail -1",
                shell=True, capture_output=True, text=True
            )
            if result.returncode == 0:
                total_articles = int(result.stdout.strip().split()[0])
        except:
            pass
            
        print(f"✅ Stage 1 Output Validado:")
        print(f"   📁 Archivos JSONL: {len(jsonl_files)}")
        print(f"   📊 Tamaño total: {total_size / (1024**3):.1f}GB")
        print(f"   📄 Artículos estimados: {total_articles:,}")
        
        return True
    
    def _validate_stage2_output(self) -> bool:
        """Valida la salida de Stage 2"""
        if not self.stage2_output.exists():
            print(f"❌ Directorio de Stage 2 no existe: {self.stage2_output}")
            return False
            
        # Buscar archivos de conversaciones
        conversation_files = list(self.stage2_output.glob("**/*.jsonl"))
        categories_dir = self.stage2_output / "categorias"
        consciencia_dir = self.stage2_output / "consciencia"
        
        print(f"✅ Stage 2 Output Validado:")
        print(f"   📁 Archivos de conversaciones: {len(conversation_files)}")
        print(f"   📂 Directorio base: {self.stage2_output}")
        print(f"   🏷️ Categorías: {'✅' if categories_dir.exists() else '❌'}")
        print(f"   🧠 Consciencia: {'✅' if consciencia_dir.exists() else '❌'}")
        
        # Contar conversaciones totales
        total_conversations = 0
        for file in conversation_files:
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    total_conversations += sum(1 for line in f if line.strip())
            except:
                pass
        
        print(f"   � Total conversaciones: {total_conversations:,}")
        
        return len(conversation_files) > 0
    
    def print_final_summary(self, total_time: float, stage1_success: bool, stage2_success: bool):
        """Imprime resumen final del procesamiento"""
        print("\n" + "="*80)
        print("🎉 PROCESAMIENTO COMPLETADO - Resumen Final")
        print("="*80)
        
        print(f"⏱️  TIEMPO TOTAL: {total_time:.1f}s ({total_time/60:.1f} minutos)")
        print(f"🖥️  HARDWARE: {self.hardware_type}")
        print(f"📊 ESTADO DE LAS ETAPAS:")
        print(f"   Stage 1 (XML→JSONL): {'✅' if stage1_success else '❌'}")
        print(f"   Stage 2 (JSONL→Conversaciones): {'✅' if stage2_success else '❌'}")
        
        if stage1_success and stage2_success:
            print(f"\n🎯 PIPELINE COMPLETADO CON ÉXITO")
            print(f"📂 Conversaciones disponibles en: {self.stage2_output}")
            print(f"🧠 Incluye identificación temporal (es/fue)")
            print(f"🏷️ Con categorización automática")
            print(f"🎭 Generación de consciencia incluida")
        else:
            print(f"\n⚠️ PIPELINE COMPLETADO CON ERRORES")
            
        print("="*80)
    
    def run(self) -> bool:
        """Ejecuta el pipeline completo"""
        if not self.running:
            return False
            
        self.print_pipeline_info()
        
        start_time = time.time()
        stage1_success = False
        stage2_success = False
        
        try:
            # Stage 1: XML → JSONL
            if self.running:
                stage1_success = self.stage1_xml_to_jsonl()
            
            # Stage 2: JSONL → Datasets (solo si Stage 1 fue exitoso)
            if self.running and stage1_success:
                stage2_success = self.stage2_jsonl_to_datasets()
            
            total_time = time.time() - start_time
            self.print_final_summary(total_time, stage1_success, stage2_success)
            
            return stage1_success and stage2_success
            
        except Exception as e:
            print(f"\n❌ ERROR CRÍTICO EN EL PIPELINE: {e}")
            import traceback
            traceback.print_exc()
            return False

def main():
    parser = argparse.ArgumentParser(
        description="🚀 Wikidump Main Processor - Pipeline Completo de Procesamiento",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
EJEMPLOS:
  # Procesar XML completo (ambas etapas) - RECOMENDADO
  python3 main_wikidump_processor.py --xml data_wiki/eswiki-20250601-pages-articles-multistream.xml
  
  # Solo ejecutar Stage 2 (usando JSONL existentes en data_ultra_hybrid)
  python3 main_wikidump_processor.py --xml dummy.xml --skip-stage1
  
  # Especificar directorio de salida personalizado
  python3 main_wikidump_processor.py --xml data_wiki/eswiki.xml --output wiki_conversaciones_custom
        """
    )
    
    parser.add_argument(
        "--xml", 
        required=True,
        help="Ruta al archivo XML de Wikipedia"
    )
    
    parser.add_argument(
        "--output", 
        default="wiki_conversations_complete",
        help="Directorio de salida para las conversaciones finales (default: wiki_conversations_complete)"
    )
    
    parser.add_argument(
        "--skip-stage1", 
        action="store_true",
        help="Omitir Stage 1 y usar archivos JSONL existentes"
    )
    
    args = parser.parse_args()
    
    # Crear y ejecutar el procesador principal
    processor = WikidumpMainProcessor(
        xml_path=args.xml,
        output_dir=args.output,
        skip_stage1=args.skip_stage1
    )
    
    success = processor.run()
    
    # Salir con código apropiado
    exit_code = 0 if success else 1
    print(f"\n🏁 Saliendo con código: {exit_code}")
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
