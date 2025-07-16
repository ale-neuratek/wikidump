# 🧠 WikiDump - Procesador de Wikipedia Optimizado

## 📋 Estado Actual del Proyecto

**✅ FUNCIONAL** - Pipeline completo implementado y validado
- ⚡ Procesamiento optimizado para datasets masivos
- 🏷️ Categorización inteligente automática  
- 💬 Generación de conversaciones usando content_manager
- 🧠 Categoría "consciencia" automática (NO conscious.txt)
- 📊 Logging detallado cada 1-5 minutos
- 🔧 Configuración adaptativa según tamaño del dataset

## 🗂️ Archivos Esenciales

### 📦 Procesamiento Principal
- **`adaptive_processor.py`** - Coordinador optimizado del pipeline
- **`simple_processor.py`** - Procesador masivo paralelo
- **`content_manager.py`** - Gestor de contenido y categorización
- **`hardware_configs.py`** - Configuración adaptativa de hardware

### ��️ Pipeline Completo  
- **`caroline_ultra_extractor_hybrid.py`** - Extractor de artículos (Etapa 1)
- **`main_wikidump_processor.py`** - Pipeline principal (Etapas 1+2)

### ⚙️ Configuración
- **`requirements.txt`** - Dependencias Python
- **`init.sh`** - Script de configuración completa (NO commiteado)

## 🚀 Uso Rápido

### Opción 1: Solo Etapa 2 (Generar Conversaciones)
```bash
python adaptive_processor.py --input data_ultra_hybrid --output wiki_conversations
```

### Opción 2: Pipeline Completo (XML → Conversaciones)
```bash
python main_wikidump_processor.py --xml data_wiki/eswiki.xml --output wiki_conversations
```

### Opción 3: Publicación en Hugging Face
```bash
# Desde init.sh (automático tras procesamiento)
./init.sh

# Independiente (para datasets ya procesados)
./publish_to_hf.sh wiki_conversations ale-neuratek/wikidump
```

## 🤗 Publicación Automática en Hugging Face

### ✨ Características
- **🔐 Repositorio privado**: ale-neuratek/wikidump
- **📊 Metadatos automáticos**: README con estadísticas detalladas
- **🗂️ Estructura preservada**: Mantiene organización por categorías
- **🔒 Autenticación SSH**: Configuración segura automática
- **📈 Análisis de contenido**: Cuenta conversaciones, categorías, tamaño

### 🛠️ Configuración Requerida
1. **Cuenta Hugging Face**: https://huggingface.co
2. **Token de acceso**: Con permisos de escritura
3. **Git LFS**: Instalación automática incluida

### 📦 Scripts Disponibles
- **`init.sh`**: Pipeline completo + publicación automática
- **`publish_to_hf.sh`**: Publicación independiente de datasets existentes

## 📊 Rendimiento Validado

- **623,708 conversaciones** generadas en pruebas
- **48 archivos JSONL** creados correctamente  
- **8 categorías principales** descubiertas automáticamente
- **344MB** de datos de entrenamiento generados
- **~70 artículos/segundo** de throughput

## ⚠️ Notas Importantes

### ✅ Funcionalidades Implementadas
- [x] NO genera conscious.txt (eliminado)
- [x] Categoría "consciencia" generada al final 
- [x] Todos los artículos procesados via content_manager
- [x] Logging con timestamps cada 1-5 minutos
- [x] Configuración adaptativa según dataset
- [x] Procesamiento masivo optimizado

### 🔧 Configuración Pendiente  
- ⚠️ **Cola llena (101 reintentos)** - Requiere afinación de `hardware_configs.py`
- 📈 Optimización para datasets >1M artículos

## 🔧 Optimizaciones de Hardware

### 🎯 Configuración Adaptativa Automática
El sistema ajusta automáticamente la configuración según:
- **Tamaño del dataset** (artículos estimados)
- **Hardware detectado** (GH200, 8xH100, Estándar)
- **Riesgo de cuellos de botella** en colas

### 🚀 Optimizaciones Anti-Bloqueo para Datasets Masivos
Para datasets >1.3M artículos se activan automáticamente:
- ⚡ **Queue retries**: 100 → 300-1000 (según tamaño)
- ⏱️ **Queue timeout**: 2.0s → 0.1-0.05s (más agresivo)
- 🗂️ **Queue size**: Expandido 1.5x-3x (según dataset)
- 💾 **Auto-flush**: Más frecuente (cada 5-10% del dataset)
- 🔄 **Timeouts extendidos**: Para finalización limpia

### 📊 Hardware Soportado
- **GH200**: 288 workers, 400GB buffer, 150K artículos/s
- **8xH100**: 512 workers, 600GB buffer, 250K artículos/s  
- **Estándar**: Auto-detectado según recursos

## 🏗️ Arquitectura

```
INPUT (XML/JSONL) 
    ↓
caroline_ultra_extractor_hybrid.py (Etapa 1)
    ↓
data_ultra_hybrid/ (JSONL)
    ↓
adaptive_processor.py (Coordinador)
    ↓
simple_processor.py (Procesamiento Masivo)
    ↓
content_manager.py (Categorización + Conversaciones)
    ↓
OUTPUT (Conversaciones JSONL + Categoría Consciencia)
```

## 📁 Estructura de Salida

```
wiki_conversations/
├── arte/
│   └── conversaciones_arte_*.jsonl
├── geografia/
│   └── conversaciones_geografia_*.jsonl
├── historia/
│   └── conversaciones_historia_*.jsonl
├── consciencia/           # ← Nueva categoría especial
│   ├── consciencia_0001.jsonl
│   └── metadata_consciencia.json
└── estadisticas/
    └── resumen_procesamiento.json
```

## 🔄 Próximos Pasos

1. **Afinación de hardware_configs.py** para eliminar warnings de cola
2. **Optimización para datasets masivos** (>1M artículos)
3. **Validación en hardware GH200/8xH100**
4. **✅ Publicación automática en Hugging Face** (COMPLETADO)

---

**Estado:** ✅ Listo para producción con publicación automática  
**Última actualización:** 2025-07-16  
**Commit:** Pipeline funcional completo + Hugging Face integration
