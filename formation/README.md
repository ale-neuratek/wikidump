# Content Manager - Arquitectura Refactorizada

## 📁 Estructura de Archivos

El `content_manager.py` ha sido refactorizado para separar la lógica de configuración en archivos JSON independientes organizados en la carpeta `formation/`.

### 🗂️ Carpeta Formation

```
formation/
├── fundamental.jsonl          # Configuración base y patrones generales
├── arte.jsonl                # Configuración específica para Arte
├── biologia.jsonl             # Configuración específica para Biología  
├── ciencias.jsonl             # Configuración específica para Ciencias
├── deportes.jsonl             # Configuración específica para Deportes
├── economia.jsonl             # Configuración específica para Economía
├── educacion.jsonl            # Configuración específica para Educación
├── general.jsonl              # Configuración específica para General
├── geografia.jsonl            # Configuración específica para Geografía
├── historia.jsonl             # Configuración específica para Historia
├── medicina.jsonl             # Configuración específica para Medicina
├── politica.jsonl             # Configuración específica para Política
└── tecnologia.jsonl           # Configuración específica para Tecnología
```

## 📋 Formato de Archivos JSONL

Cada archivo JSONL contiene múltiples líneas JSON con diferentes tipos de configuración:

### fundamental.jsonl
- `title_patterns`: Patrones para inferir tipos de contenido
- `music_patterns`: Patrones específicos para música
- `art_patterns`: Patrones específicos para arte
- `deceased_patterns`: Patrones para detectar personas fallecidas
- `organization_patterns`: Patrones para organizaciones
- `defunct_patterns`: Patrones para organizaciones extintas
- `historical_patterns`: Patrones para eventos históricos
- `conversation_templates_general`: Plantillas generales de conversación
- `verb_tenses`: Configuración de tiempos verbales
- `fallback_responses`: Respuestas de fallback por tipo

### {categoria}.jsonl
- `category_config`: Configuración principal de la categoría (keywords, patterns, weight)
- `subcategory_patterns`: Patrones para subcategorías
- `conversation_templates`: Plantillas específicas de conversación
- `category_keywords`: Keywords específicas para métricas de confianza

## 🔧 Componentes Refactorizados

### FormationLoader
- **Propósito**: Carga y gestiona configuraciones desde archivos JSON
- **Métodos principales**:
  - `load_all_configurations()`: Carga todas las configuraciones
  - `get_fundamental_data(type)`: Obtiene datos fundamentales
  - `get_category_data(category, type)`: Obtiene datos de categoría específica

### TitleInferenceEngine  
- **Propósito**: Infiere tipos de contenido basado en títulos
- **Configuración**: Carga patrones desde `fundamental.jsonl`
- **Mejoras**: Más flexible y configurable externamente

### ConfidenceMetrics
- **Propósito**: Calcula métricas de confianza para categorización
- **Configuración**: Usa keywords desde archivos de categoría
- **Mejoras**: Métricas más precisas y configurables

### IntelligentCategorizer
- **Propósito**: Categorización inteligente de artículos
- **Configuración**: Carga todos los patrones desde archivos JSON
- **Mejoras**: Completamente configurable sin hardcoding

### ContentManager
- **Propósito**: Gestor principal de contenido
- **Configuración**: Inicializa todos los componentes con FormationLoader
- **Compatibilidad**: Mantiene la misma API externa

## 🚀 Beneficios del Refactor

### ✅ Mantenibilidad
- Configuraciones separadas por categoría
- Fácil edición sin tocar código Python
- Versionado independiente de configuraciones

### ✅ Escalabilidad  
- Nuevas categorías se añaden con nuevos archivos JSON
- Patrones modificables sin recompilación
- Configuración modular y reutilizable

### ✅ Flexibilidad
- Plantillas de conversación personalizables
- Patrones de detección ajustables
- Métricas de confianza configurables

### ✅ Separación de Responsabilidades
- Lógica de negocio en Python
- Configuración de datos en JSON
- Mantenimiento independiente

## 🧪 Testing

Ejecutar pruebas de verificación:
```bash
python test_refactor.py
```

## 📈 Compatibilidad

El refactor mantiene **100% compatibilidad** con el código existente:
- Misma API pública en ContentManager
- Mismos métodos y parámetros
- Misma estructura de respuesta
- Compatible con adaptive_processor.py y main_wikidump_processor.py

## 🔧 Configuración Personalizada

Para personalizar categorías o patrones:

1. **Editar archivos existentes**: Modificar `.jsonl` en `formation/`
2. **Añadir nueva categoría**: Crear nuevo archivo `{categoria}.jsonl`
3. **Modificar patrones fundamentales**: Editar `fundamental.jsonl`

### Ejemplo de nueva categoría:
```json
{"type": "category_config", "data": {"name": "mi_categoria", "keywords": ["palabra1", "palabra2"], "patterns": ["\\bpatron1\\b"], "weight": 8}}
{"type": "conversation_templates", "data": {"basic": ["¿Qué es {topic}?"], "deep": "Explica completamente {topic}."}}
```

## 📊 Monitoreo

El sistema mantiene todas las métricas originales:
- Estadísticas de procesamiento
- Métricas de confianza  
- Análisis de artículos de baja confianza
- Historial de categorizaciones

---

*Esta refactorización mejora significativamente la mantenibilidad y flexibilidad del sistema sin comprometer el rendimiento.*
