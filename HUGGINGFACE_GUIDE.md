# 🤗 GUÍA DE PUBLICACIÓN EN HUGGING FACE

## 📋 Resumen

Esta integración permite publicar automáticamente los datasets procesados de Wikipedia en un repositorio privado de Hugging Face usando SSH y autenticación por token.

## 🎯 Repositorio Destino

- **Nombre**: `ale-neuratek/wikidump`
- **Tipo**: Dataset privado
- **Acceso**: Solo usuarios autorizados

## 🚀 Opciones de Uso

### 1. Pipeline Completo + Publicación Automática

```bash
# Procesar Wikipedia completa y publicar automáticamente
./init.sh
```

**Qué hace:**
- Descarga dump de Wikipedia
- Procesa XML → JSONL → Conversaciones
- Al finalizar, pregunta si publicar en HF
- Configura autenticación automáticamente
- Publica con metadatos completos

### 2. Publicación Independiente

```bash
# Solo publicar datasets ya procesados
./publish_to_hf.sh [directorio] [repositorio]

# Ejemplos:
./publish_to_hf.sh wiki-datasets ale-neuratek/wikidump
./publish_to_hf.sh ../wiki_conversations ale-neuratek/wikidump-v2
```

**Ventajas:**
- No requiere reprocesar datos
- Útil para múltiples versiones
- Control granular sobre qué publicar

### 3. Validación Previa

```bash
# Validar configuración sin subir nada
python3 validate_hf_setup.py [directorio]

# Opciones avanzadas:
python3 validate_hf_setup.py wiki-datasets --test-speed
python3 validate_hf_setup.py data_custom --repo mi-org/mi-dataset
```

**Funciones:**
- Verifica token de HF
- Prueba permisos de escritura
- Analiza datos locales
- Estima tiempo de subida

### 4. Demostración Rápida

```bash
# Demo con datos de ejemplo
./demo_hf.sh
```

**Incluye:**
- Datos sintéticos de prueba
- Validación completa
- Opciones de publicación
- Limpieza automática

## 🔐 Configuración Inicial

### Paso 1: Token de Hugging Face

1. Ve a https://huggingface.co/settings/tokens
2. Crea un token con permisos **Write**
3. El sistema te pedirá el token en el primer uso
4. Se guarda automáticamente en `~/.cache/huggingface/token`

### Paso 2: Verificar Acceso al Repositorio

```bash
# Verificar que tienes acceso de escritura
python3 validate_hf_setup.py --repo ale-neuratek/wikidump
```

### Paso 3: Personalizar Configuración (Opcional)

Edita `hf_config.env` para cambiar:
- Repositorio destino
- Configuración de privacidad
- Metadatos del dataset
- Opciones de upload

## 📊 Estructura de Publicación

### Archivos Generados en HF

```
ale-neuratek/wikidump/
├── README.md                          # Metadatos automáticos
├── arte/
│   ├── conversaciones_arte_001.jsonl
│   ├── conversaciones_arte_002.jsonl
│   └── ...
├── geografia/
│   ├── conversaciones_geografia_001.jsonl
│   └── ...
├── historia/
│   └── conversaciones_historia_001.jsonl
└── ...
```

### README Automático Incluye:

- **Estadísticas completas**: Conversaciones, categorías, tamaño
- **Metadatos de generación**: Fecha, fuente, versión
- **Ejemplos de uso**: Código Python para cargar datos
- **Información de licencia**: CC BY-SA 3.0
- **Categorías encontradas**: Lista completa organizada

## 🛠️ Configuración Avanzada

### Cambiar Repositorio Destino

```bash
# Opción 1: Parámetro directo
./publish_to_hf.sh wiki-datasets mi-organizacion/mi-dataset

# Opción 2: Editar hf_config.env
nano hf_config.env
# Cambiar: HF_REPO_NAME="mi-organizacion/mi-dataset"
```

### Repositorio Público vs Privado

```bash
# En hf_config.env
HF_PRIVATE_REPO=false  # Público
HF_PRIVATE_REPO=true   # Privado (default)
```

### Configurar Múltiples Tokens

```bash
# Token específico por repositorio
HF_TOKEN=mi_token_especial ./publish_to_hf.sh datos repo-especial
```

## 🚨 Solución de Problemas

### Error: "Token inválido"

```bash
# Reconfigurar token
rm ~/.cache/huggingface/token
python3 validate_hf_setup.py
```

### Error: "Repositorio no existe"

```bash
# El script creará automáticamente el repositorio
# Asegúrate de tener permisos en la organización
```

### Error: "Git LFS no instalado"

```bash
# Se instala automáticamente, pero si falla:
curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | sudo bash
sudo apt-get install git-lfs
git lfs install
```

### Archivos Muy Grandes

El sistema usa Git LFS automáticamente para archivos >100MB.
Configurar en `hf_config.env`:

```bash
LFS_THRESHOLD_MB=50  # Usar LFS para archivos >50MB
```

## 📈 Optimizaciones

### Para Datasets Grandes

```bash
# Configurar uploads paralelos
MAX_PARALLEL_UPLOADS=10

# Aumentar timeout de red
NETWORK_TIMEOUT=600
```

### Monitoreo de Progreso

```bash
# Ver logs detallados
tail -f hf_upload.log

# Verificar progreso en HF
# https://huggingface.co/datasets/ale-neuratek/wikidump
```

## 🔒 Consideraciones de Seguridad

- ✅ **Repositorio privado** por defecto
- ✅ **Token seguro** en directorio de usuario
- ✅ **Archivos sensibles** excluidos del git
- ✅ **Autenticación SSH** para git
- ✅ **Sin credenciales** en el código

## 📞 Soporte

Si encuentras problemas:

1. Ejecuta el validador: `python3 validate_hf_setup.py`
2. Revisa los logs: `cat hf_upload.log`
3. Prueba el demo: `./demo_hf.sh`
4. Verifica permisos en el repositorio HF

---

**Estado**: ✅ Completamente funcional  
**Última actualización**: 2025-07-16  
**Autor**: Alejandro Iglesias - Neuratek.cl
