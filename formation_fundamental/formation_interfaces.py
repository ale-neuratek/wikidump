#!/usr/bin/env python3
"""
📋 FORMATION SYSTEM INTERFACE - Contrato para sistemas de formación
==================================================================
Define la interface que debe implementar cualquier sistema de formación
para ser compatible con content_manager y otros componentes.

PROPÓSITO:
- Permitir inyección de dependencias
- Facilitar testing con mocks
- Habilitar futuras implementaciones mejoradas
- Mantener compatibilidad hacia atrás
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from pathlib import Path


class IFormationSystem(ABC):
    """Interface base para sistemas de formación de conversaciones"""
    
    @abstractmethod
    def get_conversation_templates(self, content_type: str = None) -> Dict[str, Any]:
        """
        Obtiene templates de conversación para un tipo de contenido
        
        Args:
            content_type: Tipo de contenido (persona, lugar, concepto, etc.)
            
        Returns:
            Dict con templates de preguntas y respuestas
        """
        pass
    
    @abstractmethod
    def get_title_patterns(self) -> Dict[str, Any]:
        """
        Obtiene patrones para clasificación de títulos
        
        Returns:
            Dict con patrones de reconocimiento de tipos de contenido
        """
        pass
    
    @abstractmethod
    def get_verb_tenses(self) -> Dict[str, Dict[str, str]]:
        """
        Obtiene configuración de tiempos verbales
        
        Returns:
            Dict con conjugaciones verbales (presente/pasado)
        """
        pass
    
    @abstractmethod
    def get_fallback_responses(self) -> Dict[str, str]:
        """
        Obtiene respuestas de fallback por tipo de contenido
        
        Returns:
            Dict con templates de respuesta por defecto
        """
        pass
    
    @abstractmethod
    def get_quality_metrics_config(self) -> Dict[str, Any]:
        """
        Obtiene configuración de métricas de calidad
        
        Returns:
            Dict con configuración para evaluación de calidad
        """
        pass
    
    @abstractmethod
    def validate_conversation(self, conversation: Dict) -> Dict[str, Any]:
        """
        Valida una conversación generada
        
        Args:
            conversation: Conversación a validar
            
        Returns:
            Dict con resultado de validación y métricas
        """
        pass
    
    @abstractmethod
    def optimize_from_feedback(self, feedback_data: List[Dict]) -> bool:
        """
        Optimiza el sistema basado en feedback de conversaciones
        
        Args:
            feedback_data: Lista de datos de feedback
            
        Returns:
            bool: True si la optimización fue exitosa
        """
        pass
    
    @abstractmethod
    def get_system_version(self) -> str:
        """
        Obtiene la versión del sistema de formación
        
        Returns:
            str: Versión del sistema
        """
        pass
    
    @abstractmethod
    def reload_configuration(self) -> bool:
        """
        Recarga la configuración desde fundamental.jsonl
        
        Returns:
            bool: True si la recarga fue exitosa
        """
        pass


class IFormationDataProvider(ABC):
    """Interface para proveedores de datos de formación"""
    
    @abstractmethod
    def load_fundamental_data(self, data_type: str = None) -> Dict[str, Any]:
        """
        Carga datos fundamentales desde el archivo de configuración
        
        Args:
            data_type: Tipo específico de datos a cargar
            
        Returns:
            Dict con los datos solicitados
        """
        pass
    
    @abstractmethod
    def save_fundamental_data(self, data: Dict[str, Any]) -> bool:
        """
        Guarda datos en el archivo fundamental
        
        Args:
            data: Datos a guardar
            
        Returns:
            bool: True si el guardado fue exitoso
        """
        pass
    
    @abstractmethod
    def get_data_timestamp(self) -> str:
        """
        Obtiene el timestamp de la última modificación de datos
        
        Returns:
            str: Timestamp de última modificación
        """
        pass


class IConversationGenerator(ABC):
    """Interface para generadores de conversaciones"""
    
    @abstractmethod
    def generate_conversation(self, title: str, content: str, 
                            conversation_type: str = "basic") -> Dict[str, Any]:
        """
        Genera una conversación desde contenido
        
        Args:
            title: Título del artículo
            content: Contenido del artículo
            conversation_type: Tipo de conversación (basic, deep, specific)
            
        Returns:
            Dict con la conversación generada
        """
        pass
    
    @abstractmethod
    def generate_multiple_conversations(self, articles: List[Dict], 
                                      max_per_article: int = 3) -> List[Dict]:
        """
        Genera múltiples conversaciones desde una lista de artículos
        
        Args:
            articles: Lista de artículos con title y content
            max_per_article: Máximo número de conversaciones por artículo
            
        Returns:
            List de conversaciones generadas
        """
        pass


class IQualityValidator(ABC):
    """Interface para validadores de calidad"""
    
    @abstractmethod
    def validate_quality(self, conversation: Dict) -> Dict[str, float]:
        """
        Valida la calidad de una conversación
        
        Args:
            conversation: Conversación a validar
            
        Returns:
            Dict con métricas de calidad
        """
        pass
    
    @abstractmethod
    def get_quality_threshold(self, conversation_type: str) -> float:
        """
        Obtiene el umbral de calidad para un tipo de conversación
        
        Args:
            conversation_type: Tipo de conversación
            
        Returns:
            float: Umbral de calidad mínimo
        """
        pass


class FormationSystemException(Exception):
    """Excepción base para sistemas de formación"""
    pass


class ConfigurationException(FormationSystemException):
    """Excepción para errores de configuración"""
    pass


class ValidationException(FormationSystemException):
    """Excepción para errores de validación"""
    pass


class DataProviderException(FormationSystemException):
    """Excepción para errores del proveedor de datos"""
    pass
