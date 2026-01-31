"""
Script para descargar datos de partidos de Sportradar API.

Este script descarga los schedules de temporadas específicas y luego
descarga los summaries de cada evento/partido individual.
"""

import os
import time
import logging
from pathlib import Path
from typing import List, Dict
import xml.etree.ElementTree as ET

import requests
from requests.exceptions import RequestException


# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Constantes
API_KEY = "YOUR-SPORTRADAR-API-KEY"
BASE_URL = "https://api.sportradar.com/soccer-extended/production/v4/en"

# Estos Id se obtienen de la API de Sportradar de Seasons, al ser solo 3 ids 
# los identificamos visualmente en el API y no fue necesario automatizar su obtencion.
# Los Id corresponden a las temporadas 2023-24, 2024-25 y 2025-26.

SEASONS = ["sr:season:106501", "sr:season:118691", "sr:season:130805"]
GAMES_FOLDER = Path("./ETL/games")
SLEEP_SECONDS = 1
REQUEST_TIMEOUT = 30  # segundos


def create_games_folder() -> None:
    """Crea la carpeta 'games' si no existe."""
    GAMES_FOLDER.mkdir(exist_ok=True)
    logger.info(f"Carpeta '{GAMES_FOLDER}' lista para guardar archivos.")


def download_xml(url: str, max_retries: int = 3) -> str:
    """
    Descarga contenido XML desde una URL.
    
    Args:
        url: URL del endpoint a consultar
        max_retries: Número máximo de reintentos en caso de fallo
        
    Returns:
        Contenido XML como string
        
    Raises:
        RequestException: Si la descarga falla después de todos los reintentos
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.text
        except RequestException as e:
            if attempt < max_retries - 1:
                logger.warning(f"Intento {attempt + 1} falló. Reintentando... Error: {e}")
                time.sleep(2)
            else:
                logger.error(f"Error descargando {url}: {e}")
                raise
    
    return ""


def get_season_schedules(season_id: str) -> str:
    """
    Descarga el schedule completo de una temporada. El schedule tiene todos los juegos de la temporada
    y cada juego tiene un id que se utiliza para descargar el API con las estadisticas de cada uno de los juego.
    
    Args:
        season_id: ID de la temporada (ej: sr:season:118691)
        
    Returns:
        Contenido XML del schedule
    """
    url = f"{BASE_URL}/seasons/{season_id}/schedules.xml?api_key={API_KEY}"
    logger.info(f"Descargando schedules para temporada {season_id}...")
    return download_xml(url)


def parse_events_from_schedule(xml_content: str) -> List[Dict[str, str]]:
    """
    Extrae información de eventos desde el XML de schedules.
    
    Args:
        xml_content: Contenido XML del schedule
        
    Returns:
        Lista de diccionarios con información de eventos
    """
    events = []
    
    try:
        root = ET.fromstring(xml_content)
        
        # Namespace handling
        namespace = {'ns': 'http://schemas.sportradar.com/sportsapi/soccer-extended/v4'}
        
        for schedule in root.findall('.//ns:schedule', namespace):
            sport_event = schedule.find('.//ns:sport_event', namespace)
            
            if sport_event is not None:
                event_id = sport_event.get('id')
                
                # Extraer nombres de competidores
                competitors = sport_event.findall('.//ns:competitor', namespace)
                home_team = away_team = "Unknown"
                
                for competitor in competitors:
                    qualifier = competitor.get('qualifier')
                    name = competitor.get('name')
                    if qualifier == 'home':
                        home_team = name
                    elif qualifier == 'away':
                        away_team = name
                
                events.append({
                    'id': event_id,
                    'home': home_team,
                    'away': away_team
                })
        
        logger.info(f"Encontrados {len(events)} eventos en el schedule.")
        
    except ET.ParseError as e:
        logger.error(f"Error parseando XML: {e}")
    
    return events


def download_event_summary(event_id: str, home_team: str, away_team: str) -> bool:
    """
    Descarga el summary de un evento específico y lo guarda como XML.
    
    Args:
        event_id: ID del evento (ej: sr:sport_event:50852081)
        home_team: Nombre del equipo local
        away_team: Nombre del equipo visitante
        
    Returns:
        True si la descarga fue exitosa, False en caso contrario
    """
    logger.info(f"Descargando juego {event_id}: {home_team} vs {away_team}")
    
    url = f"{BASE_URL}/sport_events/{event_id}/summary.xml?api_key={API_KEY}"
    
    try:
        xml_content = download_xml(url)
        
        # Crear nombre de archivo seguro
        filename = f"{event_id.replace(':', '_')}.xml"
        filepath = GAMES_FOLDER / filename
        
        # Guardar XML
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(xml_content)
        
        logger.info(f"✓ Guardado: {filename}")
        return True
        
    except RequestException as e:
        logger.error(f"✗ Error descargando {event_id}: {e}")
        return False


def process_season(season_id: str) -> int:
    """
    Procesa una temporada completa: descarga schedules y todos los summaries.
    
    Args:
        season_id: ID de la temporada
        
    Returns:
        Número de eventos descargados exitosamente
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"Procesando temporada: {season_id}")
    logger.info(f"{'='*60}\n")
    
    # Descargar schedules
    try:
        schedules_xml = get_season_schedules(season_id)
    except RequestException:
        logger.error(f"No se pudo obtener el schedule de {season_id}. Saltando temporada.")
        return 0
    
    # Parsear eventos
    events = parse_events_from_schedule(schedules_xml)
    
    if not events:
        logger.warning(f"No se encontraron eventos para {season_id}.")
        return 0
    
    # Descargar cada evento
    successful_downloads = 0
    
    for idx, event in enumerate(events, 1):
        logger.info(f"[{idx}/{len(events)}] Procesando evento...")
        
        if download_event_summary(event['id'], event['home'], event['away']):
            successful_downloads += 1
        
        # Sleep entre descargas (excepto en la última)
        if idx < len(events):
            time.sleep(SLEEP_SECONDS)
    
    logger.info(f"\nTemporada {season_id}: {successful_downloads}/{len(events)} eventos descargados.\n")
    return successful_downloads


def main() -> None:
    """Función principal que ejecuta el proceso completo."""
    logger.info("Iniciando descarga de datos de Sportradar...")
    logger.info(f"Temporadas a procesar: {', '.join(SEASONS)}\n")
    
    # Crear carpeta de destino
    create_games_folder()
    
    # Procesar cada temporada
    total_downloads = 0
    start_time = time.time()
    
    for season_id in SEASONS:
        downloads = process_season(season_id)
        total_downloads += downloads
    
    # Resumen final
    elapsed_time = time.time() - start_time
    logger.info(f"\n{'='*60}")
    logger.info(f"RESUMEN FINAL")
    logger.info(f"{'='*60}")
    logger.info(f"Total de eventos descargados: {total_downloads}")
    logger.info(f"Tiempo total: {elapsed_time:.2f} segundos")
    logger.info(f"Archivos guardados en: {GAMES_FOLDER.absolute()}")
    logger.info(f"{'='*60}\n")


if __name__ == "__main__":
    main()
