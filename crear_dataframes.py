"""
Script para convertir archivos XML de Sportradar a DataFrames de pandas.

Este script procesa todos los archivos XML de la carpeta 'games' y genera
múltiples DataFrames con información estructurada para análisis posterior.
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional
import xml.etree.ElementTree as ET

import pandas as pd
from tqdm import tqdm


# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# Constantes
GAMES_FOLDER = Path("games")
OUTPUT_FOLDER = Path("dataframes")
NAMESPACE = {'ns': 'http://schemas.sportradar.com/sportsapi/soccer-extended/v4'}


def create_output_folder() -> None:
    """Crea la carpeta de salida para los DataFrames si no existe."""
    OUTPUT_FOLDER.mkdir(exist_ok=True)
    logger.info(f"Carpeta '{OUTPUT_FOLDER}' lista para guardar DataFrames.")


def parse_event_info(root: ET.Element) -> Optional[Dict]:
    """
    Extrae información general del evento deportivo.
    
    Returns:
        Diccionario con información del evento o None si hay error
    """
    try:
        sport_event = root.find('.//ns:sport_event', NAMESPACE)
        sport_event_status = root.find('.//ns:sport_event_status', NAMESPACE)
        
        if sport_event is None or sport_event_status is None:
            return None
        
        # Información básica del evento
        event_id = sport_event.get('id')
        start_time = sport_event.get('start_time')
        
        # Información del contexto
        context = sport_event.find('.//ns:sport_event_context', NAMESPACE)
        sport = context.find('.//ns:sport', NAMESPACE)
        category = context.find('.//ns:category', NAMESPACE)
        competition = context.find('.//ns:competition', NAMESPACE)
        season = context.find('.//ns:season', NAMESPACE)
        round_elem = context.find('.//ns:round', NAMESPACE)
        
        # Competidores
        competitors = sport_event.findall('.//ns:competitor', NAMESPACE)
        home_team = away_team = None
        home_id = away_id = None
        
        for comp in competitors:
            if comp.get('qualifier') == 'home':
                home_team = comp.get('name')
                home_id = comp.get('id')
            elif comp.get('qualifier') == 'away':
                away_team = comp.get('name')
                away_id = comp.get('id')
        
        # Venue
        venue = sport_event.find('.//ns:venue', NAMESPACE)
        
        # Condiciones del evento
        conditions = sport_event.find('.//ns:sport_event_conditions', NAMESPACE)
        attendance = None
        if conditions is not None:
            att_elem = conditions.find('.//ns:attendance', NAMESPACE)
            if att_elem is not None:
                attendance = att_elem.get('count')
        
        # Resultado
        status = sport_event_status.get('status')
        match_status = sport_event_status.get('match_status')
        home_score = sport_event_status.get('home_score')
        away_score = sport_event_status.get('away_score')
        
        return {
            'event_id': event_id,
            'start_time': start_time,
            'sport_name': sport.get('name') if sport is not None else None,
            'category_name': category.get('name') if category is not None else None,
            'competition_name': competition.get('name') if competition is not None else None,
            'competition_id': competition.get('id') if competition is not None else None,
            'season_name': season.get('name') if season is not None else None,
            'season_id': season.get('id') if season is not None else None,
            'round_number': round_elem.get('number') if round_elem is not None else None,
            'home_team': home_team,
            'home_team_id': home_id,
            'away_team': away_team,
            'away_team_id': away_id,
            'venue_name': venue.get('name') if venue is not None else None,
            'venue_city': venue.get('city_name') if venue is not None else None,
            'venue_capacity': venue.get('capacity') if venue is not None else None,
            'attendance': attendance,
            'status': status,
            'match_status': match_status,
            'home_score': home_score,
            'away_score': away_score
        }
    
    except Exception as e:
        logger.error(f"Error parseando información del evento: {e}")
        return None


def parse_team_statistics(root: ET.Element, event_id: str) -> List[Dict]:
    """
    Extrae estadísticas de equipos del partido.
    
    Returns:
        Lista de diccionarios con estadísticas por equipo
    """
    team_stats = []
    
    try:
        competitors = root.findall('.//ns:statistics/ns:totals/ns:competitors/ns:competitor', NAMESPACE)
        
        for comp in competitors:
            stats = {
                'event_id': event_id,
                'team_id': comp.get('id'),
                'team_name': comp.get('name'),
                'team_qualifier': comp.get('qualifier')
            }
            
            # Extraer todas las estadísticas disponibles
            statistics_elem = comp.find('.//ns:statistics', NAMESPACE)
            if statistics_elem is not None:
                for key, value in statistics_elem.attrib.items():
                    stats[key] = value
            
            team_stats.append(stats)
    
    except Exception as e:
        logger.error(f"Error parseando estadísticas de equipos para {event_id}: {e}")
    
    return team_stats


def parse_player_statistics(root: ET.Element, event_id: str) -> List[Dict]:
    """
    Extrae estadísticas individuales de jugadores.
    
    Returns:
        Lista de diccionarios con estadísticas por jugador
    """
    player_stats = []
    
    try:
        # Buscar todos los competidores con sus jugadores
        competitors = root.findall('.//ns:statistics/ns:totals/ns:competitors/ns:competitor', NAMESPACE)
        
        for comp in competitors:
            team_id = comp.get('id')
            team_name = comp.get('name')
            team_qualifier = comp.get('qualifier')
            
            players = comp.findall('.//ns:players/ns:player', NAMESPACE)
            
            for player in players:
                stats = {
                    'event_id': event_id,
                    'team_id': team_id,
                    'team_name': team_name,
                    'team_qualifier': team_qualifier,
                    'player_id': player.get('id'),
                    'player_name': player.get('name'),
                    'starter': player.get('starter')
                }
                
                # Extraer todas las estadísticas del jugador
                statistics_elem = player.find('.//ns:statistics', NAMESPACE)
                if statistics_elem is not None:
                    for key, value in statistics_elem.attrib.items():
                        stats[key] = value
                
                player_stats.append(stats)
    
    except Exception as e:
        logger.error(f"Error parseando estadísticas de jugadores para {event_id}: {e}")
    
    return player_stats


def process_xml_file(xml_path: Path) -> tuple:
    """
    Procesa un archivo XML y extrae toda la información.
    
    Returns:
        Tupla (event_info, team_stats, player_stats)
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Información general del evento
        event_info = parse_event_info(root)
        
        if event_info is None:
            return None, [], []
        
        event_id = event_info['event_id']
        
        # Estadísticas de equipos
        team_stats = parse_team_statistics(root, event_id)
        
        # Estadísticas de jugadores
        player_stats = parse_player_statistics(root, event_id)
        
        return event_info, team_stats, player_stats
    
    except ET.ParseError as e:
        logger.error(f"Error parseando XML {xml_path.name}: {e}")
        return None, [], []
    except Exception as e:
        logger.error(f"Error procesando {xml_path.name}: {e}")
        return None, [], []


def process_all_games() -> tuple:
    """
    Procesa todos los archivos XML de la carpeta games.
    
    Returns:
        Tupla con tres listas: (events_data, teams_data, players_data)
    """
    xml_files = list(GAMES_FOLDER.glob("*.xml"))
    
    if not xml_files:
        logger.warning(f"No se encontraron archivos XML en {GAMES_FOLDER}")
        return [], [], []
    
    logger.info(f"Procesando {len(xml_files)} archivos XML...")
    
    events_data = []
    teams_data = []
    players_data = []
    
    # Usar tqdm para mostrar barra de progreso
    for xml_file in tqdm(xml_files, desc="Procesando XMLs"):
        event_info, team_stats, player_stats = process_xml_file(xml_file)
        
        if event_info:
            events_data.append(event_info)
            teams_data.extend(team_stats)
            players_data.extend(player_stats)
    
    logger.info(f"Procesamiento completado:")
    logger.info(f"  - {len(events_data)} eventos procesados")
    logger.info(f"  - {len(teams_data)} registros de estadísticas de equipos")
    logger.info(f"  - {len(players_data)} registros de estadísticas de jugadores")
    
    return events_data, teams_data, players_data


def create_dataframes(events_data: List[Dict], teams_data: List[Dict], 
                     players_data: List[Dict]) -> Dict[str, pd.DataFrame]:
    """
    Crea DataFrames a partir de los datos extraídos.
    
    Returns:
        Diccionario con los DataFrames creados
    """
    logger.info("Creando DataFrames...")
    
    dataframes = {}
    
    # DataFrame de eventos
    if events_data:
        df_events = pd.DataFrame(events_data)
        
        # Convertir tipos de datos apropiados
        df_events['start_time'] = pd.to_datetime(df_events['start_time'])
        df_events['home_score'] = pd.to_numeric(df_events['home_score'], errors='coerce')
        df_events['away_score'] = pd.to_numeric(df_events['away_score'], errors='coerce')
        df_events['attendance'] = pd.to_numeric(df_events['attendance'], errors='coerce')
        df_events['venue_capacity'] = pd.to_numeric(df_events['venue_capacity'], errors='coerce')
        df_events['round_number'] = pd.to_numeric(df_events['round_number'], errors='coerce')
        
        dataframes['events'] = df_events
        logger.info(f"  ✓ DataFrame de eventos: {df_events.shape}")
    
    # DataFrame de estadísticas de equipos
    if teams_data:
        df_teams = pd.DataFrame(teams_data)
        
        # Convertir columnas numéricas
        numeric_columns = df_teams.columns.difference(['event_id', 'team_id', 'team_name', 'team_qualifier'])
        for col in numeric_columns:
            df_teams[col] = pd.to_numeric(df_teams[col], errors='coerce')
        
        dataframes['team_statistics'] = df_teams
        logger.info(f"  ✓ DataFrame de estadísticas de equipos: {df_teams.shape}")
    
    # DataFrame de estadísticas de jugadores
    if players_data:
        df_players = pd.DataFrame(players_data)
        
        # Convertir columnas numéricas
        numeric_columns = df_players.columns.difference([
            'event_id', 'team_id', 'team_name', 'team_qualifier', 
            'player_id', 'player_name', 'starter'
        ])
        for col in numeric_columns:
            df_players[col] = pd.to_numeric(df_players[col], errors='coerce')
        
        # Convertir starter a booleano
        df_players['starter'] = df_players['starter'] == 'true'
        
        dataframes['player_statistics'] = df_players
        logger.info(f"  ✓ DataFrame de estadísticas de jugadores: {df_players.shape}")
    
    return dataframes


def save_dataframes(dataframes: Dict[str, pd.DataFrame]) -> None:
    """
    Guarda los DataFrames en múltiples formatos.
    
    - CSV: formato universal, fácil de compartir
    - Parquet: formato eficiente y comprimido
    - Pickle: formato nativo de pandas con tipos de datos preservados
    """
    logger.info("\nGuardando DataFrames...")
    
    for name, df in dataframes.items():
        # Guardar como CSV (universal)
        csv_path = OUTPUT_FOLDER / f"{name}.csv"
        df.to_csv(csv_path, index=False, encoding='utf-8')
        logger.info(f"  ✓ CSV guardado: {csv_path}")
        
        # Guardar como Parquet (eficiente)
        parquet_path = OUTPUT_FOLDER / f"{name}.parquet"
        df.to_parquet(parquet_path, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"  ✓ Parquet guardado: {parquet_path}")
        
        # Guardar como Pickle (tipos de datos preservados)
        pickle_path = OUTPUT_FOLDER / f"{name}.pkl"
        df.to_pickle(pickle_path)
        logger.info(f"  ✓ Pickle guardado: {pickle_path}")


def generate_summary_report(dataframes: Dict[str, pd.DataFrame]) -> None:
    """Genera un reporte resumen de las estadísticas extraídas."""
    logger.info("\n" + "="*60)
    logger.info("REPORTE RESUMEN DE DATOS EXTRAÍDOS")
    logger.info("="*60)
    
    if 'events' in dataframes:
        df_events = dataframes['events']
        logger.info(f"\n📊 EVENTOS:")
        logger.info(f"  - Total de eventos: {len(df_events)}")
        logger.info(f"  - Temporadas únicas: {df_events['season_name'].nunique()}")
        logger.info(f"  - Competiciones únicas: {df_events['competition_name'].nunique()}")
        logger.info(f"  - Equipos únicos: {pd.concat([df_events['home_team'], df_events['away_team']]).nunique()}")
        logger.info(f"  - Rango de fechas: {df_events['start_time'].min()} a {df_events['start_time'].max()}")
        
        # Distribución por temporada
        logger.info(f"\n  Distribución por temporada:")
        season_counts = df_events['season_name'].value_counts()
        for season, count in season_counts.items():
            logger.info(f"    • {season}: {count} eventos")
    
    if 'team_statistics' in dataframes:
        df_teams = dataframes['team_statistics']
        logger.info(f"\n⚽ ESTADÍSTICAS DE EQUIPOS:")
        logger.info(f"  - Total de registros: {len(df_teams)}")
        logger.info(f"  - Columnas disponibles: {len(df_teams.columns)}")
        logger.info(f"  - Estadísticas por evento: {len(df_teams) / len(dataframes['events']):.1f}")
    
    if 'player_statistics' in dataframes:
        df_players = dataframes['player_statistics']
        logger.info(f"\n👤 ESTADÍSTICAS DE JUGADORES:")
        logger.info(f"  - Total de registros: {len(df_players)}")
        logger.info(f"  - Jugadores únicos: {df_players['player_id'].nunique()}")
        logger.info(f"  - Columnas de estadísticas: {len(df_players.columns)}")
        logger.info(f"  - Jugadores titulares: {df_players['starter'].sum()}")
        logger.info(f"  - Jugadores suplentes: {(~df_players['starter']).sum()}")
    
    logger.info("\n" + "="*60)
    logger.info(f"Archivos guardados en: {OUTPUT_FOLDER.absolute()}")
    logger.info("="*60 + "\n")


def main() -> None:
    """Función principal que ejecuta el proceso completo."""
    logger.info("Iniciando proceso de creación de DataFrames...\n")
    
    # Crear carpeta de salida
    create_output_folder()
    
    # Procesar todos los archivos XML
    events_data, teams_data, players_data = process_all_games()
    
    if not events_data:
        logger.error("No se pudieron extraer datos de los archivos XML.")
        return
    
    # Crear DataFrames
    dataframes = create_dataframes(events_data, teams_data, players_data)
    
    # Guardar DataFrames
    save_dataframes(dataframes)
    
    # Generar reporte resumen
    generate_summary_report(dataframes)
    
    logger.info("✅ Proceso completado exitosamente!")
    logger.info(f"\nPara usar los datos en tu análisis:")
    logger.info(f"  import pandas as pd")
    logger.info(f"  df_events = pd.read_parquet('{OUTPUT_FOLDER}/events.parquet')")
    logger.info(f"  df_teams = pd.read_parquet('{OUTPUT_FOLDER}/team_statistics.parquet')")
    logger.info(f"  df_players = pd.read_parquet('{OUTPUT_FOLDER}/player_statistics.parquet')")


if __name__ == "__main__":
    main()
