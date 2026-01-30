"""
Script para extraer estadísticas de equipos de archivos XML de Sportradar.

Este script procesa todos los archivos XML de la carpeta 'games' y genera
un DataFrame con las estadísticas de equipos.
"""

import logging
from pathlib import Path
from typing import List, Dict
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
GAMES_FOLDER = Path("./ETL/games")
OUTPUT_FOLDER = Path("./ETL/dataframes")
NAMESPACE = {'ns': 'http://schemas.sportradar.com/sportsapi/soccer-extended/v4'}


def create_output_folder() -> None:
    """Crea la carpeta de salida para los DataFrames si no existe."""
    OUTPUT_FOLDER.mkdir(exist_ok=True)
    logger.info(f"Carpeta '{OUTPUT_FOLDER}' lista para guardar DataFrames.")


def parse_team_statistics(root: ET.Element, event_id: str) -> List[Dict]:
    """
    Extrae estadísticas de equipos del partido.
    
    Returns:
        Lista de diccionarios con estadísticas por equipo
    """
    team_stats = []
    
    try:
        # Obtener el status del evento
        sport_event_status = root.find('.//ns:sport_event_status', NAMESPACE)
        event_status = sport_event_status.get('status') if sport_event_status is not None else None
        match_status = sport_event_status.get('match_status') if sport_event_status is not None else None
        home_score = sport_event_status.get('home_score') if sport_event_status is not None else None
        away_score = sport_event_status.get('away_score') if sport_event_status is not None else None
        
        competitors = root.findall('.//ns:statistics/ns:totals/ns:competitors/ns:competitor', NAMESPACE)
        
        for comp in competitors:
            qualifier = comp.get('qualifier')
            stats = {
                'event_id': event_id,
                'team_id': comp.get('id'),
                'team_name': comp.get('name'),
                'team_qualifier': qualifier,
                'event_status': event_status,
                'match_status': match_status,
                'score': home_score if qualifier == 'home' else away_score
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
    Procesa un archivo XML y extrae las estadísticas de equipos y jugadores.
    
    Returns:
        Tupla (team_stats_list, player_stats_list)
    """
    try:
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Obtener el event_id
        sport_event = root.find('.//ns:sport_event', NAMESPACE)
        if sport_event is None:
            logger.warning(f"No se encontró sport_event en {xml_path.name}")
            return [], []
        
        event_id = sport_event.get('id')
        
        # Extraer estadísticas
        team_stats = parse_team_statistics(root, event_id)
        player_stats = parse_player_statistics(root, event_id)
        
        return team_stats, player_stats
    
    except ET.ParseError as e:
        logger.error(f"Error parseando XML {xml_path.name}: {e}")
        return [], []
    except Exception as e:
        logger.error(f"Error procesando {xml_path.name}: {e}")
        return [], []


def process_all_games() -> tuple:
    """
    Procesa todos los archivos XML de la carpeta games.
    
    Returns:
        Tupla (all_teams_data, all_players_data)
    """
    xml_files = list(GAMES_FOLDER.glob("*.xml"))
    
    if not xml_files:
        logger.warning(f"No se encontraron archivos XML en {GAMES_FOLDER}")
        return [], []
    
    logger.info(f"Procesando {len(xml_files)} archivos XML...")
    
    all_teams_data = []
    all_players_data = []
    
    # Usar tqdm para mostrar barra de progreso
    for xml_file in tqdm(xml_files, desc="Procesando XMLs"):
        team_stats, player_stats = process_xml_file(xml_file)
        all_teams_data.extend(team_stats)
        all_players_data.extend(player_stats)
    
    logger.info(f"Procesamiento completado:")
    logger.info(f"  - {len(xml_files)} archivos procesados")
    logger.info(f"  - {len(all_teams_data)} registros de estadísticas de equipos")
    logger.info(f"  - {len(all_players_data)} registros de estadísticas de jugadores")
    
    return all_teams_data, all_players_data


def create_team_statistics_dataframe(teams_data: List[Dict]) -> pd.DataFrame:
    """
    Crea el DataFrame de estadísticas de equipos.
    
    Returns:
        DataFrame con las estadísticas de equipos
    """
    if not teams_data:
        logger.error("No hay datos de equipos para crear el DataFrame")
        return pd.DataFrame()
    
    logger.info("Creando DataFrame de estadísticas de equipos...")
    
    df_teams = pd.DataFrame(teams_data)
    
    # Convertir columnas numéricas (excluyendo campos categóricos)
    non_numeric_columns = ['event_id', 'team_id', 'team_name', 'team_qualifier', 'event_status', 'match_status']
    numeric_columns = df_teams.columns.difference(non_numeric_columns)
    for col in numeric_columns:
        df_teams[col] = pd.to_numeric(df_teams[col], errors='coerce')
    
    logger.info(f"  ✓ DataFrame creado: {df_teams.shape}")
    logger.info(f"  ✓ Columnas: {len(df_teams.columns)}")
    
    return df_teams


def create_player_statistics_dataframe(players_data: List[Dict]) -> pd.DataFrame:
    """
    Crea el DataFrame de estadísticas de jugadores.
    
    Returns:
        DataFrame con las estadísticas de jugadores
    """
    if not players_data:
        logger.error("No hay datos de jugadores para crear el DataFrame")
        return pd.DataFrame()
    
    logger.info("Creando DataFrame de estadísticas de jugadores...")
    
    df_players = pd.DataFrame(players_data)
    
    # Convertir columnas numéricas
    non_numeric_columns = [
        'event_id', 'team_id', 'team_name', 'team_qualifier', 
        'player_id', 'player_name', 'starter'
    ]
    numeric_columns = df_players.columns.difference(non_numeric_columns)
    for col in numeric_columns:
        df_players[col] = pd.to_numeric(df_players[col], errors='coerce')
    
    # Convertir starter a booleano
    if 'starter' in df_players.columns:
        df_players['starter'] = df_players['starter'] == 'true'
    
    logger.info(f"  ✓ DataFrame creado: {df_players.shape}")
    logger.info(f"  ✓ Columnas: {len(df_players.columns)}")
    
    return df_players


def save_dataframes(dataframes: Dict[str, pd.DataFrame]) -> None:
    """
    Guarda los DataFrames en múltiples formatos.
    
    - CSV: formato universal, fácil de compartir
    - Parquet: formato eficiente y comprimido
    - Pickle: formato nativo de pandas con tipos de datos preservados
    """
    logger.info("\nGuardando DataFrames...")
    
    for name, df in dataframes.items():
        if df.empty:
            continue
            
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
    logger.info("REPORTE RESUMEN DE EXTRACCIÓN")
    logger.info("="*60)
    
    if 'team_statistics' in dataframes:
        df = dataframes['team_statistics']
        logger.info(f"\n⚽ ESTADÍSTICAS DE EQUIPOS:")
        logger.info(f"  - Total de registros: {len(df)}")
        logger.info(f"  - Eventos únicos: {df['event_id'].nunique()}")
        logger.info(f"  - Equipos únicos: {df['team_id'].nunique()}")
        logger.info(f"  - Columnas disponibles: {len(df.columns)}")
    
    if 'player_statistics' in dataframes:
        df = dataframes['player_statistics']
        logger.info(f"\n👤 ESTADÍSTICAS DE JUGADORES:")
        logger.info(f"  - Total de registros: {len(df)}")
        logger.info(f"  - Eventos únicos: {df['event_id'].nunique()}")
        logger.info(f"  - Jugadores únicos: {df['player_id'].nunique()}")
        logger.info(f"  - Jugadores titulares: {df['starter'].sum() if 'starter' in df.columns else 'N/A'}")
        logger.info(f"  - Columnas disponibles: {len(df.columns)}")
    
    logger.info("\n" + "="*60)
    logger.info(f"Archivos guardados en: {OUTPUT_FOLDER.absolute()}")
    logger.info("="*60 + "\n")


def main() -> None:
    """Función principal que ejecuta el proceso completo."""
    logger.info("Iniciando proceso de extracción de estadísticas...\n")
    
    # Crear carpeta de salida
    create_output_folder()
    
    # Procesar todos los archivos XML
    teams_data, players_data = process_all_games()
    
    if not teams_data and not players_data:
        logger.error("No se pudieron extraer datos de los archivos XML.")
        return
    
    dataframes = {}
    
    # Crear DataFrames
    if teams_data:
        dataframes['team_statistics'] = create_team_statistics_dataframe(teams_data)
    
    if players_data:
        dataframes['player_statistics'] = create_player_statistics_dataframe(players_data)
    
    # Guardar DataFrames
    save_dataframes(dataframes)
    
    # Generar reporte resumen
    generate_summary_report(dataframes)
    
    logger.info("✅ Proceso completado exitosamente!")
    logger.info(f"\nPara usar los datos en tu análisis:")
    logger.info(f"  import pandas as pd")
    logger.info(f"  df_teams = pd.read_parquet('{OUTPUT_FOLDER}/team_statistics.parquet')")
    logger.info(f"  df_players = pd.read_parquet('{OUTPUT_FOLDER}/player_statistics.parquet')")


if __name__ == "__main__":
    main()
