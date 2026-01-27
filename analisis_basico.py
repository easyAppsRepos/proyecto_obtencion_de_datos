"""
Script básico para explorar DataFrames - Ideal para principiantes en EDA.

Este script carga los datos y muestra información básica de cada DataFrame.
EDA = Exploratory Data Analysis (Análisis Exploratorio de Datos)
"""

import pandas as pd
from pathlib import Path


# Ruta donde están guardados los DataFrames
DATAFRAMES_FOLDER = Path("dataframes")


def main():
    """
    Función principal que carga y explora los DataFrames.
    """
    
    print("\n" + "="*70)
    print("EXPLORACIÓN BÁSICA DE DATOS DE FÚTBOL")
    print("="*70 + "\n")
    
    # ========================================================================
    # 1. CARGAR DATOS
    # ========================================================================
    print("PASO 1: Cargando datos...\n")
    
    # Cargar DataFrame de eventos (partidos)
    df_events = pd.read_parquet(DATAFRAMES_FOLDER / "events.parquet")
    print(f"✓ Eventos cargados: {len(df_events)} partidos")
    
    # Cargar DataFrame de estadísticas de equipos
    df_teams = pd.read_parquet(DATAFRAMES_FOLDER / "team_statistics.parquet")
    print(f"✓ Estadísticas de equipos: {len(df_teams)} registros")
    
    # Cargar DataFrame de estadísticas de jugadores
    df_players = pd.read_parquet(DATAFRAMES_FOLDER / "player_statistics.parquet")
    print(f"✓ Estadísticas de jugadores: {len(df_players)} registros")
    
    
    # ========================================================================
    # 2. EXPLORAR EVENTOS (PARTIDOS)
    # ========================================================================
    print("\n" + "="*70)
    print("DATAFRAME 1: EVENTOS (Partidos)")
    print("="*70)
    
    # ¿Cuántas filas y columnas tiene?
    print(f"\n📊 Dimensiones: {df_events.shape[0]} filas x {df_events.shape[1]} columnas")
    
    # Ver las primeras 5 filas
    print("\n� Primeras 5 filas:")
    print(df_events.head())
    
    # Ver información general del DataFrame
    print("\n📈 Información general:")
    print(df_events.info())
    
    
    # ========================================================================
    # 3. EXPLORAR ESTADÍSTICAS DE EQUIPOS
    # ========================================================================
    print("\n" + "="*70)
    print("DATAFRAME 2: ESTADÍSTICAS DE EQUIPOS")
    print("="*70)
    
    # ¿Cuántas filas y columnas tiene?
    print(f"\n📊 Dimensiones: {df_teams.shape[0]} filas x {df_teams.shape[1]} columnas")
    
    # Ver las primeras 5 filas
    print("\n� Primeras 5 filas:")
    print(df_teams.head())
    
    # Ver información general del DataFrame
    print("\n📈 Información general:")
    print(df_teams.info())
    
    
    # ========================================================================
    # 4. EXPLORAR ESTADÍSTICAS DE JUGADORES
    # ========================================================================
    print("\n" + "="*70)
    print("DATAFRAME 3: ESTADÍSTICAS DE JUGADORES")
    print("="*70)
    
    # ¿Cuántas filas y columnas tiene?
    print(f"\n📊 Dimensiones: {df_players.shape[0]} filas x {df_players.shape[1]} columnas")
    
    # Ver las primeras 5 filas
    print("\n� Primeras 5 filas:")
    print(df_players.head())
    
    # Ver información general del DataFrame
    print("\n� Información general:")
    print(df_players.info())
    
    
    # ========================================================================
    # 5. RESUMEN FINAL
    # ========================================================================
    print("\n" + "="*70)
    print("✅ EXPLORACIÓN COMPLETADA")
    print("="*70)
    print("\n💡 Próximos pasos para tu análisis:")
    print("   - Usa df_events.describe() para ver estadísticas numéricas")
    print("   - Usa df_events.columns para ver todas las columnas")
    print("   - Usa df_events['columna'].value_counts() para contar valores únicos")
    print("   - Crea gráficas con matplotlib o seaborn")
    print("\n")
    
    # Retornar los DataFrames por si quieres usarlos después
    return df_events, df_teams, df_players


if __name__ == "__main__":
    # Ejecutar el análisis
    df_events, df_teams, df_players = main()
