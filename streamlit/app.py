import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path

# Configuración de la página
st.set_page_config(
    page_title="Asignatura Obtención y Extracción de Datos",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Estilo personalizado para un look premium
st.markdown("""
    <style>
    .main {
        background-color: #f8f9fa;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    h1, h2, h3 {
        color: #1e3a8a;
        font-family: 'Outfit', sans-serif;
    }
    .description {
        font-size: 1.1rem;
        color: #4b5563;
        line-height: 1.6;
    }
    </style>
    """, unsafe_allow_html=True)

# Función para cargar datos
@st.cache_data
def load_data():
    # Buscamos el archivo en la ruta relativa desde la raíz del proyecto
    file_path = Path("ETL/dataframes/team_statistics_clean.parquet")
    if not file_path.exists():
        # Intento alternativo si se corre desde la carpeta streamlit/
        file_path = Path("../ETL/dataframes/team_statistics_clean.parquet")
    
    if file_path.exists():
        return pd.read_parquet(file_path)
    else:
        st.error(f"No se encontró el archivo de datos en {file_path}. Asegúrate de que la ruta sea correcta.")
        return None

# Carga de datos
df = load_data()

if df is not None:
    # --- TÍTULO Y DESCRIPCIÓN ---
    st.title("⚽ Dashboard de Estadísticas de Equipos")
    st.markdown("""
    <div class="description">
    Bienvenido a la herramienta de análisis interactivo para nuestro <b>Proyecto de Máster: Obtención y Extracción de Datos</b>. 
    Esta aplicación permite explorar el rendimiento de los equipos de fútbol mediante métricas avanzadas extraídas y procesadas durante el proyecto.
    Usa los controles laterales para filtrar y jugar con los datos.
    </div>
    """, unsafe_allow_html=True)
    st.divider()

    # --- INFORMACIÓN DEL GRUPO Y METODOLOGÍA (Cuadrícula 2x2) ---
    st.subheader("📑 Ficha Técnica del Proyecto")
    row1_col1, row1_col2 = st.columns(2)
    with row1_col1:
        with st.expander("👤 Integrantes del Grupo 2", expanded=False):
            st.write("""
            - **Jose Raul Alfaro Guillen**
            - **Enma Sarai Carias Manzanares**
            - **Lucas Jose da Silva**
            - **Juan Luis Herrera Lozano**
            - **Priscila Guaiba Von Pfuhl**
            """)
            st.caption("Master en Big Data & Business Intelligence")

    with row1_col2:
        with st.expander("🌐 Fuentes de Datos", expanded=False):
            st.write("""
            **Sportradar Soccer API (v4):**
            - **Proveedor:** Sportradar Group AG, líder global en datos deportivos oficiales.
            - **Justificación:** Elegida por su alta granularidad, fiabilidad y carácter oficial.
            - **Endpoints:** Acceso a temporadas, calendarios y resúmenes detallados mediante autenticación segura.
            """)
            st.caption("Fuente: developer.sportradar.com")

    row2_col1, row2_col2 = st.columns(2)
    with row2_col1:
        with st.expander("📡 Metodología de Obtención", expanded=False):
            st.write("""
            **Proceso Metodológico:**
            1. **Extracción Jerárquica:** De macro (Ligas) a micro (Partidos ID).
            2. **Fases:** Mapeo de temporadas 2023-2025 y descarga de 969 partidos.
            3. **Publicación:** Los XML originales son privados; se comparten los DataFrames finales procesados.
            """)

    with row2_col2:
        with st.expander("ℹ️ Detalles Técnicos (ETL)", expanded=False):
            st.write("""
            **Procesamiento de Datos:**
            1. **Filtrado:** Solo se incluyen partidos finalizados (`closed` / `ended`).
            2. **Agregación:** Las estadísticas no disponibles a nivel de equipo se reconstruyeron sumando las estadísticas de los jugadores.
            3. **Imputación:** Valores faltantes en posesión se rellenaron con la media histórica del equipo.
            4. **Ingeniería de Variables:** Se crearon métricas como *Shot Accuracy* (Precisión) y variables de perfil táctico.
            """)
    
    st.divider()

    # --- SECCIÓN DE ESTADÍSTICAS ---
    st.subheader("📊 Análisis de Rendimiento")
    st.markdown("Explora las métricas clave y comparativas avanzadas de los equipos seleccionados.")
    
    st.sidebar.header("🧠 Laboratorio Táctico")
    
    # 1. Filtro de métricas (Ahora primero)
    st.sidebar.subheader("Selecciona Métrica")
    all_metrics = [
        'ball_possession', 'chances_created', 'corner_kicks', 
        'shots_total', 'shots_on_target', 'shot_accuracy', 
        'passes_total', 'fouls', 'score'
    ]
    
    metric_labels = {
        'ball_possession': 'Posesión de Balón (%)',
        'chances_created': 'Ocasiones Creadas',
        'corner_kicks': 'Saques de Esquina',
        'shots_total': 'Remates Totales',
        'shots_on_target': 'Remates a Puerta',
        'shot_accuracy': 'Precisión de Remate',
        'passes_total': 'Pases Totales',
        'fouls': 'Faltas Cometidas',
        'score': 'Goles Marcados'
    }

    selected_metric = st.sidebar.selectbox(
        "Elige qué medir:",
        options=all_metrics,
        format_func=lambda x: metric_labels[x]
    )

    st.sidebar.divider()

    # 2. Filtro de equipos con checkboxes (Ahora segundo)
    st.sidebar.subheader("Selecciona Equipos")
    
    teams = sorted(df['team_name'].unique())
    selected_teams = []
    
    # Definimos unos equipos por defecto para que no empiece vacío
    default_teams = ['Real Madrid', 'FC Barcelona', 'Girona FC']
    
    with st.sidebar.expander("Lista de Clubes", expanded=True):
        for team in teams:
            # Marcar por defecto si está en nuestra lista ideal o si solo hay pocos equipos
            is_default = team in default_teams
            is_selected = st.checkbox(team, value=is_default, key=f"v2_check_{team}")
            if is_selected:
                selected_teams.append(team)

    # Filtrado del DataFrame
    df_filtered = df[df['team_name'].isin(selected_teams)]

    if df_filtered.empty:
        st.warning("⚠️ Por favor selecciona al menos un equipo en el panel lateral.")
    else:
        # --- FILA 1: MÉTRICAS GENERALES (KPIs) ---
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Goles Totales", df_filtered['score'].sum(), help="Suma total de goles marcados por los equipos seleccionados en el periodo analizado.")
            st.caption("Capacidad goleadora acumulada.")
        with col2:
            st.metric("Promedio Goles", round(df_filtered['score'].mean(), 2), help="Goles promedio anotados por partido.")
            st.caption("Efectividad goleadora media.")
        with col3:
            st.metric("Posesión Media", f"{round(df_filtered['ball_possession'].mean(), 1)}%", help="Porcentaje promedio de tiempo con el control del balón.")
            st.caption("Dominio del esférico.")
        with col4:
            st.metric("Precisión Media", f"{round(df_filtered['shot_accuracy'].mean() * 100, 1)}%", help="Porcentaje de tiros que fueron a puerta (entre los tres palos).")
            st.caption("Puntería de cara al arco.")

        st.divider()

        # --- FILA 2: GRÁFICOS PRINCIPALES ---
        c1, c2 = st.columns(2)

        with c1:
            st.subheader(f"📊 {metric_labels[selected_metric]} por Equipo")
            # Agregamos por equipo
            df_agg = df_filtered.groupby('team_name')[selected_metric].mean().sort_values(ascending=False).reset_index()
            
            fig_bar = px.bar(
                df_agg,
                x='team_name',
                y=selected_metric,
                color=selected_metric,
                labels={'team_name': 'Equipo', selected_metric: metric_labels[selected_metric]},
                color_continuous_scale="Viridis",
                template="plotly_white"
            )
            fig_bar.update_layout(showlegend=False, coloraxis_showscale=False)
            st.plotly_chart(fig_bar, use_container_width=True)
            st.write(f"Este gráfico muestra el promedio de **{metric_labels[selected_metric]}** de cada equipo seleccionado.")

        with c2:
            st.subheader("🕸️ Perfil Táctico (Radar)")
            
            # Definir las dimensiones del radar
            categories = ['Posesión', 'Puntería', 'Amenaza', 'Control', 'Presión']
            
            # Preparar datos: Necesitamos normalizar para que todo esté en escala 0-100
            # Usamos el máximo del dataset filtrado para comparar
            radar_data = []
            
            for team in selected_teams[:3]:  # Limitamos a 3 para que sea legible
                team_stats = df_filtered[df_filtered['team_name'] == team]
                
                # Cálculo de métricas normalizadas
                m1 = team_stats['ball_possession'].mean()
                m2 = team_stats['shot_accuracy'].mean() * 100
                m3 = (team_stats['chances_created'].mean() / df['chances_created'].max()) * 100
                m4 = (team_stats['passes_successful'].mean() / df['passes_successful'].max()) * 100
                m5 = (team_stats['corner_kicks'].mean() / df['corner_kicks'].max()) * 100
                
                radar_data.append(go.Scatterpolar(
                    r=[m1, m2, m3, m4, m5],
                    theta=categories,
                    fill='toself',
                    name=team
                ))

            fig_radar = go.Figure(data=radar_data)
            fig_radar.update_layout(
                polar=dict(
                    radialaxis=dict(visible=True, range=[0, 100])
                ),
                showlegend=True,
                template="plotly_white",
                margin=dict(l=40, r=40, t=20, b=20)
            )
            
            st.plotly_chart(fig_radar, use_container_width=True)
            st.write("**Identidad del equipo:** Este gráfico compara el estilo de juego. Nota: Limitado a los primeros 3 equipos seleccionados para mayor claridad.")

        st.divider()

        # --- FILA 3: ANÁLISIS DE PRECISIÓN ---
        st.subheader("📉 Distribución de Goles")
        fig_hist = px.histogram(
            df_filtered,
            x='score',
            color='team_name',
            barmode='group',
            labels={'score': 'Goles Marcados', 'count': 'Frecuencia'},
            template="plotly_white"
        )
        st.plotly_chart(fig_hist, use_container_width=True)
        st.write("¿Cuántos goles suele marcar cada equipo? Este histograma muestra la frecuencia de resultados.")

        st.divider()

        # --- FILA 4: VICTORIAS, DERROTAS Y EMPATES ---
        st.subheader("🏆 Historial de Resultados")
        
        # Calcular resultados (Win/Loss/Draw)
        # Necesitamos comparar el score de cada equipo con su oponente en el mismo event_id
        @st.cache_data
        def calculate_results(_df_full):
            # Obtener el score del oponente para cada fila
            match_scores = _df_full[['event_id', 'team_id', 'score']]
            df_with_opp = _df_full.merge(match_scores, on='event_id', suffixes=('', '_opp'))
            # Filtrar para quedarse solo con el oponente real (distinto team_id)
            df_with_opp = df_with_opp[df_with_opp['team_id'] != df_with_opp['team_id_opp']]
            
            def get_result(row):
                if row['score'] > row['score_opp']: return 'Victoria'
                elif row['score'] < row['score_opp']: return 'Derrota'
                else: return 'Empate'
            
            df_with_opp['resultado'] = df_with_opp.apply(get_result, axis=1)
            return df_with_opp[['event_id', 'team_id', 'resultado']]

        results_map = calculate_results(df)
        # Unir los resultados calculados con el dataframe filtrado
        df_outcomes = df_filtered.merge(results_map, on=['event_id', 'team_id'])
        
        # Agrupar para el gráfico
        df_res_agg = df_outcomes.groupby(['team_name', 'resultado']).size().reset_index(name='cantidad')
        
        fig_results = px.bar(
            df_res_agg,
            x='team_name',
            y='cantidad',
            color='resultado',
            title="Victorias, Empates y Derrotas por Equipo",
            labels={'team_name': 'Equipo', 'cantidad': 'Número de Partidos', 'resultado': 'Resultado'},
            color_discrete_map={'Victoria': '#22c55e', 'Empate': '#eab308', 'Derrota': '#ef4444'},
            barmode='group',
            template="plotly_white"
        )
        
        st.plotly_chart(fig_results, use_container_width=True)
        st.write("Este gráfico compara la efectividad final de los equipos seleccionados, mostrando cuántos partidos han resultado en victoria, empate o derrota.")

    st.divider()

    # --- SECCIÓN FINAL: TODOS LOS DATOS ---
    st.subheader("Explorador Global de Datos")
    st.markdown("Acceso completo al dataset procesado utilizado en este proyecto.")
    
    with st.expander("Ver DataFrame Completo"):
        st.dataframe(df, use_container_width=True)
        st.caption(f"El dataset contiene un total de {len(df)} registros y {len(df.columns)} columnas.")

# FOOTER
st.divider()
st.markdown("""
<div style="text-align: center; color: #9ca3af; font-size: 0.8rem;">
    Desarrollado para la asignatura de Obtención y Extracción de Datos | Master en Big Data & Business Intelligence
</div>
""", unsafe_allow_html=True)
