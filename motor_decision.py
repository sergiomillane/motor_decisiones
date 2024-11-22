import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from gspread_dataframe import get_as_dataframe
from google.oauth2.service_account import Credentials
import gspread

# Configuración de claves de Google Cloud desde st.secrets
key_data = st.secrets["GOOGLE_CLOUD_KEY_JSON"]
creds = Credentials.from_service_account_info(key_data)
client = gspread.authorize(creds)

# Configuración de conexión a la base de datos con SQLAlchemy
db_config = st.secrets["DATABASE"]
database_url = f"mssql+pymssql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(database_url)

# Configuración de la aplicación
st.sidebar.title("Navegación")
page = st.sidebar.radio("Ir a", ["Evaluación de Crédito", "Base Crédito", "Evaluación Cliente Nuevo"])

@st.cache_data
def cargar_datos():
    # Configuración de conexión de Google Sheets
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(key_data, scopes=SCOPES)
    client = gspread.authorize(creds)

    # Carga de datos de Google Sheets
    spreadsheet_id = "1w2hMUpuWAJfc2rNv2IbH_WfiX8hVe8U7M47dOdzkrsg"
    worksheet_credito = client.open_by_key(spreadsheet_id).worksheet("CREDITO")
    worksheet_originacion = client.open_by_key(spreadsheet_id).worksheet("ORIGINACIÓN")

    credito = get_as_dataframe(worksheet_credito, evaluate_formulas=True)
    credito = credito[["Fecha de asignación", "FOLIO", "Cliente", "Resultado"]]
    credito = credito.rename(columns={"Cliente": "ID_CLIENTE"})

    originacion = get_as_dataframe(worksheet_originacion, evaluate_formulas=True)
    originacion = originacion[["Fecha de asignación", "Folio", "Cliente", "Estatus"]]
    
    originacion = originacion.rename(columns={"Estatus": "Resultado", "Cliente": "ID_CLIENTE","Folio":"FOLIO"})
    


    credito = pd.concat([credito, originacion], ignore_index=True)

    # Consulta de base de datos con SQLAlchemy
    query3 = """SELECT [SapIdCliente], CAST([FechaGenerado] AS DATE) AS FechaGenerado, [Fecha], [Mensualidad]
                FROM [CreditoyCobranza].[dbo].[Cartera_Financiera_Diaria]"""
    CF = pd.read_sql(query3, engine)
    Mensualidad = CF.groupby("SapIdCliente")[["Mensualidad"]].sum()

    query4 = """SELECT * FROM MODELO_GESTIONES"""
    posturas_gestiones = pd.read_sql(query4, engine)
    posturas_gestiones["ID_CLIENTE"] = pd.to_numeric(posturas_gestiones["ID_CLIENTE"], errors="coerce").astype("Int64")
    posturas_gestiones = posturas_gestiones.rename(columns={"Resultado": "Marca_Gestiones"})

    # Cargar datos de Excel
    vector_apvap = pd.read_excel("data/ULTIMOS_APVAP_VECTOR.xlsx", "Hoja1")
    vector_apvap = vector_apvap.rename(columns={"SapIdCliente": "ID_CLIENTE"})

    return credito, Mensualidad, posturas_gestiones, vector_apvap

# Cargar los datos iniciales en caché
credito, Mensualidad, posturas_gestiones, vector_apvap = cargar_datos()

if page == "Base Crédito":
    st.title("Base Crédito Consolidada")

    # Filtro para la columna 'Resultado'
    resultado_unico = credito["Resultado"].dropna().unique().tolist()
    filtro_resultado = st.selectbox("Filtrar por Resultado", options=["Todos"] + resultado_unico)

    # Aplicar el filtro si no se selecciona "Todos"
    if filtro_resultado != "Todos":
        credito_filtrado = credito[credito["Resultado"] == filtro_resultado]
    else:
        credito_filtrado = credito

    st.dataframe(credito_filtrado)

elif page == "Evaluación de Crédito":
    st.title("Evaluación de Crédito")
    ID_CLIENTE = st.number_input("ID CLIENTE", min_value=1, step=1)
    score_buro = st.number_input("Score Buro", min_value=0, step=1)
    score_nohit = st.number_input("Score No Hit", min_value=0, step=1)
    mensualidad_moto = st.number_input("Mensualidad Moto", min_value=0, step=1)

    if st.button("Calcular Resultado"):
        vector_apvap["ID_CLIENTE"] = pd.to_numeric(vector_apvap["ID_CLIENTE"], errors="coerce").astype("Int64")
        credito["ID_CLIENTE"] = pd.to_numeric(credito["ID_CLIENTE"], errors="coerce").astype("Int64")
        Mensualidad = Mensualidad.reset_index()
        Mensualidad["SapIdCliente"] = Mensualidad["SapIdCliente"].astype("int64")

        vector_apvap["AP3_U6M"] = vector_apvap.apply(
            lambda row: 30 if "AP3" in str(row.values) or "AP4" in str(row.values) else 0, axis=1
        )
        vector_apvap = vector_apvap.drop_duplicates(subset="ID_CLIENTE")
        vector_U6M = vector_apvap[["ID_CLIENTE", "AP3_U6M"]]

        base_credito = pd.merge(credito, vector_U6M, on="ID_CLIENTE", how="left")
        base_credito = pd.merge(base_credito, posturas_gestiones, on="ID_CLIENTE", how="left")
        base_credito = pd.merge(base_credito, Mensualidad, left_on="ID_CLIENTE", right_on="SapIdCliente", how="left")

        base_credito["Score_Buro"] = score_buro
        base_credito["Not_HIT"] = score_nohit

        base_credito["Mensualidad_Total"] = base_credito.apply(
            lambda row: row["Mensualidad"] + mensualidad_moto if not pd.isnull(row["Mensualidad"]) else mensualidad_moto,
            axis=1,
        )

        base_credito["Resultado_Mensualidad"] = base_credito.apply(
            lambda row: 40 if row["Mensualidad_Total"] > row["Mensualidad"] * 2 else 0, axis=1
        )

        def resultado_buro(row):
            if pd.isna(row["Score_Buro"]):
                return "Sin historial"
            elif row["Score_Buro"] == 0:
                if row["Not_HIT"] >= 500 and row["Not_HIT"] <= 610:
                    return 20
                elif row["Not_HIT"] > 610 and row["Not_HIT"] < 640:
                    return 10
                elif row["Not_HIT"] >= 640 and row["Not_HIT"] < 800:
                    return 0
                else:
                    return 20
            elif row["Not_HIT"] == 0:
                if row["Score_Buro"] > 500 and row["Score_Buro"] <= 570:
                    return 20
                elif row["Score_Buro"] > 570 and row["Score_Buro"] <= 600:
                    return 10
                elif row["Score_Buro"] > 600:
                    return 0
                else:
                    return 20
            else:
                return 99999

        base_credito["Resultado_Buro"] = base_credito.apply(resultado_buro, axis=1)

        base_credito["Marca_Gestiones"] = base_credito["Marca_Gestiones"].apply(lambda x: "SIN GESTION" if pd.isnull(x) else x)

        def resultado_gestiones(row):
            if row["Marca_Gestiones"] == "EXCELENTE":
                return 0
            elif row["Marca_Gestiones"] in ["BUENA", "SIN GESTION"]:
                return 10
            elif row["Marca_Gestiones"] in ["MALA", "SIN CONTACTO"]:
                return 20
            else:
                return None

        base_credito["Resultado_Gestiones"] = base_credito.apply(resultado_gestiones, axis=1)

        def calcular_puntaje(row):
            return sum([
                pd.to_numeric(row["AP3_U6M"], errors="coerce") or 0,
                pd.to_numeric(row["Resultado_Mensualidad"], errors="coerce") or 0,
                pd.to_numeric(row["Resultado_Buro"], errors="coerce") or 0,
                pd.to_numeric(row["Resultado_Gestiones"], errors="coerce") or 0,
            ])

        base_credito["Puntaje"] = base_credito.apply(calcular_puntaje, axis=1)

        base_credito["Resultado"] = base_credito["Puntaje"].apply(lambda x: "Aceptado" if x <= 50 else "Rechazado")

        resultado_cliente = base_credito[base_credito["ID_CLIENTE"] == ID_CLIENTE]
        if not resultado_cliente.empty:
            puntaje = resultado_cliente.iloc[0]["Puntaje"]
            resultado = resultado_cliente.iloc[0]["Resultado"]

            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Puntaje Total:** {puntaje}")
            with col2:
                if pd.isna(puntaje):
                    st.markdown("🔴 No aplica para este análisis!")
                else:
                    st.markdown(f"*Resultado:* {'🔴 Rechazado' if resultado == 'Rechazado' else '🟢 Aceptado'}")
        else:
            st.error("No se encontró información para el cliente ingresado.")

elif page == "Evaluación Cliente Nuevo":
    st.title("Evaluación Cliente Nuevo")
    id_cliente = st.number_input("ID Cliente", value=53535, step=1)
    buro_score = st.number_input("Score Buro", value=530, step=1)
    score_nohit = st.number_input("Score No Hit", value=0, step=1)
    edad = st.number_input("Edad", value=26, step=1)
    vivienda = st.selectbox("Vivienda", options=["RENTADA", "PROPIA", "TRANSPASO"])
    dependientes_economicos = st.number_input("Dependientes Económicos", value=4, step=1)
    ingreso_estimado = st.number_input("Ingreso Estimado", value=9000, step=100)
    mensualidad = st.number_input("Mensualidad", value=1000, step=100)

    mensualidad_estimada = ingreso_estimado / (dependientes_economicos * 2)

    dic_general = {
        "ID_Cliente": [id_cliente],
        "Score_Buro": [buro_score],
        "No_HIT": [score_nohit],
        "Edad": [edad],
        "Vivienda": [vivienda],
        "Dependientes": [dependientes_economicos],
        "Ingreso": [ingreso_estimado],
        "Mensualidad": [mensualidad]
    }

    base_general = pd.DataFrame(dic_general)
    base_general["Puntaje_Score"] = base_general["Score_Buro"].apply(lambda x: 20 if x < 580 else 0)
    base_general["Puntaje_Edad"] = base_general["Edad"].apply(lambda x: 10 if x < 30 else 0)
    base_general["Puntaje_Mensualidad"] = base_general.apply(lambda row: 20 if row["Mensualidad"] > mensualidad_estimada * 1.5 else 0, axis=1)
    base_general["Puntaje_Vivienda"] = base_general["Vivienda"].apply(lambda x: 20 if x == "RENTADA" else (10 if x == "TRANSPASO" else 0))

    base_general["Puntaje"] = base_general[["Puntaje_Score", "Puntaje_Edad", "Puntaje_Mensualidad", "Puntaje_Vivienda"]].sum(axis=1)
    base_general["Resultado"] = base_general["Puntaje"].apply(lambda x: "Aceptado" if x < 50 else "Rechazado")

    if st.button("Calcular Resultado"):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Puntaje Total:** {base_general.iloc[0]['Puntaje']}")
        with col2:
            resultado = base_general.iloc[0]["Resultado"]
            st.markdown(f"**Resultado:** {'🟢 Aceptado' if resultado == 'Aceptado' else '🔴 Rechazado'}")
