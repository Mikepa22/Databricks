import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

catalog = "test_demo"
bronze_schema = "bronze"
silver_schema = "silver"
USD_COP = 4030.0

# ============= TABLA SILVER UNIFICADA =============
@dlt.table(
    name=f"{catalog}.{silver_schema}.fx_rates_silver",
    comment="Normalized FX rates from APIs and CSV - Silver layer",
    table_properties={
        "quality": "silver"
    })

def fx_rates_silver():
    """
    Silver layer: Combina datos de APIs y CSV histórico
    """
    
    #  PROCESAR DATOS DE APIs
    bronze_api_df = spark.table("test_demo.bronze.fx_rates_raw")
    
    # Parse Venezuela API
    venezuela_api_df = bronze_api_df.select(
        lit("API").alias("source_type"),
        lit("Venezuela").alias("country"),
        lit("USD").alias("currency_from"),
        lit("VES").alias("currency_to"),
        col("venezuela.compra").cast("double").alias("exchange_rate_buy"),
        col("venezuela.venta").cast("double").alias("exchange_rate_sell"),
        col("venezuela.promedio").cast("double").alias("exchange_rate_avg"),
        col("venezuela.fuente").alias("source"),
        col("venezuela.fechaActualizacion").alias("api_last_update"),
        lit("America/Caracas").alias("timezone"),
        to_timestamp(col("extraction_timestamp")).alias("date_utc"),
        to_date(col("extraction_timestamp")).alias("extraction_date"),
        lit(1).alias("data_priority")  # APIs tienen prioridad alta
    )
    
    # Parse Argentina API
    argentina_api_df = bronze_api_df.select(
        lit("API").alias("source_type"),
        lit("Argentina").alias("country"),
        lit("USD").alias("currency_from"),
        lit("ARS").alias("currency_to"),
        col("argentina.compra").cast("double").alias("exchange_rate_buy"),
        col("argentina.venta").cast("double").alias("exchange_rate_sell"),
        ((col("argentina.compra") + col("argentina.venta")) / 2).cast("double").alias("exchange_rate_avg"),
        col("argentina.casa").alias("source"),
        col("argentina.fechaActualizacion").alias("api_last_update"),
        lit("America/Buenos_Aires").alias("timezone"),
        to_timestamp(col("extraction_timestamp")).alias("date_utc"),
        to_date(col("extraction_timestamp")).alias("extraction_date"),
        lit(1).alias("data_priority")
    )
    
    # Parse Mexico API
    mexico_api_df = bronze_api_df.select(
        lit("API").alias("source_type"),
        lit("Mexico").alias("country"),
        lit("USD").alias("currency_from"),
        lit("MXN").alias("currency_to"),
        col("mexico.compra").cast("double").alias("exchange_rate_buy"),
        col("mexico.venta").cast("double").alias("exchange_rate_sell"),
        ((col("mexico.compra") + col("mexico.venta")) / 2).cast("double").alias("exchange_rate_avg"),
        col("mexico.nombre").alias("source"),
        col("mexico.fechaActualizacion").alias("api_last_update"),
        lit("America/Mexico_City").alias("timezone"),
        to_timestamp(col("extraction_timestamp")).alias("date_utc"),
        to_date(col("extraction_timestamp")).alias("extraction_date"),
        lit(1).alias("data_priority")
    )
    
    # Parse Brazil API
    brazil_api_df = bronze_api_df.select(
        lit("API").alias("source_type"),
        lit("Brazil").alias("country"),
        lit("USD").alias("currency_from"),
        lit("BRL").alias("currency_to"),
        col("brazil.compra").cast("double").alias("exchange_rate_buy"),
        col("brazil.venda").cast("double").alias("exchange_rate_sell"),
        ((col("brazil.compra") + col("brazil.venda")) / 2).cast("double").alias("exchange_rate_avg"),
        col("brazil.nome").alias("source"),
        col("brazil.dataAtualizacao").alias("api_last_update"),
        lit("America/Sao_Paulo").alias("timezone"),
        to_timestamp(col("extraction_timestamp")).alias("date_utc"),
        to_date(col("extraction_timestamp")).alias("extraction_date"),
        lit(1).alias("data_priority")
    )
    
    # Unir todos los datos de APIs
    api_unified_df = venezuela_api_df.union(argentina_api_df).union(mexico_api_df).union(brazil_api_df)
    
    # ========== 2. PROCESAR DATOS DEL CSV ==========
    try:
        csv_bronze_df = spark.table("test_demo.bronze.historical_rates_csv")
        
        csv_data_df = csv_bronze_df.select(
            lit("CSV").alias("source_type"),
            
            # Estandarizar nombres de países
            when(upper(col("Pais")) == "VENEZUELA", "Venezuela")
            .when(upper(col("Pais")) == "ARGENTINA", "Argentina")
            .when(upper(col("Pais")) == "MEXICO", "Mexico")
            .when(upper(col("Pais")).isin("BRASIL", "BRAZIL"), "Brazil")
            .otherwise(initcap(col("Pais"))).alias("country"),
            
            # Estandarizar moneda
            when(col("Moneda").isin("Dollar", "DOLLAR", "USD", "Dólares"), "USD")
            .otherwise(upper(col("Moneda"))).alias("currency_from"),
            
            # Determinar currency_to basado en el país
            when(upper(col("Pais")) == "VENEZUELA", "VES")
            .when(upper(col("Pais")) == "ARGENTINA", "ARS")
            .when(upper(col("Pais")) == "MEXICO", "MXN")
            .when(upper(col("Pais")).isin("BRASIL", "BRAZIL"), "BRL")
            .otherwise("UNKNOWN").alias("currency_to"),
            
            col("Compra").cast("double").alias("exchange_rate_buy"),
            col("Venta").cast("double").alias("exchange_rate_sell"),
            
            # Calcular promedio, manejando nulls y ceros
            when(col("Venta").isNull() | (col("Venta") == 0), col("Compra"))
            .when(col("Compra").isNull() | (col("Compra") == 0), col("Venta"))
            .otherwise((col("Compra") + col("Venta")) / 2).alias("exchange_rate_avg"),
            
            lit("Historical").alias("source"),
            lit(None).cast("string").alias("api_last_update"),  # CSV no tiene este campo
            
            # Asignar timezone según país
            when(upper(col("Pais")) == "VENEZUELA", "America/Caracas")
            .when(upper(col("Pais")) == "ARGENTINA", "America/Buenos_Aires")
            .when(upper(col("Pais")) == "MEXICO", "America/Mexico_City")
            .when(upper(col("Pais")).isin("BRASIL", "BRAZIL"), "America/Sao_Paulo")
            .otherwise("UTC").alias("timezone"),
            
            to_timestamp(col("Fecha")).alias("date_utc"),
            to_date(col("Fecha")).alias("extraction_date"),
            lit(2).alias("data_priority")  # CSV tiene prioridad menor
        )
        
        # ========== 3. UNIFICAR APIs + CSV ==========
        unified_df = api_unified_df.unionByName(csv_data_df, allowMissingColumns=True)
        
    except Exception as e:
        # Si no existe la tabla CSV, solo usar datos de API
        print(f"Nota: No se encontró tabla CSV histórica. Usando solo datos de API. Error: {e}")
        unified_df = api_unified_df
    
    # ========== 4. AGREGAR MÉTRICAS CALCULADAS ==========
    return unified_df.withColumn(
        "spread", 
        col("exchange_rate_sell") - col("exchange_rate_buy")
    ).withColumn(
        "spread_percentage",
        round(((col("exchange_rate_sell") - col("exchange_rate_buy")) / 
               col("exchange_rate_buy")) * 100, 2)
    ).withColumn(
        "date_local",
        from_utc_timestamp(col("date_utc"), col("timezone"))
    ).withColumn(
        # CÁLCULO CORRECTO: Cuántos COP vale 1 unidad de moneda local
        "cop_per_unit",
        when(col("exchange_rate_avg").isNull() | (col("exchange_rate_avg") == 0), lit(None))
        .otherwise(lit(USD_COP) / col("exchange_rate_avg"))
    ).withColumn(
        # Compra en COP
        "cop_per_unit_buy",
        when(col("exchange_rate_buy").isNull() | (col("exchange_rate_buy") == 0), lit(None))
        .otherwise(lit(USD_COP) / col("exchange_rate_buy"))
    ).withColumn(
        # Venta en COP
        "cop_per_unit_sell",
        when(col("exchange_rate_sell").isNull() | (col("exchange_rate_sell") == 0), lit(None))
        .otherwise(lit(USD_COP) / col("exchange_rate_sell"))
    ).withColumn(
        # Spread en COP
        "spread_cop",
        col("cop_per_unit_sell") - col("cop_per_unit_buy"))
    """.withColumn(
        "cop_per_unit",
        # Conversión aproximada a COP
        when(col("currency_to") == "VES", col("exchange_rate_avg") * 24)
        .when(col("currency_to") == "ARS", col("exchange_rate_avg") * 4)
        .when(col("currency_to") == "MXN", col("exchange_rate_avg") * 220)
        .when(col("currency_to") == "BRL", col("exchange_rate_avg") * 800)
        
    )""".withColumn(
        "processing_timestamp",
        current_timestamp()
    )

# ============= TABLA DE AGREGACIÓN DIARIA =============
@dlt.table(
    name=f"{catalog}.{silver_schema}.fx_rates_daily",
    comment="Daily aggregated FX rates - Silver layer",
    table_properties={
        "quality": "silver"
    }
)
def fx_rates_daily():
    """
    Agregación diaria con todas las métricas calculadas en COP
    """
    silver_df = dlt.read("fx_rates_silver")
    
    return silver_df.groupBy(
        "country",
        "currency_to",
        "extraction_date",
        "timezone"
    ).agg(
        # ===== MÉTRICAS PRINCIPALES EN COP =====
        round(avg("cop_per_unit"), 2).alias("avg_cop_per_unit"),
        round(min("cop_per_unit"), 2).alias("min_cop_per_unit"),
        round(max("cop_per_unit"), 2).alias("max_cop_per_unit"),
        round(stddev("cop_per_unit"), 2).alias("stddev_cop"),
        
        # Coeficiente de Variación en COP (métrica clave de estabilidad)
        round((stddev("cop_per_unit") / avg("cop_per_unit")) * 100, 2).alias("cv_cop"),
        
        # Rango en COP
        round(max("cop_per_unit") - min("cop_per_unit"), 2).alias("range_cop"),
        round(((max("cop_per_unit") - min("cop_per_unit")) / min("cop_per_unit")) * 100, 2).alias("range_pct_cop"),
        
        # Spread promedio en COP
        round(avg("spread_cop"), 2).alias("avg_spread_cop"),
        
        # ===== MÉTRICAS EN MONEDA LOCAL (para referencia) =====
        round(avg("exchange_rate_avg"), 4).alias("avg_rate_local"),
        round(min("exchange_rate_avg"), 4).alias("min_rate_local"),
        round(max("exchange_rate_avg"), 4).alias("max_rate_local"),
        
        # ===== METADATA =====
        countDistinct("source_type").alias("num_source_types"),
        count("*").alias("total_readings"),
        first("exchange_rate_avg").alias("first_rate_of_day"),
        last("exchange_rate_avg").alias("last_rate_of_day"),
        max("date_utc").alias("last_update"),
        collect_set("source").alias("sources_available")
    ).withColumn(
        # Indicador de volatilidad basado en CV de COP
        "volatility_cop",
        when(col("cv_cop") > 10, "High")
        .when(col("cv_cop") > 5, "Medium")
        .otherwise("Low")
    ).withColumn(
        # Cambio intradiario en COP
        "intraday_change_cop",
        col("max_cop_per_unit") - col("min_cop_per_unit")
    )


# ============= TABLA DE ÚLTIMAS TASAS =============

@dlt.table(
    name=f"{catalog}.{silver_schema}.fx_rates_latest",
    comment="Latest FX rates by country with COP values - Silver layer",
    table_properties={
        "quality": "silver"
    }
)


def fx_rates_latest():
    """
    Última tasa de cambio por país con valores en COP
    """
    from pyspark.sql.window import Window
    
    silver_df = dlt.read("fx_rates_silver")
    
    window_spec = Window.partitionBy("country").orderBy(
        col("data_priority").asc(),
        col("date_utc").desc()
    )
    
    return silver_df.withColumn(
        "rank",
        row_number().over(window_spec)
    ).filter(
        col("rank") == 1
    ).drop("rank").select(
        "country",
        "currency_to",
        "exchange_rate_avg",
        "cop_per_unit",
        "cop_per_unit_buy",
        "cop_per_unit_sell",
        "spread_cop",
        "source",
        "date_utc",
        "date_local",
        "timezone"
    ).withColumn(
        "is_current",
        lit(True)
    ).withColumn(
        "minutes_since_update",
        round((unix_timestamp(current_timestamp()) - 
               unix_timestamp(col("date_utc"))) / 60, 0)
    )
    