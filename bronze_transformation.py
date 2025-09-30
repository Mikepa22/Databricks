from pyspark import pipelines as dp
import requests
from datetime import datetime

def get_venezuela_rates():
    url = "https://ve.dolarapi.com/v1/dolares/oficial"
    response = requests.get(url)
    return response.json()

def get_argentina_rates():
    url = "https://dolarapi.com/v1/dolares/oficial"
    response = requests.get(url)
    return response.json()

def get_mexico_rates():
    url = "https://mx.dolarapi.com/v1/cotizaciones/usd"
    response = requests.get(url)
    return response.json()

def get_brazil_rates():
    url = "https://br.dolarapi.com/v1/cotacoes/usd"
    response = requests.get(url)
    return response.json()

@dp.table(
    name="fx_rates_raw",
    comment="Raw FX rates from multiple countries"
)
def fx_rates_raw():
    raw_data = {
        'venezuela': get_venezuela_rates(),
        'argentina': get_argentina_rates(),
        'mexico': get_mexico_rates(),
        'brazil': get_brazil_rates(),
        'extraction_timestamp': datetime.now().isoformat()
    }
    return spark.createDataFrame([raw_data])