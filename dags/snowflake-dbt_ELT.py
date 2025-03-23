from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import dag, task

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 20),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='budapest_weather_email',
    default_args=default_args,
    schedule=None,  
    catchup=False,
)
def weather_etl():
    create_table_task = SQLExecuteQueryOperator(
        task_id="create_table",
        sql="""
        CREATE TABLE IF NOT EXISTS raw_weather_data (
            date TIMESTAMP_NTZ,
            apparent_temperature FLOAT,
            temperature_2m FLOAT,
            precipitation FLOAT
        );
        """,
        conn_id="snowflake_connect",
    )

    @task
    def weather_extract():
        """Extract from Open-Meteo API """
        import pandas as pd
        import openmeteo_requests
        
        # API 请求配置
        om = openmeteo_requests.Client()
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 47.4984,
            "longitude": 19.0404,
            "hourly": ["apparent_temperature", "temperature_2m", "precipitation"],
            "forecast_days": 16
        }
        
        responses = om.weather_api(url, params=params)
        response = responses[0]
        
        hourly = response.Hourly()
        time_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
        
        df = pd.DataFrame({
            "date": time_range.strftime("%Y-%m-%d %H:%M:%S"),  
            "apparent_temperature": hourly.Variables(0).ValuesAsNumpy(),
            "temperature_2m": hourly.Variables(1).ValuesAsNumpy(),
            "precipitation": hourly.Variables(2).ValuesAsNumpy()
        })
        
        return df.to_dict('records')

    @task
    def weather_load(data: list):
        """load to Snowflake"""
        if not data:
            raise ValueError("No data to load")
        
        # values = ",".join([
        #     f"('{row['date']}', {row['apparent_temperature']})" 
        #     for row in data
        # ])

        values = ",".join([
            f"('{row['date']}', {row['apparent_temperature']}, {row['temperature_2m']}, {row['precipitation']})" 
            for row in data
        ])

        
        sql = f"""
        INSERT INTO raw_weather_data (date, apparent_temperature, temperature_2m, precipitation)
        VALUES {values};
        """
        
        SQLExecuteQueryOperator(
            task_id="load_data",
            sql=sql,
            conn_id="snowflake_connect",
            autocommit=True
        ).execute(context={})


    extract_data = weather_extract()
    load_data = weather_load(extract_data)
    
    create_table_task >> extract_data >> load_data

dag_instance = weather_etl()