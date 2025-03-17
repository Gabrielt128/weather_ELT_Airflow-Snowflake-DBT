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
            apparent_temperature FLOAT
        );
        """,
        conn_id="snowflake_connect",
    )

    @task
    def weather_extract():
        """从 Open-Meteo API 提取天气数据"""
        import pandas as pd
        import openmeteo_requests
        
        # API 请求配置
        om = openmeteo_requests.Client()
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 47.4984,
            "longitude": 19.0404,
            "hourly": "apparent_temperature",
            "forecast_days": 16
        }
        
        # 获取数据
        responses = om.weather_api(url, params=params)
        response = responses[0]
        
        # 解析数据
        hourly = response.Hourly()
        time_range = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )
        
        # 转换为 DataFrame
        df = pd.DataFrame({
            "date": time_range.strftime("%Y-%m-%d %H:%M:%S"),  # 转换为 Snowflake 兼容的格式
            "apparent_temperature": hourly.Variables(0).ValuesAsNumpy()
        })
        
        return df.to_dict('records')

    @task
    def weather_load(data: list):
        """加载数据到 Snowflake"""
        if not data:
            raise ValueError("No data to load")
        
        # 生成批量插入 SQL
        values = ",".join([
            f"('{row['date']}', {row['apparent_temperature']})" 
            for row in data
        ])
        
        sql = f"""
        INSERT INTO raw_weather_data (date, apparent_temperature)
        VALUES {values};
        """
        
        # 执行加载
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