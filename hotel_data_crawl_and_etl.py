from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import csv
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length, lower, monotonically_increasing_id, trim
from pytz import timezone

# ===========================================
# Cấu hình múi giờ địa phương
# ===========================================
local_timezone = timezone('Asia/Ho_Chi_Minh')  # Thay đổi nếu cần

# ===========================================
# Hàm Crawl Dữ Liệu với Checkpoint
# ===========================================
def fetch_hotel_data(**kwargs):
    try:
        # Lấy checkpoint (số trang đã crawl trước đó) từ XCom
        ti = kwargs['ti']
        last_page = ti.xcom_pull(task_ids='fetch_hotel_data', key='last_page', default=1)

        # API endpoint và các tham số
        url = 'https://engine.hotellook.com/api/v2/lookup.json'
        token = 'b7f26609c05d7a284e7433702629d8c2'  # Thay bằng token thực tế của bạn
        params = {
            'query': 'all',
            'lookFor': 'hotel',
            'lang': 'en',
            'limit': 10,
            'token': token,
            'page': last_page  # Sử dụng số trang lấy từ XCom (checkpoint)
        }

        # Đường dẫn lưu file CSV
        csv_file_path = '/opt/airflow/dags/all_hotels_data.csv'  # Thay đổi theo đường dẫn của bạn
        csv_headers = [
            'Hotel ID', 'Label', 'Location Name', 'Full Name', 'Location ID',
            'City Name', 'Country Name', 'Country Code', 'IATA', 'Latitude',
            'Longitude', 'Hotels Count'
        ]

        # Mở file CSV và ghi dữ liệu vào
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            # Ghi header nếu file mới
            if os.stat(csv_file_path).st_size == 0:
                writer.writerow(csv_headers)

            page_number = last_page  # Bắt đầu từ trang cuối cùng đã crawl
            while page_number <= last_page + 100:
                params['page'] = page_number
                response = requests.get(url, params=params)

                if response.status_code == 200:
                    data = response.json()
                    location_data = data.get('results', {}).get('location', {})
                    hotels = data.get('results', {}).get('hotels', [])

                    if not hotels:
                        break

                    # Dữ liệu chung về vị trí
                    city_name = location_data.get('cityName', '')
                    country_name = location_data.get('countryName', '')
                    country_code = location_data.get('countryCode', '')
                    iata = location_data.get('iata', '')
                    hotels_count = location_data.get('hotelsCount', 0)

                    for hotel in hotels:
                        writer.writerow([ 
                            hotel.get('id'),
                            hotel.get('label'),
                            hotel.get('locationName'),
                            hotel.get('fullName'),
                            hotel.get('locationId'),
                            city_name,
                            country_name,
                            country_code,
                            iata,
                            hotel.get('location', {}).get('lat', ''),
                            hotel.get('location', {}).get('lon', ''),
                            hotels_count
                        ])

                    print(f"Page {page_number} processed successfully.")
                    page_number += 1

                    # Lưu trạng thái (checkpoint) vào XCom để lần sau tiếp tục từ đây
                    ti.xcom_push(key='last_page', value=page_number)

                else:
                    print(f"Error: {response.status_code} - {response.text}")
                    break

    except Exception as e:
        print(f"Task failed with error: {str(e)}")
        # Không push XCom khi có lỗi


# ===========================================
# Hàm ETL (Xử lý dữ liệu với PySpark)
# ===========================================
def process_hotel_data(**kwargs):
    # Tạo SparkSession
    spark = SparkSession.builder.appName("Hotel Data Processing").getOrCreate()

    # Đọc file CSV đã crawl
    df = spark.read.csv("/opt/airflow/dags/all_hotels_data.csv", header=True, inferSchema=True)
    print(len(df.columns))

    # Data Cleaning
    # Cập nhật tên cột để khớp với cột thực tế trong dataset của bạn
    df_cleaned = df.dropna(
        subset=["Hotel ID", "Label", "Location Name", "Country Name", "Latitude", "Longitude"]
    ).dropDuplicates()

    # Chuẩn hóa tên quốc gia thành chữ thường
    df_cleaned = df_cleaned.withColumn("Country Name", lower(col("Country Name")))

    # Tạo bảng DimCountry
    dim_country = df_cleaned.select("Country Name").distinct().withColumn("Country ID", monotonically_increasing_id())

    # Tạo bảng DimLocation
    dim_location = df_cleaned.select("Location ID", "Location Name", "Latitude", "Longitude").dropDuplicates()

    # Tạo bảng DimHotel
    dim_hotel = df_cleaned.select(
        col("Hotel ID").alias("Hotel ID"), "Label", "Location ID", "Country Name"
    ).dropDuplicates().join(dim_country, "Country Name", "left").select("Hotel ID", "Label", "Location ID", "Country ID")

    # Tạo bảng FactHotelStay
    fact_hotel_stay = df_cleaned.select(
        col("Hotel ID").alias("Hotel ID"), "Hotels Count", "Location ID", "Country Name"
    ).dropDuplicates().join(dim_country, "Country Name", "left").select("Hotel ID", "Hotels Count", "Location ID", "Country ID")

    # Lưu dữ liệu vào CSV
    dim_country.coalesce(1).write.csv("/opt/airflow/dags/dataset/dim_country.csv", header=True, mode="overwrite")
    dim_location.coalesce(1).write.csv("/opt/airflow/dags/dataset/dim_location.csv", header=True, mode="overwrite")
    dim_hotel.coalesce(1).write.csv("/opt/airflow/dags/dataset/dim_hotel.csv", header=True, mode="overwrite")
    fact_hotel_stay.coalesce(1).write.csv("/opt/airflow/dags/dataset/fact_hotel_stay.csv", header=True, mode="overwrite")


# ===========================================
# Định nghĩa DAG
# ===========================================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hotel_data_crawl_and_etl_dag',
    default_args=default_args,
    description='A DAG to crawl hotel data with checkpoint, process it, and save to CSV',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, tzinfo=local_timezone),  # Cấu hình múi giờ Việt Nam
    catchup=False,
) as dag:

    # Task Crawl Dữ Liệu và Lưu Trạng Thái
    fetch_data_task = PythonOperator(
        task_id='fetch_hotel_data',
        python_callable=fetch_hotel_data,
        provide_context=True
    )

    # Task ETL Xử lý Dữ Liệu
    process_data_task = PythonOperator(
        task_id='process_hotel_data',
        python_callable=process_hotel_data,
        provide_context=True
    )

    # Xác định thứ tự chạy các task
    fetch_data_task >> process_data_task
