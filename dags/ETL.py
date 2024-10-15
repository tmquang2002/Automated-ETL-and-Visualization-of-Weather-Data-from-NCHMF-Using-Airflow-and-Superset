from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from bs4 import BeautifulSoup
import json
from pymongo import MongoClient
import requests
import psycopg2


def get_weather_data(location_id):
    # Gửi request tới trang web với ID thay thế
    url = f'https://nchmf.gov.vn/Kttv/vi-VN/{location_id}/mapinfor.html'
    response = requests.get(url)

    # Phân tích cú pháp HTML bằng BeautifulSoup
    soup = BeautifulSoup(response.content, 'html.parser')

    # Tìm tất cả các thông tin dự báo thời tiết
    weather_info = {}

    # Lấy thông tin thời tiết hiện tại (tên địa phương)
    location = soup.find(id="_ctl1__ctl0__ctl0_lbl_ThoiTietHienTai").text
    weather_info['Địa phương'] = location.split(":")[1].strip()

    # Lấy thông tin nhiệt độ
    temperature = soup.find(id="_ctl1__ctl0__ctl0_lbl_HTNhietdo").find_next('strong').text
    weather_info['Nhiệt độ'] = temperature.split('o')[0]

    # Lấy thông tin độ ẩm
    humidity = soup.find(id="_ctl1__ctl0__ctl0_lblHT_Doam").find_next('strong').text
    weather_info['Độ ẩm'] = humidity

    # Lấy thông tin hướng gió
    wind_direction = soup.find(id="_ctl1__ctl0__ctl0_lblHT_Gio").find_next('strong').text
    weather_info['Hướng gió'] = wind_direction

    # Tìm thêm các thông tin khác nếu có
    update_time = soup.find(id="_ctl1__ctl0__ctl0_lbl_DuBaoThoiTietCapNhatLuc").parent.text.strip()
    weather_info['Cập nhật lúc'] = update_time.split(":")[1]

    # Tìm thẻ <script> chứa mã Highcharts
    script_tags = soup.find_all('script', text=lambda t: t and 'Highcharts.chart' in t)

    # Lấy nội dung của thẻ <script>
    script_content = script_tags[1].string

    # Tìm thông tin tiêu đề
    title = script_content.split("title: {")[1].split("text: ")[1].split(",")[0].strip().strip("'\"")

    # Tìm thông tin trục x
    x_axis = script_content.split("xAxis: {")[1].split("categories: ")[1].split("]")[0].strip().strip("[]").split(",")

    # Tìm thông tin trục y
    y_axis_title = script_content.split("yAxis: {")[1].split("title: {")[1].split("text: ")[1].split(",")[0].strip().strip("'\"")

    # Tìm thông tin dữ liệu series
    series_data = script_content.split("series: [")[1].split("data: [")[1].split("]")[0].split(",")

    # Tạo một từ điển data chứa tất cả thông tin
    data = {
        "Địa phương": weather_info['Địa phương'],
        "Thời tiết": {
            "Nhiệt độ": weather_info['Nhiệt độ'],
            "Độ ẩm": weather_info['Độ ẩm'],
            "Hướng gió": weather_info['Hướng gió'],
            "Cập nhật lúc": weather_info['Cập nhật lúc']
        },
        "Biểu đồ": {
            "Tiêu đề": title,
            "Trục X": [x.strip().strip("'\"") for x in x_axis],
            "Dữ liệu series": [int(value.strip()) for value in series_data]
        }
    }

    # Chuyển đổi từ điển thành định dạng JSON
    json_data = json.dumps(data, ensure_ascii=False, indent=4)
    return json_data
def extract():
    # Kết nối đến MongoDB
    MONGO_URL = "mongodb+srv://20120559:20120559@cluster0.banxgu8.mongodb.net/"
    client = MongoClient(MONGO_URL)
    db = client['Weather']  # Tên database

    weather_data = get_weather_data(2)
    weather_data = json.loads(weather_data)

    # Lấy thời gian cập nhật và tạo tên collection
    update_time_str = weather_data['Thời tiết']['Cập nhật lúc']
    update_time_parts = update_time_str.split()  # Chia theo khoảng trắng
    time_str = update_time_parts[0].replace(":", "_") + "_" + update_time_parts[1].replace("/", "_")
    collection_name = f"{time_str}"

    # Kiểm tra xem collection đã tồn tại chưa
    if collection_name in db.list_collection_names():
        print(f"Collection '{collection_name}' already exists. Exiting.")
    else:
        # Tạo collection mới và thêm dữ liệu vào đó
        collection = db[collection_name]
        # Thêm dữ liệu mới vào collection
        for i in range(2, 65):
            wd = get_weather_data(i)
            wd = json.loads(wd)
            wd['_id'] = i
            try:
                collection.insert_one(wd)
            except Exception as e:
                print(f"Error inserting data for location_id {i}: {e}")
def getlocationname():
    weather_data = get_weather_data(2)
    weather_data = json.loads(weather_data)
    # Lấy thời gian cập nhật và tạo tên collection
    update_time_str = weather_data['Thời tiết']['Cập nhật lúc']
    update_time_parts = update_time_str.split()  # Chia theo khoảng trắng
    time_str = update_time_parts[0].replace(":", "_") + "_" + update_time_parts[1].replace("/", "_")
    collection_name = f"{time_str}"
    return collection_name

def tranform_load():
    MONGO_URL = "mongodb+srv://20120559:20120559@cluster0.banxgu8.mongodb.net/"
    client = MongoClient(MONGO_URL)
    db = client['Weather']  # Tên database
    collection = db[getlocationname()]  # Tên collection

    # Truy vấn dữ liệu từ MongoDB
    mongo_data = collection.find()

    conn = psycopg2.connect(
        database='postgres',
        user='postgres',
        password='postgres',
        host='host.docker.internal',
        port=5433       
    )
    pg_cursor = conn.cursor()

    # Xóa bảng nếu tồn tại
    pg_cursor.execute("DROP TABLE IF EXISTS weather_series;")
    pg_cursor.execute("DROP TABLE IF EXISTS weather_info;")

    # Tạo bảng weather_info
    create_weather_info_table = """
    CREATE TABLE IF NOT EXISTS weather_info (
        id INT PRIMARY KEY,
        location VARCHAR(255),
        temperature INT,
        humidity INT,
        wind_direction VARCHAR(255),
        updated_at TEXT
    );
    """
    pg_cursor.execute(create_weather_info_table)

    # Tạo bảng weather_series
    create_weather_series_table = """
    CREATE TABLE IF NOT EXISTS weather_series (
        id INT,
        x_axis TIMESTAMP,
        series_data INT,
        PRIMARY KEY (id, x_axis),
        FOREIGN KEY (id) REFERENCES weather_info(id)
    );
    """
    pg_cursor.execute(create_weather_series_table)

    # Hàm chuyển đổi chuỗi thời gian thành định dạng timestamp hợp lệ cho PostgreSQL
    def convert_to_timestamp(date_str):
        # Giả sử date_str có định dạng '2/10(1h)' - nghĩa là '2 tháng 10, 1 giờ'
        day_month, hour_str = date_str.split('(')  # Tách ngày và giờ
        day, month = map(int, day_month.split('/'))  # Tách ngày và tháng
        hour = int(hour_str.strip('h)'))  # Lấy giờ (loại bỏ 'h' và ')')

        # Giả định năm hiện tại là 2024 (bạn có thể thay đổi nếu cần)
        year = datetime.now().year

        # Tạo một đối tượng datetime
        dt = datetime(year, month, day, hour)

        # Chuyển đổi thành chuỗi thời gian PostgreSQL
        return dt.strftime('%Y-%m-%d %H:%M:%S')

        # Hàm chèn dữ liệu vào bảng weather_info
    def insert_weather_info(record):
        pg_cursor.execute("""
            INSERT INTO weather_info (id, location, temperature, humidity, wind_direction, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;  -- Bỏ qua nếu khóa chính đã tồn tại
        """, (record['_id'], record['Địa phương'], record['Thời tiết']['Nhiệt độ'],
            record['Thời tiết']['Độ ẩm'], record['Thời tiết']['Hướng gió'],
            record['Thời tiết']['Cập nhật lúc']))  # Giữ nguyên updated_at là text

    # Hàm chèn dữ liệu vào bảng weather_series
    def insert_weather_series(id, x_axis, series_data):
        # Chuyển đổi x_axis từ chuỗi thời gian thành timestamp
        x_axis_timestamp = convert_to_timestamp(x_axis)  # Chuyển đổi x_axis thành timestamp
        pg_cursor.execute("""
            INSERT INTO weather_series (id, x_axis, series_data)
            VALUES (%s, %s, %s)
            ON CONFLICT (id, x_axis) DO NOTHING;  -- Bỏ qua nếu cặp (id, x_axis) đã tồn tại
        """, (id, x_axis_timestamp, series_data))

    # Duyệt qua dữ liệu MongoDB và chèn vào PostgreSQL
    for record in mongo_data:
        insert_weather_info(record)

        # Lấy dữ liệu trục X và series, chèn vào bảng weather_series
        x_axis_data = record['Biểu đồ']['Trục X']
        series_data = record['Biểu đồ']['Dữ liệu series']
        
        for x_value, series_value in zip(x_axis_data, series_data):
            insert_weather_series(record['_id'], x_value, series_value)

    # Lưu các thay đổi và đóng kết nối
    conn.commit()
    pg_cursor.close()
    conn.close()
    client.close()

# Định nghĩa DAG
with DAG(
    'ETL',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 10, 11, 6, 15),  # Bắt đầu vào 6h015, 11/10/2023
    },
    schedule_interval='0 */3 * * *',  # Chạy mỗi 3 giờ
    catchup=False,
) as dag:

    # Tạo một tác vụ PythonOperator cho việc trích xuất dữ liệu
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    # Tạo một tác vụ PythonOperator cho việc chuyển đổi và tải dữ liệu
    transform_and_load_task = PythonOperator(
        task_id='transform_and_load',
        python_callable=tranform_load,  # Tên hàm chuyển đổi và tải
    )

    # Đặt thứ tự thực hiện các tác vụ
    extract_task >> transform_and_load_task  
