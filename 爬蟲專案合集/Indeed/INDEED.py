import time
import requests
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pyodbc
import os
import shutil

base_download_dir = r"Z:\08_人資部\A+人資課共用\E.海外\B. 任用\★加盟專案\02.客戶名單\人力平台來源\indeed"
chrome_options = Options()

# 初始化 WebDriver
options = uc.ChromeOptions()
options.add_argument("--incognito")  # 使用隱身模式
options.add_argument("--start-maximized")  # 最大化瀏覽器窗口
options.add_argument("--disable-extensions")  # 禁用擴展
options.add_argument("--disable-gpu")  # 禁用 GPU 加速
options.add_argument("--no-sandbox")  # 禁用沙盒模式
options.add_argument("--disable-dev-shm-usage")  # 避免內存不足問題
prefs = {
    "download.default_directory": base_download_dir,  # 设置下载路径
    "download.prompt_for_download": False,      # 禁止弹出下载对话框
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True                # 启用安全下载
}
chrome_options.add_experimental_option("prefs", prefs)
# 使用 undetected_chromedriver
driver = uc.Chrome(options=options)
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=a73731d9-b479-445c-85a4-4964a47cca78"


# SQL Server 连接参数
server = ''
database = ''
username = ''
password = ''

# 建立连接字符串
connection_string = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)

try:
    # 连接到数据库
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # 查找 Cookies
    select_query = "SELECT indeed FROM [bidb].[dbo].[HR_COOKIES]"
    cursor.execute(select_query)
    result = cursor.fetchone()

    if result:
        cookies_string = result[0]
        print("查找到的 Cookies：")
        print(cookies_string)
    else:
        print("未找到匹配的记录！")
        cookies_string = ""

except Exception as e:
    print(f"数据库连接错误：{e}")

# 转换 Cookies 为字典格式
def convert_cookies_to_dict(cookies_string):
    cookies_list = []
    cookies = cookies_string.split("; ")
    for cookie in cookies:
        name, value = cookie.split("=", 1)
        cookies_list.append({
            "name": name.strip(),
            "value": value.strip(),
            "domain": ".indeed.com"
        })
    return cookies_list

cookies = convert_cookies_to_dict(cookies_string)

try:
    driver.get("https://employers.indeed.com/candidates?statusName=All&id=0")
    for cookie in cookies:
        driver.add_cookie(cookie)
    driver.refresh()
    time.sleep(5)
    print("Cookies 已加载完成！")

except Exception as e:
    print(f"Cookies 加载失败：{e}")


def send_wechat_notification(message):
    payload = {
        "msgtype": "text",
        "text": {
            "content": message
        }
    }
    response = requests.post(webhook_url, json=payload)
    if response.status_code == 200:
        print("提醒已发送！")
    else:
        print(f"提醒发送失败：{response.status_code}, {response.text}")



def monitor_element(css_selector, detection_times=5, interval=1.5, max_attempts=5):
    """
    :param detection_times: 连续检测到的次数
    :param interval: 每次检测的间隔时间（秒）
    :param max_attempts: 最大检测尝试次数
    :return: True 表示连续检测到目标元素，False 表示未能连续检测到
    """
    detection_count = 0
    attempts = 0

    while attempts < max_attempts:
        try:
            WebDriverWait(driver, interval).until( EC.presence_of_element_located((By.CSS_SELECTOR, css_selector)))
            detection_count += 1
            print(f"检测到目标元素，当前计数：{detection_count}")
            if detection_count == detection_times:
                send_wechat_notification("@善良的人兒呀，我餓了，但是餅乾過期了~")
                return True
        except Exception:
            print("目标元素未检测到，计数重置。")
            detection_count = 0 
        attempts += 1
        print(f"检测尝试次数：{attempts}/{max_attempts}")
    print("未连续检测到目标元素，达到最大尝试次数。")
    return False


element_css_selector = "#passpage-container > main > div > div > div.pass-PageLayout-content.css-awqki6.eu4oa1w0 > div > div.dd-privacy-allow.css-hxk5yu.eu4oa1w0"
is_element_found = monitor_element(element_css_selector)
if is_element_found:
    print("目标元素检测完成，已提醒用户更新 Cookies。")
else:
    print("目标元素未连续检测到，继续执行后续程序。")
print("执行后续操作...")




try:
    WebDriverWait(driver, 15).until( EC.presence_of_element_located((By.CSS_SELECTOR, "table tbody")))
    candidate_element = driver.find_element(By.CSS_SELECTOR, "#app-root > div.css-1gorjcl.e37uo190 > div.css-lamjma.e37uo190 > div.css-13jgm14.e37uo190 > div.css-1yai4xz.eu4oa1w0 > main > div.css-ytbbwf.e37uo190 > div > div > div.css-ciaba6.e37uo190 > div > div > div > div > table > tbody:nth-child(3) > tr > td:nth-child(2) > div > div:nth-child(1) > h2")
    candidate_element.click()
    print("已成功点击第一个候选人！")
except Exception as e:
    print(f"出現錯誤: {e}")
finally:
    # 保持浏览器开着，方便调试
    print("操作完成，保持瀏覽器開啟狀態...")




def get_existing_userids():
    try:
        with pyodbc.connect(connection_string) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT userid FROM dbo.HR_INDEED")
            rows = cursor.fetchall()  
            print(f"查询到的用户ID: {rows}") 
            return set(row[0] for row in rows)  
    except Exception as e:
        print(f"数据库查询失败: {e}")
        return set()



try:
    driver.get("https://employers.indeed.com/candidates?statusName=All&id=0")

    all_ids = set()
    existing_userids = get_existing_userids()

    while True:
        try:
            WebDriverWait(driver, 10).until(   EC.presence_of_element_located((By.CSS_SELECTOR, "input[aria-controls]")) )

            checkbox = driver.find_element(By.CSS_SELECTOR, "input[aria-controls]")
            aria_controls = checkbox.get_attribute("aria-controls")
            current_ids = set(aria_controls.split())

            existing_ids = current_ids & existing_userids
            new_ids = current_ids - existing_userids
            all_ids.update(new_ids)

            print(f"目前页面找到的 ID 数量: {len(current_ids)}")
            print(f"其中已存在的 ID 数量: {len(existing_ids)}")
            print(f"新增的 ID 数量: {len(new_ids)}")

            # "下一页" 
            next_button = driver.find_element(By.CSS_SELECTOR, "#app-root > div.css-1gorjcl.e37uo190 > div.css-lamjma.e37uo190 > div.css-13jgm14.e37uo190 > div.css-1yai4xz.eu4oa1w0 > main > div.css-ytbbwf.e37uo190 > div > div > div.css-ciaba6.e37uo190 > div > div > div > div > table > tfoot > tr > td > nav > ul > li:nth-child(2) > button")

            # 最后一页?
            if next_button.get_attribute("aria-disabled") == "true":
                print("已到最后一页，无法继续点击。")
                break

            next_button.click()
            print("已点击下一页，等待加载...")
            time.sleep(5)

        except Exception as e:
            print(f"出现错误: {e}")
            break
    print(f"总共找到 {len(all_ids)} 个新增 ID:")
    print(all_ids)
except Exception as e:
    print(f"程序出现错误: {e}")



import time
import base64
import pandas as pd
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

default_download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
task_start_time = time.strftime("%Y%m%d_%H")
base_download_dir = os.path.join(base_download_dir, task_start_time)
if not os.path.exists(base_download_dir):
    os.makedirs(base_download_dir)
    print(f"Created new folder: {base_download_dir}")
else:
    print(f"Folder already exists: {base_download_dir}")



def download_blob(driver, download_url, output_path):
    """通过 blob URL 下载文件并保存"""
    try:
        response = driver.execute_script("""
        return fetch(arguments[0])
            .then(res => res.blob())
            .then(blob => {
                return new Promise(resolve => {
                    const reader = new FileReader();
                    reader.onloadend = () => resolve(reader.result.split(',')[1]);
                    reader.readAsDataURL(blob);
                });
            });
        """, download_url)
        file_data = base64.b64decode(response)
        with open(output_path, "wb") as f:
            f.write(file_data)

        print(f"文件已保存到: {output_path}")
    except Exception as e:
        print(f"无法下载 Blob 文件: {e}")

def download_resume(driver, url, download_dir):
    """訪問候選人頁面並提取簡歷信息與下載鏈接。"""
    temp_data = {
        'userid': url.split('=')[-1],
        'name': None,
        'city': None,
        'job': None,
        'fileName': None,
        'download_URL': None,
        "time": task_start_time,
    }

    try:
        driver.get(url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-testid='download-resume-inline']"))
        )
        print("頁面加載完成，開始提取信息...")

        temp_data['name'] = driver.find_element(By.CSS_SELECTOR, "[data-testid='namePlate-candidateName']").text.strip()
        temp_data['job'] = driver.find_element(By.CSS_SELECTOR, "#candidateProfileContainer > div.css-vpxps5.eu4oa1w0 > div.css-a0je4k.e37uo190 > div.css-u74ql7.eu4oa1w0").text.replace("已申請 ", "").strip()
        temp_data['city'] = driver.find_element(By.CSS_SELECTOR, "#candidateProfileContainer > div.css-vpxps5.eu4oa1w0 > div.css-a0je4k.e37uo190 > div.css-1yid6im.e37uo190 > div.css-1afmp4o.e37uo190").text.strip()

        try:
            download_button = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[data-testid='download-resume-inline']"))
            )
            temp_data['download_URL'] = download_button.get_attribute("href")
            print(f"下載 URL: {temp_data['download_URL']}")
        except Exception as e:
            print(f"無法獲取下載 URL: {e}")
            temp_data['status'] = 'Failed to Retrieve URL'
            return temp_data

        if temp_data['download_URL']:
            file_name = f"[{temp_data['name']}]_{temp_data['job'].replace(' ', '_')}.pdf"
            output_path = os.path.join(download_dir, file_name)
            download_blob(driver, temp_data['download_URL'], output_path)
            time.sleep(1)
            temp_data['fileName'] = file_name
            temp_data['status'] = 'Success'

    except Exception as e:
        print(f"處理候選人頁面時出現錯誤: {e}")
        temp_data['status'] = 'Failed'

    return temp_data

# 主流程
try:
    columns = ['userid', 'name', 'city', 'job', 'fileName', 'download_URL', 'status', 'time']
    data_df = pd.DataFrame(columns=columns)

    for candidate_id in all_ids:
        candidate_url = f"https://employers.indeed.com/candidates/view?id={candidate_id}"
        temp_data = download_resume(driver, candidate_url, base_download_dir)
        temp_df = pd.DataFrame([temp_data])
        data_df = pd.concat([data_df, temp_df], ignore_index=True)
        print(f"處理完成: {temp_data}")

    data_df = data_df[data_df['name'].notna()]
    print(f"清洗後的數據: \n{data_df}")
    success_count = (data_df['status'] == 'Success').sum()
    failed_count = (data_df['status'] == 'Failed').sum()
    print(f"成功數: {success_count}, 失敗數: {failed_count}")

except Exception as e:
    print(f"程序出現錯誤: {e}")


#data_df = data_df[~data_df['name'].isnull() & (data_df['name'] != '') & ~(data_df['fileName'].isnull()) & (data_df['fileName'] != '')]
data_df = data_df[~data_df['name'].isnull() & (data_df['name'] != '')]
df = data_df[['name', 'job', 'city', 'fileName']]
excel_path = os.path.join(base_download_dir, f"{task_start_time}_data.xlsx")
df.to_excel(excel_path, index=False)
print(f"Excel file saved to: {excel_path}")


try:
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    select_query = "SELECT userid FROM dbo.HR_INDEED"
    cursor.execute(select_query)
    existing_ids = {row[0] for row in cursor.fetchall()}  

    append_df = data_df[~data_df['userid'].isin(existing_ids)]

    if not append_df.empty:
        insert_query = """
        INSERT INTO dbo.HR_INDEED (
            userid, name, city, job, fileName, download_URL, status, time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        for _, row in append_df.iterrows():
            cursor.execute(insert_query, (
                row['userid'],
                row['name'],
                row['city'],
                row['job'],
                row['fileName'],
                row['download_URL'],
                row['status'],
                row['time']
            ))

        connection.commit()
        print(f"{len(append_df)} records appended to HR_INDEED.")
    else:
        print("No new records to append.")

except Exception as e:
    print("Error:", e)

finally:
    connection.close()



import json
upload_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key=a73731d9-b479-445c-85a4-4964a47cca78&type=file"
file_path = excel_path
try:
    with open(file_path, "rb") as file:
        files = {"media": file}
        response = requests.post(upload_url, files=files)
        response_data = response.json()

        if response_data["errcode"] == 0:
            media_id = response_data["media_id"]
            print(f"File uploaded successfully. Media ID: {media_id}")
        else:
            print(f"Failed to upload file: {response_data}")
            exit()
except Exception as e:
    print(f"Error reading file: {e}")
    exit()

send_file_payload = { "msgtype": "file","file": {"media_id": media_id},}

try:
    response = requests.post(webhook_url, data=json.dumps(send_file_payload))
    response_data = response.json()
    if response_data["errcode"] == 0:
        print("File sent successfully to the group.")
    else:
        print(f"Failed to send file: {response_data}")
except Exception as e:
    print(f"Error sending file: {e}")





us_target_dir = r"Z:\08_人資部\A+人資課共用\E.海外\B. 任用\★加盟專案\02.客戶名單\人力平台來源\indeed\美國"
canada_target_dir = r"Z:\08_人資部\A+人資課共用\E.海外\B. 任用\★加盟專案\02.客戶名單\人力平台來源\indeed\加拿大"

os.makedirs(us_target_dir, exist_ok=True)
os.makedirs(canada_target_dir, exist_ok=True)

for file_name in os.listdir(base_download_dir):
    file_path = os.path.join(base_download_dir, file_name)

    if os.path.isfile(file_path):
        if "US" in file_name:
            shutil.move(file_path, os.path.join(us_target_dir, file_name))
            print(f"移動文件 {file_name} 到 美國目錄")
        elif "Canada" in file_name:
            shutil.move(file_path, os.path.join(canada_target_dir, file_name))
            print(f"移動文件 {file_name} 到 加拿大目錄")
        else:
            print(f"保留文件 {file_name} 在原始目錄")