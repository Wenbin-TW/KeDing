from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


base_download_dir = r"Z:\08_人資部\A+人資課共用\E.海外\B. 任用\★加盟專案\02.客戶名單\人力平台來源\印尼"

chromedriver_path = r"C:\Users\TW0002\Downloads\chromedriver-win64\chromedriver.exe"

options = webdriver.ChromeOptions()
options.add_argument("user-data-dir=C:\\Temp\\ChromeUserData")
options.add_argument("--start-maximized")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--remote-debugging-port=9222")
options.add_argument("--disable-gpu")
options.add_argument("--disable-software-rasterizer")
options.add_argument("--disable-extensions")

service = Service(chromedriver_path)
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

try:
    driver.get("https://id.employer.seek.com/id/dashboard/")
    driver.maximize_window()

    try:
        sign_in_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Sign in"))
        )
        print("Sign in 按钮找到并可点击。")
        sign_in_button.click()
    except Exception as e:
        print("无法找到或点击 Sign in 按钮:", e)

    try:
        email_field = WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "emailAddress"))
        )
        print("Email 输入框找到。")
    except Exception as e:
        print("无法找到 Email 输入框:", e)

    try:
        password_field = driver.find_element(By.ID, "password")
        print("Password 输入框找到。")
    except Exception as e:
        print("无法找到 Password 输入框:", e)

    try:
        email_field.send_keys("kdhr.global@twkd.com")
        password_field.send_keys("Kd80214519!")
        print("登录信息已输入。")
    except Exception as e:
        print("无法输入登录信息:", e)

    try:
        login_button = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[data-cy='login']"))
        )
        print("登录按钮找到并可点击。")
        login_button.click()
    except Exception as e:
        print("无法找到或点击登录按钮:", e)
    try:
        selectable_link = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#\\36 1097208-selectable-link"))
        )
        print("#\\36 1097208-selectable-link 找到并可点击。")
        selectable_link.click()
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.loaded-content"))  # 替换为加载完成后的标识符
        )
        print("页面加载完成。")
    except Exception as e:
        print("无法找到或点击 #\\36 1097208-selectable-link:", e)
except Exception as e:
    print("程序运行中出现错误:", e)
finally:
    print("检查完成。")

import os
import time
import shutil
import pandas as pd
import logging
import pyodbc
import requests
import json


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
def get_page_number_from_url(url: str) -> int:
    """ 嘗試從 URL 中解析出 p=xxx 的值，若不存在或解析失敗，返回 1。 """
    if "p=" not in url:
        return 1
    try:
        page_part = url.split("p=")[1].split("&")[0]
        return int(page_part)
    except (IndexError, ValueError):
        return 1

jobid_list = [81160600]
stype_list = ['Rejected', 'Accept', 'Offer', 'Interview', 'Shortlist', 'Prescreen', '']
max_page = 200
data = []  # 用於存儲最終結果

for jobid in jobid_list:
    for stype in stype_list:
        for page in range(1, max_page + 1):
            if stype == '':
                url = f"https://id.employer.seek.com/id/candidates?jobid={jobid}&p={page}"
            else:
                url = f"https://id.employer.seek.com/id/candidates?jobid={jobid}&p={page}&s={stype}"
            driver.get(url)

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[id^='avatar-']"))
                )
            except TimeoutException:
                print(f"[Timeout] jobid={jobid}, stype={stype}, page={page}. Break.")
                break

            current_url = driver.current_url
            current_page = get_page_number_from_url(current_url)
            if current_page < page:
                print(f"[Break] jobid={jobid}, stype={stype}, 要抓第 {page} 頁, 但網站實際跳到第 {current_page} 頁")
                break
            id_elements = driver.find_elements(By.CSS_SELECTOR, "div[id^='avatar-']")
            candidate_ids = [
                element.get_attribute("id").replace("avatar-", "") for element in id_elements
            ]
            if not candidate_ids:
                print(f"[No candidates] jobid={jobid}, stype={stype}, page={page}. Break.")
                break
            for cid in candidate_ids:
                data.append({
                    "jobid": jobid,
                    "stype": stype,
                    "page": page,
                    "candidate_id": cid
                })

            print(f"Processed jobid: {jobid}, stype: {stype}, page: {page}, found candidates: {len(candidate_ids)}")
df = pd.DataFrame(data)
df['unique_id'] = df.apply(lambda row: f"{row['jobid']}_{row['candidate_id']}", axis=1)
server = '192.168.1.119'
database = 'bidb' 
username = 'kdmis'
password = 'Kd0123456'
connection_string = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)
connection = None
cursor = None

try:
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    cursor.execute("SELECT unique_id FROM dbo.HR_SEEK")
    existing_unique_ids = set(row.unique_id for row in cursor.fetchall())
    new_data = df[~df['unique_id'].isin(existing_unique_ids)]
    print("New data to be inserted:")
    print(new_data)

except Exception as e:
    print("Error:", e)

df = new_data.reset_index(drop = True)
logging.basicConfig(filename='../task.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
default_download_dir = os.path.join(os.path.expanduser("~"), "Downloads")
task_start_time = time.strftime("%Y%m%d_%H")
base_download_dir = os.path.join(base_download_dir, task_start_time)
if not os.path.exists(base_download_dir):
    os.makedirs(base_download_dir)
    print(f"Created new folder: {base_download_dir}")
else:
    print(f"Folder already exists: {base_download_dir}")
chrome_options = webdriver.ChromeOptions()
prefs = {"safebrowsing.enabled": "false"}
chrome_options.add_experimental_option("prefs", prefs)
url_template = "https://id.employer.seek.com/id/candidates?jobid={a}&p={c}&s={b}&selected={d}&tab=resume"
progress_file = os.path.join(base_download_dir, "progress.txt")

if os.path.exists(progress_file):
    with open(progress_file, 'r') as f:
        last_processed_unique_id = f.read().strip()
    logging.info(f"Resuming from unique_id: {last_processed_unique_id}")
else:
    last_processed_unique_id = None
    logging.info("Starting from the beginning.")
data = []
current_jobid = None
jobid_folder = None
failed_urls = []
success_count = 0
fail_count = 0
total_count = len(df)
for index, row_ in df.iterrows():
    unique_id = f"{row_['jobid']}_{row_['candidate_id']}"
    if last_processed_unique_id and unique_id <= last_processed_unique_id:
        continue
    attempt_times = 1
    candidate_data = None

    for attempt_i in range(attempt_times):
        temp_data = {
            "unique_id": unique_id,
            "jobid": row_['jobid'],
            "stype": row_['stype'],
            "page": row_['page'],
            "candidate_id": row_['candidate_id'],
            "status": "Pending",
            "name": None,
            "email": None,
            "phone": None,
            "city": None,
            "date": None,
            "downloaded_file": None,
            "time": task_start_time,
        }

        try:
            if row_['jobid'] != current_jobid:
                current_jobid = row_['jobid']
                jobid_folder = os.path.join(base_download_dir, f"JobID_{current_jobid}")
                os.makedirs(jobid_folder, exist_ok=True)
            url = url_template.format(
                a=row_['jobid'],
                b=row_['stype'],
                c=row_['page'],
                d=row_['candidate_id']
            )

            driver.get(url)
            WebDriverWait(driver, 40).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#details-view-drawer"))
            )
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            try:
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#tab-select-detail-view_1_label > div"))
                )
                print("Details tab loaded. Proceeding to extract data...")
            except TimeoutException:
                print("Timeout waiting for details tab to load.")
                temp_data["status"] = "Failed"
                continue  # 跳過這個候選人
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(3)  # 全局延迟以等待加载
            try:
                name_element = WebDriverWait(driver, 20).until(
                    EC.visibility_of_element_located((
                        By.CSS_SELECTOR,
                        "#details-view-drawer > div > div._5r6dm10._1m093377j._1m0933784._1m093378n._1m0933798._1m09337b7._1m09337aw._1m09337a3._1m093379s._1m093373._1m093375f._1m09337p._1m09337n._12dphxx9._17ld4pa18._17ld4pa1b._1m0933733._1m0933736 > div > div._5r6dm10.qqfmcw0 > div._5r6dm10._1m093375b._1m09337hf._1m09337i3 > div > div._5r6dm10._1m093374z._1m09337r._1m09337p._1m09337i3._1m09337b7 > div > div > div:nth-child(1) > div > div._5r6dm10._1m093375b._1m09337h7._1m09337gr._13s4i862j > div._5r6dm10._1m093374z._1m09337r._1m09337p._1m09337i3._1m09337bv > div > div:nth-child(1) > h2"
                    ))
                )
                temp_data["name"] = name_element.text.strip() if name_element.text else None
                print(f"Name: {temp_data['name']}")
            except TimeoutException:
                print("Name did not load in time.")
                temp_data["name"] = None
            except Exception as e:
                print(f"Error loading name element: {e}")
                temp_data["name"] = None



            try:
                email_element = driver.find_element(By.CSS_SELECTOR, "#details-view-drawer a[href^='mailto:']")
                temp_data["email"] = email_element.text
            except NoSuchElementException:
                pass

            try:
                phone_element = driver.find_element(By.CSS_SELECTOR, "#details-view-drawer a[href^='tel:']")
                temp_data["phone"] = phone_element.text
            except NoSuchElementException:
                pass

            try:
                city_element = driver.find_element(
                    By.CSS_SELECTOR,
                                                       "#details-view-drawer > div > div._5r6dm10._1m093377j._1m0933784._1m093378n._1m0933798._1m09337b7._1m09337aw._1m09337a3._1m093379s._1m093373._1m093375f._1m09337p._1m09337n._12dphxx9._17ld4pa18._17ld4pa1b._1m0933733._1m0933736 > div > div._5r6dm10.qqfmcw0 > div._5r6dm10._1m093375b._1m09337hf._1m09337i3 > div > div._5r6dm10._1m093374z._1m09337r._1m09337p._1m09337i3._1m09337b7 > div > div > div:nth-child(1) > div > div._5r6dm10._1m093375b._1m09337h7._1m09337gr._13s4i862j > div._5r6dm10._1m093374z._1m09337r._1m09337p._1m09337i3._1m09337bv > div > div:nth-child(3) > span > span > span:nth-child(2)")

                temp_data["city"] = city_element.text
            except NoSuchElementException:
                pass

            try:
                date_element = driver.find_element(By.XPATH, "//div[@id='details-view-drawer']//span[@aria-describedby]")
                temp_data["date"] = date_element.get_attribute("aria-describedby")
                print(f"Extracted aria-describedby: {temp_data['date']}")
            except NoSuchElementException:
                logging.error(f"Error processing candidate: {e}")
                temp_data["date"] = None
                print("No element with aria-describedby found.")
            found_download_button = False
            for attempt_btn in range(2):  
                try:
                    download_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "#download-document-viewer"))
                    )
                    found_download_button = True
                    break
                except TimeoutException:
                    if attempt_btn < 1:
                        driver.refresh()

            if found_download_button:
                download_button.click()
                time.sleep(3)

                downloaded_file_path = None
                start_t = time.time()
                while True:
                    files = os.listdir(default_download_dir)
                    files = [f for f in files if f.endswith(('.pdf', '.docx', '.doc', '.txt'))]
                    if files:
                        downloaded_file_path = max(
                            [os.path.join(default_download_dir, f) for f in files],
                            key=os.path.getmtime
                        )
                        break

                    if time.time() - start_t > 30:
                        break
                    time.sleep(3)

                if downloaded_file_path:
                    try:
                        ext = os.path.splitext(downloaded_file_path)[1].lower()
                        new_file_name = f"[{temp_data['name']}]_{temp_data['jobid']}_{task_start_time}{ext}"
                        if not temp_data['name'] or temp_data['name'].strip() == "":
                            logging.warning(f"Invalid name for file: {downloaded_file_path}. Deleting and retrying...")
                            os.remove(downloaded_file_path)  
                            continue 
                        new_file_name = new_file_name.replace("Resume", "").replace("resume", "").strip()
                        target_file_path = os.path.join(jobid_folder, new_file_name)

                        shutil.move(downloaded_file_path, target_file_path)
                        temp_data["downloaded_file"] = new_file_name
                    except Exception as move_err:
                        logging.error(f"Error moving file: {move_err}")
                mandatory_fields = ["name", "email", "phone", "city", "date"]
                all_ok = all(temp_data[mf] for mf in mandatory_fields)

                if all_ok:
                    candidate_data = temp_data
                    break 
                else:
                    logging.warning(f"Missing mandatory fields for: {unique_id}. Retrying...")
                    time.sleep(2) 


        except Exception as e:
            logging.error(f"[Error attempt {attempt_i+1}] {unique_id} => {e}")
            time.sleep(2)
    if candidate_data is not None:
        data.append(candidate_data)
        success_count += 1
    else:
        temp_data["downloaded_file"] = ""  
        print(temp_data)
        data.append(temp_data)
        logging.warning(f"Download failed for unique_id: {unique_id}.")
        failed_urls.append(unique_id)
        fail_count += 1
    done_count = index + 1
    remain = total_count - done_count
    print(f"[Progress] total={total_count}, done={done_count}, success={success_count}, fail={fail_count}, remain={remain}")
    with open(progress_file, 'w') as f:
        f.write(unique_id)
result_df = pd.DataFrame(data)

print("\n--- Crawl Finished ---")
print(f"Total: {total_count}, Success: {success_count}, Fail: {fail_count}")
if failed_urls:
    print("Failed unique_ids:", failed_urls)
result_df = result_df[~result_df['name'].isnull() & (result_df['name'] != '')]
df = result_df[['name', 'email', 'phone', 'city', 'date', 'downloaded_file']]

excel_path = os.path.join(base_download_dir, f"{task_start_time}_ID_data.xlsx")
df.to_excel(excel_path, index=False)
print(f"Excel file saved to: {excel_path}")



try:
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()
    select_query = "SELECT unique_id FROM dbo.HR_SEEK"
    cursor.execute(select_query)
    existing_ids = {row[0] for row in cursor.fetchall()} 
    append_df = result_df[~result_df['unique_id'].isin(existing_ids)]

    if not append_df.empty:
        insert_query = """
        INSERT INTO dbo.HR_SEEK (
            unique_id, jobid, stype, page, candidate_id, status, name, email, phone, city, date, downloaded_file, time
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        filtered_append_df = append_df[~append_df['name'].isnull() & (append_df['name'] != '')]

        for _, row in append_df.iterrows():
            cursor.execute(insert_query, (
                row['unique_id'],
                row['jobid'],
                row['stype'],
                row['page'],
                row['candidate_id'],
                row['status'],
                row['name'],
                row['email'],
                row['phone'],
                row['city'],
                row['date'],
                row['downloaded_file'],
                row['time']
            ))

        connection.commit()
        print(f"{len(append_df)} records appended to HR_SEEK.")
    else:
        print("No new records to append.")

except Exception as e:
    print("Error:", e)
webhook_url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=a73731d9-b479-445c-85a4-4964a47cca78"
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
send_file_payload = {
    "msgtype": "file",
    "file": {"media_id": media_id},
}

try:
    response = requests.post(webhook_url, data=json.dumps(send_file_payload))
    response_data = response.json()

    if response_data["errcode"] == 0:
        print("File sent successfully to the group.")
    else:
        print(f"Failed to send file: {response_data}")
except Exception as e:
    print(f"Error sending file: {e}")




try:
    url = f"https://id.employer.seek.com/id/candidates?jobid={jobid}"
    driver.get(url)
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#app"))  # 等待加载
        )
        print("页面刷新完成。")
    except Exception as e:
        print("页面加载超时或未找到主要内容:", e)
    try:
        first_element = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#app > div > div > div > div.pj6h9n0 > header > div > div > div > div._5r6dm10._1m093375b._1m09337hn._1m09337i3 > div._5r6dm10._1m093375b._1m09337gz._10huuc13 > div > div > div > div > div.ivznks0 > div._5r6dm10._1m093375f > label > span"))
        )
        print("第一个元素找到并可点击。")
        first_element.click()
    except Exception as e:
        print("无法找到或点击第一个元素:", e)
    try:
        second_element = WebDriverWait(driver, 15).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#user-menu-content > div > div > nav > div > a:nth-child(10) > span"))
        )
        print("第二个元素找到并可点击。")
        second_element.click()
    except Exception as e:
        print("无法找到或点击第二个元素:", e)

except Exception as e:
    print("程序运行中出现错误:", e)
finally:
    print("检查完成。")
    driver.quit()

