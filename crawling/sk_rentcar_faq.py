import time
import pandas as pd
import os

from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

def get_FAQ_data(driver):
    page_links = driver.find_elements(By.CLASS_NAME, "page_link")
    page_len = len(page_links) # 총 FAQ 페이지 수
    
    faq_data = []
    for i in range(page_len):
        time.sleep(2)
        
        # 한 페이지에 있는 질문 리스트 추출
        branch_search = driver.find_element(By.CLASS_NAME, 'branch_serch')
        branch_list = branch_search.find_elements(By.CLASS_NAME, 'branch_wrap')

        print(f"----------- {i+1}번째 데이터 길이: ", len(branch_list), "-----------")

        for j in range(len(branch_list)):
            # 질문 및 답변 추출
            try:
                btn = driver.find_element(By.XPATH, f'//*[@id="root"]/div/div/div[1]/div/div[1]/div[3]/div[{j+1}]/a')
                time.sleep(2) 
                btn.click()

                time.sleep(3)
                
                question = driver.find_element(By.CSS_SELECTOR, '#root > div > div > div.container > div > div.contents > div.board_detail > div.board_title > p').text
                answer = driver.find_element(By.CSS_SELECTOR, '#root > div > div > div.container > div > div.contents > div.board_detail > div.board_contents.mt20 > p').text

                # 데이터 저장
                faq_data.append([question, answer])

                # 목록으로 돌아가기
                back_btn = driver.find_element(By.CSS_SELECTOR, '#root > div > div > div.container > div > div.contents > div.btn_group.single > a')
                back_btn.click()
                time.sleep(3)

            except Exception as e:
                    print(f"Error on page {i+1}, question {j+1}: {e}")
                    break
        
        print(f"----------- {i+1} 번째 페이지 크롤링 종료 -----------")
        time.sleep(2)

        if i < page_len - 1:
            next_page_btn = driver.find_element(By.CSS_SELECTOR, f'#root > div > div > div.container > div > div.contents > div.branch_serch > nav > ol > li:nth-child({i+2})')
            next_page_btn.click()
            time.sleep(3)  # 다음 페이지 로드 대기
        
        # 스크롤을 맨 위로 이동
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(2)  # 스크롤 후 대기
            
    print(f"----------- FAQ 크롤링 종료 -----------")
    return faq_data
    
def save_data(df):
    result_path = os.path.join(os.getcwd(), 'result')
    os.makedirs(result_path, exist_ok=True)
    df.to_csv(os.path.join(result_path, 'faq_df.csv'), index=False, encoding='cp949')
    
def get_faq():
    # 크롬 드라이버 생성 및 url 접속
    url = "https://homepage.skcarrental.com/customer/faq"
    driver = Chrome()
    driver.get(url)
    time.sleep(3)
    
    # 웹 크롤링 시작
    result = get_FAQ_data(driver)
    faq_df = pd.DataFrame(result, columns=['Question', 'Answer'])
    save_data(faq_df)
    print(" >> 데이터 저장 완료")
