import time
import json
from random import uniform
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from try_to_surf import try_to_surf
import re
import os
from math import floor


BASE_URL = 'https://ru.myfin.by/bank/'

def get_banks(context):
    all_banks = []
    for i in range(1, 19):
        url = f'https://ru.myfin.by/banki?page={i}'

        html_content = try_to_surf(context, url, 'banks-list')
        soup = BeautifulSoup(html_content, 'html.parser')
        banks = soup.find_all('div', class_='bank-info__name')
        for bank in banks:
            link = bank.find('a')
            if link:
                bank_name = link.get_text(strip=True)  # Чистое имя банка
                bank_href = link.get('href')
                
                # Добавляем в общий список словарей
                all_banks.append({bank_name: bank_href})
    return all_banks

def get_data_good(context, bank_url):
    url = f'{bank_url}/otzyvy?limit=10000'
    try:
        html_content = try_to_surf(context, url, 'main-container')
        assert html_content != {}
    except: 
        return {}
    soup = BeautifulSoup(html_content, 'html.parser')

    res = {}
    # try:
    main_div = soup.find('div', class_='main-container')
    print(url)
    all_resp = main_div.find_all('div', class_='reviews-list__item')
    all_scripts = main_div.find_all('script', type='application/ld+json')
    for idx, x in enumerate(all_resp):
        res[str(idx)] = {}
        res[str(idx)]['bank_name'] = x.find('a', class_='review-block__logo').find('img').get('alt')
        
        try:
            title_link = x.find('div', class_='review-block__title').find('a')
            res[str(idx)]['review_theme'] = title_link.get_text(strip=True)
        except:
            title_link = x.find('div', class_='review-block__title')
            res[str(idx)]['review_theme'] = title_link.get_text(strip=True)

        grade_div = x.find('div', class_='star-rating__text')
        res[str(idx)]['rating'] = grade_div.get_text(strip=True)

        review_text_div = all_scripts[idx].get_text()
        if review_text_div:
            try:
                review_data = json.loads(review_text_div)
                raw_text = review_data.get('reviewBody', '')
                clean_text = re.sub(r'\s+', ' ', raw_text).strip()
                res[str(idx)]['review_text'] = clean_text

            except json.JSONDecodeError as e:
                print(f"Ошибка парсинга JSON: {e}")

        date_span = x.find('div', class_='review-info__date')
        res[str(idx)]['review_date'] = date_span.get_text(strip=True) if date_span else ''

        type_div = x.find('div', class_='review-info__product')
        res[str(idx)]['review_type'] = type_div.get_text(strip=True)


    return res
    # except:
        # return {}
    # except KeyboardInterrupt:
    #     return {}

def main():
    output_dir = "jsons"
    os.makedirs(output_dir, exist_ok=True) 
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        page = context.new_page()

        banks = get_banks(context)

        for bank in banks:

            goods_data = []
            
            output_file = f'jsons/{bank[next(iter(bank))].split('/')[-1]}.json'

            # try:
            good_data = get_data_good(context, bank[next(iter(bank))])
            goods_data.append(good_data)
            if not good_data:
                continue
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump([], f, ensure_ascii=False, indent=4)

            with open(output_file, 'r', encoding='utf-8') as f:
                current_data = json.load(f)

            current_data.append(good_data)

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(current_data, f, ensure_ascii=False, indent=4)

                    # Пауза между запросами
                    # time.sleep(uniform(1, 3))

        # except KeyboardInterrupt:
        #     print("Операция прервана пользователем.")
        #     break
        # except Exception as e:
        #     print(f"Произошла ошибка: {e}")
        #     continue

        browser.close()



if __name__ == '__main__':
    main()