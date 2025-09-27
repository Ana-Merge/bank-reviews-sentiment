import time
import json
import gc
import psutil  # Для мониторинга памяти, нужно добавить в requirements.txt
from random import uniform
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from try_to_surf import try_to_surf
import os

BASE_URL = 'https://www.banki.ru/services/responses/list/product/'
PARAMETR = '?type=all'
LINKS = [
    # 'hypothec'
    # 'debitcards',
    # 'autocredits', 'credits', 'restructing', # Арам
    # 'deposits', 'transfers', 'remote', 'mobile_app', # Арсен
    'other'
    # 'individual', 'rko', 'acquiring', 'salary_project', 'businessdeposits',
    # 'businesscredits', 'bank_guarantee', 'leasing', 'business_other',
    # 'business_remote', 'business_mobile_app', 
    # 'legal'
]

def get_data_good(context, link, page_num):
    url = f'{BASE_URL}{link}/{PARAMETR}&page={page_num}'
    html_content = try_to_surf(context, url, 'Panel__sc-1g68tnu-1')
    soup = BeautifulSoup(html_content, 'html.parser')

    res = {'link': url}
    try:
        main_div = soup.find('div', class_='Responsesstyled__StyledList-sc-150koqm-5')
        print(url)
        all_resp = main_div.find_all('div', {'data-test': 'responses__response'})
        if not all_resp:
            return {}
        
        for idx, x in enumerate(all_resp):
            res[str(idx)] = {}
            res[str(idx)]['bank_name'] = x.get('data-test-bank', '')
            
            title_link = x.find('h3', class_='TextResponsive__sc-hroye5-0')
            res[str(idx)]['review_theme'] = title_link.get_text(strip=True) if title_link else ''

            grade_div = x.find('div', class_='Grade__sc-m0t12o-0')
            res[str(idx)]['rating'] = grade_div.get_text(strip=True) if grade_div else 'Без оценки'

            status_span = x.find('span', class_='GradeAndStatusstyled__StyledText-sc-11h7ddv-0')
            res[str(idx)]['verification_status'] = status_span.get_text(strip=True) if status_span else ''

            review_text_div = x.find('div', class_='Responsesstyled__StyledItemText-sc-150koqm-3')
            if review_text_div:
                review_link = review_text_div.find('a')
                res[str(idx)]['review_text'] = review_link.get_text(strip=True) if review_link else ''
            else:
                res[str(idx)]['review_text'] = ''

            date_span = x.find('span', class_='Responsesstyled__StyledItemSmallText-sc-150koqm-4')
            res[str(idx)]['review_date'] = date_span.get_text(strip=True) if date_span else ''

        return res
    finally:
        soup.decompose()
        del soup
        gc.collect()

def restart_context(p):
    global context, page
    if 'context' in globals():
        context.close()
    context = p.chromium.launch(headless=True).new_context()
    page = context.new_page()

def main():
    output_dir = "jsons"
    os.makedirs(output_dir, exist_ok=True)
    
    with sync_playwright() as p:
        global context, page
        restart_context(p)

        MEMORY_THRESHOLD = 4 * 1024 * 1024 * 1024  # 4 ГБ в байтах
        PAGE_RESET_THRESHOLD = 6000  # Перезапуск контекста каждые 1000 страниц

        BATCH_SIZE = 200

        for category_name in LINKS:
            print(f'Обрабатывается категория: {category_name}')
            output_file = f'jsons/{category_name}.jsonl'

            if os.path.exists(output_file):
                os.remove(output_file)

            batch = []
            pages_processed = 0
            start_page = 1
            if category_name == 'other':
                start_page = 1201
            for page_num in range(start_page, 100000):
                good_data = None
                for attempt in range(15):
                    try:
                        good_data = get_data_good(context, category_name, page_num)
                        if good_data:
                            break
                    except KeyboardInterrupt:
                        print("Операция прервана пользователем.")
                        context.close()
                        # browser.close()
                        return
                    except Exception as e:
                        print(f"Попытка {attempt + 1} не удалась для страницы {page_num}: {e}")
                        continue

                if not good_data:
                    print(f"Нет данных для страницы {page_num}, завершаем категорию {category_name}")
                    break

                batch.append(good_data)
                pages_processed += 1

                # Проверка потребления памяти и перезапуск контекста
                process = psutil.Process(os.getpid())
                memory_usage = process.memory_info().rss
                if memory_usage > MEMORY_THRESHOLD or pages_processed >= PAGE_RESET_THRESHOLD:
                    print(f"Потребление памяти: {memory_usage / 1024 / 1024:.2f} MB, перезапуск контекста")
                    restart_context(p)
                    pages_processed = 0
                    gc.collect()

                if len(batch) >= BATCH_SIZE or page_num == 99999:
                    with open(output_file, 'a', encoding='utf-8') as f:
                        for data in batch:
                            json.dump(data, f, ensure_ascii=False)
                            f.write('\n')
                    print(f"Выгружено {len(batch)} страниц в {output_file}")
                    batch = []  # Очищаем буфер
                    gc.collect()

            # Выгружаем оставшиеся данные
            if batch:
                with open(output_file, 'a', encoding='utf-8') as f:
                    for data in batch:
                        json.dump(data, f, ensure_ascii=False)
                        f.write('\n')
                print(f"Выгружены оставшиеся страницы в {output_file}")

            # Очистка памяти после завершения категории
            batch = []
            del batch
            gc.collect()
            print(f"Завершена обработка категории {category_name}, память очищена")

        context.close()
        # browser.close()

if __name__ == '__main__':
    main()