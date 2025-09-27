import time
from random import randint, uniform
import requests

def try_to_surf(context, url, wait_class):
    page = context.new_page()
    counter = 1
    while True:
        try:
            response = page.goto(url)
            current_url = page.url
            if current_url != url:
                print(f"Страница перенаправлена с {url} на {current_url}")
                page.close()
                return {}  # Возвращаем пустой словарь при редиректе
            try:
                page.wait_for_selector('.' + wait_class, timeout=15000) 
            except Exception as e:
                page.screenshot(path="error.png")
                raise Exception(e)
            
            html = page.content()
            page.close()
            return html
        except Exception as e:
            error_message = str(e)
            if "net::ERR_SSL_PROTOCOL_ERROR" in error_message:
                print(f"Ошибка SSL-протокола: {e}")
                page.screenshot(path='ssl_error.png')
                continue
            else:
                print(url)
                page.screenshot(path="error.png")
                print(f"Ошибка: {e}. Сайт перенаправляет или селектор отсутствует.")
            # time_sleep = uniform(600, 800) * counter
            time_sleep = uniform(1, 5) * counter
            print(f"Ожидание {round(time_sleep)} секунд перед повторной попыткой...")
            time.sleep(time_sleep)
            counter += 1