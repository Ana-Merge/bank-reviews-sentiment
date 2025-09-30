# try_to_surf.py
import time
from random import randint, uniform
import requests

def try_to_surf(context, url, wait_class):
    """
    Попытка загрузить страницу и дождаться появления элемента.
    
    Args:
        context: Контекст браузера
        url: URL для загрузки
        wait_class: CSS класс элемента, которого нужно дождаться
    
    Returns:
        str: HTML содержимое страницы
    
    Raises:
        Exception: Если не удалось загрузить страницу после нескольких попыток
    """
    page = context.new_page()
    counter = 1
    while True:
        try:
            # Переход по URL
            page.goto(url)
            try:
                # Ожидание появления элемента с указанным классом
                page.wait_for_selector('.' + wait_class, timeout=15000) 
            except Exception as e:
                # Создание скриншота при ошибке
                page.screenshot(path="error.png")
                raise Exception(e)

            # Получение HTML содержимого
            html = page.content()
            page.close()
            return html
        except Exception as e:
            error_message = str(e)
            # Обработка SSL ошибок
            if "net::ERR_SSL_PROTOCOL_ERROR" in error_message:
                print(f"Ошибка SSL-протокола: {e}")
                page.screenshot(path='ssl_error.png')
                continue
            else:
                print(url)
                page.screenshot(path="error.png")
                print(f"Ошибка: {e}. Сайт перенаправляет или селектор отсутствует.")
            
            # Экспоненциальная задержка между попытками
            time_sleep = uniform(1, 5) * counter
            print(f"Ожидание {round(time_sleep)} секунд перед повторной попыткой...")
            time.sleep(time_sleep)
            counter += 1