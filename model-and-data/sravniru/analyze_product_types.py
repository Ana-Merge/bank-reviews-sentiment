import json
from collections import defaultdict
import uuid

# Функция для генерации UUID
def generate_uuid():
    return str(uuid.uuid4())

# Функция для чтения JSON и создания дерева тегов и продуктов
def build_tag_product_tree(json_file_path, output_file_path):
    # Чтение JSON файла
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    
    # Создаем словарь для хранения дерева: {reviewTag: set(specificProductName)}
    tag_product_tree = defaultdict(set)
    
    # Извлекаем данные из JSON
    for item in data['items']:
        review_tag = item.get('reviewTag', 'Unknown')
        # Преобразуем None в строку "None" для корректной обработки
        product_name = item.get('specificProductName', 'None') or 'None'
        tag_product_tree[review_tag].add(product_name)
    
    # Формируем markdown-вывод
    markdown_output = "# Дерево тегов и продуктов\n\n"
    for tag, products in sorted(tag_product_tree.items()):
        markdown_output += f"- **{tag}**\n"
        # Сортируем продукты, заменяя None на строку "None" для корректной сортировки
        sorted_products = sorted(products, key=lambda x: x if x != 'None' else '')  # "None" будет в начале
        for product in sorted_products:
            markdown_output += f"  - {product}\n"
        markdown_output += "\n"
    
    # Сохраняем результат в файл
    with open(output_file_path, 'w', encoding='utf-8') as file:
        file.write(markdown_output)
    
    print(f"Дерево успешно сохранено в {output_file_path}")

# Пример использования
if __name__ == "__main__":
    json_file_path = "reviews/reviews_Газпромбанк.json"  # Путь к входному JSON файлу
    output_file_path = "tag_product_tree.md"    # Путь к выходному markdown файлу
    build_tag_product_tree(json_file_path, output_file_path)