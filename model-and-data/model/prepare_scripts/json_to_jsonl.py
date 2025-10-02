import json
import os
import sys

input_dir = "data\\bankiru_raw\\2024_json"
output_dir = "data\\bankiru_raw\\2024"

os.makedirs(output_dir, exist_ok=True)

for filename in os.listdir(input_dir):
    if filename.endswith('.json'):
        input_path = os.path.join(input_dir, filename)
        output_filename = filename.rsplit('.', 1)[0] + '.jsonl'
        output_path = os.path.join(output_dir, output_filename)
        
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            with open(output_path, 'w', encoding='utf-8') as f:
                for item in data:
                    json.dump(item, f, ensure_ascii=False)
                    f.write('\n')