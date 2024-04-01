import json


file_path = 'cats.data'
with open(file_path, 'r', encoding='utf-8') as f:
    test_data = f.read()

data = json.loads(test_data)
for i in data["data"]["product_categories"]:
    if i["id"] != "":
        print(i["id"])
