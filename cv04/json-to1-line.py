import json

input_file = './articles.json'  # replace with your input file name
output_file = 'articles-1-line.json'  # replace with desired output file name
# Otevřete soubor s explicitním nastavením kódování
with open(input_file, 'r', encoding='utf-8') as infile:
    data = json.load(infile)

# Uložte soubor jako jeden řádek
with open(output_file, 'w', encoding='utf-8') as outfile:
    json.dump(data, outfile, separators=(',', ':'), ensure_ascii=False)  # ensure_ascii=False zachovává české znaky