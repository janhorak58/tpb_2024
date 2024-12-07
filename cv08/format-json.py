import json

input_file = "/files/articles.json"
output_file = "/files/articles_fixed.json"

with open(input_file, "r") as infile, open(output_file, "w") as outfile:
    data = json.load(infile)  # Načte celý JSON
    for entry in data:
        outfile.write(json.dumps(entry) + "\n")  # Zapíše každý objekt na nový řádek