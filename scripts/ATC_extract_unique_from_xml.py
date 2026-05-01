import os
import xml.etree.ElementTree as ET

def clean_tag(elem):
    return elem.tag.split("}")[-1].strip()

# --- Path ---
base_dir = os.path.dirname(__file__)
output_dir = os.path.join(base_dir, "..", "..", "output")
xml_file = os.path.join(output_dir, "latestHumanlist.xml")

print("XML path:", xml_file)

# --- Parse XML ---
tree = ET.parse(xml_file)
root = tree.getroot()

# --- Collect ATC codes ---
atc_set = set()

for elem in root.iter():
    if clean_tag(elem) == "ATC" and elem.text:
        atc_set.add(elem.text.strip())

# --- Results ---
print("\nTotal unique ATC codes in XML:", len(atc_set))

# Show sample
print("\nFirst 50 ATC codes:")
for i, code in enumerate(sorted(atc_set)):
    print(code)
    if i >= 49:
        break

# --- Save to CSV ---
output_file = os.path.join(output_dir, "XML_unique_ATC_codes.csv")

with open(output_file, "w") as f:
    f.write("ATC_Code\n")
    for code in sorted(atc_set):
        f.write(f"{code}\n")

print("\nSaved:", output_file)
