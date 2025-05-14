import pandas as pd

# Load your CSV file
df = pd.read_csv("data.csv", encoding='ISO-8859-1')

# Example: Add a fixed cost per product
# Let's assume a cost of 70% of the unit price

df['UnitPrice'] = pd.to_numeric(df['UnitPrice'], errors='coerce')
df['cost'] = (df['UnitPrice'] * 0.7).round(2)

# Save back to CSV
df.to_csv("updated_file.csv", index=False)

print("Cost and profit columns added.")
