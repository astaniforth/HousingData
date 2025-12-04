#!/usr/bin/env python3
"""
Add address fallback cell to notebook after cell 13
"""

import json
import shutil
from datetime import datetime

notebook_path = "/Users/andrewstaniforth/Documents/Programming/HousingData/run_workflow.ipynb"

# Backup the notebook
backup_path = notebook_path + f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
shutil.copy2(notebook_path, backup_path)
print(f"Created backup: {backup_path}")

# Read the notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

print(f"Current notebook has {len(notebook['cells'])} cells")

# Find cell 13 (execution_count = 13)
cell_13_index = None
for i, cell in enumerate(notebook['cells']):
    if cell.get('cell_type') == 'code' and cell.get('execution_count') == 13:
        cell_13_index = i
        break

if cell_13_index is None:
    print("ERROR: Could not find cell 13")
    exit(1)

print(f"Found cell 13 at index {cell_13_index}")

# Read the new cell code
with open("/Users/andrewstaniforth/Documents/Programming/HousingData/testing_debugging/address_fallback_cell_code.py", 'r') as f:
    new_cell_code = f.read()

# Create markdown cell for step header
markdown_cell = {
    "cell_type": "markdown",
    "metadata": {},
    "source": [
        "## 🏛️ Step 3C: Address-Based Fallback (Tier 3)\n",
        "\n",
        "For buildings that still don't have DOB data after BIN and BBL queries, try querying by address. This handles:\n",
        "- Lot splits/mergers\n",
        "- BIN/BBL mismatches between HPD and DOB\n",
        "- Address discrepancies (when addresses match exactly)"
    ]
}

# Create code cell
code_cell = {
    "cell_type": "code",
    "execution_count": None,
    "metadata": {},
    "outputs": [],
    "source": new_cell_code.split('\n')
}

# Insert cells after cell 13
insert_position = cell_13_index + 1
notebook['cells'].insert(insert_position, markdown_cell)
notebook['cells'].insert(insert_position + 1, code_cell)

print(f"Inserted 2 new cells at position {insert_position}")
print(f"New notebook has {len(notebook['cells'])} cells")

# Write the modified notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=1)

print(f"✅ Successfully updated {notebook_path}")
print(f"   Added Step 3C: Address-Based Fallback after cell 13")


