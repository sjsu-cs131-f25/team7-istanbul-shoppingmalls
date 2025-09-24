# team7-Shopping Mall Data in Istanbul
Team Members: Hugo Garcia, Ashley Dai, Yi Xiao, Dionne Bui, Ngoc Huy Tran  
Dataset Description: This data was gathered in 2021 to 2023 from 10 different shopping malls in Istanbul. It includes: the type of item, quantity, price, payment method, the shopping mall, invoice date, gender, age, customer id, invoice number.  
# Customer Shopping Dataset (Not Included in Repo)

This project uses the [Customer Shopping Dataset](https://www.kaggle.com/datasets/mehmettahiraslan/customer-shopping-dataset/data), which contains shopping transactions from 10 malls in Istanbul (2021–2023).

## How to Download

1. Install the file from the data from kaggle and put them in data/Shopping Mall Data in Istanbul/raw/






## How to Run the Project

From the repo root, run the main analysis script:

```bash
bash scripts/run_project2.sh "data/Shopping Mall Data in Istanbul/customer_shopping_data.csv"
```
All outputs will be written to the `out/` and `data/samples/` directories.







## Data Card

**Source & Link:**
- Name:Customer Shopping Dataset
- Source:Kaggle
- URL:https://www.kaggle.com/datasets/mehmettahiraslan/customer-shopping-dataset/data
- License/Terms:CC0: Public Domain (https://creativecommons.org/publicdomain/zero/1.0/)

**Files & Formats:**
- Files:customer_shopping_data.csv
- Formats:CSV
- Compression:none
- Approx Size (uncompressed):7.2MB
- Row/Column Counts:99,458

**Structure:**
- Delimiter:,
- Header:present
- Encoding:us-ascii

**Quality Notes:**
- Missing fields:none detected(grep -c ",," = 0)
- Known oddities:Dates use DD/MM/YYYY format (day–month–year), which differs from the MM/DD/YYYY format common in the U.S.
- Duplicates:none in invoice_no

**Access & Snapshots (to be filled in later):**
- Will add in part B
