# Smart_Contact_Manager
Manage contacts
import yaml
import glob
import os
import pandas as pd
from lxml import etree
import pyodbc
from datetime import datetime, date

# Load configuration from a YAML file
def load_config(path='config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

# Clean and parse XML
def clean_and_parse(xml_file):
    with open(xml_file, 'rb') as f:
        content = f.read()
    start = content.find(b'<?xml')
    cleaned = content[start:] if start != -1 else content
    return etree.fromstring(cleaned)

# Extract common fields from XML
def extract_common_fields(tree, common_paths, ns):
    def get(path):
        el = tree.find(path, namespaces=ns)
        return el.text.strip() if el is not None and el.text else 'NULL'
    return {k: get(v) for k, v in common_paths.items()}

# Extract main data from XML
def extract_data(tree, xpath_expr, ns, common_fields):
    elements = tree.xpath(xpath_expr, namespaces=ns)
    if not elements:
        print("[WARNING] No matching elements found.")
        return None

    data = []
    for elem in elements:
        row = {}
        for child in elem.iter():
            tag = child.tag.split('}')[-1]
            if child.text and child.text.strip():
                row[tag] = child.text.strip()
        if row:
            data.append(row)

    df = pd.DataFrame(data)
    for k, v in common_fields.items():
        df[k] = v
    return df

# Save extracted data to SQL Server table
def save_to_db(df, table_name, db_config):
    try:
        conn = pyodbc.connect(
            Driver='{SQL Server}',
            Server=db_config['server'],
            Database=db_config['database'],
            Trusted_Connection='yes'
        )
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?)
            BEGIN
                CREATE TABLE [{table_name}] ({','.join(f'[{col}] NVARCHAR(MAX)' for col in df.columns)})
            END
        """, (table_name,))

        # Add missing columns if necessary
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME=?", (table_name,))
        existing_columns = set(row[0] for row in cursor.fetchall())
        for col in df.columns:
            if col not in existing_columns:
                cursor.execute(f"ALTER TABLE [{table_name}] ADD [{col}] NVARCHAR(MAX)")

        # Insert data
        for _, row in df.iterrows():
            placeholders = ','.join(['?'] * len(row))
            column_names = ','.join(f'[{col}]' for col in row.index)
            insert_sql = f"INSERT INTO [{table_name}] ({column_names}) VALUES ({placeholders})"
            cursor.execute(insert_sql, tuple(str(row[col]) if not pd.isna(row[col]) else None for col in row.index))

        conn.commit()
        print(f"[INFO] Inserted {len(df)} rows into {table_name}")
        return len(df), "Success"
    except Exception as e:
        print(f"[ERROR] DB insert failed: {e}")
        return 0, f"Failed: {e}"
    finally:
        if 'conn' in locals():
            conn.close()

# Log stats to FileProcessingStats table
def log_stats(xml_file, table_name, start_time, end_time, count, status, db_config):
    try:
        conn = pyodbc.connect(
            Driver='{SQL Server}',
            Server=db_config['server'],
            Database=db_config['database'],
            Trusted_Connection='yes'
        )
        cursor = conn.cursor()

        # Create stats table if it doesn't exist
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'FileProcessingStats')
            BEGIN
                CREATE TABLE FileProcessingStats (
                    ID INT IDENTITY(1,1) PRIMARY KEY,
                    xml_file NVARCHAR(255),
                    staging_table NVARCHAR(255),
                    system_date DATE,
                    start_time DATETIME,
                    end_time DATETIME,
                    record_count INT,
                    status NVARCHAR(255)
                )
            END
        """)

        # Insert into stats
        insert_sql = """
        INSERT INTO FileProcessingStats 
        (xml_file, staging_table, system_date, start_time, end_time, record_count, status)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(insert_sql, (
            os.path.basename(xml_file),
            table_name,
            date.today(),
            start_time,
            end_time,
            count,
            status
        ))
        conn.commit()
        print(f"[INFO] Stats logged for {xml_file} -> {table_name}")
    except Exception as e:
        print(f"[ERROR] Failed to log stats: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

# Main function
def main():
    config = load_config()
    ns = config['namespaces']
    db_config = config['database']

    user_input_dir = input("Enter the full path to the directory containing XML files: ").strip()
    if not os.path.isdir(user_input_dir):
        print("[ERROR] Invalid directory path provided.")
        return

    xml_files = glob.glob(os.path.join(user_input_dir, "*.xml"))
    if not xml_files:
        print("[INFO] No XML files found in the provided directory.")
        return

    for xml_file in xml_files:
        print(f"\n[INFO] Processing file: {xml_file}")
        try:
            tree = clean_and_parse(xml_file)
        except Exception as e:
            print(f"[ERROR] Could not parse XML file {xml_file}: {e}")
            continue

        for table_cfg in config['tables']:
            table_name = table_cfg['name']
            xpath_expr = table_cfg['xpath']
            common_paths = table_cfg['common_fields']

            start_time = datetime.now()
            try:
                common_values = extract_common_fields(tree, common_paths, ns)
                df = extract_data(tree, xpath_expr, ns, common_values)
                if df is not None:
                    count, status = save_to_db(df, table_name, db_config)
                else:
                    count, status = 0, "No data found"
            except Exception as e:
                count, status = 0, f"Failed: {e}"
            end_time = datetime.now()

            log_stats(xml_file, table_name, start_time, end_time, count, status, db_config)

# Entry point
if __name__ == "__main__":
    main()
