import pandas as pd
import sys
import os
import time
import logging
from datetime import datetime

def setup_logging(log_dir="logs"):
    """Configures logging to both console (stdout) and a time-stamped file."""
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"file_transform_{timestamp}.log")

    # Basic configuration: logs everything of INFO level and above
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            # 1. File Handler: Writes to the log file
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            # 2. Console Handler: Prints to standard output
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info(f"Logging initiated. All output directed to console and file: {log_file}")
    logging.info("-" * 50)
    
def apply_enrollment_transforms(df: pd.DataFrame):
    """Applies the specific set of transformations for Enrollment data."""
    logging.info("Applying Enrollment-specific transformations...")

    # --- 1. Put text in double quotes for a list of columns ---
    double_quote_cols = [
        'CUSTOMER_NAME', 'CUSTOMER_SERVICE_ADDRESS', 'CUSTOMER_SERVICE_CITY_STATE_ZIP',
        'TX_TAR_SHORT_DESC', 'TX_TAR_SCH_DESC'
    ]
    logging.info(f"Encapsulating text in double quotes for columns: {', '.join(double_quote_cols)}")
    for col in double_quote_cols:
        if col in df.columns:
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].astype(str).apply(lambda x: f'"{x}"')

    # --- 2. Put text in custom quotes for a specific column ---
    triple_quote_col = 'TX_SERV_SUPP'
    # FIX: Corrected f-string syntax by using double curly braces '{{}}' for literal braces and escaping internal quotes.
    logging.info(f"Applying custom encapsulation ('\"\\'{{value}}\\'\"') to column: {triple_quote_col}")
    if triple_quote_col in df.columns:
        mask = df[triple_quote_col].notna()
        df.loc[mask, triple_quote_col] = df.loc[mask, triple_quote_col].astype(str).apply(lambda x: f'"\'{x}\'"')

    # --- 3. Format dates to "YYYY-MM-DD" for a list of columns ---
    date_cols = ['DT_EFF', 'CUST_ENR_START_DATE', 'CUST_EDI_DROP_DATE', 'LAST_UPDATE']
    logging.info(f"Formatting dates to YYYY-MM-DD for columns: {', '.join(date_cols)}")
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    # --- 4. Zero-padding for specific columns ---
    padding_cols = {'CITY_GATE': 4, 'KY_MTR_BILL_GRP': 2, 'CD_SERV_SUPP': 4}
    logging.info(f"Applying zero-padding for columns: {padding_cols}")
    for col, length in padding_cols.items():
        if col in df.columns:
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].astype(str).str.split('.').str[0].str.zfill(length)
    
    # --- 5. Remove comma separators from specific columns ---
    comma_removal_cols = [
        'TOT_ANNUAL_USAGE', 'CUST_PEAK_DAY', 'CUST_BASE_LOAD', 'CUST_THERMAL_RESPONSE'
    ]
    logging.info(f"Removing commas from columns: {', '.join(comma_removal_cols)}")
    for col in comma_removal_cols:
        if col in df.columns:
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].astype(str).str.replace(',', '')
    
    return df

def apply_usage_transforms(df: pd.DataFrame):
    """Applies the specific set of transformations for Usage data."""
    logging.info("Applying Usage-specific transformations...")
    
    # --- 1. Put text in double quotes for a list of columns ---
    double_quote_cols = [
        'CUST_NAME', 'CUST_SERV_ADDR', 'CUST_SERV_CITY_ST_ZIP', 'CUST_POOL_ID'
    ]
    logging.info(f"Encapsulating text in double quotes for columns: {', '.join(double_quote_cols)}")
    for col in double_quote_cols:
        if col in df.columns:
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].astype(str).apply(lambda x: f'"{x}"')

    # --- 2. Format dates to "YYYY-MM-DD" for a list of columns ---
    date_cols = ['DT_LST_BLLD', 'DT_RDG_FROM', 'DT_RDG_TO', 'DT_ENTERED']
    logging.info(f"Formatting dates to YYYY-MM-DD for columns: {', '.join(date_cols)}")
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
    
    # --- 3. Zero-padding for specific columns ---
    logging.info("Applying zero-padding for various columns...")
    
    # KY_MTR_BILL_GRP needs 2 chars
    if 'KY_MTR_BILL_GRP' in df.columns:
        mask = df['KY_MTR_BILL_GRP'].notna()
        df.loc[mask, 'KY_MTR_BILL_GRP'] = df.loc[mask, 'KY_MTR_BILL_GRP'].astype(str).str.split('.').str[0].str.zfill(2)

    # CITY_GATE needs 4 chars
    if 'CITY_GATE' in df.columns:
        mask = df['CITY_GATE'].notna()
        df.loc[mask, 'CITY_GATE'] = df.loc[mask, 'CITY_GATE'].astype(str).str.split('.').str[0].str.zfill(4)

    # CD_BILL_PRCS_INSTR needs 4 chars
    if 'CD_BILL_PRCS_INSTR' in df.columns:
        mask = df['CD_BILL_PRCS_INSTR'].notna()
        df.loc[mask, 'CD_BILL_PRCS_INSTR'] = df.loc[mask, 'CD_BILL_PRCS_INSTR'].astype(str).str.split('.').str[0].str.zfill(4)

    # CD_SERV_SUPP needs 4 chars
    if 'CD_SERV_SUPP' in df.columns:
        mask = df['CD_SERV_SUPP'].notna()
        df.loc[mask, 'CD_SERV_SUPP'] = df.loc[mask, 'CD_SERV_SUPP'].astype(str).str.split('.').str[0].str.zfill(4)

    # --- 4. Remove comma separators from specific columns ---
    comma_removal_cols = ['USAGE', 'QY_BTU_FACTOR']
    logging.info(f"Removing commas from columns: {', '.join(comma_removal_cols)}")
    for col in comma_removal_cols:
        if col in df.columns:
            mask = df[col].notna()
            df.loc[mask, col] = df.loc[mask, col].astype(str).str.replace(',', '')
    
    return df

def transform_excel_data(input_file: str, output_file: str):
    """
    Reads an Excel file and applies either Enrollment or Usage transforms
    based on keywords in the filename, then saves the result as a CSV file.
    """
    file_name_lower = os.path.basename(input_file).lower()
    
    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        logging.info(f"Successfully read file: {input_file}")
    except FileNotFoundError:
        logging.error(f"Error: The input file '{input_file}' was not found.")
        return False
    except Exception as e:
        logging.error(f"An error occurred while reading the file: {e}")
        return False

    # Dispatch to the correct transformation function based on keywords
    try:
        if 'enrollment' in file_name_lower:
            df = apply_enrollment_transforms(df)
        elif 'usage' in file_name_lower:
            df = apply_usage_transforms(df)
        else:
            logging.warning("Filename does not contain 'Enrollment' or 'Usage'. Skipping transformations.")
            return False
    except KeyError as e:
        # Catches common errors where a column expected by the transformation logic is missing.
        logging.error("Transformation Failed: A required column was not found in the Excel file.")
        logging.error(f"Missing column: {e}. Please ensure the input file schema is correct.")
        return False
    except Exception as e:
        # Catches any other unexpected error during the transformation logic execution.
        logging.error(f"Transformation Failed unexpectedly during data processing: {e}")
        return False

    df.to_csv(output_file, index=False)
    logging.info(f"Excel transformation complete! Data saved to intermediate CSV: {output_file}")
    return True

def find_and_replace_quotes(input_file, output_file):
    """
    Reads a file, finds and replaces quotes, then saves to a new file.
    """
    try:
        if not os.path.exists(input_file):
            logging.error(f"Error: The intermediate input file '{input_file}' was not found for quote replacement.")
            return

        logging.info(f"Starting quote cleanup on intermediate file: {input_file}")
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # The core cleanup step: replaces three double quotes (""") with one double quote (")
        modified_content = content.replace('"""', '"')

        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(modified_content)

        logging.info(f"Final find-and-replace process complete! Data saved to final CSV: {output_file}")
    except Exception as e:
        logging.error(f"An error occurred during find and replace: {e}")

if __name__ == '__main__':
    # Setup logging first
    setup_logging()

    # --- Automatic Folder Scan Mode ---
    
    # 1. Define target folder: Navigate to the script's immediate environment and then into "FileTransform"
    try:
        # Get the full absolute path of the directory containing the running script.
        # This resolves to the folder where the script file resides (e.g., ...\TestFileEdit).
        script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
        
        # We assume the 'FileTransform' folder is a CHILD of the script's directory (a sibling of the script if run from root).
        target_dir = os.path.join(script_dir, 'FileTransform')

        # Fallback check: If 'FileTransform' is not found as a child, check one level up (parent).
        if not os.path.exists(target_dir):
            parent_dir = os.path.dirname(script_dir)
            target_dir_parent = os.path.join(parent_dir, 'FileTransform')
            
            if os.path.exists(target_dir_parent):
                target_dir = target_dir_parent


    except IndexError:
        # Fallback for interactive environments or non-standard execution (start from CWD)
        current_dir = os.getcwd()
        # First, try CWD + 'FileTransform' (sibling if CWD is where script is run)
        target_dir = os.path.join(current_dir, 'FileTransform')
        # Second, try CWD + '..' + 'FileTransform' (parent directory)
        if not os.path.exists(target_dir):
             target_dir = os.path.join(current_dir, os.pardir, 'FileTransform')

    logging.info("\n" + "="*50)
    logging.info(f"Starting automatic scan for Excel files in: {target_dir}")
    logging.info("Files containing 'Enrollment' or 'Usage' will be processed.")
    logging.info("="*50)
    
    if not os.path.exists(target_dir):
        logging.error(f"Target directory not found: {target_dir}")
        logging.error("Please ensure the 'FileTransform' folder is located either in the same directory as the script or one level up.")
        sys.exit(1)

    processed_count = 0
    
    for filename in os.listdir(target_dir):
        # 2. Construct the full path
        input_path = os.path.join(target_dir, filename)

        # 3. Skip directories, temporary files, and non-Excel files
        if os.path.isdir(input_path) or filename.startswith('~') or not filename.lower().endswith(('.xlsx', '.xls')):
            logging.debug(f"Skipping non-Excel file or directory: {filename}")
            continue

        logging.info(f"\n--- Processing File: {filename} ---")

        # 4. Determine output file names
        base_name = os.path.splitext(filename)[0]
        intermediate_csv_filename = os.path.join(target_dir, f"{base_name}_intermediate.csv")
        final_output_filename = os.path.join(target_dir, f"{base_name}_final.csv")

        # 5. Determine file type for logging
        file_name_lower = filename.lower()
        file_type = "Enrollment" if 'enrollment' in file_name_lower else "Usage" if 'usage' in file_name_lower else "Unknown"
        logging.info(f"File Type Detected: {file_type}")
        
        # 6. Execute the transformation and cleanup pipeline
        if transform_excel_data(input_path, intermediate_csv_filename):
            find_and_replace_quotes(intermediate_csv_filename, final_output_filename)
            processed_count += 1
            
            # Cleanup
            try:
                logging.info("Starting cleanup of intermediate file...")
                os.remove(intermediate_csv_filename)
                logging.info(f"Intermediate file '{intermediate_csv_filename}' removed successfully.")
            except OSError as e:
                logging.error(f"Error removing intermediate file: {e}")
        
    logging.info("\n" + "="*50)
    logging.info(f"Automatic folder scan finished. {processed_count} files processed.")
    logging.info("="*50 + "\n")
