import yaml
import pyodbc
from datetime import datetime
import pandas as pd
import numpy as np
import warnings

# Suppress pandas warning about DBAPI2 connections
warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy*", category=UserWarning)

BATCH_SIZE = 1000
SIMPLE_TABLE = "ml.mv_facility_waste_simple_train"
GRAPH_TABLE = "ml.mv_facility_waste_graph_train"

TRAIN_DATA_PCT = 0.7
TEST_DATA_PCT = 0.15
VAL_DATA_PCT = 0.15

def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)

class DataLoader:
    def __init__(self):
        self.conn = None
        self.simple_offset = 0
        self.graph_offset = 0
        self.simple_categorical_levels = {}
        self.simple_expected_ohe_columns = []
        self.graph_categorical_levels = {}
        self.graph_expected_ohe_columns = []
        self.connect_to_database('config.yaml')

    def reset_offsets(self):
        self.simple_offset = 0
        self.graph_offset = 0

    def connect_to_database(self, config_file):
        with open(config_file, 'r') as file:
            config = yaml.safe_load(file)

        pg = config['postgres']
        driver = pg.get('odbc_driver', 'PostgreSQL Unicode')
        
        log(f"Attempting to connect to database at {pg['host']}:{pg['port']}...")
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={pg['host']};"
            f"PORT={pg['port']};"
            f"DATABASE={pg['database']};"
            f"UID={pg['user']};"
            f"PWD={pg['password']};"
        )
        self.conn = pyodbc.connect(conn_str)

        log(f"Connection initialized: driver={driver}, server={pg['host']}, port={pg['port']}, database={pg['database']}")
        return self.conn

    def get_total_pages(self, table, batch_size=BATCH_SIZE, process='train'):
        count_query = f"SELECT COUNT(*) FROM {table}"
        cursor = self.conn.cursor()
        cursor.execute(count_query)
        total_records = cursor.fetchone()[0]
        total_pages = (total_records + batch_size - 1) // batch_size
        
        if process == 'train':
            total_pages = int(total_pages * TRAIN_DATA_PCT)
            total_records = int(total_records * TRAIN_DATA_PCT)
        elif process == 'test':
            total_pages = int(total_pages * TEST_DATA_PCT)
            total_records = int(total_records * TEST_DATA_PCT)
        elif process == 'validation':
            total_pages = int(total_pages * VAL_DATA_PCT)
            total_records = int(total_records * VAL_DATA_PCT)

        return total_pages, total_records

    def load_pandas_next_page(self, query, batch_size=BATCH_SIZE, offset=0, process='train'):
        # pull data based on process type, select every nth record based on the percentage splits defined at the top of this file
        if process == 'train':
            paginated_query = f"{query} WHERE uid % 100 < {int(TRAIN_DATA_PCT * 100)} LIMIT {batch_size} OFFSET {offset}"
        elif process == 'test':
            paginated_query = f"{query} WHERE uid % 100 >= {int(TRAIN_DATA_PCT * 100)} AND uid % 100 < {int((TRAIN_DATA_PCT + TEST_DATA_PCT) * 100)} LIMIT {batch_size} OFFSET {offset}"
        elif process == 'validation':
            paginated_query = f"{query} WHERE uid % 100 >= {int((TRAIN_DATA_PCT + TEST_DATA_PCT) * 100)} AND uid % 100 < {int((TRAIN_DATA_PCT + TEST_DATA_PCT + VAL_DATA_PCT) * 100)} LIMIT {batch_size} OFFSET {offset}"

        df = pd.read_sql(paginated_query, self.conn)
        return df, len(df)
    
    def pandas_page_to_training_data(self, df):
        X = df.drop(columns=['target'])
        y = df['target']

        # x and y should be numpy arrays of type float32 for keras and 1d array of labels for sklearn
        X = X.to_numpy().astype(np.float32)
        y = y.to_numpy().astype(np.float32)

        return X, y
    
    def transform_output_to_labels(self, df):
        # df target will be 0 if target_class is 'decrease', 1 if 'same', and 2 if 'increase'
        df['target'] = df['target_class'].apply(lambda x: 0 if x == 'decrease' else (2 if x == 'increase' else 1))
        df.drop(columns=['target_class'], inplace=True)
        return df
    
    def get_categorical_domains(self, table, categorical_cols):
        if self.conn is None:
            raise RuntimeError("Database connection must be initialized before loading categorical domains.")

        categorical_levels = {}
        for col in categorical_cols:
            query = f"SELECT DISTINCT {col} FROM {table} WHERE {col} IS NOT NULL"
            values = pd.read_sql(query, self.conn)[col]
            categorical_levels[col] = sorted(values.astype(str).unique())

        sample = pd.DataFrame({
            col: pd.Categorical([], categories=categorical_levels[col])
            for col in categorical_cols
        })
        expected_columns = pd.get_dummies(sample, columns=categorical_cols, drop_first=True).columns.tolist()
        return categorical_levels, expected_columns

    def get_simple_categorical_domains(self):
        categorical_cols = ['waste_code', 'mode_form_code', 'mode_source_code']
        self.simple_categorical_levels, self.simple_expected_ohe_columns = self.get_categorical_domains(
            SIMPLE_TABLE,
            categorical_cols,
        )
        return self.simple_categorical_levels

    def get_graph_categorical_domains(self):
        categorical_cols = ['management_method_code', 'form_code', 'source_code']
        self.graph_categorical_levels, self.graph_expected_ohe_columns = self.get_categorical_domains(
            GRAPH_TABLE,
            categorical_cols,
        )
        return self.graph_categorical_levels

    def one_hot_encode_categoricals(self, df, categorical_cols, categorical_levels, expected_ohe_columns, table=None):
        if len(categorical_cols) == 0:
            return df

        if not expected_ohe_columns:
            if table is None:
                raise RuntimeError("Table name must be provided when expected OHE columns are not yet initialized.")
            categorical_levels, expected_ohe_columns = self.get_categorical_domains(table, categorical_cols)

        encode_cols = [col for col in categorical_cols if col in df.columns]
        if not encode_cols:
            return df

        for col in encode_cols:
            if col in categorical_levels:
                df[col] = pd.Categorical(df[col].astype(str), categories=categorical_levels[col])

        df = pd.get_dummies(df, columns=encode_cols, drop_first=True)

        if expected_ohe_columns:
            non_ohe_cols = [c for c in df.columns if c not in expected_ohe_columns]
            df = df.reindex(columns=non_ohe_cols + expected_ohe_columns, fill_value=0)

        return df

    def transform_page_simple(self, df):
        df = self.transform_output_to_labels(df)

        # drop uid, year_quarter, quarter, and facility_epaid as features for the simple model
        df.drop(columns=['uid', 'year_quarter', 'quarter_start', 'facility_epaid', 'next_qty_tons'], inplace=True)

        # drop rows with nulls for lags
        df.dropna(inplace=True)

        # Drop any remaining object columns except the categorical ones we want to encode
        object_cols = df.select_dtypes(include=['object', 'string']).columns
        cols_to_drop = [c for c in object_cols if c not in ['waste_code', 'mode_form_code', 'mode_source_code']]
        df.drop(columns=cols_to_drop, inplace=True)

        df = self.one_hot_encode_categoricals(
            df,
            categorical_cols=['waste_code', 'mode_form_code', 'mode_source_code'],
            categorical_levels=self.simple_categorical_levels,
            expected_ohe_columns=self.simple_expected_ohe_columns,
            table=SIMPLE_TABLE,
        )

        return df

    def transform_page_graph(self, df):
        df = self.transform_output_to_labels(df)

        # drop features 
        df.drop(columns=['uid', 'year_quarter', 'year', 'quarter', 'facility_epaid', 'facility_node_id', 'waste_stream_key', 'display_name', 'first_shipped_date', 'last_shipped_date'], inplace=True)

        # drop rows with nulls for lags
        df.dropna(subset=[c for c in df.columns if 'lag' in c], inplace=True)

        # make nans 0 for non-lag features
        non_lag_cols = [c for c in df.columns if 'lag' not in c and c != 'target']
        df[non_lag_cols] = df[non_lag_cols].fillna(0)

        object_cols = df.select_dtypes(include=['object', 'string']).columns

        needed_cats_list = ['management_method_code', 'form_code', 'source_code']

        cols_to_drop = [c for c in object_cols if c not in needed_cats_list]

        df.drop(columns=cols_to_drop, inplace=True)

        df = self.one_hot_encode_categoricals(
            df,
            categorical_cols=needed_cats_list,
            categorical_levels=self.graph_categorical_levels,
            expected_ohe_columns=self.graph_expected_ohe_columns,
            table=GRAPH_TABLE,
        )

        return df
    ###########################

    def get_next_page_simple(self, batch_size=BATCH_SIZE, process='train'):
        df, num_records = self.load_pandas_next_page(f"SELECT * FROM {SIMPLE_TABLE}", batch_size, self.simple_offset, process)
        
        if num_records == 0:
            return None, None
        self.simple_offset += batch_size
        
        transformed_df = self.transform_page_simple(df)
        
        if transformed_df.shape[0] == 0:
            return None, None
        
        X, y = self.pandas_page_to_training_data(transformed_df)
        return X, y

    def get_next_page_graph(self, batch_size=BATCH_SIZE, process='train'):
        df, num_records = self.load_pandas_next_page(f"SELECT * FROM {GRAPH_TABLE}", batch_size, self.graph_offset, process)
        
        if num_records == 0:
            return None, None
        self.graph_offset += batch_size
        
        transformed_df = self.transform_page_graph(df)

        if transformed_df.shape[0] == 0:
            return None, None

        X, y = self.pandas_page_to_training_data(transformed_df)
        return X, y
    
    def setup_simple(self):
        self.get_simple_categorical_domains()
        return self.get_total_pages(SIMPLE_TABLE)

    def setup_graph(self):
        return self.get_total_pages(GRAPH_TABLE)

if __name__ == "__main__":
    dataloader = DataLoader()
    dataloader.connect_to_database('config.yaml')

    # write numpy to csv for one test page load for simple table
    X_simple, y_simple = dataloader.get_next_page_simple()
    if X_simple is not None and y_simple is not None:
        print(X_simple.shape, y_simple.shape)
        np.savetxt('X_simple_test_page.csv', X_simple, delimiter=',')
        np.savetxt('y_simple_test_page.csv', y_simple, delimiter=',')
        log("Saved test page for simple table to CSV files.")
    else:
        log("No records found in simple table.")


    # write numpy to csv for one test page load for graph table
    X_graph, y_graph = dataloader.get_next_page_graph()
    if X_graph is not None and y_graph is not None:
        print(X_graph.shape, y_graph.shape)
        np.savetxt('X_graph_test_page.csv', X_graph, delimiter=',')
        np.savetxt('y_graph_test_page.csv', y_graph, delimiter=',')
        log("Saved test page for graph table to CSV files.")
    else:
        log("No records found in graph table.")


