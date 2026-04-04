import yaml
import pyodbc

with open("config.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

print("Installed drivers:", pyodbc.drivers())

pg = cfg["postgres"]
conn_str = (
    f"DRIVER={{{pg['odbc_driver']}}};"
    f"SERVER={pg['host']};"
    f"PORT={pg['port']};"
    f"DATABASE={pg['database']};"
    f"UID={pg['user']};"
    f"PWD={pg['password']};"
)

conn = pyodbc.connect(conn_str, autocommit=True)
cur = conn.cursor()
cur.execute("SELECT version();")
print(cur.fetchone())
cur.close()
conn.close()