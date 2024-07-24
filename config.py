import os, cx_Oracle

ORACLE_USERNAME = os.getenv("ORACLE_USERNAME")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = cx_Oracle.makedsn(
    host='azuoratwprdr.strykercorp.com', port='1521', service_name='TWPRDR'
)

AZURE_OAI_ENDPOINT = os.getenv("AZURE_OAI_ENDPOINT")
AZURE_OAI_KEY = os.getenv("AZURE_OAI_KEY")
AZURE_OAI_VERSION = os.getenv("AZURE_OAI_VERSION")