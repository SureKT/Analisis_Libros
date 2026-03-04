import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession

# Detectar el ejecutable correcto para los workers
raw_exe = Path(sys.executable)

# Si nos han lanzado con el launcher de Windows (py.exe), forzamos python.exe
if raw_exe.name.lower() == "py.exe":
    executable = raw_exe.with_name("python.exe")
else:
    executable = raw_exe

print(f"sys.executable original: {sys.executable}")
print(f"Ejecutable que usará Spark: {executable}")
print(f"Python versión      : {sys.version}")

# Asegurar que driver y workers usan ESTE ejecutable
os.environ["PYSPARK_PYTHON"] = str(executable)
os.environ["PYSPARK_DRIVER_PYTHON"] = str(executable)
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

print("--- TEST INICIADO ---")

try:
    spark = (
        SparkSession.builder
        .appName("TestSimple")
        .master("local[1]")
        .config("spark.pyspark.python", str(executable))
        .config("spark.pyspark.driver.python", str(executable))
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    print("--- CONEXIÓN LOGRADA ---")
    df = spark.createDataFrame([(1, "Prueba")], ["id", "valor"])
    df.show()
    print("--- SI VES ESTO, TU SPARK YA FUNCIONA ---")
    spark.stop()
except Exception as e:
    print(f"ERROR EN DRIVER: {e}")