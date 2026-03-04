from __future__ import annotations

import csv
import os
import re
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from operator import add
from pathlib import Path
from typing import Any, Callable, Optional

from pyspark import StorageLevel  # type: ignore[import-not-found]
from pyspark.sql import SparkSession  # type: ignore[import-not-found]
from pyspark.sql import functions as F  # type: ignore[import-not-found]
from pyspark.sql.types import (  # type: ignore[import-not-found]
    LongType,
    StringType,
    StructField,
    StructType,
)

executable = sys.executable
os.environ["PYSPARK_PYTHON"] = executable
os.environ["PYSPARK_DRIVER_PYTHON"] = executable
print(f"--- Forzando Spark a usar Python en: {executable} ---")


LOG_COLUMNS = ["FASE", "STEP", "F_Ini", "F_Fin", "Length", "TIPO"]

STOP_WORDS_ES = {
    "a",
    "al",
    "algo",
    "algunos",
    "ante",
    "antes",
    "aquel",
    "aquella",
    "aquellas",
    "aquello",
    "aquellos",
    "aqui",
    "as",
    "asi",
    "aun",
    "aunque",
    "bajo",
    "bien",
    "cada",
    "casi",
    "como",
    "con",
    "contra",
    "cual",
    "cuales",
    "cuando",
    "de",
    "del",
    "desde",
    "donde",
    "dos",
    "el",
    "ella",
    "ellas",
    "ello",
    "ellos",
    "en",
    "entre",
    "era",
    "erais",
    "eran",
    "eras",
    "eres",
    "es",
    "esa",
    "esas",
    "ese",
    "eso",
    "esos",
    "esta",
    "estaba",
    "estabais",
    "estaban",
    "estabas",
    "estad",
    "estada",
    "estadas",
    "estado",
    "estados",
    "estais",
    "estamos",
    "estando",
    "estar",
    "estaremos",
    "estará",
    "estarán",
    "estarás",
    "estaré",
    "estaréis",
    "estaría",
    "estaríais",
    "estaríamos",
    "estarían",
    "estarías",
    "estas",
    "este",
    "estemos",
    "esto",
    "estos",
    "estoy",
    "estuve",
    "estuviera",
    "estuvierais",
    "estuvieran",
    "estuvieras",
    "estuvieron",
    "estuviese",
    "estuvieseis",
    "estuviesen",
    "estuvieses",
    "estuvimos",
    "estuviste",
    "estuvisteis",
    "estuvo",
    "está",
    "estábamos",
    "estáis",
    "están",
    "estás",
    "esté",
    "estéis",
    "estén",
    "estés",
    "fue",
    "fuera",
    "fuerais",
    "fueran",
    "fueras",
    "fueron",
    "fuese",
    "fueseis",
    "fuesen",
    "fueses",
    "fui",
    "fuimos",
    "fuiste",
    "fuisteis",
    "ha",
    "habia",
    "habéis",
    "haber",
    "había",
    "habíais",
    "habíamos",
    "habían",
    "habías",
    "han",
    "has",
    "hasta",
    "hay",
    "he",
    "hemos",
    "hube",
    "hubiera",
    "hubierais",
    "hubieran",
    "hubieras",
    "hubieron",
    "hubiese",
    "hubieseis",
    "hubiesen",
    "hubieses",
    "hubimos",
    "hubiste",
    "hubisteis",
    "hubo",
    "la",
    "las",
    "le",
    "les",
    "lo",
    "los",
    "mas",
    "me",
    "mi",
    "mis",
    "mucho",
    "muchos",
    "muy",
    "más",
    "mí",
    "mía",
    "mías",
    "mío",
    "míos",
    "nada",
    "ni",
    "no",
    "nos",
    "nosotras",
    "nosotros",
    "nuestra",
    "nuestras",
    "nuestro",
    "nuestros",
    "o",
    "os",
    "otra",
    "otras",
    "otro",
    "otros",
    "para",
    "pero",
    "poco",
    "por",
    "porque",
    "que",
    "quien",
    "quienes",
    "qué",
    "se",
    "sea",
    "seais",
    "sean",
    "seas",
    "ser",
    "será",
    "serán",
    "serás",
    "seré",
    "seréis",
    "sería",
    "seríais",
    "seríamos",
    "serían",
    "serías",
    "si",
    "sido",
    "siendo",
    "sin",
    "sobre",
    "sois",
    "somos",
    "son",
    "soy",
    "su",
    "sus",
    "suya",
    "suyas",
    "suyo",
    "suyos",
    "sí",
    "tambien",
    "también",
    "tan",
    "tanto",
    "te",
    "tendremos",
    "tendrá",
    "tendrán",
    "tendrás",
    "tendré",
    "tendréis",
    "tendría",
    "tendríais",
    "tendríamos",
    "tendrían",
    "tendrías",
    "tened",
    "tenemos",
    "tenga",
    "tengais",
    "tengan",
    "tengas",
    "tengo",
    "tenida",
    "tenidas",
    "tenido",
    "tenidos",
    "teniendo",
    "tenéis",
    "tenía",
    "teníais",
    "teníamos",
    "tenían",
    "tenías",
    "ti",
    "tiene",
    "tienen",
    "tienes",
    "todo",
    "todos",
    "tu",
    "tus",
    "tuve",
    "tuviera",
    "tuvierais",
    "tuvieran",
    "tuvieras",
    "tuvieron",
    "tuviese",
    "tuvieseis",
    "tuviesen",
    "tuvieses",
    "tuvimos",
    "tuviste",
    "tuvisteis",
    "tuvo",
    "tuya",
    "tuyas",
    "tuyo",
    "tuyos",
    "tú",
    "un",
    "una",
    "uno",
    "unos",
    "vosotras",
    "vosotros",
    "vuestra",
    "vuestras",
    "vuestro",
    "vuestros",
    "y",
    "ya",
}

STOP_WORDS_EN = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "he",
    "her",
    "his",
    "i",
    "in",
    "is",
    "it",
    "its",
    "me",
    "my",
    "not",
    "of",
    "on",
    "or",
    "our",
    "she",
    "so",
    "that",
    "the",
    "their",
    "them",
    "there",
    "they",
    "this",
    "to",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "who",
    "will",
    "with",
    "you",
    "your",
}

_NON_WORD_RE = re.compile(r"[^\wáéíóúüñ]+", flags=re.IGNORECASE)

SANDERSON_TERMS = {
    "vin": "Personaje",
    "kelsier": "Personaje",
    "kaladin": "Personaje",
    "dalinar": "Personaje",
    "alomancia": "Concepto",
    "potenciación": "Concepto",
}


def now_ts() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


class CsvLogTable:
    def __init__(self, path: Path) -> None:
        self.path = path

    def append(
        self,
        # *,
        fase: str,
        step: str,
        f_ini: str,
        f_fin: str,
        length: int,
        tipo: str,
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.path.exists() and self.path.stat().st_size > 0

        with self.path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=LOG_COLUMNS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(
                {
                    "FASE": fase,
                    "STEP": step,
                    "F_Ini": f_ini,
                    "F_Fin": f_fin,
                    "Length": int(length),
                    "TIPO": tipo,
                }
            )


def print_log_summary(log_path: Path) -> None:
    if not log_path.exists() or log_path.stat().st_size == 0:
        print(f"[LOG] No existe o está vacío: {log_path}")
        return

    rows: list[dict[str, str]] = []
    with log_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r:
                rows.append(r)

    totals: dict[tuple[str, str, str], int] = {}
    for r in rows:
        key = (r.get("FASE", ""), r.get("STEP", ""), r.get("TIPO", ""))
        try:
            length = int(r.get("Length", "0") or 0)
        except ValueError:
            length = 0
        totals[key] = totals.get(key, 0) + length

    print("\n=== Resumen Tabla Log ===")
    print(f"Archivo: {log_path}")
    print(f"Filas: {len(rows)}")
    print("Por (FASE, STEP, TIPO) -> Length total:")
    for (fase, step, tipo), length in sorted(totals.items()):
        print(f"- {fase} | {step} | {tipo} -> {length}")
    print("=========================\n")


def _extract_length(result: Any) -> tuple[Any, Optional[int]]:
    if isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], int):
        return result[0], result[1]

    if isinstance(result, int):
        return result, result

    count_fn = getattr(result, "count", None)
    if callable(count_fn):
        try:
            return result, int(count_fn())
        except Exception:
            return result, None

    return result, None


def log_step(*, fase: str, step: str, tipo: str = "Auto") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fn)
        def wrapped(self: Any, *args: Any, **kwargs: Any) -> Any:
            f_ini = now_ts()
            length: Optional[int] = None
            try:
                result = fn(self, *args, **kwargs)
                result, length = _extract_length(result)
                return result
            finally:
                f_fin = now_ts()
                self.log_table.append(
                    fase=fase,
                    step=step,
                    f_ini=f_ini,
                    f_fin=f_fin,
                    length=0 if length is None else length,
                    tipo=tipo,
                )

        return wrapped

    return decorator


@dataclass(frozen=True)
class ProjectPaths:
    root: Path

    @property
    def data_raw(self) -> Path:
        return self.root / "data" / "raw"

    @property
    def data_interim(self) -> Path:
        return self.root / "data" / "interim"

    @property
    def data_output(self) -> Path:
        return self.root / "data" / "output"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def log_csv(self) -> Path:
        return self.logs / "tabla_logs.csv"

    def ensure(self) -> None:
        self.data_raw.mkdir(parents=True, exist_ok=True)
        self.data_interim.mkdir(parents=True, exist_ok=True)
        self.data_output.mkdir(parents=True, exist_ok=True)
        self.logs.mkdir(parents=True, exist_ok=True)


class SandersonAnalyzer:
    def __init__(self, spark: SparkSession, paths: ProjectPaths) -> None:
        self.spark = spark
        self.paths = paths
        self.log_table = CsvLogTable(paths.log_csv)

    def run_logged_step(
        self,
        *,
        fase: str,
        step: str,
        tipo: str,
        fn: Callable[[], Any],
    ) -> Any:
        f_ini = now_ts()
        length: Optional[int] = None
        try:
            result = fn()
            result, length = _extract_length(result)
            return result
        finally:
            f_fin = now_ts()
            self.log_table.append(
                fase=fase,
                step=step,
                f_ini=f_ini,
                f_fin=f_fin,
                length=0 if length is None else length,
                tipo=tipo,
            )

    def fase_1_ingesta(self) -> None:
        """Fase 1 (Ingesta): leer .txt desde /data/raw y crear RDD(s) base."""
        sc = self.spark.sparkContext
        stop_words = set(STOP_WORDS_ES) | set(STOP_WORDS_EN)

        def build_clean_tokens_rdd():
            raw_dir = self.paths.data_raw
            files = sorted(raw_dir.glob("*.txt"))

            # Si no hay ficheros, devolvemos un RDD vacío. Esto evita que Spark falle
            # al resolver el patrón de entrada y mantiene el resto del pipeline intacto.
            if not files:
                return sc.emptyRDD()

            # En entorno normal (Hadoop correctamente configurado) usamos la API estándar
            # de Spark para leer texto como RDD de líneas, en lugar de paralelizar el
            # contenido leído desde Python (workaround usado en el entorno anterior).
            raw_dir_resolved = raw_dir.resolve()
            # En Windows/Hadoop funciona bien usando ruta local con comodín sobre *.txt.
            pattern = f"{raw_dir_resolved.as_posix()}/*.txt"
            lines_rdd = sc.textFile(pattern)

            tokens_rdd = (
                lines_rdd.map(lambda s: s.lower())
                .map(lambda s: _NON_WORD_RE.sub(" ", s))
                .flatMap(lambda s: s.split())
                .map(lambda w: w.strip())
                .filter(lambda w: bool(w))
            )

            clean_rdd = tokens_rdd.filter(lambda w: w not in stop_words).persist(StorageLevel.MEMORY_AND_DISK)
            return clean_rdd

        clean_tokens_rdd = self.run_logged_step(
            fase="Fase 1",
            step="RDD Clean Tokens (stopwords+tokenizacion)",
            tipo="Auto",
            fn=build_clean_tokens_rdd,
        )

        out_dir = self.paths.data_interim / "fase1_clean_tokens_parquet"

        if clean_tokens_rdd.isEmpty():
            empty_schema = StructType([StructField("token", StringType(), True)])
            empty_rdd = self.spark.sparkContext.emptyRDD()
            df = self.spark.createDataFrame(empty_rdd, empty_schema)
        else:
            df = self.spark.createDataFrame(clean_tokens_rdd.map(lambda w: (w,)), ["token"])

        df.write.mode("overwrite").parquet(str(out_dir.resolve()))

    def fase_2_limpieza(self) -> None:
        """Fase 2 (Limpieza): normalizar/filtrar datos y persistir a /data/interim (Parquet)."""
        in_dir = self.paths.data_interim / "fase1_clean_tokens_parquet"
        tokens_df = self.spark.read.parquet(str(in_dir.resolve()))

        def build_wordcount_rdd():
            tokens_rdd = tokens_df.select("token").rdd.map(lambda r: r[0])
            wc_rdd = (
                tokens_rdd.map(lambda w: (w, 1))
                .reduceByKey(add)
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            return wc_rdd

        wc_rdd = self.run_logged_step(
            fase="Fase 2",
            step="WordCount (distinct tokens)",
            tipo="Auto",
            fn=build_wordcount_rdd,
        )

        sanderson_terms = set(SANDERSON_TERMS.keys())

        def build_insights_rdd():
            insights_rdd = (
                wc_rdd.filter(lambda kv: kv[0] in sanderson_terms)
                .map(lambda kv: (kv[0], int(kv[1]), SANDERSON_TERMS.get(kv[0], "Universo")))
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            return insights_rdd

        insights_rdd = self.run_logged_step(
            fase="Fase 2",
            step="Insights Sanderson terms (wordcount filtered)",
            tipo="Auto",
            fn=build_insights_rdd,
        )

        if insights_rdd.isEmpty():
            empty_schema = StructType(
                [
                    StructField("Palabra", StringType(), True),
                    StructField("Frecuencia", LongType(), True),
                    StructField("Categoría", StringType(), True),
                ]
            )
            empty_rdd = self.spark.sparkContext.emptyRDD()
            insights_df = self.spark.createDataFrame(empty_rdd, empty_schema)
        else:
            insights_df = self.spark.createDataFrame(insights_rdd, ["Palabra", "Frecuencia", "Categoría"])
        self.fase2_insights_df = insights_df  # type: ignore[attr-defined]

        out_dir = self.paths.data_interim / "fase2_insights_parquet"
        insights_df.write.mode("overwrite").parquet(str(out_dir.resolve()))

    def fase_3_analisis_exportacion(self) -> None:
        """Fase 3 (Análisis -> Exportación): métricas/insights y export final a /data/output (CSV Power BI)."""
        in_dir = self.paths.data_interim / "fase2_insights_parquet"
        insights_df = self.spark.read.parquet(str(in_dir.resolve()))

        # Columnas claras para Power BI
        powerbi_df = (
            insights_df.select(
                F.col("Palabra").alias("Termino"),
                F.col("Frecuencia").cast("long").alias("Repeticiones"),
                F.lit("Desconocido").alias("Libro_Origen"),
            )
            .orderBy(F.col("Repeticiones").desc())
        )

        output_dir = self.paths.data_output
        output_dir.mkdir(parents=True, exist_ok=True)
        final_csv = output_dir / "sanderson_insights.csv"
        tmp_dir = output_dir / "_tmp_powerbi_csv"

        def export_single_csv() -> Any:
            if tmp_dir.exists():
                shutil.rmtree(tmp_dir, ignore_errors=True)

            powerbi_df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp_dir.resolve()))

            part_files = list(tmp_dir.glob("part-*.csv"))
            if not part_files:
                raise FileNotFoundError(f"No se encontró 'part-*.csv' en {tmp_dir}")

            if final_csv.exists():
                final_csv.unlink()

            part_files[0].replace(final_csv)
            shutil.rmtree(tmp_dir, ignore_errors=True)
            return powerbi_df

        # Loguea el nº de filas exportadas (términos filtrados)
        self.run_logged_step(
            fase="Fase 3",
            step="Export PowerBI CSV (single file)",
            tipo="Auto",
            fn=export_single_csv,
        )

    def run(self) -> None:
        self.paths.ensure()
        self.fase_1_ingesta()
        self.fase_2_limpieza()
        self.fase_3_analisis_exportacion()


def build_spark(app_name: str = "SandersonBooksAnalysis", master: Optional[str] = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    paths = ProjectPaths(root=root)
    spark = build_spark(master="local[*]")
    analyzer = SandersonAnalyzer(spark=spark, paths=paths)
    try:
        analyzer.run()
    finally:
        print_log_summary(paths.log_csv)
        spark.stop()


if __name__ == "__main__":
    main()
