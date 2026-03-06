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
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from constants import *

executable = sys.executable
os.environ["PYSPARK_PYTHON"] = executable
os.environ["PYSPARK_DRIVER_PYTHON"] = executable
print(f"--- Forzando Spark a usar Python en: {executable} ---")

_NON_WORD_RE = re.compile(r"[^\wáéíóúüñ]+", flags=re.IGNORECASE)

# Normalización de términos de "universo Sanderson" para que sea coherente con la
# tokenización de la Fase 1: limpieza por regex + split en tokens, y filtrado
# de stopwords.
#
# La Fase 2 opera con:
# - **unigramas**: tokens individuales
# - **bigrams/ngrams**: secuencias de tokens (separadas por espacios) derivadas del
#   texto ya limpiado (sin stopwords).
#
# Además, para reducir falsos positivos, asumimos:
# - **Personajes**: solo cuentan si aparecen con mayúscula inicial en el texto.
STOP_WORDS_ALL = STOP_WORDS_ES | STOP_WORDS_EN


def _normalize_to_tokens(text: str) -> list[str]:
    return [t for t in _NON_WORD_RE.sub(" ", text.lower()).split() if t]


def _build_sanderson_term_rules() -> tuple[
    dict[str, tuple[str, str, bool]],
    dict[int, dict[str, tuple[str, str, bool]]],
]:
    """
    Devuelve:
    - reglas unigram: token_variante -> (termino_canonico, categoria, requiere_mayuscula_inicial_en_texto)
    - reglas ngram: n -> { "tok1 tok2 ...": (termino_canonico, categoria, requiere_mayuscula_inicial_en_texto) }
    """
    unigram_rules: dict[str, tuple[str, str, bool]] = {}
    ngram_rules: dict[int, dict[str, tuple[str, str, bool]]] = {}

    for raw_term, category in SANDERSON_TERMS.items():
        canonical_term = str(raw_term)
        cat = str(category)
        requires_cap = cat.lower() == "personaje"
        requires_cap = cat.lower() == "personaje"

        tokens = [t for t in _normalize_to_tokens(raw_term) if t not in STOP_WORDS_ALL]
        if not tokens:
            continue

        if len(tokens) == 1:
            token = tokens[0]

            # Singular/plural muy básico: añadimos variantes simples para que
            # "esfera" también recoja "esferas", etc.
            variants = {token}
            if token.endswith("s"):
                root = token.rstrip("s")
                if root:
                    variants.add(root)
            else:
                if token.endswith(("a", "e", "i", "o", "u")):
                    variants.add(token + "s")
                else:
                    variants.add(token + "es")

            for v in variants:
                unigram_rules.setdefault(v, (canonical_term, cat, requires_cap))
            continue

        n = len(tokens)
        phrase = " ".join(tokens)
        bucket = ngram_rules.setdefault(n, {})
        bucket.setdefault(phrase, (canonical_term, cat, requires_cap))

    return unigram_rules, ngram_rules


SANDERSON_UNIGRAM_RULES, SANDERSON_NGRAM_RULES = _build_sanderson_term_rules()
SANDERSON_NGRAM_NS = tuple(sorted(SANDERSON_NGRAM_RULES.keys()))


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

            raw_dir_resolved = raw_dir.resolve()
            pattern = f"{raw_dir_resolved.as_posix()}/*.txt"
            # Leemos (ruta_fichero, contenido) y derivamos el nombre del libro a partir
            # del nombre del fichero .txt (sin extensión).
            lines_rdd = sc.wholeTextFiles(pattern).flatMap(
                lambda kv: (
                    (os.path.splitext(os.path.basename(kv[0]))[0], line)
                    for line in kv[1].splitlines()
                )
            )

            # Importante:
            # - Guardamos el token normalizado en minúsculas (para el WordCount)
            # - Además guardamos el token con casing original + flag de mayúscula inicial
            #   para poder distinguir usos de "Celeste" (nombre) vs "celeste" (adjetivo).
            #
            # También añadimos (line_id, pos) para poder generar bigramas/ngramas en Fase 2.
            # line_id es un índice global de línea dentro del corpus.
            lines_with_id_rdd = lines_rdd.zipWithIndex()  # ((book, line), line_id)

            def tokenize_row(
                row: tuple[tuple[str, str], int]
            ) -> list[tuple[str, int, int, str, str, bool]]:
                (book, line), line_id = row
                cleaned = _NON_WORD_RE.sub(" ", line)
                raw_tokens = [t for t in cleaned.split() if t]

                out: list[tuple[str, int, int, str, str, bool]] = []
                pos = 0
                for raw in raw_tokens:
                    token = raw.lower().strip()
                    if not token:
                        continue
                    if token in stop_words:
                        continue
                    is_capitalized = raw[:1].isupper()
                    out.append((book, int(line_id), pos, token, raw, bool(is_capitalized)))
                    pos += 1
                return out

            clean_rdd = (
                lines_with_id_rdd.flatMap(tokenize_row).persist(StorageLevel.MEMORY_AND_DISK)
            )
            return clean_rdd

        clean_tokens_rdd = self.run_logged_step(
            fase="Fase 1",
            step="RDD Clean Tokens (stopwords+tokenizacion)",
            tipo="Auto",
            fn=build_clean_tokens_rdd,
        )

        out_dir = self.paths.data_interim / "fase1_clean_tokens_parquet"

        if clean_tokens_rdd.isEmpty():
            empty_schema = StructType(
                [
                    StructField("book", StringType(), True),
                    StructField("line_id", LongType(), True),
                    StructField("pos", LongType(), True),
                    StructField("token", StringType(), True),
                    StructField("raw_token", StringType(), True),
                    StructField("is_capitalized", BooleanType(), True),
                ]
            )
            empty_rdd = self.spark.sparkContext.emptyRDD()
            df = self.spark.createDataFrame(empty_rdd, empty_schema)
        else:
            df = self.spark.createDataFrame(
                clean_tokens_rdd,
                ["book", "line_id", "pos", "token", "raw_token", "is_capitalized"],
            )

        df.write.mode("overwrite").parquet(str(out_dir.resolve()))

    def fase_2_limpieza(self) -> None:
        """Fase 2 (Limpieza): normalizar/filtrar datos y persistir a /data/interim (Parquet)."""
        in_dir = self.paths.data_interim / "fase1_clean_tokens_parquet"
        tokens_df = self.spark.read.parquet(str(in_dir.resolve()))
        sc = self.spark.sparkContext

        def build_wordcount_rdd():
            tokens_rdd = (
                tokens_df.select("book", "token", "is_capitalized")
                .rdd.map(lambda r: (r[0], r[1], bool(r[2])))
            )
            # ((book, token), (count_total, count_capitalized))
            wc2_rdd = (
                tokens_rdd.map(lambda t: ((t[0], t[1]), (1, 1 if t[2] else 0)))
                .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                .persist(StorageLevel.MEMORY_AND_DISK)
            )
            return wc2_rdd

        wc_rdd = self.run_logged_step(
            fase="Fase 2",
            step="WordCount (distinct tokens)",
            tipo="Auto",
            fn=build_wordcount_rdd,
        )

        unigram_rules_bc = sc.broadcast(SANDERSON_UNIGRAM_RULES)
        ngram_rules_bc = sc.broadcast(SANDERSON_NGRAM_RULES)
        ngram_ns = SANDERSON_NGRAM_NS

        def build_insights_rdd():
            # --- Unigramas ---
            # wc_rdd: ((book, token), (count_total, count_capitalized))
            def unigram_by_book_mapper(
                kv: tuple[tuple[str, str], tuple[int, int]]
            ) -> Optional[tuple[str, int, str, str]]:
                (book, token), (tot, caps) = kv
                rules = unigram_rules_bc.value
                if token not in rules:
                    return None
                canonical, cat, requires_cap = rules[token]
                freq = caps if requires_cap else tot
                if freq <= 0:
                    return None
                return (canonical, int(freq), cat, book)

            unigram_insights = (
                wc_rdd.map(unigram_by_book_mapper)
                .filter(lambda x: x is not None)  # type: ignore[func-returns-value]
            )

            # Total por término canónico (acumulando libros)
            unigram_total = (
                unigram_insights.map(
                    lambda t: ((t[0], t[2]), t[1])  # (canonical, categoria) -> freq
                )
                .reduceByKey(add)
                .map(lambda kv: (kv[0][0], int(kv[1]), kv[0][1], "TOTAL"))
            )

            # --- Bigrams / Ngrams ---
            # Reconstruimos tokens por línea usando (book, line_id, pos)
            by_line_rdd = tokens_df.select(
                "book", "line_id", "pos", "token", "is_capitalized"
            ).rdd.map(
                lambda r: ((r[0], int(r[1])), (int(r[2]), r[3], bool(r[4])))
            )

            def emit_ngrams(items: list[tuple[int, str, bool]]) -> list[tuple[str, int]]:
                # items: [(pos, token, is_capitalized), ...] de una línea de un libro
                items_sorted = sorted(items, key=lambda x: x[0])
                toks = [it[1] for it in items_sorted]
                caps = [it[2] for it in items_sorted]
                out: list[tuple[str, int]] = []

                rules_all = ngram_rules_bc.value
                for n in ngram_ns:
                    rules_n = rules_all.get(n)
                    if not rules_n:
                        continue
                    if len(toks) < n:
                        continue
                    for i in range(0, len(toks) - n + 1):
                        phrase = " ".join(toks[i : i + n])
                        rule = rules_n.get(phrase)  # (canonical, cat, requires_cap)
                        if rule is None:
                            continue
                        _canonical, _cat, requires_cap = rule
                        if requires_cap and not all(caps[i : i + n]):
                            continue
                        out.append((phrase, 1))
                return out

            # Por libro
            ngram_counts_by_book = (
                by_line_rdd.groupByKey()
                .flatMap(
                    lambda kv: (
                        ((kv[0][0], phrase), c)
                        for phrase, c in emit_ngrams(list(kv[1]))
                    )
                )
                .reduceByKey(add)
            )

            def ngram_to_insight_by_book(
                kv: tuple[tuple[str, str], int]
            ) -> tuple[str, int, str, str]:
                (book, phrase), count = kv
                n = phrase.count(" ") + 1
                rules_n = ngram_rules_bc.value.get(n, {})
                canonical, cat, _req = rules_n.get(phrase, (phrase, "Universo", False))
                return (canonical, int(count), cat, book)

            ngram_insights_by_book = ngram_counts_by_book.map(ngram_to_insight_by_book)

            # Totales ngram por término canónico
            ngram_insights_total = (
                ngram_insights_by_book.map(
                    lambda t: ((t[0], t[2]), t[1])  # (canonical, categoria) -> freq
                )
                .reduceByKey(add)
                .map(lambda kv: (kv[0][0], int(kv[1]), kv[0][1], "TOTAL"))
            )

            insights_rdd = (
                unigram_insights.union(unigram_total)
                .union(ngram_insights_by_book)
                .union(ngram_insights_total)
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
                    StructField("Libro", StringType(), True),
                ]
            )
            empty_rdd = self.spark.sparkContext.emptyRDD()
            insights_df = self.spark.createDataFrame(empty_rdd, empty_schema)
        else:
            insights_df = self.spark.createDataFrame(
                insights_rdd, ["Palabra", "Frecuencia", "Categoría", "Libro"]
            )
        self.fase2_insights_df = insights_df  # type: ignore[attr-defined]

        out_dir = self.paths.data_interim / "fase2_insights_parquet"
        insights_df.write.mode("overwrite").parquet(str(out_dir.resolve()))

    def fase_3_analisis_exportacion(self) -> None:
        """Fase 3 (Análisis -> Exportación): métricas/insights y export final a /data/output (CSV Power BI)."""
        in_dir = self.paths.data_interim / "fase2_insights_parquet"
        insights_df = self.spark.read.parquet(str(in_dir.resolve()))

        # Métricas por término (sin incluir la fila TOTAL)
        per_book_df = insights_df.filter(F.col("Libro") != F.lit("TOTAL"))

        # Número de libros distintos por término
        libros_stats_df = per_book_df.groupBy("Palabra").agg(
            F.countDistinct("Libro").alias("Num_Libros_Con_Termino")
        )

        num_libros_totales = (
            per_book_df.select("Libro").distinct().count()
        )

        # Es_Global: aparece al menos en 3 libros (umbral configurable)
        libros_stats_df = libros_stats_df.withColumn(
            "Es_Global",
            (F.col("Num_Libros_Con_Termino") >= F.lit(3)),
        )

        # Metadatos de libros desde BOOK_METADATA (si existen)
        # Creamos columnas Saga/Mundo/Orden_Publicacion vía UDF sobre el nombre del libro
        from constants import BOOK_METADATA  # type: ignore[import-not-found]

        def _book_meta(book: str, key: str) -> Optional[object]:
            if book is None:
                return None
            meta = BOOK_METADATA.get(book)
            if not isinstance(meta, dict):
                return None
            return meta.get(key)

        saga_udf = F.udf(lambda b: _book_meta(b, "Saga"), StringType())
        mundo_udf = F.udf(lambda b: _book_meta(b, "Mundo"), StringType())
        orden_udf = F.udf(lambda b: _book_meta(b, "Orden_Publicacion"), LongType())
        anio_udf = F.udf(lambda b: _book_meta(b, "Anio_Publicacion"), LongType())

        # Columnas claras para Power BI
        powerbi_df = (
            insights_df.select(
                F.col("Palabra").alias("Termino"),
                F.col("Frecuencia").cast("long").alias("Repeticiones"),
                F.col("Libro").alias("Libro_Origen"),
            )
            .join(
                libros_stats_df,
                on=F.col("Termino") == F.col("Palabra"),
                how="left",
            )
            .withColumn(
                "TFIDF_like",
                F.when(
                    (F.col("Libro_Origen") != F.lit("TOTAL"))
                    & (F.col("Num_Libros_Con_Termino") > 0),
                    F.col("Repeticiones")
                    * F.log(
                        F.lit(float(num_libros_totales))
                        / F.col("Num_Libros_Con_Termino").cast("double")
                    ),
                ).otherwise(F.lit(None)),
            )
            .withColumn("Saga", saga_udf(F.col("Libro_Origen")))
            .withColumn("Mundo", mundo_udf(F.col("Libro_Origen")))
            .withColumn("Orden_Publicacion", orden_udf(F.col("Libro_Origen")))
            .withColumn("Anio_Publicacion", anio_udf(F.col("Libro_Origen")))
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
