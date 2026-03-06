
## Proyecto: Análisis de libros de Brandon Sanderson con PySpark (RDDs)

Este proyecto analiza varios libros de Brandon Sanderson usando **PySpark** y **RDDs** para obtener métricas de frecuencia y relevancia de términos del “Cosmere”, y exportarlas a un CSV listo para **Power BI**.

### 1. Objetivo general

- **Limpiar y tokenizar** el texto de varios libros (`data/raw/*.txt`).
- **Contar apariciones** de términos relevantes del universo Sanderson (personajes, conceptos, lugares, etc.).
- **Distinguir por libro y en total**:
  - cuántas veces aparece cada término en cada libro,
  - cuántas apariciones totales tiene sumando todos los libros,
  - si un término es “global” (aparece en al menos 3 libros).
- **Exportar resultados** a un CSV único (`sanderson_insights.csv`) para visualizarlos en Power BI.

El proyecto está pensado para mostrar un uso **idiomático de PySpark con RDDs** y una pequeña arquitectura de “data pipeline” por fases.

### 2. Estructura de carpetas

- `data/raw/`  
  Ficheros `.txt` de los libros (uno por libro).  
  El nombre de archivo (sin `.txt`) se usa como **nombre del libro** en la salida.

- `data/interim/`  
  Resultados intermedios en **Parquet**:
  - `fase1_clean_tokens_parquet/` → tokens limpios (por libro, línea y posición).
  - `fase2_insights_parquet/` → tabla de insights por término/libro.

- `data/output/`  
  - `sanderson_insights.csv` → CSV final para Power BI (un solo fichero).

- `logs/`  
  - `tabla_logs.csv` → log de ejecución de la pipeline (por fase y paso).

- `src/`  
  - `main.py` → orquesta las 3 fases con PySpark y el sistema de logging.
  - `constants.py` → stopwords y diccionario de términos del Cosmere + metadata de libros.
  - `convert_books.py` → utilidades auxiliares (por ejemplo, para preparar ficheros de entrada).
  - `Readme.md` → este fichero.

### 3. Flujo por fases

Todo el pipeline se ejecuta desde `main.py` al llamar a `main()`:

```bash
python -m src.main
```

Internamente se crean:

1. **SparkSession**  
   - Modo local: `local[*]`.  
   - Se fuerza a usar el ejecutable de Python actual (`sys.executable`) para evitar problemas en Windows.

2. **Rutas del proyecto** (`ProjectPaths`)  
   - Se asegura que existen `data/raw`, `data/interim`, `data/output`, `logs`.

3. **SandersonAnalyzer.run()**  
   Llama secuencialmente a:
   - `fase_1_ingesta()`
   - `fase_2_limpieza()`
   - `fase_3_analisis_exportacion()`

#### 3.1. Fase 1 – Ingesta y limpieza básica

Archivo: `main.py`, método `SandersonAnalyzer.fase_1_ingesta`.

- Lee **todos los `.txt` de `data/raw`** con PySpark usando `sc.wholeTextFiles(...)`.
  - A partir del nombre de fichero se obtiene el campo **`book`** (nombre del libro).
- Divide cada libro en líneas y luego en tokens:
  - pasa a texto limpio mediante una regex (`_NON_WORD_RE`),
  - separa en palabras,
  - elimina tokens vacíos y **stopwords** (español + un pequeño conjunto en inglés).
- Para cada token se guarda:
  - `book` → nombre del libro,
  - `line_id` → índice global de línea,
  - `pos` → posición del token en la línea,
  - `token` → versión en minúsculas (para el WordCount),
  - `raw_token` → forma original (con mayúsculas),
  - `is_capitalized` → si empieza por mayúscula (sirve para distinguir nombres propios como “Kaladin” de usos comunes como “celeste”).
- Se escribe un DataFrame Parquet en `data/interim/fase1_clean_tokens_parquet/`.

Esta fase utiliza **RDDs** para todo el flujo de tokenización y luego se convierte a DataFrame solo al persistir a Parquet.

#### 3.2. Fase 2 – WordCount + términos Sanderson

Archivo: `main.py`, método `SandersonAnalyzer.fase_2_limpieza`.

Entradas:
- Parquet de Fase 1 (`fase1_clean_tokens_parquet`).
- Diccionario `SANDERSON_TERMS` definido en `constants.py`:
  - clave: término tal como se quiere ver en los resultados (ej. `"Kaladin"`, `"Padre Tormenta"`, `"Esfera"`),
  - valor: categoría (ej. `"personaje"`, `"Concepto"`, `"Magia"`, `"Ciudad"`, etc.).

Pasos principales:

1. **Normalización de términos de Sanderson** (`_build_sanderson_term_rules`):
   - Se generan reglas para:
     - **unigramas**: token → (término canónico, categoría, requiere_mayúscula),
     - **ngrams** (bigrams, trigrams, etc.): secuencia limpia de tokens → (término canónico, categoría, requiere_mayúscula).
   - Para unigramas se añaden variantes muy sencillas de singular/plural:
     - `"esfera"` también matchea `"esferas"`, `"esfera"` / `"esferas"` siempre se reportan como término canónico `"Esfera"`.
   - Si la categoría es `"personaje"`, el término **solo cuenta** si aparece con mayúscula inicial en el texto; para otros tipos (`Concepto`, `Magia`, …) se cuentan minúsculas y mayúsculas.

2. **WordCount por libro** (usando RDDs):
   - Se construye un RDD con claves `((book, token), (count_total, count_capitalized))`.

3. **Insights de unigramas**:
   - Para cada token que exista en las reglas:
     - Por libro:
       - se decide si usar `count_capitalized` o `count_total` en función de la categoría (para personajes se usa solo capitalizado);
       - se emite `(TerminoCanónico, Frecuencia, Categoría, Libro)`.
     - Totales:
       - se suman las frecuencias de todos los libros y se emite una fila con `Libro = "TOTAL"`.

4. **Insights de n‑gramas** (bigrams/trigrams, etc.):
   - Se reconstruyen las secuencias de tokens por libro y línea usando `(book, line_id, pos)`.
   - Para cada línea se generan n‑gramas de los tamaños presentes en `SANDERSON_TERMS` y se aplica la regla de mayúsculas cuando corresponda.
   - De nuevo se generan filas por libro y por total (Libro `"TOTAL"`).

5. **DataFrame de insights de Fase 2**:
   - Esquema final:
     - `Palabra` → término canónico tal cual en `constants.py` (ej. `"Kaladin"`, `"Padre Tormenta"`, `"Esfera"`),
     - `Frecuencia` → recuento,
     - `Categoría` → tipo de término,
     - `Libro` → nombre del libro (`"TOTAL"` para filas agregadas).
   - Se escribe en `data/interim/fase2_insights_parquet/`.

#### 3.3. Fase 3 – Métricas adicionales y export a CSV

Archivo: `main.py`, método `SandersonAnalyzer.fase_3_analisis_exportacion`.

Entradas:
- Parquet de Fase 2 (`fase2_insights_parquet`).
- Parquet de Fase 1 (para contar palabras totales por libro).
- `BOOK_METADATA` (en `constants.py`) con información de sagas y mundos.

Pasos:

1. **Cálculo de métricas por término** (excluyendo la fila `Libro = "TOTAL"`):
   - `Num_Libros_Con_Termino` → en cuántos libros distintos aparece el término.
   - `Es_Global` → `TRUE` si el término aparece al menos en **3 libros** (criterio configurado).

2. **Metadatos de libros**:
   - Se enriquece cada fila con:
     - `Saga` (ej. `"El Archivo de las Tormentas"`, `"Nacidos de la Bruma (Era 1)"`),
     - `Mundo` (ej. `"Roshar"`, `"Scadrial"`, `"Nalthis"`),
     - `Orden_Publicacion` (número entero para ordenar cronológicamente dentro de la saga).

3. **TF‑IDF simplificado**:
   - Para filas donde `Libro_Origen != "TOTAL"` se calcula:
     \[
     \text{TFIDF\_like} = \text{Repeticiones} \times \log\left(\frac{N}{\text{Num\_Libros\_Con\_Termino}}\right)
     \]
     donde \(N\) es el número total de libros del corpus.
   - Esto permite ver qué términos son especialmente característicos de un libro concreto.

4. **Construcción del DataFrame final para Power BI**:
   - Columnas:
     - `Termino`
     - `Repeticiones`
     - `Libro_Origen` (nombre del libro o `"TOTAL"`)
     - `Num_Libros_Con_Termino`
     - `Es_Global`
     - `TFIDF_like`
     - `Saga`
     - `Mundo`
     - `Orden_Publicacion`
   - Se ordena por `Repeticiones` descendente.

5. **Escritura del CSV único**:
   - Se escribe a un directorio temporal con `coalesce(1)` y cabecera.
   - Se renombra el `part-*.csv` resultante a `data/output/sanderson_insights.csv`.

### 4. Sistema de logging

Archivo: `main.py`, clases/funciones:
- `CsvLogTable`
- `log_step` (decorador)
- `SandersonAnalyzer.run_logged_step`

Características:

- Cada paso importante de las fases se ejecuta envuelto en un log:
  - `FASE` (Fase 1 / Fase 2 / Fase 3),
  - `STEP` (descripción breve del paso),
  - `F_Ini`, `F_Fin` (timestamps),
  - `Length` (normalmente el número de elementos procesados, usando `.count()`),
  - `TIPO` (`"Auto"` en este proyecto).
- Los logs se guardan en `logs/tabla_logs.csv`.
- Al final de `main()`, se imprime un resumen agregando `Length` por `(FASE, STEP, TIPO)`, lo que permite evaluar:
  - cuántos tokens totales se han procesado,
  - cuántos términos Sanderson filtrados se han generado,
  - cuántas filas se han exportado al CSV.

### 5. Cómo ejecutar el proyecto

1. Asegurarse de tener:
   - **Python 3.10+**,
   - **PySpark** instalado (ej. `pip install pyspark==3.5.1`),
   - **Java 17** instalado y accesible en el `PATH`.

2. Colocar los libros `.txt` en `data/raw/`  
   - El nombre del fichero (sin `.txt`) será el nombre del libro en la salida:
     - `data/raw/El Imperio Final.txt` → `Libro_Origen = "El Imperio Final"`.

3. Desde la raíz del proyecto:

```bash
python -m src.main
```

4. Revisar:
   - `data/output/sanderson_insights.csv` → tabla final para Power BI.
   - `logs/tabla_logs.csv` → registro de pasos y volúmenes procesados.

### 6. Uso en Power BI (resumen)

- Importar `data/output/sanderson_insights.csv` como fuente de datos.
- Campos clave:
  - `Termino` → eje de gráficos.
  - `Repeticiones` → medida principal.
  - `Libro_Origen` → para filtrar por libro o mostrar libros vs total.
  - `Num_Libros_Con_Termino`, `Es_Global` → distinguir términos globales vs locales.
  - `TFIDF_like` → seleccionar términos característicos de un libro.
  - `Saga`, `Mundo`, `Orden_Publicacion` → segmentadores y ordenaciones por saga/universo.

Con este diseño, el profesor puede evaluar:
- El uso correcto de **PySpark y RDDs** (no solo DataFrames).
- La organización del pipeline en **tres fases claras** con logs.
- La preparación de datos **pensada para BI**, con dimensiones de libro, saga y tipo de término, y con métricas agregadas y normalizadas. 

