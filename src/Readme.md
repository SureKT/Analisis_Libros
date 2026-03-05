2) Diseño propuesto de cuadro de mando en Power BI
Asumiendo que importas data/output/sanderson_insights.csv y lo llamas, por ejemplo, tabla Insights con columnas:
Termino
Repeticiones
Libro_Origen
TFIDF_like
Saga
Mundo
Orden_Publicacion
Anio_Publicacion
Num_Libros_Con_Termino
Es_Global (True/False)
Te propongo este cuadro de mando con varias páginas:
Pantalla 1 – Visión general del Cosmere
Objetivo: ver de un vistazo qué términos son más relevantes y cómo se distribuyen por sagas y mundos.
Visuales:
Tarjetas (Cards):
Nº total de libros (DISTINCTCOUNT(Libro_Origen))
Nº total de términos (DISTINCTCOUNT(Termino))
Nº de términos “globales” (COUNTROWS(FILTER(Insights, Es_Global = TRUE())))
Gráfico de barras agrupadas:
Eje X: Termino (Top 20 por TFIDF_like o Repeticiones)
Valor: Repeticiones (o TFIDF_like como medida)
Leyenda: Saga
Gráfico de columnas apiladas por Mundo:
Eje X: Mundo
Valor: suma de Repeticiones
Segmentadores (Slicers):
Saga
Mundo
Es_Global (Sí/No)
Uso:
Ver qué personajes/conceptos son más dominantes globalmente y en qué sagas/mundos.
Pantalla 2 – Evolución temporal por año de publicación
Objetivo: analizar cómo aparecen los términos a lo largo del tiempo.
Visuales:
Gráfico de líneas:
Eje X: Anio_Publicacion
Eje Y: suma de Repeticiones
Leyenda: Saga
Gráfico de dispersión (scatter):
Eje X: Anio_Publicacion
Eje Y: TFIDF_like (o Repeticiones)
Tamaño burbuja: Repeticiones
Leyenda/Color: Mundo o Saga
Tabla o matriz:
Filas: Libro_Origen
Columnas: Anio_Publicacion, Saga, Mundo
Medida: suma de Repeticiones
Segmentadores:
Termino (para seguir un personaje/Concepto concreto en el tiempo)
Uso:
Ver cómo la presencia de un término (por ejemplo Kaladin, Kelsier, “Alomancia”) evoluciona según el orden y año de publicación de los libros.
Pantalla 3 – Personajes vs Libros
Objetivo: centrarse en personajes del Cosmere y su importancia relativa en cada libro/saga.
Preparación en Power BI:
Crea una columna calculada Es_Personaje:
Verdadero cuando la categoría interna sea “Personaje” (según cómo quieras filtrar; si quieres, puedes filtrar directamente por Termino lista blanca o derivar esta info desde el modelo si la añades al CSV).
Visuales:
Matriz:
Filas: Termino
Columnas: Libro_Origen
Valores: suma de Repeticiones o TFIDF_like
Filtro de nivel de visual: solo términos considerados “Personaje”.
Gráfico de barras horizontales:
Eje Y: Termino (Top 15 personajes)
Valor: suma de Repeticiones
Filtro de página: Es_Global = TRUE() para ver personajes que aparecen en muchos libros.
Segmentadores:
Saga
Libro_Origen
Uso:
Comparar qué personajes dominan cada libro, ver “reparto de foco” por novela/saga.
Pantalla 4 – Conceptos y magia (Alomancia, Potenciación, etc.)
Objetivo: analizar términos de tipo “Concepto” (magias, mundos, conceptos cosmerológicos).
Visuales:
Gráfico de barras:
Eje X: Termino (filtrado por Conceptos clave: Alomancia, Potenciación, Luz tormentosa, Cosmere, etc.)
Valor: suma de Repeticiones
Mapa de calor (Matrix/Heatmap):
Filas: Termino
Columnas: Saga o Mundo
Valor: suma de TFIDF_like
Segmentador:
Mundo (para concentrarse, por ejemplo, solo en Scadrial o Roshar)
Uso:
Ver qué sistemas de magia o conceptos son más centrales en cada saga/mundo y cómo se solapan.