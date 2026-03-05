import os
import subprocess

def procesar_biblioteca(directorio_origen, directorio_destino):
    extensiones_admitidas = ('.azw3', '.mobi', '.epub')
    
    if not os.path.exists(directorio_origen):
        print(f"❌ Error: La carpeta de origen no existe: {directorio_origen}")
        return

    if not os.path.exists(directorio_destino):
        os.makedirs(directorio_destino)
        print(f"Carpeta creada: {directorio_destino}")

    convertidos = {os.path.splitext(f)[0] for f in os.listdir(directorio_destino) if f.endswith('.txt')}
    archivos_en_origen = [f for f in os.listdir(directorio_origen) if f.lower().endswith(extensiones_admitidas)]
    
    print(f"--- Iniciando proceso en: {directorio_origen} ---")
    print(f"Encontrados {len(archivos_en_origen)} libros potenciales.")

    for archivo in archivos_en_origen:
        nombre_base = os.path.splitext(archivo)[0]
        ruta_entrada = os.path.join(directorio_origen, archivo)
        ruta_salida = os.path.join(directorio_destino, f"{nombre_base}.txt")

        if nombre_base in convertidos:
            print(f"⏩ Saltando (ya existe): {nombre_base}")
            continue

        print(f"🔄 Convirtiendo: {archivo}...")
        
        try:
            # CORRECCIÓN: Usamos DEVNULL tanto para stdout como para stderr
            subprocess.run([
                "ebook-convert", 
                ruta_entrada, 
                ruta_salida,
                "--txt-output-format=plain"
            ], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            
            print(f"✅ Completado: {nombre_base}")
        except Exception as e:
            print(f"❌ Error con {archivo}: {e}")

    print("\n--- Proceso finalizado ---")

# --- CONFIGURACIÓN DE RUTAS DINÁMICAS ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CARPETA_LIBROS = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "ebooks"))
CARPETA_TXT = os.path.abspath(os.path.join(BASE_DIR, "..", "data", "raw"))

if __name__ == "__main__":
    print(f"DEBUG - Origen: {CARPETA_LIBROS}")
    print(f"DEBUG - Destino: {CARPETA_TXT}")
    procesar_biblioteca(CARPETA_LIBROS, CARPETA_TXT)