import pymysql
import csv
import boto3

db_host = "mysql_universidad"       
db_user = "root"       
db_pass = "utec"     
db_name = "universidad"       
db_table = "alumnos"      

fichero_csv = "data.csv"

nombre_bucket = "diazzz-storage"
ruta_s3 = "ingesta/" + fichero_csv

def exportar_mysql_a_csv():
    """Conecta a MySQL, lee la tabla completa y guarda en CSV"""
    try:
        # Conexión MySQL
        conn = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_pass,
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Conectado a MySQL ✅")

        with conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {db_table}")
            rows = cursor.fetchall()

            if not rows:
                print("La tabla está vacía ❌")
                return False

            # Escribir en CSV
            with open(fichero_csv, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

            print(f"Archivo CSV generado: {fichero_csv} ✅")
            return True

    except Exception as e:
        print("Error en exportación MySQL:", e)
        return False

    finally:
        if 'conn' in locals():
            conn.close()


def subir_csv_a_s3():
    """Sube el archivo CSV a S3"""
    try:
        s3 = boto3.client('s3')
        s3.upload_file(fichero_csv, nombre_bucket, ruta_s3)
        print(f"Archivo subido a S3: s3://{nombre_bucket}/{ruta_s3} ✅")
    except Exception as e:
        print("Error subiendo a S3:", e)


if __name__ == "__main__":
    if exportar_mysql_a_csv():
        subir_csv_a_s3()
        print("Ingesta completada 🚀")
    else:
        print("Ingesta fallida ❌")
