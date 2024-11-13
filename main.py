from flask import Flask, request, jsonify, abort
from functools import wraps
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

app = Flask(__name__)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
API_KEY = os.getenv("API_KEY")

# Vérification de la clé API
def require_api_key(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        if request.args.get("api_key") != API_KEY:
            abort(401)  # Non autorisé
        return func(*args, **kwargs)
    return decorated_function

# Connexion à la base de données en fonction du nom de la base
def get_db_connection(db_name):
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=db_name
    )

# Endpoint pour récupérer les données d'une table avec colonnes et filtres
@app.route('/data/<table_name>', methods=['GET'])
@require_api_key
def get_table_data(table_name):
    db_name = request.args.get("db_name")
    columns = request.args.get("columns")
    filter_criteria = request.args.get("filter")

    if not db_name:
        abort(400, description="Nom de la base de données manquant")
    
    try:
        connection = get_db_connection(db_name)
        cursor = connection.cursor(dictionary=True)
        
        # Validation de l'existence de la table
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            abort(404, description="Table non trouvée")
        
        # Construction de la liste des colonnes à sélectionner
        columns_sql = "*" if not columns else ", ".join(columns.split(","))
        
        # Ajout du critère de filtre
        query = f"SELECT {columns_sql} FROM {table_name}"
        if filter_criteria:
            query += f" WHERE {filter_criteria} LIMIT 10"
        
        # Exécution de la requête
        cursor.execute(query)
        data = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        return jsonify(data), 200
    
    except mysql.connector.Error as err:
        abort(500, description=f"Erreur de connexion à la base de données : {err}")

# Endpoint pour récupérer un élément spécifique par id_unique
@app.route('/data/<table_name>/<id_unique>', methods=['GET'])
@require_api_key
def get_specific_item(table_name, id_unique):
    db_name = request.args.get("db_name")
    
    if not db_name:
        abort(400, description="Nom de la base de données manquant")
    
    try:
        connection = get_db_connection(db_name)
        cursor = connection.cursor(dictionary=True)
        
        # Validation de l'existence de la table
        cursor.execute("SHOW TABLES LIKE %s", (table_name,))
        if not cursor.fetchone():
            abort(404, description="Table non trouvée")
        
        # Requête pour récupérer l'élément par `id_unique`
        query = f"SELECT * FROM {table_name} WHERE id_unique = %s"
        cursor.execute(query, (id_unique,))
        data = cursor.fetchone()
        
        cursor.close()
        connection.close()
        
        if data is None:
            abort(404, description="Élément non trouvé")
        
        return jsonify(data), 200
    
    except mysql.connector.Error as err:
        abort(500, description=f"Erreur de connexion à la base de données : {err}")

# Point d'entrée de l'application
if __name__ == '__main__':
    app.run(debug=True)
