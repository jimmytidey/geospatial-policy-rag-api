import psycopg2,os
from psycopg2 import pool, extras
from psycopg2.extras import RealDictCursor

from dotenv import load_dotenv
import json


load_dotenv()

class Postgres:

    def __init__(self):
      
        DATABASE_URL = os.environ["DATABASE_URL"]
        self.db_pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=DATABASE_URL  # Pass the URL as the DSN (Data Source Name)
        )            

    def insert(self, query, params=None):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            if params is None:
                cursor.execute(query)
            else:
                cursor.execute(query, params)
            
            self.conn.commit()
            cursor.close()
            self.db_pool.putconn(conn) 

        except psycopg2.Error as e:
            print("Error:", e)

    def query(self, query, params=None):
        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            if params is None:
                cursor.execute(query)
            else:   
                cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            self.db_pool.putconn(conn)
            return results

        except psycopg2.Error as e:
            print("Error:", e)


    def add_labels_to_cmetadata(self, record_id, labels, label_type):
        """
        Adds an array of labels to the 'labels' key in the cmetadata JSON column for a given record.
        If the 'labels' key does not exist, it will create it and add the new labels.
        
        :param record_id: The unique id of the record (assumed to be the primary key)
        :param labels: A list of labels (Python array) to add
        """
        try:
            conn = self.db_pool.getconn()
            with conn.cursor() as cursor:
                # Convert the Python list to a JSON array string
                labels_json = json.dumps(labels)
                print(labels_json)
                print("***********"*10)
                # Update the cmetadata JSON field by creating the 'labels' key or appending to it if it already exists
                
                json_path = '{' + label_type + '}'

                query = """
                    UPDATE langchain_pg_embedding
                    SET cmetadata = jsonb_set(
                        cmetadata,
                        %s,
                        (COALESCE(cmetadata->%s, '[]'::jsonb) || %s::jsonb),
                        true
                    )
                    WHERE id = %s;
                """

                # Executing the query, dynamically passing the JSON path as well
                cursor.execute(query, (json_path, label_type, labels_json, record_id))
                
                # Commit the transaction
                self.conn.commit()
                self.db_pool.putconn(conn) 
                print(f"Labels {labels} with {label_type} added to record with ID {record_id}.")

        except psycopg2.Error as e:
            print(f"Error updating labels: {e}")
            self.conn.rollback()

    def insert_text_fragments(self, batch):

        try:
            conn = self.db_pool.getconn()
            cursor = conn.cursor()
            insert_query = "INSERT INTO text_fragments (plain_text, full_text, metadata, vector, filename, hash) VALUES (%s, to_tsvector('english', %s), %s, %s, %s, %s)"     
            #interpolated_query = cursor.mogrify(insert_query, (text, text, json.dumps(metadata), vector, filename, hash))
            #print("Interpolated SQL query:", interpolated_query.decode("utf-8"))  
            extras.execute_batch(cursor, insert_query, batch)
            self.conn.commit()
            cursor.close()
            self.db_pool.putconn(conn) 

        except psycopg2.Error as e:
            print("Error:", e)

    def close(self):
        self.conn.close()
