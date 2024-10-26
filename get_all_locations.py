import json
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from postgres import Postgres

def get_all_locations(labels=None):
    pg = Postgres()



    # Modify the base query with an optional filter for openai_label
    query = """
        WITH joined_data AS (
            SELECT 
                locations.lat AS latitude,
                locations.lng AS longitude,
                locations.chunk_id AS chunk_id,
                locations.location_name AS location_name,
                jsonb_build_object(
                    'title', langchain_pg_embedding.cmetadata->>'title', 
                    'sections', langchain_pg_embedding.cmetadata->>'sections',
                    'text', langchain_pg_embedding.cmetadata->>'text',
                    'labels', langchain_pg_embedding.cmetadata->'openai_labels'
                ) AS text_fragment
            FROM 
                locations
            JOIN 
                langchain_pg_embedding 
            ON 
                locations.chunk_id = langchain_pg_embedding.id
    """

    # Apply the filter directly in the CTE to limit to fragments with the specified labels
    if labels:
        print(labels)
        labels_jsonb = json.dumps(labels)
        query += """
            WHERE langchain_pg_embedding.cmetadata->'openai_labels' @> %s::jsonb
        """
        params = (labels_jsonb,)
    else:
        print("No labels")
        params = None

    # Complete the CTE and aggregate results
    query += """
        )
        SELECT 
            latitude,
            longitude,
            array_agg(DISTINCT location_name) AS location_names,
            COUNT(DISTINCT text_fragment) AS text_fragment_count,
            array_agg(DISTINCT text_fragment) AS text_fragments
        FROM 
            joined_data
        GROUP BY 
            latitude, 
            longitude
        HAVING 
            COUNT(DISTINCT text_fragment) <= 10 AND 
            COUNT(DISTINCT text_fragment) > 4
        ORDER BY 
            text_fragment_count DESC
        LIMIT 200;
    """

    result = pg.query(query, params) if params else pg.query(query)

    
    # Check if result is empty and handle it
    if not result:
        response = {"locations": []}  # Return an empty list if no data

    else:
        # Process results for JSON response
        response = {
            "locations": [
                {
                    "latitude": row[0],
                    "longitude": row[1],
                    "location_names": row[2],
                    "text_fragment_count": row[3],
                    "text_fragments": row[4]
                }
                for row in result
            ]
        }

    json_compatible_data = jsonable_encoder(response)
    return JSONResponse(content=json_compatible_data)