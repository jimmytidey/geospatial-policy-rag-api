import json
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from postgres import Postgres

def get_map_pins(labels=None):
    pg = Postgres()

    # Base query without a join, assuming columns are already in `locations`
    query = """
        WITH selected_data AS (
            SELECT 
                lat AS latitude,
                lng AS longitude,
                chunk_id AS chunk_id,
                location_name AS location_name,

            FROM 
                locations
    """

    # Apply label filtering if `labels` is provided
    if labels:
        labels_jsonb = json.dumps(labels)
        query += """
            WHERE openai_labels @> %s::jsonb
        """
        params = (labels_jsonb,)
    else:
        params = None

    # Aggregate results based on latitude and longitude
    query += """
        )
        SELECT 
            latitude,
            longitude,
            array_agg(DISTINCT location_name) AS location_names,
            COUNT(DISTINCT chunk_id) AS chunk_count,
         
        FROM 
            selected_data
        GROUP BY 
            latitude, 
            longitude
        HAVING 
            COUNT(DISTINCT chunk_count) <= 10 AND 
            COUNT(DISTINCT chunk_count) > 4
        ORDER BY 
            text_fragment_count DESC
        LIMIT 200;
    """

    # Execute the query
    result = pg.query(query, params) if params else pg.query(query)
    
    # Format the response
    response = {
        "locations": [
            {
                "latitude": row[0],
                "longitude": row[1],
                "location_names": row[2],
                "text_fragment_count": row[3],
            }
            for row in result
        ]
    }

    return response


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