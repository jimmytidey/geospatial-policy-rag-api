
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from postgres import Postgres 



def get_all_locations():
    
    pg = Postgres()

    query = """
        WITH joined_data AS (
            SELECT 
                locations.lat AS latitude,  -- Explicit alias for lat
                locations.lng AS longitude,  -- Explicit alias for lng
                locations.chunk_id AS chunk_id,  -- Explicit alias for chunk_id
                locations.location_name AS location_name,  -- Explicit alias for location_name
                jsonb_build_object(  -- Create a JSON object with title, sections, and text
                    'title', langchain_pg_embedding.cmetadata->>'title', 
                    'sections', langchain_pg_embedding.cmetadata->>'sections',
                    'text', langchain_pg_embedding.cmetadata->>'text'
                ) AS text_fragment  -- Create a custom JSON object for text fragments
            FROM 
                locations
            JOIN 
                langchain_pg_embedding 
            ON 
                locations.chunk_id = langchain_pg_embedding.id  -- Join on chunk_id and id
        )
        SELECT 
            latitude,  -- Named as latitude
            longitude,  -- Named as longitude
            array_agg(DISTINCT location_name) AS location_names,
            COUNT(DISTINCT text_fragment) AS text_fragment_count,  -- Named as text_fragment_count
            array_agg(DISTINCT text_fragment) AS text_fragments  -- Return the text fragments as an array of JSON objects
        FROM 
            joined_data
        GROUP BY 
            latitude, 
            longitude
        HAVING 
            COUNT(DISTINCT text_fragment) <= 20  -- Only include entries with 20 or fewer text fragments
        ORDER BY 
            text_fragment_count DESC  -- Order by the number of text fragments, in descending order
        LIMIT 500;
    """
    result = pg.query(query)
    
    # Structure the result into a dictionary with descriptive names
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

    # Convert the result to a JSON-compatible format using jsonable_encoder
    json_compatible_data = jsonable_encoder(response)

    # Return the JSON response
    return JSONResponse(content=json_compatible_data)