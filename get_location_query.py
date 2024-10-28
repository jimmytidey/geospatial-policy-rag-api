from postgres import Postgres
import json

def get_location_query(lat, lng, labels=None):
    pg = Postgres()

    print('Longitude:', lng)
    print('Latitude:', lat)

    # Base query for locations
    query = """
        SELECT 
            ST_Y(geom) AS lat,  
            ST_X(geom) AS lng,  
            chunk_id AS chunk_id,
            location_name AS location_name,
            title AS title,
            sections AS sections,
            text AS text,
            openai_labels AS labels
        FROM 
            locations
    """

    # Check if `labels` is provided to add label filtering
    if labels:
        labels_jsonb = json.dumps(labels)
        print('Labels JSONB:', labels_jsonb)
        query += """
            WHERE openai_labels @> %s::jsonb
            AND ST_DWithin(
                geom::geography,  
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 
                20  
            );
        """
        # Note: lng should come before lat for ST_MakePoint in the params
        params = (labels_jsonb, lng, lat)
    else:
        query += """
        WHERE 
            ST_DWithin(
                geom::geography,  
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 
                20  
            );
        """
        params = (lng, lat)

    print("Query:", query)

    # Execute the query
    result = pg.query(query, params) if params else pg.query(query)

    # Return an empty list if the result is None or empty
    return result if result else []
