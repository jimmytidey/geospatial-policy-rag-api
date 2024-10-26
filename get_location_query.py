from postgres import Postgres
import json

def get_location_query(lat, lng, labels=None):
    pg = Postgres()

    # Base query without a join, assuming columns are already in `locations`
    query = """
        
            SELECT 
                lat AS latitude,
                lng AS longitude,
                chunk_id as chunk_id,
                location_name as location_name,
                title as title,
                sections as sections,
                text as text,
                openai_labels as openai_labels
            FROM 
                locations
    """

    # Apply label filtering if `labels` is provided
    if labels:
        labels_jsonb = json.dumps(labels)
        query += """
            WHERE openai_labels @> %s::jsonb
            AND lat = %s
            AND lng = %s
        """
        params = (labels_jsonb, lat, lng)
    else:
        query += """
            WHERE lat = %s
            AND lng = %s
        """
        params = (lat, lng)

    print(query)
    # Execute the query
    result = pg.query(query, params) if params else pg.query(query)


    return result
