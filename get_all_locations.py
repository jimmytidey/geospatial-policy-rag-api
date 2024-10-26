from postgres import Postgres

def get_all_locations(labels=None):
    pg = Postgres()

    # Base query without a join, assuming columns are already in `locations`
    query = """
        WITH selected_data AS (
            SELECT 
                lat AS latitude,
                lng AS longitude,
                chunk_id,
                location_name,
                jsonb_build_object(
                    'title', title, 
                    'sections', sections,
                    'text', text,
                    'labels', openai_labels
                ) AS text_fragment
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
            COUNT(DISTINCT text_fragment) AS text_fragment_count,
            array_agg(DISTINCT text_fragment) AS text_fragments
        FROM 
            selected_data
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
                "text_fragments": row[4]
            }
            for row in result
        ]
    }

    return response
