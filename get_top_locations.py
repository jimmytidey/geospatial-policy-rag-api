import json

from postgres import Postgres
def get_top_locations(labels=[]):
    pg = Postgres()
    labels_jsonb = json.dumps(labels) if labels else '[]'
    # Start the base query directly from `locations`
    query = """
    SELECT 
        ST_Y(geom) AS lat,
        ST_X(geom) AS lng,
        array_agg(DISTINCT location_name) AS location_names,
        COUNT(DISTINCT locations.chunk_id) AS chunk_count
    FROM 
        locations 
    JOIN text_chunks on text_chunks.chunk_id = locations.chunk_id 
    WHERE 
        (%s::jsonb = '[]'::jsonb OR openai_topic_labels @> %s::jsonb)  -- Handle empty array case
        AND NOT openai_topic_labels ? 'broken_fragment'
        AND (
            (geo_scope = 'neighbourhood' AND distance_from_document_geom < 6000) OR
            (geo_scope = 'quarter' AND distance_from_document_geom < 10000) OR
            (geo_scope = 'city' AND distance_from_document_geom < 30000) OR
            geo_scope IS NULL  -- Include rows without a geolocation_range for completeness
        )
    GROUP BY 
        ST_Y(geom), 
        ST_X(geom)
    HAVING 
        COUNT(DISTINCT locations.chunk_id) <= 30
    ORDER BY 
        chunk_count DESC
    LIMIT 400
    """

    # Set `params` for execution
    params = (labels_jsonb, labels_jsonb)


    print(query)

    # Execute the query
    result = pg.query(query, params) if params else pg.query(query)

    return result


   