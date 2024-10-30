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
        COUNT(DISTINCT chunk_id) AS chunk_count
    FROM 
        locations
    WHERE 
        (%s::jsonb = '[]'::jsonb OR openai_labels @> %s::jsonb)  -- Handle empty array case
        AND NOT openai_labels ? 'broken_fragment'
        AND (
            (geolocation_range = 'neighbourhood' AND distance_from_document_geom < 6000) OR
            (geolocation_range = 'quarter' AND distance_from_document_geom < 10000) OR
            (geolocation_range = 'city' AND distance_from_document_geom < 30000) OR
            geolocation_range IS NULL  -- Include rows without a geolocation_range for completeness
        )
    GROUP BY 
        ST_Y(geom), 
        ST_X(geom)
    HAVING 
        COUNT(DISTINCT chunk_id) <= 30
    ORDER BY 
        chunk_count DESC
    """

    # Set `params` for execution
    params = (labels_jsonb, labels_jsonb)


    print(query)

    # Execute the query
    result = pg.query(query, params) if params else pg.query(query)

    return result


   