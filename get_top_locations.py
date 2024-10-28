import json

from postgres import Postgres
def get_top_locations(labels=None):
    pg = Postgres()

    # Start the base query directly from `locations`
    query = """
        SELECT 
            ST_Y(geom) AS lat,
            ST_X(geom) AS lng,
            array_agg(DISTINCT location_name) AS location_names,
            COUNT(DISTINCT chunk_id) AS chunk_count
        FROM 
            locations
    """

    # Add filtering for labels if provided
    if labels:
        labels_jsonb = json.dumps(labels)
        query += """
            WHERE openai_labels @> %s::jsonb
        """
        params = (labels_jsonb,)
    else:
        params = None

    # Add grouping and conditions for aggregation
    query += """
        GROUP BY 
            ST_Y(geom), 
            ST_X(geom)
        HAVING 
            COUNT(DISTINCT chunk_id) <= 10 AND 
            COUNT(DISTINCT chunk_id) > 4
        ORDER BY 
            chunk_count DESC
        LIMIT 200;
    """

    print(query)

    # Execute the query
    result = pg.query(query, params) if params else pg.query(query)

    return result


   