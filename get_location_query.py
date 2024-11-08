from postgres import Postgres
import json
def get_location_query(lat, lng, radius, labels=[]):
    pg = Postgres()

    # Ensure labels JSON format is consistent for filtering
    labels_jsonb = json.dumps(labels) if labels else '[]'

    # Base query for locations with fixed filters
    query = """
        SELECT 
            ST_Y(locations.geom) AS lat,  
            ST_X(locations.geom) AS lng,  
            locations.chunk_id AS chunk_id,
            location_name AS location_name,
            document_title AS title,
            chunk_sections AS sections,
            chunk_text AS text,
            openai_topic_labels AS labels,
            ST_AsGeoJSON(geo_boundaries.geom)::json AS document_geom,
            ST_Distance(
                locations.geom::geography,  
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            ) AS distance_from_location  -- Distance in meters from query center
        FROM 
            locations

        JOIN text_chunks on text_chunks.chunk_id = locations.chunk_id 
        LEFT JOIN geo_boundaries on text_chunks.document_geo_boundary_id = geo_boundaries.id
        WHERE 
            NOT openai_topic_labels ? 'broken_fragment'  -- Exclude records with 'broken_fragment'
            AND (
                openai_topic_labels @> %s::jsonb OR %s::jsonb = '[]'::jsonb  -- Apply labels filter if provided, skip otherwise
            )
            AND ST_DWithin(
                locations.geom::geography,  
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 
                %s
            )
            AND (
                (geo_scope = 'neighbourhood' AND distance_from_document_geom < 6000) OR
                (geo_scope = 'quarter' AND distance_from_document_geom < 10000) OR
                (geo_scope = 'city' AND distance_from_document_geom < 30000) OR
                geo_scope IS NULL  -- Include rows without a geolocation_range for completeness
            )            
        ORDER BY distance_from_location ASC
        LIMIT 10
    """
    
    # Set up parameters for query
    params = (lng, lat, labels_jsonb, labels_jsonb, lng, lat, radius)

    print(query)
    print(params)
    
    # Execute query
    result = pg.query(query, params)

    return result
