from postgres import Postgres
import json
def get_location_query(lat, lng, radius, labels=[]):
    pg = Postgres()

    # Ensure labels JSON format is consistent for filtering
    labels_jsonb = json.dumps(labels) if labels else '[]'

    # Base query for locations with fixed filters
    query = """
        SELECT 
            ST_Y(geom) AS lat,  
            ST_X(geom) AS lng,  
            chunk_id AS chunk_id,
            location_name AS location_name,
            title AS title,
            sections AS sections,
            text AS text,
            openai_labels AS labels,
            ST_Distance(
                geom::geography,  
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            ) AS distance_from_location  -- Distance in meters from query center
        FROM 
            locations
        WHERE 
            NOT openai_labels ? 'broken_fragment'  -- Exclude records with 'broken_fragment'
            AND (
                openai_labels @> %s::jsonb OR %s::jsonb = '[]'::jsonb  -- Apply labels filter if provided, skip otherwise
            )
            AND ST_DWithin(
                geom::geography,  
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography, 
                %s
            )
            AND (
                (geolocation_range = 'neighbourhood' AND distance_from_document_geom < 6000) OR
                (geolocation_range = 'quarter' AND distance_from_document_geom < 10000) OR
                (geolocation_range = 'city' AND distance_from_document_geom < 30000) OR
                geolocation_range IS NULL  -- Include rows without a geolocation_range for completeness
            )            
        ORDER BY distance_from_location ASC
        LIMIT 10
    """
    
    # Set up parameters for query
    params = (lng, lat, labels_jsonb, labels_jsonb, lng, lat, radius)
    
    # Execute query
    result = pg.query(query, params)

    return result
