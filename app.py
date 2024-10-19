from fastapi import FastAPI, HTTPException, Request

from postgres import Postgres 
from helpers import summarise_text
from fastapi.middleware.cors import CORSMiddleware


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

pg = Postgres()

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/locations")
def get_all_locations():
    # SQL query to join the `locations` and `langchain_pg_embedding` tables
    query = """
        WITH joined_data AS (
            SELECT 
                locations.lat AS latitude,  -- Explicit alias for lat
                locations.lng AS longitude,  -- Explicit alias for lng
                locations.chunk_id AS chunk_id,  -- Explicit alias for chunk_id
                locations.location_name AS location_name,  -- Explicit alias for location_name
                langchain_pg_embedding.cmetadata->>'text' AS text_fragment  -- Extract the text from cmetadata with explicit alias
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
            array_agg(DISTINCT text_fragment) AS text_fragments,  -- Named as text_fragments
            array_agg(DISTINCT location_name) AS location_names,  -- Named as location_names
            COUNT(DISTINCT text_fragment) AS text_fragment_count  -- Named as text_fragment_count
        FROM 
            joined_data
        GROUP BY 
            latitude, 
            longitude
        HAVING 
            COUNT(DISTINCT text_fragment) <= 20  -- Only include entries with 20 or fewer text fragments
        ORDER BY 
            text_fragment_count DESC;  -- Order by the number of text fragments, in descending order
    """
    
    # Execute the query using pg.query() method
    result = pg.query(query)
    
    # Assuming pg.query() returns a list of dict-like rows
    return result

@app.post("/summarise_location")
async def summarise_location_text(request: Request):
    # Parse the incoming JSON request body
    data = await request.json()
    
    # Extract location_name and text_fragments from the request data
    location_name = data.get("location_name")
    text_fragments = data.get("text_fragments")
    
    # Validate input: Ensure both location_name and text_fragments are provided
    if not location_name or not isinstance(text_fragments, list):
        raise HTTPException(status_code=400, detail="Invalid input: 'location_name' and 'text_fragments' are required.")

    # Call the helper function to summarize the text
    summary = summarise_text(location_name, text_fragments)
    
    # Return the summary as a JSON response
    return {
        "location_name": location_name,
        "summary": summary
    }