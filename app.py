from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from get_top_locations import get_top_locations
from get_location_summary import get_location_summary
from typing import Optional, List

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {"message": "Route!"}

@app.get("/get_top_locations")
async def locations(labels: Optional[List[str]] = Query(None)):   
    locations = get_top_locations(labels=labels)
    json_compatible_data = jsonable_encoder(locations)
    return JSONResponse(content=json_compatible_data) 
    
@app.post("/get_location_summary")
async def summaries(request: Request):
    data = await request.json()
    return get_location_summary(data)