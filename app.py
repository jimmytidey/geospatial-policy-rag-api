from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware

from get_all_locations import get_all_locations
from get_map_pins import get_map_pins
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

@app.get("/locations")
async def locations(labels: Optional[List[str]] = Query(None)):    
    return get_all_locations(labels=labels)


@app.post("/summarise_location")
async def summarise_location(request: Request):
    data = await request.json()
    return get_location_summary(data)