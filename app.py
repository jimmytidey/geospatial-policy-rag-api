from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from get_all_locations import get_all_locations
from get_location_summary import get_location_summary

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
async def locations():
    return get_all_locations()

@app.post("/summarise_location")
async def summarise_location(request: Request):
    data = await request.json()
    return get_location_summary(data)