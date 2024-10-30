from fastapi import HTTPException
from helpers import get_openai_response 
from get_location_query import get_location_query

def get_location_summary(data):

    lat = data.get("lat")
    lng = data.get("lng")
    label = data.get("label")
    location_names = data.get("location_names")
    location_names_string = ", ".join(location_names)

    text_chunks_location = get_location_query(lat, lng, 20, label)
    prompt_snippets_location = build_prompt_snippets(text_chunks_location)

    text_chunks_near = get_location_query(lat, lng, 1500, label)

    # Remove the text chunks already retrived as part of the main locatin 
    text_chunks_near_no_dupes = [item for item in text_chunks_near if item not in text_chunks_location]

    prompt_snippets_near = build_prompt_snippets(text_chunks_near_no_dupes)
    
    if not text_chunks_location:  
        raise HTTPException(status_code=404, detail="Location not found")
    
    prompt = f"You are a planning and urbanism expert. You are describing local planning context for an area. Aim for around 150 words.  Please reference the titles of the documents you are referencing in the text provided, and try to draw from a range of documents."
    if label:
        prompt += f"\n\n In this instance, please particularly focus on topics connected to: {label}. "
    if location_names:
        prompt += f"\n\n Please also focus on the following locations: {location_names_string}."
        prompt += f"\n\n Break your summary into short paragrpahs" 

    full_prompt = prompt +  f"\n\n Details of policy documents near this location: \n\n{prompt_snippets_location}"
    full_prompt += f"\n\n Details of policy documents near this location: \n\n{prompt_snippets_near}"
    try:
        summary = get_openai_response(full_prompt)
        return {
            "summary": summary,
            "prompt": prompt,
            "text_chunks": text_chunks_location + text_chunks_near
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating summary: {str(e)}")

  
def build_prompt_snippets(result): 
    location_documents = []
    for row in result:
        # Convert RealDictRow to a regular dictionary
        row = dict(row)
        
        document = f"""
Document Title: {row['title']} 
Location Name: {row['location_name']}
Distance to location: {row['distance_from_location']}
Latitude: {row['lat']}, Longitude: {row['lng']}
Section Heading: {row['sections']}
Content:
-------
{row['text']}
--------------------------------------------\n

"""
        location_documents.append(document.strip())

    # Combine all location documents into a single string
    combined_text = "\n\n".join(location_documents)
    return combined_text
