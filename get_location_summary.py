from fastapi import HTTPException
from helpers import get_openai_response 
from get_location_query import get_location_query

def get_location_summary(data):

    lat = data.get("lat")
    lng = data.get("lng")
    label = data.get("label")
    location_names = data.get("location_names")
    location_names_string = ", ".join(location_names)

    text_chunks = get_location_query(lat, lng, label)
    prompt_snippets = build_prompt_snippets(text_chunks)
    
    if not prompt_snippets:  
        raise HTTPException(status_code=404, detail="Location not found")
    
    prompt = f"You are a planning and urbanism expert. You are describing local planning context for an area. Aim for around 150 words.  Please reference the titles of the documents you are referencing in the text provided, and try to draw from a range of documents."
    if label:
        prompt += f"\n\n In this instance, please particularly focus on topics connected to: {label}. "
    if location_names:
        prompt += f"\n\n Please also focus on the following locations: {location_names_string}."
        prompt += f"\n\n Break your summary into short paragrpahs" 

    full_prompt = prompt +  f"\n\n Text for summarization: \n\n{prompt_snippets}"
    try:
        summary = get_openai_response(full_prompt)
        return {
            "summary": summary,
            "prompt": prompt,
            "text_chunks": text_chunks
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
