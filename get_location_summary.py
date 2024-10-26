from fastapi import HTTPException
from helpers import get_openai_response 
from get_location_query import get_location_query

def get_location_summary(data):

    lat = data.get("lat")
    lng = data.get("lng")
    label = data.get("label")

    prompt_snippets = get_location_query(lat, lng, label)

    if not prompt_snippets:  
        raise HTTPException(status_code=404, detail="Location not found")
    
    prompt = f"You are a planning and urbanism expert. You are describing the policies using the below text. Aim for around 150 words. Focus on specific details, especially numbers and quantities, precise locations, or directly quote exact policies. Please reference the titles of the documents you are referencing in the text provided, and try to draw from a range of documents."
    if label:
        prompt += f"\n\n In this instance, please particularly focus on topics connected to: {label}"
    full_prompt = prompt +  f"\n\n Text for summarization: \n\n{prompt_snippets}"
    try:
        summary = get_openai_response(full_prompt)
        return {
            "summary": summary,
            "prompt": full_prompt
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating summary: {str(e)}")

  
def build_prompt_snippets(result): 
    location_documents = []
    for row in result:
        latitude, longitude, chunk_id, location_name, title, sections, text, openai_labels = row
        
        document = f"""
        Document Title: {title} 
        Location Name: {location_name}
        Latitude: {latitude}, Longitude: {longitude}
        
        Section Heading: {sections}
        
        Content:
        {text}
        """
        location_documents.append(document.strip())

    # Combine all location documents into a single string
    combined_text = "\n\n".join(location_documents)
    return combined_text