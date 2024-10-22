from fastapi import HTTPException
from helpers import get_openai_response 


def get_location_summary(data):

    location_names = data.get("location_names")
    text_fragments = data.get("text_fragments")

    if not isinstance(location_names, list) or not isinstance(text_fragments, list):
        raise HTTPException(status_code=400, detail="Invalid input: 'location_names' and 'text_fragments' are required.")

    combined_text = "\n".join(text_fragments)
    locations_names_string = ', '.join(location_names)

    prompt = f"You are planning an urbanism expert. You are describing the policies around these locations: {locations_names_string}. Aim for around 150 words. Focus on specific details, especially numbers and quantities, precise locations, or directly quote exact policies. Please reference the titles of the documents you are referencing in the text provided, and try to draw from a range of documents. Text for summarization:"
    full_prompt = prompt +  f"\n\n{combined_text}"
    try:
        summary = get_openai_response(full_prompt)
        return {
            "summary": summary,
            "prompt": prompt
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating summary: {str(e)}")

  
