from fastapi import HTTPException
from collections import Counter
import json
from helpers import get_openai_response 
from get_location_query import get_location_query
from postgres import Postgres

def get_location_summary(data):
    pg = Postgres()
    lat = data.get("lat")
    lng = data.get("lng")
    label = data.get("label")
    location_names = data.get("location_names")
    location_names_string = ", ".join(location_names)

    # get text chunks within a 20 meters radius
    text_chunks_location = get_location_query(lat, lng, 20, label) 

    # if there are no chunks 
    if not text_chunks_location:  
        raise HTTPException(status_code=404, detail="Location not found")

    prompt_snippets_location = build_prompt_snippets(text_chunks_location)

    # get text chunks within a 1500 meters radius
    text_chunks_near = get_location_query(lat, lng, 1500, label)

    # Remove the text chunks already retrived as part of the main locatin 
    text_chunks_near_no_dupes = [item for item in text_chunks_near if item not in text_chunks_location]
    prompt_snippets_near = build_prompt_snippets(text_chunks_near_no_dupes)
    

    # Get some NPPF content 
    top_labels = get_all_labels(text_chunks_location, text_chunks_near_no_dupes);

    print('lables')
    print(top_labels)


  # Build the query dynamically for each label with OR logic
    query_conditions = " OR ".join(["cmetadata->'openai_labels' @> %s::jsonb" for _ in top_labels])

    # Final query with exclusion condition
    query = f"""
        SELECT cmetadata->>'context_text' as context_text
        FROM langchain_pg_embedding
        WHERE cmetadata->>'title' = 'National Planning Policy Framework' 
        AND cmetadata->>'chunker' = 'sherpa'
        AND ({query_conditions})
        AND NOT cmetadata->'openai_labels' @> %s::jsonb
        LIMIT 5;
    """

    # Execute the query
    result = pg.query(query, [json.dumps([label]) for label in top_labels] + [json.dumps(['broken_fragment'])])

    nppf_snippets = ''
    for row in result:
        nppf_snippets += row['context_text']
        nppf_snippets += '\n\n'


    # write the prompt

    prompt = f"You are a planning and urbanism expert. You are describing local planning context for an area. Aim for around 150 words.  Please reference the titles of the documents you are referencing in the text provided, and try to draw from a range of documents."
    if label:
        prompt += f"\n\n In this instance, please particularly focus on topics connected to: {label}. "
    if location_names:
        prompt += f"\n\n Please also focus on the following locations: {location_names_string}."
        prompt += f"\n\n Break your summary into short paragrpahs" 

    full_prompt = prompt +  f"\n\n Details of policy documents near this location: \n\n{prompt_snippets_location}"
    full_prompt += f"\n\n Details of policy documents near this location: \n\n{prompt_snippets_near}"
    full_prompt += f"\n\n Please also provide a brief heading on how the above information relates to the National Planning Policy Framework. Some relevant parts of the NPPF are given below: \n\n"
    full_prompt += nppf_snippets
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

def get_all_labels(result1, result2):
    # Combine all labels into a single list including duplicates
    labels = [] 
    for row in result1:
        labels.extend(row['labels'])
    for row in result2:
        labels.extend(row['labels'])
    # return a list of the four most common labels
    # Count the occurrences of each label
    exclude_labels = ['context','map', 'policy', 'annex', 'truncated', 'broken_fragment', 'no_label', 'bad_start']
    filtered_labels = [item for item in labels if item not in exclude_labels]

    label_counts = Counter(filtered_labels)

    # Get the four most common labels
    most_common_labels = [label for label, count in label_counts.most_common(4)]

    return most_common_labels