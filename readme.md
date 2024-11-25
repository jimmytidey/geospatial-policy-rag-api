# Local Plans Geospatial RAG prototype

View this code running: https://localplans-ai-map-61938e5fe7bf.herokuapp.com

This project explores using AI to process and combine policy documents that refer to specific locations (locations, buildings, parks, etc). The goal is to decompose a corpus of policy documents into the locations they are talking about, then recombine what each policy document says about a locattion into a summaries per location of all the policies that apply.

It uses planning policy in England as an example, a topic where its easy to find lots of publicly available policy documents.

Planning in England is guided by the The National Planning Policy Framework - the national guide to built environment policy.

Local PLans are the key document that guides built environment policy in Local Authorities. Local Plans follow the guidances in the NPPF.

In turn, Neighbourhood Plans are informed by Local Plans, and are for smaller areas within Local Authorities.

This prototype investigates building a RAG that has features to support:

- Hierarchical sets of policy documents (NPPF->Local Plans->Neighbourhood Plans)
- Documents with geospatial inforation - policys about locations and areas.

# Structure

- This repo is the api code
- Data processing pipeline: https://github.com/jimmytidey/geospatial-policy-rag-pipeline
- Frontend: https://github.com/jimmytidey/geospatial-policy-rag-frontend

# Deploying

The project uses FastAPI

`git push heroku main` -- will deploy to heroku and start a FastAPI server

- This project contains all the configuration necessary to deploy to a Heroku instance
- Provide OpenAI and PostGRES paths in the .env file
