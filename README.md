# Movie Knowledge Base — Neo4j Demo

A small Python demo that uses **Neo4j** as a knowledge base for movies: nodes for movies, people (actors/directors), and genres, with relationships for who acted in or directed what, and which genre a movie belongs to.

## What it does

- **Schema:** `Movie`, `Person`, `Genre` nodes; `ACTED_IN`, `DIRECTED`, `IN_GENRE` relationships.
- **Sample data:** A few movies (e.g. The Matrix, Inception, The Dark Knight, La La Land, John Wick) with directors, actors, and genres.
- **Example queries:**
  - List all movies
  - Movies by genre (e.g. Sci-Fi)
  - Movies by actor (e.g. Keanu Reeves)
  - Movies by director (e.g. Christopher Nolan)
  - Co-actors (who appeared in the same film as a given actor)
  - Full details for one movie (directors, actors, genres)

## Prerequisites

- **Python 3.8+**
- **Neo4j** (local or Aura):
  - [Neo4j Desktop](https://neo4j.com/download/) or
  - [Neo4j Aura](https://neo4j.com/cloud/aura/) (free tier) or
  - Docker: `docker run -e NEO4J_AUTH=neo4j/password -p 7474:7474 -p 7687:7687 neo4j`

## Setup

1. **Create a virtual environment (recommended):**
   ```bash
   python3 -m venv venv
   source venv/bin/activate   # Windows: venv\Scripts\activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Neo4j (optional):**  
   By default the script uses:
   - URI: `neo4j://localhost:7687`
   - User: `neo4j`
   - Password: `password`

   Override with environment variables:
   ```bash
   export NEO4J_URI="neo4j+s://xxxx.databases.neo4j.io"
   export NEO4J_USER="neo4j"
   export NEO4J_PASSWORD="your-password"
   ```

## Run the demo

```bash
python movie_knowledge_base.py
```

The script will:

1. Connect to Neo4j  
2. Clear existing data and create uniqueness constraints  
3. Insert the sample movie graph  
4. Run the example queries and print results  

You can inspect the graph in **Neo4j Browser** (e.g. `http://localhost:7474`) with queries like:

```cypher
MATCH (n) RETURN n LIMIT 50
```

## Project layout

- `movie_knowledge_base.py` — main script (schema, data, queries)
- `requirements.txt` — Python dependency (`neo4j` driver)
- `README.md` — this file
