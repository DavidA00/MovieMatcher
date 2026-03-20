# MovieMatcher — AI-Powered Movie Night

A DataScout-inspired movie discovery interface powered by Neo4j knowledge graphs, GAT embeddings, and LLM reasoning.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Next.js Frontend (React + Tailwind + D3)               │
│  ├── Search bar + reformulation suggestions             │
│  ├── Filter sidebar (genres, decades, genre mixer)      │
│  ├── Movie result grid with AI reasoning                │
│  ├── Detail panel with KG force-graph                   │
│  └── Movie mixer (connector movies)                     │
└──────────────────┬──────────────────────────────────────┘
                   │ /api/* proxy
┌──────────────────▼──────────────────────────────────────┐
│  FastAPI Backend                                         │
│  ├── LangGraph pipeline (reformulate → filter →         │
│  │   search → explain)                                  │
│  ├── Gemini LLM (reformulations, explanations,          │
│  │   filter suggestions, group summaries)                │
│  ├── Neo4j vector ANN (semantic + graph indices)         │
│  └── Session store (in-memory, per-user)                │
└──────────────────┬──────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────┐
│  Neo4j Knowledge Graph                                   │
│  ├── Movie nodes (with embedding_semantic +             │
│  │   embedding_graph from GAT)                          │
│  ├── Genre / Actor / Director / Keyword / Decade nodes  │
│  └── Vector indices: movie_semantic_idx,                │
│      movie_graph_idx                                     │
└─────────────────────────────────────────────────────────┘
```

## Setup

### Prerequisites
- Python 3.11+
- Node.js 18+
- Neo4j instance with your movie graph loaded
- Google Cloud project with Gemini API access
- Tavily API key (for web search)

### 1. Environment variables

Create a `.env` file in the `backend/` directory:

```env
NEO4J_URI=bolt://localhost:7687
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_password
GOOGLE_CLOUD_PROJECT=your-project-id
GOOGLE_CLOUD_LOCATION=us-central1
GOOGLE_API_KEY=your-gemini-api-key
TAVILY_API_KEY=tvly-your-key
```

### 2. Backend

```bash
cd backend

# Copy your existing scripts into the backend folder
cp /path/to/search_functions.py .
cp /path/to/movie_agents.py .

# Install dependencies
pip install -r requirements.txt

# Run the API
python main.py
# → serves on http://localhost:8000
```

### 3. Frontend

```bash
cd frontend

# Install dependencies
npm install

# Run dev server
npm run dev
# → serves on http://localhost:3000
```

The Next.js dev server proxies `/api/*` requests to the FastAPI backend.

## Features

### DataScout-inspired search
- **Query reformulations**: 4 dimension-diversified suggestions (mood, era, style, theme, comparison, negative, character, pacing) generated after each search
- **Suggested filters**: AI-inferred genre and decade filters based on the query
- **AI reasoning**: Per-movie explanations grounded in KG context, explaining *why* each result matches

### Dual embedding search
- **Semantic search** (Jina v5-nano, 256-dim): Matches the vibe/mood of natural language queries
- **Graph search** (GAT, 256-dim): Leverages structural KG relationships (cast, director, genre overlap)
- **Hybrid mode**: Blends both for best results

### Genre mixer
- Slider to blend two genres (e.g. 60% Thriller + 40% Comedy)
- Uses genre entity embeddings from the GAT to steer the graph-space query vector

### Era slider
- Steer results toward a decade's "feel" using decade node embeddings
- A 2015 film can "feel like the 80s" based on its structural KG position

### Movie mixer
- Select 2+ movies, find the centroid of their embeddings
- "Connector movies" — films at the intersection of your selections
- Toggle between structural (same cast/genre) and thematic (similar plot) modes

### Interactive KG visualization
- D3 force-directed graph showing a movie's neighborhood
- Nodes colored by type (Movie, Genre, Actor, Director, Keyword, Decade)
- Draggable, with tooltips

### Like/dislike feedback loop
- Liking/disliking movies updates your session state
- Subsequent searches incorporate your preference history
- Configurable "preference intensity" slider (explore ↔ personalize)

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/search` | POST | Full LangGraph pipeline search |
| `/api/filters` | GET | Available genres and decades |
| `/api/feedback` | POST | Like/dislike a movie |
| `/api/movie/detail` | POST | Full movie metadata + decade feel |
| `/api/movie/neighborhood` | POST | KG subgraph for force-graph |
| `/api/mixer` | POST | Connector movies from selections |
| `/api/cypher` | POST | Natural language → Cypher |
| `/api/group/ready` | POST | Multi-user group recommendations |
| `/api/session/create` | POST | Create new user session |
| `/api/session/:id` | GET | Get session state |

## Next Steps (multi-user)
- Kahoot-style join links for group movie night sessions
- Per-user AI summaries of similarities and differences
- Persona agent (simulated friend discussions about movies)
- Group consensus recommendations via connector movie search
