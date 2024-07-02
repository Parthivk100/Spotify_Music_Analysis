import dotenv
import pandas as pd
from neo4j import GraphDatabase

load_status = dotenv.load_dotenv('Neo4j-40624ccf-Created-2024-06-01.txt')
if load_status is False:
    raise RuntimeError('Environment Variables not loaded')

URI = "neo4j+s://40624ccf.databases.neo4j.io"
AUTH = ("neo4j", "759q8O0vPdbeamevM3pNwYL-mKY134A-sisHPSftRKE")

with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()

def run_query(uri, auth, query):
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as session:
            session.run(query)
            print('Sucess')
    except Exception as e:
        raise RuntimeError('Failed :(')
    finally:
        driver.close()

q1 = """
LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1vUe0pjMiQ7ly-VURZj0zvtI1K2pgK2c_' AS row
CREATE (:Song {
  name: row.Name,
  date: row.Date,
  energy: toFloat(row.Energy),
  tempo: toFloat(row.Tempo),
  key: toFloat(row.Key)
});
"""
#run_query(URI, AUTH, q1)

q2 = """
MATCH (a:Song)
WITH collect(a) AS songs
UNWIND songs AS a
UNWIND songs AS b
WITH a, b
WHERE a <> b AND a.name < b.name
WITH a, b,
     sqrt(
         (a.energy - b.energy)^2 + 
         (a.tempo - b.tempo)^2 + 
         (a.key - b.key)^2
     ) AS distance
WHERE distance < 0.55
CREATE (a)-[:CONNECTED {distance: distance}]->(b)
RETURN a.name, b.name, distance
"""

#run_query(URI, AUTH, q2)

q3 = """
MATCH (s:Song)
SET s.community = id(s);
"""

q4 = """
WITH 10 AS maxIterations
UNWIND range(1, maxIterations) AS iteration
MATCH (s:Song)-[:CONNECTED]->(neighbor:Song)
WITH s, neighbor.community AS newLabel, count(*) AS frequency
ORDER BY frequency DESC, newLabel DESC
WITH s, collect(newLabel)[0] AS mostFrequentLabel
SET s.community = mostFrequentLabel;
"""
#run_query(URI, AUTH, q3)
#run_query(URI, AUTH, q4)

def fetch_communities(uri, auth):
    query = """
    MATCH (s:Song)
    RETURN s.name AS name, s.community AS community
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        result = session.run(query)
        communities = [record.data() for record in result]
    driver.close()
    return pd.DataFrame(communities)


communities_df = fetch_communities(URI, AUTH)
#Finding clusters of four or more songs, using this # for our k-means clustering algorithm

community_counts = communities_df['community'].value_counts()

# Filter out communities with fewer than 4 nodes
large_communities = community_counts[community_counts >= 4].index.tolist()

# Define colors for communities
colors = ["red", "green", "blue", "yellow", "purple", "orange", "pink", "cyan", "brown", "gray", "magenta", "turquoise"]

# Map large communities to colors
community_colors = {community: colors[i % len(colors)] for i, community in enumerate(large_communities)}

# Update nodes with color property based on community
def assign_colors(uri, auth, community_colors, large_communities):
    driver = GraphDatabase.driver(uri, auth=auth)
    try:
        with driver.session() as session:
            for community, color in community_colors.items():
                query = f"""
                MATCH (s:Song)
                WHERE s.community = {community}
                SET s.color = '{color}'
                """
                session.run(query)
                print(f"Assigned color {color} to community {community}")

            # Set color to black for smaller communities
            small_communities_query = f"""
            MATCH (s:Song)
            WHERE NOT s.community IN {large_communities}
            SET s.color = 'black'
            """
            session.run(small_communities_query)
            print("Assigned color black to smaller communities")
    except Exception as e:
        raise RuntimeError('Failed :(')
    finally:
        driver.close()

assign_colors(URI, AUTH, community_colors, large_communities)

# Fetch and display communities with colors
def fetch_communities_with_colors(uri, auth):
    query = """
    MATCH (s:Song)
    RETURN s.name AS name, s.community AS community, s.color AS color
    """
    driver = GraphDatabase.driver(uri, auth=auth)
    with driver.session() as session:
        result = session.run(query)
        communities = [record.data() for record in result]
    driver.close()
    return pd.DataFrame(communities)

communities_with_colors_df = fetch_communities_with_colors(URI, AUTH)
print(communities_with_colors_df)


"""
data = pd.read_csv('Spotify_Standardized.csv')
scores = []
for k in range(10, 20):
    kmeans = KMeans(n_clusters=k, n_init=10, random_state=0)
    labels = kmeans.fit_predict(data[['Energy', 'Tempo', 'Key']])
    scores.append(silhouette_score(data[['Energy', 'Tempo', 'Key']], labels))

print(scores)
"""










