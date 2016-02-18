from pymongo import MongoClient
import networkx as nx
from networkx_viewer import Viewer


client = MongoClient('localhost', 27017)
db = client['kugsha']
collection = db['inmotion-pages']

G=nx.DiGraph()

for page in collection.find().limit(10):
    id = page[u'_id']
    url = str(page[u'url'].encode('ascii', 'ignore'))
    G.add_node(url)
    edgs = page[u'outbound']
    for ed in edgs:
        G.add_edge(url,ed)

app = Viewer(G)
app.mainloop()