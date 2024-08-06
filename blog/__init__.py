from .views import app
from .models import User

user = User("")
user.create_user_constraint()
# GraphDatabase.cypher.execute("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Tag) REQUIRE n.name IS UNIQUE")
# GraphDatabase.cypher.execute("CREATE CONSTRAINT IF NOT EXISTS FOR (n:Post) REQUIRE n.id IS UNIQUE")
# graph.cypher.execute("CREATE INDEX FOR :Post(date)")

