from neo4j import GraphDatabase
import pandas as pd

from passlib.hash import bcrypt
from datetime import datetime
import uuid
import os


from dotenv import load_dotenv

load_dotenv()

password = os.getenv("PASSWORD")
URI = "neo4j://localhost:7687"
AUTH = ("neo4j", password)

class User:
    def __init__(self, username):
        self.username = username
    def create_users_db(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """CREATE DATABASE users IF NOT EXISTS"""
        driver.execute_query(query)
        return True
        driver.close()

    def delete_all_nodes(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """MATCH (n) DETACH DELETE n"""
        driver.execute_query(query)
        return True
        driver.close()

    def find(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
            query = """MATCH (u:User {username: $username}) 
            RETURN u.username AS user, u.password AS password"""
            records,  summary, keys =driver.execute_query(
                query,
                {"username": self.username},
                database_="users",
            )
            df = pd.DataFrame(records)
            return df

    def register(self, password):
        if self.find().empty:
            with GraphDatabase.driver(URI, auth=AUTH) as driver:
                driver.verify_connectivity()
            query = """MERGE (:User {username: $username, password:$password})"""
            driver.execute_query(
                query,
                {"username": self.username, "password": bcrypt.encrypt(password)},
                database_="users",
            )
            driver.close()
            return True
        return False

    def verify_password(self, password):
        user = self.find()
        if user.empty:
            return False
        return bcrypt.verify(password, user.iloc[0][1])

    def add_post(self, title, tags, text):
        user = self.find()
        today = datetime.now()

        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """MATCH(u:User {username: $username})
        MERGE (p:Post {post_id: $id, title:$title, text:$text, 
        timestamp:$timestamp, date:$date}) 
        MERGE (u)-[r:PUBLISHED]->(p)
        RETURN p.post_id, 
        p.title, p.text, p.timestamp, p.date """
        records,summary,keys=driver.execute_query(
            query,
            {"username":user.iloc[0][0], "id": str(uuid.uuid4()), "title": title, "text": text, "timestamp":int(today.strftime("%s")),"date":today.strftime("%F")},
            database_="users",
        )
        post=pd.DataFrame(records)
        tags = [x.strip() for x in tags.lower().split(",")]
        tags = set(tags)
        for tag in tags:
            query = """MATCH (u:User{username:$username}),(p:Post{post_id:$post_id})
                      MERGE(t:Tag{name:$name})-[:TAGGED]->(p)"""
            driver.execute_query(query,{"username":self.username,
                "post_id":post.iloc[0][0],"name":tag},
                                 database_="users")

    def like_post(self, post_id):
        user = self.find()
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """MATCH (p:Post {post_id:$post_id}), 
                  (u:User {username: $username})
                   MERGE (u)-[r:LIKES]->(p)
                   RETURN p.post_id, 
                   p.title, p.text, p.timestamp, p.date """

        return driver.execute_query(
            query,
            {"post_id":post_id, "username": self.username},
            database_="users"
        )

    def recent_posts(self, n):
        query = """
        MATCH (user:User)-[:PUBLISHED]->(post:Post)<-[:TAGGED]-(tag:Tag)
        WHERE user.username = $username
        RETURN post, COLLECT(tag.name) AS tags
        ORDER BY post.timestamp DESC LIMIT $n
        """

        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()

        records, summary, keys = driver.execute_query(query,
                {"username": self.username, "n": n}, database_="users")

        return records

    def similar_users(self, n):
        query = """
        MATCH (user1:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag1:Tag),
        (user2:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag2:Tag)
        WHERE user1.username = $username AND user1 <> user2 AND tag1.name=tag2.name
        WITH user2, COLLECT(DISTINCT tag1.name) AS tags, COUNT(DISTINCT tag1.name) AS tag_count
        ORDER BY tag_count DESC LIMIT $n
        RETURN user2.username AS similar_user, tags
        """
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()

        records, summary, keys = driver.execute_query(query,
                {"username": self.username, "n": n}, database_="users")
        return records

    def commonality_of_user(self, user):
        query1 = """
                MATCH (user1:User)-[:PUBLISHED]->(post:Post)<-[:LIKES]-(user2:User)
                WHERE user1.username = $username1 AND user2.username = $username2
                RETURN COUNT(post) AS likes
                """
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()

        records,summary,keys = driver.execute_query(query1,
                    { "username1":self.username, "username2":user.username},
                                                    database_="users")
        for record in records:
            likes = record['likes']

        query2 = """
                MATCH (user1:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag1:Tag),
                      (user2:User)-[:PUBLISHED]->(:Post)<-[:TAGGED]-(tag2:Tag)
                WHERE user1.username = $username1 AND user2.username = $username2 
                AND tag1.name=tag2.name
                RETURN COLLECT(DISTINCT tag1.name) AS tags
                """

        records,summary,keys = driver.execute_query(query2,
                { "username1":self.username, "username2":user.username}, database_="users")
        for record in records:
            tags = record['tags']

        return {"likes": likes, "tags": tags}

    # -------- CONSTRAINTS --------------
    def create_user_constraint(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """CREATE CONSTRAINT  IF NOT EXISTS FOR (n:User) 
               REQUIRE n.username IS UNIQUE"""
        driver.execute_query(query,database_="users")
        return True
        driver.close()

    def create_post_index(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """CREATE INDEX post_date_index IF NOT EXISTS FOR (p:Post) ON (p.date)"""
        driver.execute_query(query,database_="users")
        driver.close()

    def create_post_constraint(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """CREATE CONSTRAINT IF NOT EXISTS FOR (n:Post) REQUIRE n.id IS UNIQUE"""
        driver.execute_query(query, database_="users")
        driver.close()

    def create_tag_constraint(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """CREATE CONSTRAINT IF NOT EXISTS FOR (n:Tag) REQUIRE n.name IS UNIQUE"""
        driver.execute_query(query, database_="users")
        driver.close()



def todays_recent_posts(n):
    query = """
    MATCH (user:User)-[:PUBLISHED]->(post:Post)<-[:TAGGED]-(tag:Tag)
    WHERE post.date = $today
    RETURN user.username AS username, post, COLLECT(tag.name) AS tags
    ORDER BY post.timestamp DESC LIMIT $n
    """
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        driver.verify_connectivity()
    today = datetime.now().strftime("%F")

    records, summary, keys=driver.execute_query(query,
                         {"today":today, "n":n}, database_="users")

    return records



