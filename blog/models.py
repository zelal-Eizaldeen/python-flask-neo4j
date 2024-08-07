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
            query = """MATCH (u:User {username: $username}) RETURN u.username AS user, u.password AS password"""
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
        print(f"I am {user}")
        if user.empty:
            return False
        return bcrypt.verify(password, user.iloc[0][1])

    def create_user_constraint(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """CREATE CONSTRAINT  IF NOT EXISTS FOR (n:User) REQUIRE n.username IS UNIQUE"""
        driver.execute_query(query,database_="users")
        return True
        driver.close()

    def add_post(self, title, tags, text):
        user = self.find()
        today = datetime.now()

        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """MERGE (p:Post {id: $id, title:$title, text:$text, timestamp:$timestamp, date:$date}) 
        RETURN p.id, 
        p.title, p.text, p.timestamp, p.date """
        records,summary,keys=driver.execute_query(
            query,
            {"id": str(uuid.uuid4()), "title": title, "text": text, "timestamp":int(today.strftime("%s")),"date":today.strftime("%F")},
            database_="users",
        )
        post=pd.DataFrame(records)
        query = """MATCH(u:User {username: $username}),
                        (p:Post)
                       MERGE (u)-[r:PUBLISH]->(p)"""
        driver.execute_query(
                     query, {"username":user.iloc[0][0]}, database_="users")

        tags = [x.strip() for x in tags.lower().split(",")]
        tags = set(tags)
        for tag in tags:
            query = """MATCH (u:User{username:$username}),(p:Post{id:$id})
                      MERGE(t:Tag{name:$name})-[:TAGGED]->(p)"""
            driver.execute_query(query,{"username":self.username,"id":post.iloc[0][0],"name":tag},
                                 database_="users")





# user1 = User("Hiba")
# user1.register("YES")


