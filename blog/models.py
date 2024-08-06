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
            query = """MATCH (u:User {username: $username}) RETURN u.username AS user"""
            records,  summary, keys =driver.execute_query(
                query,
                {"username": self.username},
                database_="users",
            )
            df = pd.DataFrame(records)

            # df = df.rename(columns={0: 'Username'})
            # user = df['Username']
            print(df)
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
    def create_user_constraint(self):
        with GraphDatabase.driver(URI, auth=AUTH) as driver:
            driver.verify_connectivity()
        query = """CREATE CONSTRAINT  IF NOT EXISTS FOR (n:User) REQUIRE n.username IS UNIQUE"""
        driver.execute_query(query)
        return True
        driver.close()


# user1 = User("Hiba")
# user1.register("YES")


