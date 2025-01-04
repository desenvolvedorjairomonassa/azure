from azure.cosmos import CosmosClient, PartitionKey, exceptions
from dotenv import load_dotenv
import os
import __main__
import json

load_dotenv()

URI = os.getenv("COSMOS_URI")
KEY = os.getenv("COSMOS_KEY")
DATABASE_NAME = os.getenv("COSMOS_DATABASE_NAME")
CONTAINER_NAME = os.getenv("COSMOS_CONTAINER_NAME")

client = CosmosClient(URI, KEY)

# function create database in cosmos db
def create_database(database_name):
    try:
        database = client.create_database(database_name)
        print(f"Database '{database_name}' created successfully.")
        return database
    except exceptions.CosmosResourceExistsError:
        print(f"Database '{database_name}' already exists.")
        return client.get_database_client(database_name)

# function create container in cosmos db
def create_container(database_name=None, database=None, container_name=None, partition_key=None):
    if database_name is None and database is None:
        raise ValueError("Either database_name or database must be provided.")
    if database is None:
        database = client.get_database_client(database_name)
    try:
        container = database.create_container(id=container_name, partition_key=PartitionKey(path=partition_key))
        print(f"Container '{container_name}' created successfully.")
        return container
    except exceptions.CosmosResourceExistsError:
        print(f"Container '{container_name}' already exists.")
        return database.get_container_client(container_name)

# function in order to create item in container
def create_item(database_name=None, database=None, container_name=None, container=None, item={}):
    if database_name is None and database is None:
        raise ValueError("Either database_name or database must be provided.")
    if container_name is None and container is None:
        raise ValueError("Either container_name or container must be provided.")
    if container is None:
        container = database.get_container_client(container_name)
    if database is None:
        database = client.get_database_client(database_name)
    
    try:
        container.create_item(item)
        # print productid
        print(f"Item {item['ProductID']} created successfully.")
    except exceptions.CosmosResourceExistsError as e:
        print(f'Item existed')
    except exceptions.CosmosHttpResponseError as e:
        print(f"An error occurred: {e.message}")

# function in order to delete item
def delete_item (container_name=None, container=None, item=None, partition_key={}):
    if container is None:
        container = database.get_container_client(container_name)
    try:
        container.delete_item(item=item, partition_key=1)
        print("Item excluded")
    except exceptions.CosmosResourceNotFoundError as e:
        print('Item not found')

# function in order to upsert item in container
def upsert_item(database_name=None, database=None, container_name=None, container=None, item={}):
    if database_name is None and database is None:
        raise ValueError("Either database_name or database must be provided.")
    if container_name is None and container is None:
        raise ValueError("Either container_name or container must be provided.")
    if container is None:
        container = database.get_container_client(container_name)
    if database is None:
        database = client.get_database_client(database_name)

    try:
        container.upsert_item(item)
        print(f"Item {item['ProductID']} upserted successfully.")
    except exceptions.CosmosHttpResponseError as e:
        print(f"An error occurred: {e.message}")

# read item 
def read_item(database_name=None, database=None, container_name=None, container=None, itemID=None, partition_key=None):
    if database_name is None and database is None:
        raise ValueError("Either database_name or database must be provided.")
    if container_name is None and container is None:
        raise ValueError("Either container_name or container must be provided.")
    if container is None:
        container = database.get_container_client(container_name)
    if database is None:
        database = client.get_database_client(database_name)
    try:
        item = container.read_item(item=itemID, partition_key=partition_key)
    except exceptions.CosmosResourceExistsError as e:
        print('Item not found')

    return item

def query_items(database_name=None, database=None, container_name=None, container=None, query=None):
    if database_name is None and database is None:
        raise ValueError("Either database_name or database must be provided.")
    if container_name is None and container is None:
        raise ValueError("Either container_name or container must be provided.")
    if container is None:
        container = database.get_container_client(container_name)
    if database is None:
        database = client.get_database_client(database_name)
    if query is None:
        raise ValueError("No query")
    try:
        items = list(container.query_items(query=query, enable_cross_partition_query=True))
        print(f"Items queried successfully.")
        return items
    except exceptions.CosmosHttpResponseError as e:
        print(f"An error occurred: {e.message}")

if __name__ == '__main__':
    database = create_database('ProductID')
    container = create_container(database=database, container_name='ProductID', partition_key='/ProductID')
    
    dir = 'c:\ETL\\az-204\cosmodb\\'
    file = os.path.join(dir, 'json', 'product01.json')
    # insert item from import json at /json/Product*.json
    with open(file) as f:
        data = json.load(f)
    create_item(database=database, container=container, item=data)
    file = os.path.join(dir, 'json', 'product01-1.json')
    with open(file) as f:
        data = json.load(f)
    upsert_item(database=database, container=container, item=data)
    
    items = query_items(database=database, container=container,query="select * from c")
    for item in items: 
        print(f"ProductID {item['ProductID']}, ID {item['id']}")
    item = read_item(database=database, container=container, itemID='a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6', partition_key='CAM-2023-DSLR-123')
    
    if item is not None:
        print(f"Item found: ProductID {item['ProductID']}, ID {item['id']}")     
    
    # delete_item(container=container, item='a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6', partition_key='CAM-2023-DSLR-123')
