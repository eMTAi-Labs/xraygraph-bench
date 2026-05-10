#!/usr/bin/env python3
"""Set up NebulaGraph: register hosts, create space, create schema."""
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import time

config = Config()
config.max_connection_pool_size = 10
pool = ConnectionPool()
pool.init([("127.0.0.1", 9669)], config)

session = pool.get_session("root", "nebula")

# Add storage host
result = session.execute("ADD HOSTS 127.0.0.1:9779")
print("ADD HOSTS:", result.is_succeeded(), result.error_msg())

time.sleep(10)

# Check hosts
result = session.execute("SHOW HOSTS")
print("SHOW HOSTS:", result.is_succeeded())

# Create LDBC space
result = session.execute(
    "CREATE SPACE IF NOT EXISTS ldbc("
    "partition_num=10, replica_factor=1, "
    "vid_type=INT64"
    ")"
)
print("CREATE SPACE:", result.is_succeeded(), result.error_msg())

time.sleep(15)  # Wait for space to be ready

# Use the space
result = session.execute("USE ldbc")
print("USE ldbc:", result.is_succeeded(), result.error_msg())

# Create tags (node types)
tags = [
    "CREATE TAG IF NOT EXISTS Person(firstName STRING, lastName STRING, gender STRING, birthday STRING, creationDate STRING, locationIP STRING, browserUsed STRING)",
    "CREATE TAG IF NOT EXISTS Forum(title STRING, creationDate STRING)",
    "CREATE TAG IF NOT EXISTS Comment(creationDate STRING, locationIP STRING, browserUsed STRING, content STRING, length STRING)",
    "CREATE TAG IF NOT EXISTS Post(imageFile STRING, creationDate STRING, locationIP STRING, browserUsed STRING, language STRING, content STRING, length STRING)",
    "CREATE TAG IF NOT EXISTS Organisation(type STRING, name STRING, url STRING)",
    "CREATE TAG IF NOT EXISTS Place(name STRING, url STRING, type STRING)",
    "CREATE TAG IF NOT EXISTS Tag(name STRING, url STRING)",
    "CREATE TAG IF NOT EXISTS TagClass(name STRING, url STRING)",
]

for tag in tags:
    result = session.execute(tag)
    tag_name = tag.split("NOT EXISTS ")[1].split("(")[0]
    print(f"  {tag_name}: {result.is_succeeded()}", result.error_msg() if not result.is_succeeded() else "")

# Create edge types
edges = [
    "CREATE EDGE IF NOT EXISTS KNOWS()",
    "CREATE EDGE IF NOT EXISTS LIKES()",
    "CREATE EDGE IF NOT EXISTS HAS_MEMBER()",
    "CREATE EDGE IF NOT EXISTS HAS_CREATOR()",
    "CREATE EDGE IF NOT EXISTS REPLY_OF()",
    "CREATE EDGE IF NOT EXISTS CONTAINER_OF()",
    "CREATE EDGE IF NOT EXISTS HAS_MODERATOR()",
    "CREATE EDGE IF NOT EXISTS IS_LOCATED_IN()",
    "CREATE EDGE IF NOT EXISTS STUDY_AT()",
    "CREATE EDGE IF NOT EXISTS WORK_AT()",
]

for edge in edges:
    result = session.execute(edge)
    edge_name = edge.split("NOT EXISTS ")[1].split("(")[0]
    print(f"  {edge_name}: {result.is_succeeded()}", result.error_msg() if not result.is_succeeded() else "")

time.sleep(5)

# Create indexes
indexes = [
    "CREATE TAG INDEX IF NOT EXISTS idx_person ON Person()",
    "CREATE TAG INDEX IF NOT EXISTS idx_forum ON Forum()",
    "CREATE TAG INDEX IF NOT EXISTS idx_comment ON Comment()",
    "CREATE TAG INDEX IF NOT EXISTS idx_post ON Post()",
]
for idx in indexes:
    result = session.execute(idx)
    print(f"  Index: {result.is_succeeded()}", result.error_msg() if not result.is_succeeded() else "")

# Rebuild indexes
result = session.execute("REBUILD TAG INDEX idx_person, idx_forum, idx_comment, idx_post")
print("REBUILD INDEX:", result.is_succeeded(), result.error_msg())

print("\nNebulaGraph setup complete!")

session.release()
pool.close()
