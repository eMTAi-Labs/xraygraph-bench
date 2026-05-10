// ============================================================
// LDBC SNB Interactive v1 — LOAD CSV for xrayGraphDB
// Format: CsvCompositeMergeForeign-LongDateFormatter (pipe-delimited)
// Usage: cat ldbc_load_csv.cypher | xgconsole --host=127.0.0.1 --port=7687
//   OR: run each block via neo4j driver
// ============================================================
// Set SF variable: replace 'sf1' with 'sf10' for scale factor 10
// Base path: /opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter

// ============================================================
//  PHASE 1: INDEXES (create BEFORE loading for lookup performance)
// ============================================================

CREATE INDEX ON :Place(id);
CREATE INDEX ON :Organisation(id);
CREATE INDEX ON :TagClass(id);
CREATE INDEX ON :Tag(id);
CREATE INDEX ON :Person(id);
CREATE INDEX ON :Forum(id);
CREATE INDEX ON :Comment(id);
CREATE INDEX ON :Post(id);
CREATE INDEX ON :Message(id);

// ============================================================
//  PHASE 2: STATIC NODES
// ============================================================

// --- Place (1,460 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/static/place_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:Place {id: toInteger(row.id), name: row.name, url: row.url, type: row.type, isPartOf: toInteger(row.isPartOf)});

// --- TagClass (71 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/static/tagclass_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:TagClass {id: toInteger(row.id), name: row.name, url: row.url, isSubclassOf: toInteger(row.isSubclassOf)});

// --- Tag (16,080 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/static/tag_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:Tag {id: toInteger(row.id), name: row.name, url: row.url, hasType: toInteger(row.hasType)});

// --- Organisation (7,955 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/static/organisation_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:Organisation {id: toInteger(row.id), type: row.type, name: row.name, url: row.url, place: toInteger(row.place)});

// ============================================================
//  PHASE 3: DYNAMIC NODES
// ============================================================

// --- Person (9,892 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:Person {id: toInteger(row.id), firstName: row.firstName, lastName: row.lastName, gender: row.gender, birthday: toInteger(row.birthday), creationDate: toInteger(row.creationDate), locationIP: row.locationIP, browserUsed: row.browserUsed, place: toInteger(row.place), language: row.language, email: row.email});

// --- Forum (90,492 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/forum_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:Forum {id: toInteger(row.id), title: row.title, creationDate: toInteger(row.creationDate), moderator: toInteger(row.moderator)});

// --- Post (1,003,605 rows) — use periodic commit ---
CALL {
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/post_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:Post:Message {id: toInteger(row.id), imageFile: row.imageFile, creationDate: toInteger(row.creationDate), locationIP: row.locationIP, browserUsed: row.browserUsed, language: row.language, content: row.content, length: toInteger(row.length), creator: toInteger(row.creator), forumId: toInteger(row.`Forum.id`), place: toInteger(row.place)})
} IN TRANSACTIONS OF 10000 ROWS;

// --- Comment (2,052,169 rows) — use periodic commit ---
CALL {
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/comment_0_0.csv' AS row FIELDTERMINATOR '|'
CREATE (:Comment:Message {id: toInteger(row.id), creationDate: toInteger(row.creationDate), locationIP: row.locationIP, browserUsed: row.browserUsed, content: row.content, length: toInteger(row.length), creator: toInteger(row.creator), place: toInteger(row.place), replyOfPost: toInteger(row.replyOfPost), replyOfComment: toInteger(row.replyOfComment)})
} IN TRANSACTIONS OF 10000 ROWS;

// ============================================================
//  PHASE 4: STATIC RELATIONSHIPS (from embedded foreign keys)
// ============================================================

// --- Place IS_PART_OF Place ---
MATCH (p:Place) WHERE p.isPartOf IS NOT NULL
MATCH (parent:Place {id: p.isPartOf})
CREATE (p)-[:IS_PART_OF]->(parent);

// --- Tag HAS_TYPE TagClass ---
MATCH (t:Tag)
MATCH (tc:TagClass {id: t.hasType})
CREATE (t)-[:HAS_TYPE]->(tc);

// --- TagClass IS_SUBCLASS_OF TagClass ---
MATCH (tc:TagClass) WHERE tc.isSubclassOf IS NOT NULL
MATCH (parent:TagClass {id: tc.isSubclassOf})
CREATE (tc)-[:IS_SUBCLASS_OF]->(parent);

// --- Organisation IS_LOCATED_IN Place ---
MATCH (o:Organisation)
MATCH (p:Place {id: o.place})
CREATE (o)-[:IS_LOCATED_IN]->(p);

// --- Person IS_LOCATED_IN Place ---
MATCH (per:Person)
MATCH (p:Place {id: per.place})
CREATE (per)-[:IS_LOCATED_IN]->(p);

// ============================================================
//  PHASE 5: DYNAMIC RELATIONSHIPS (from separate CSV files)
// ============================================================

// --- Person KNOWS Person (180,623 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_knows_person_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (a:Person {id: toInteger(row.`Person.id`)}), (b:Person {id: toInteger(row.`Person.id`)})
CREATE (a)-[:KNOWS {creationDate: toInteger(row.creationDate)}]->(b);

// --- Person HAS_INTEREST Tag (229,166 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_hasInterest_tag_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: toInteger(row.`Person.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
CREATE (p)-[:HAS_INTEREST]->(t);

// --- Person STUDY_AT Organisation (7,949 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_studyAt_organisation_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: toInteger(row.`Person.id`)}), (o:Organisation {id: toInteger(row.`Organisation.id`)})
CREATE (p)-[:STUDY_AT {classYear: toInteger(row.classYear)}]->(o);

// --- Person WORK_AT Organisation (21,654 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_workAt_organisation_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: toInteger(row.`Person.id`)}), (o:Organisation {id: toInteger(row.`Organisation.id`)})
CREATE (p)-[:WORK_AT {workFrom: toInteger(row.workFrom)}]->(o);

// --- Person LIKES Post (751,677 rows) ---
CALL {
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_likes_post_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: toInteger(row.`Person.id`)}), (m:Post {id: toInteger(row.`Post.id`)})
CREATE (p)-[:LIKES {creationDate: toInteger(row.creationDate)}]->(m)
} IN TRANSACTIONS OF 5000 ROWS;

// --- Person LIKES Comment (1,438,418 rows) ---
CALL {
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/person_likes_comment_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Person {id: toInteger(row.`Person.id`)}), (c:Comment {id: toInteger(row.`Comment.id`)})
CREATE (p)-[:LIKES {creationDate: toInteger(row.creationDate)}]->(c)
} IN TRANSACTIONS OF 5000 ROWS;

// --- Forum HAS_MEMBER Person (1,611,869 rows) ---
CALL {
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/forum_hasMember_person_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (f:Forum {id: toInteger(row.`Forum.id`)}), (p:Person {id: toInteger(row.`Person.id`)})
CREATE (f)-[:HAS_MEMBER {joinDate: toInteger(row.joinDate)}]->(p)
} IN TRANSACTIONS OF 5000 ROWS;

// --- Forum HAS_TAG Tag (309,766 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/forum_hasTag_tag_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (f:Forum {id: toInteger(row.`Forum.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
CREATE (f)-[:HAS_TAG]->(t);

// --- Post HAS_TAG Tag (713,258 rows) ---
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/post_hasTag_tag_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (p:Post {id: toInteger(row.`Post.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
CREATE (p)-[:HAS_TAG]->(t);

// --- Comment HAS_TAG Tag (2,698,393 rows) ---
CALL {
LOAD CSV WITH HEADERS FROM 'file:///opt/ldbc-snb/sf1/social_network-sf1-CsvCompositeMergeForeign-LongDateFormatter/dynamic/comment_hasTag_tag_0_0.csv' AS row FIELDTERMINATOR '|'
MATCH (c:Comment {id: toInteger(row.`Comment.id`)}), (t:Tag {id: toInteger(row.`Tag.id`)})
CREATE (c)-[:HAS_TAG]->(t)
} IN TRANSACTIONS OF 5000 ROWS;

// ============================================================
//  PHASE 6: EMBEDDED RELATIONSHIP CREATION (from node foreign keys)
// ============================================================

// --- Post HAS_CREATOR Person ---
CALL {
MATCH (p:Post) WHERE p.creator IS NOT NULL
MATCH (per:Person {id: p.creator})
CREATE (p)-[:HAS_CREATOR]->(per)
} IN TRANSACTIONS OF 10000 ROWS;

// --- Post CONTAINER_OF Forum ---
CALL {
MATCH (p:Post) WHERE p.forumId IS NOT NULL
MATCH (f:Forum {id: p.forumId})
CREATE (f)-[:CONTAINER_OF]->(p)
} IN TRANSACTIONS OF 10000 ROWS;

// --- Post IS_LOCATED_IN Place ---
CALL {
MATCH (p:Post) WHERE p.place IS NOT NULL
MATCH (pl:Place {id: p.place})
CREATE (p)-[:IS_LOCATED_IN]->(pl)
} IN TRANSACTIONS OF 10000 ROWS;

// --- Comment HAS_CREATOR Person ---
CALL {
MATCH (c:Comment) WHERE c.creator IS NOT NULL
MATCH (per:Person {id: c.creator})
CREATE (c)-[:HAS_CREATOR]->(per)
} IN TRANSACTIONS OF 10000 ROWS;

// --- Comment IS_LOCATED_IN Place ---
CALL {
MATCH (c:Comment) WHERE c.place IS NOT NULL
MATCH (pl:Place {id: c.place})
CREATE (c)-[:IS_LOCATED_IN]->(pl)
} IN TRANSACTIONS OF 10000 ROWS;

// --- Comment REPLY_OF Post ---
CALL {
MATCH (c:Comment) WHERE c.replyOfPost IS NOT NULL AND c.replyOfPost <> 0
MATCH (p:Post {id: c.replyOfPost})
CREATE (c)-[:REPLY_OF]->(p)
} IN TRANSACTIONS OF 10000 ROWS;

// --- Comment REPLY_OF Comment ---
CALL {
MATCH (c:Comment) WHERE c.replyOfComment IS NOT NULL AND c.replyOfComment <> 0
MATCH (parent:Comment {id: c.replyOfComment})
CREATE (c)-[:REPLY_OF]->(parent)
} IN TRANSACTIONS OF 10000 ROWS;

// --- Forum HAS_MODERATOR Person ---
MATCH (f:Forum) WHERE f.moderator IS NOT NULL
MATCH (p:Person {id: f.moderator})
CREATE (f)-[:HAS_MODERATOR]->(p);

// ============================================================
//  PHASE 7: VERIFICATION
// ============================================================

MATCH (n) RETURN labels(n)[0] AS label, count(n) AS count ORDER BY label;
MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY type;
