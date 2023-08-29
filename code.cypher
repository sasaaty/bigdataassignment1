CALL apoc.periodic.iterate(
'CALL apoc.load.json("https://github.com/sasaaty/bigdataassignment1/blob/main/tweets.json") YIELD value',
'WITH
 value.id AS id
 ,datetime({ epochMillis: apoc.date.parse(value.postedTime, "ms",
 "yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'")}) AS postedTimestamp
 ,value.text AS text
 ,value.twitter_lang AS language
 ,value.retweetCount AS retweetCount
 ,value.favoritesCount AS favoritesCount
 MERGE (t:Tweet{id:id})
 ON CREATE SET
 t.postedTimestamp = postedTimestamp
 ,t.text = text
 ,t.language = language
 ,t.retweetCount = retweetCount
 ,t.favoritesCount = favoritesCount
',
{batchSize:500})
YIELD * ; 