package net.michaelho.db.mongo;

import org.bson.BsonDocument;
import org.bson.BsonDocumentWrapper;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * This class is intended to allow simple construction of Mongo queries by providing helper methods and chaining capabilities.
 */
public class Query implements Bson {
    public static final String KEY_MONGO_ID = "_id";
    private final Map<String, Object> mapping;

    public static Query addToSet (Query query) {
        return new Query("$addToSet", query);
    }

    public static Query and (Query... queries) {
        return mergeQueries("$and", queries);
    }

    public static Query avg (Query avgQuery) {
        return new Query("$avg", avgQuery);
    }

    public static Query avg (String field) {
        return new Query("$avg", field);
    }

    public static <E> Query each (List<E> objects) {
        return new Query("$each", objects);
    }

    public static Query elemMatch (String field, Query value) {
        return nestQuery("$elemMatch", field, value);
    }

    public static Query emptyQuery () {
        return new Query();
    }

    public static Query exists (String field) {
        return new Query(field, true).exists();
    }

    public static Query first (String field) {
        return new Query("$first", field);
    }

    public static Query group (Query groupQuery) {
        return new Query("$group", groupQuery);
    }

    public static Query match (Query matchQuery) {
        return new Query("$match", matchQuery);
    }

    public static String nestedField (String parent, Object field) {
        String child = field.toString();
        if (field instanceof String) {
            // no need to format it
            child = (String)field;
        }
        return parent + '.' + child;
    }
    
    public static Query notExists (String field) {
        return new Query(field, false).exists();
    }

    public static Query newNearQuery (String field, Double longitude, Double latitude, int maxDistance) {
        Query geometry = new Query("type", "Point");
        geometry.append(new Query("coordinates", new AppendableArrayList<>(longitude, latitude)));
        Query near = new Query("$maxDistance", maxDistance).append(new Query("$geometry", geometry));
        return new Query(field, new Query("$near", near));
    }

    public static Query or (Query... queries) {
        return mergeQueries("$or", queries);
    }

    public static Query project (Query projectQuery) {
        return new Query("$project", projectQuery);
    }

    public static Query pull (Query pullQuery) {
        return new Query("$pull", pullQuery);
    }

    public static Query push (Query pushQuery) {
        return new Query("$push", pushQuery);
    }

    public static Query regex (String field, String pattern, String options) {
        Query patternQuery = new Query("$regex", pattern);
        if (options != null) {
            patternQuery.append(new Query("$options", options));
        }
        return new Query(field, patternQuery);
    }

    public static Query set (Query setQuery) {
        return new Query("$set", setQuery);
    }

    public static Query setOnInsert (Query setOnInsertQuery) {
        return new Query("$setOnInsert", setOnInsertQuery);
    }

    public static Query slice (int num) {
        return new Query("$slice", num);
    }

    public static Query sum (Query sumQuery) {
        return new Query("$sum", sumQuery);
    }

    public static Query sum (String field) {
        return new Query("$sum", field);
    }

    public static Query sum (int counter) {
        return new Query("$sum", counter);
    }

    public static Query unwind (String field) {
        return new Query("$unwind", field);
    }

    public static Query uuid (String field, UUID uuid) {
        return uuid(field, uuid, true);
    }

    public static Query uuid (String field, UUID uuid, boolean isString) {
        if (isString) {
            return new Query(field, uuid.toString());
        }
        return new Query(field, uuid);
    }

    public Query () {
        mapping = new HashMap<>();
    }

    public Query (String field, Object value) {
        this();
        mapping.put(field, value);
    }

    /**
     * This converts a Document to a Query object.
     * @param document the Document to translate to a Query
     */
    public Query (Document document) {
        this();
        mapping.putAll(document);
    }

    @Override
    public int hashCode () {
        return mapping.hashCode();
    }

    @Override
    public boolean equals (Object obj) {
        if (!(obj instanceof Query)) {
            return false;
        }
        Query checkQuery = (Query)obj;
        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            Object otherValue = checkQuery.mapping.get(entry.getKey());
            if (otherValue instanceof List) {
                if (!(entry.getValue() instanceof List)) {
                    return false;
                }
                // convert to set to compare
                @SuppressWarnings("unchecked")
                Set<Query> thisValueSet = new HashSet<>((List<Query>)entry.getValue());
                @SuppressWarnings("unchecked")
                Set<Query> otherValueSet = new HashSet<>((List<Query>)otherValue);
                if (!thisValueSet.equals(otherValueSet)) {
                    return false;
                }
                continue;
            }
            if (!entry.getValue().equals(otherValue)) {
                return false;
            }
        }
        return true;
    }

    public ObjectId getId () {
        return (ObjectId) mapping.get(KEY_MONGO_ID);
    }

    public Document toDocument () {
        if (mapping.isEmpty()) {
            return new Document();
        }

        Document object = new Document();

        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Query) {
                object.append(entry.getKey(), ((Query) value).toDocument());
                continue;
            }
            if (value instanceof List) {
                // we specifically want to make sure that the order is preserved for the sake of things like coordinates
                List<Object> values = new AppendableArrayList<>();
                for (Object nestedValue : (List)value) {
                    if (nestedValue instanceof Query) {
                        values.add(((Query)nestedValue).toDocument());
                        continue;
                    }
                    values.add(nestedValue);
                }
                object.append(entry.getKey(), values);
                continue;
            }
            if (value instanceof Collection) {
                Set<Object> values = new HashSet<>();
                for (Object nestedValue : (Collection)value) {
                    if (nestedValue instanceof Query) {
                        values.add(((Query) nestedValue).toDocument());
                        continue;
                    }
                    values.add(nestedValue);
                }
                object.append(entry.getKey(), values);
                continue;
            }
            object.append(entry.getKey(), value);
        }

        return object;
    }

    public String toString () {
        return toDocument().toString();
    }

    @SuppressWarnings("unchecked")
    public Query and (Query anotherQuery) {
        return mergeQueries("$and", this, anotherQuery);
    }

    public Query append (Query anotherQuery) {
        mapping.putAll(anotherQuery.mapping);
        return this;
    }

    @SuppressWarnings("unchecked")
    public Query or (Query anotherQuery) {
        return mergeQueries("$or", this, anotherQuery);
    }

    public Query elemMatch () {
        return nestQuery("$elemMatch");
    }

    public Query elemMatch (String field) {
        return nestQuery("$elemMatch", field);
    }

    public Query exists () {
        return nestQuery("$exists");
    }

    public Query gt () {
        return nestQuery("$gt");
    }

    public Query gte () {
        return nestQuery("$gte");
    }

    public Query lt () {
        return nestQuery("$lt");
    }

    public Query lte () {
        return nestQuery("$lte");
    }

    public Query ne () {
        return nestQuery("$ne");
    }

    public Query not () {
        return nestQuery("$not");
    }

    @Override
    public <TDocument> BsonDocument toBsonDocument (Class<TDocument> tDocumentClass, CodecRegistry codecRegistry) {
        return new BsonDocumentWrapper<>(this, codecRegistry.get(Query.class));
    }

    @SuppressWarnings("unchecked")
    private static Query mergeQueries (String op, Query... queries) {
        if (queries.length == 0) {
            return new Query();
        }
        if (queries.length == 1) {
            return queries[0];
        }
        Set<Query> merges = new HashSet<>();
        for (Query query : queries) {
            if (query.mapping.isEmpty()) {
                continue;
            }
            if (query.mapping.size() == 1 && query.mapping.containsKey(op)) {
                Object nested = query.mapping.get(op);
                if (nested instanceof Query) {
                    // add the individual queries within the query
                    Query nestedQuery = (Query)nested;
                    merges.addAll(nestedQuery.getNestedQueries());
                    continue;
                }
                if (nested instanceof Collection) {
                    merges.addAll((Collection)nested);
                    continue;
                }
//                merges.add((Query)query.mapping.get(op));
                continue;
            }
            merges.add(query);
        }
        if (merges.size() == 1) {
            return merges.toArray(new Query[merges.size()])[0];
        }

        return new Query (op, merges);
    }

    private Set<Query> getNestedQueries () {
        Set<Query> nestedQueries = new HashSet<>();
        for (Map.Entry<String, Object> entry : mapping.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof UUID) {
                // need to convert UUIDs to string
                nestedQueries.add(Query.uuid(entry.getKey(), (UUID)value));
                continue;
            }
            nestedQueries.add(new Query(entry.getKey(), value));
        }
        return nestedQueries;
    }

    private Query nestQuery (String op) {
        if (mapping.isEmpty()) {
            return new Query();
        }
        // this method only works if one field exists
        String key = (String)mapping.keySet().toArray()[0];
        Object value = mapping.get(key);
        return nestQuery(op, key, value);
    }

    private Query nestQuery (String op, String field) {
        return new Query(field, new Query(op, this));
    }

    private static Query nestQuery (String op, String field, Object value) {
        return new Query(field, new Query(op, value));
    }
}

