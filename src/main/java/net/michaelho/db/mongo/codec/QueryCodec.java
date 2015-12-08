package net.michaelho.db.mongo.codec;

import net.michaelho.db.mongo.Query;
import org.bson.BsonReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.CollectibleCodec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.types.ObjectId;

/**
 * This codec needs to be added to the codec registry in order for Mongo 3.0+ to recognize and use Query as a Mongo object. This simplifies the calling code in that toDocument() does not need to be called every time a Query object is passed to a MongoCollection method. Other than this convenience, however, this is otherwise optional.
 */
public class QueryCodec implements CollectibleCodec<Query> {
    private Codec<Document> documentCodec;

    public QueryCodec () {
        this.documentCodec = new DocumentCodec();
    }

    public QueryCodec (Codec<Document> codec) {
        this.documentCodec = codec;
    }

    @Override
    public Query generateIdIfAbsentFromDocument (Query document) {
        if (!documentHasId(document)) {
            document.append(new Query(Query.KEY_MONGO_ID, ObjectId.get()));
            return document;
        }
        return document;
    }

    @Override
    public boolean documentHasId (Query document) {
        return document.getId() != null;
    }

    @Override
    public BsonValue getDocumentId (Query document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("The document does not contain an _id");
        }
        return new BsonString(document.getId().toString());
    }

    @Override
    public Query decode (BsonReader reader, DecoderContext decoderContext) {
        Document document = documentCodec.decode(reader, decoderContext);
        return new Query(document);
    }

    @Override
    public void encode (BsonWriter writer, Query value, EncoderContext encoderContext) {
        Document document = value.toDocument();
        documentCodec.encode(writer, document, encoderContext);
    }

    @Override
    public Class<Query> getEncoderClass () {
        return Query.class;
    }
}
