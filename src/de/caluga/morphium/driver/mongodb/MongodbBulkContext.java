package de.caluga.morphium.driver.mongodb;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import de.caluga.morphium.Logger;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 13.11.15
 * Time: 23:06
 * <p>
 * TODO: Add documentation here
 */
public class MongodbBulkContext extends BulkRequestContext {
    private Logger log = new Logger(MongodbBulkContext.class);
    private Driver driver;
    private boolean ordered;
    private String db;
    private String collection;
    private WriteConcern wc;

    private List<BulkRequest> requests;

    public MongodbBulkContext(String db, String collection, Driver driver, boolean ordered, WriteConcern wc) {
        this.driver = driver;
        this.ordered = ordered;
        this.db = db;
        this.collection = collection;
        this.wc = wc;

        requests = new ArrayList<>();
    }

    @Override
    public void addRequest(BulkRequest br) {
        requests.add(br);
    }

    @Override
    public Map<String, Object> execute() {
        List<WriteModel<? extends Document>> lst = new ArrayList<>();
        Map<Document, Map<String, Object>> inserts = new HashMap<>();
        for (BulkRequest br : requests) {
            if (br instanceof InsertBulkRequest) {
                //Insert...
                InsertBulkRequest ib = (InsertBulkRequest) br;

                for (Map<String, Object> o : ib.getToInsert()) {
                    Document document = new Document(o);
                    lst.add(new InsertOneModel<>(document));
                    inserts.put(document, o);
                }

            } else if (br instanceof DeleteBulkRequest) {
                DeleteBulkRequest dbr = (DeleteBulkRequest) br;
                if (dbr.isMultiple()) {
                    lst.add(new DeleteManyModel<>(new Document(dbr.getQuery())));
                } else {
                    lst.add(new DeleteOneModel<>(new Document(dbr.getQuery())));
                }
            } else {
                //update
                UpdateBulkRequest up = (UpdateBulkRequest) br;
                if (up.isMultiple()) {
                    lst.add(new UpdateManyModel(new Document(up.getQuery()), new Document(up.getCmd())));
                } else {
                    lst.add(new UpdateOneModel(new Document(up.getQuery()), new Document(up.getCmd())));
                }
            }
        }

        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        bulkWriteOptions.ordered(ordered);
        BulkWriteResult bulkWriteResult = driver.getCollection(driver.getDb(db), collection, null, wc).bulkWrite(lst, bulkWriteOptions);

        if (inserts.size() != 0) {
            for (Map.Entry<Document, Map<String, Object>> e : inserts.entrySet()) {
                e.getValue().put("_id", e.getKey().get("_id"));
                if (e.getValue().get("_id") == null) {
                    log.fatal("objects were stored, but no _id returned!!!!!!!!!");
                }
            }
        }

        Map<String, Object> res = new HashMap<>();

        res.put("num_del", bulkWriteResult.getDeletedCount());
        res.put("num_matched", bulkWriteResult.getMatchedCount());
        res.put("num_insert", bulkWriteResult.getInsertedCount());
        res.put("num_modified", bulkWriteResult.getModifiedCount());
        res.put("num_upserts", bulkWriteResult.getUpserts().size());
        return res;
    }
}
