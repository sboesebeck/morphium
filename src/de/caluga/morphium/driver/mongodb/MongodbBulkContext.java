package de.caluga.morphium.driver.mongodb;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.ReadPreference;
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

    public MongodbBulkContext(Morphium m, String db, String collection, Driver driver, boolean ordered, int batchSize, WriteConcern wc) {
        super(m);
        this.driver = driver;
        this.ordered = ordered;
        this.db = db;
        this.collection = collection;
        this.wc = wc;
        setBatchSize(batchSize);

        requests = new ArrayList<>();
    }

    public void addRequest(BulkRequest br) {
        requests.add(br);
    }

    @Override
    public UpdateBulkRequest addUpdateBulkRequest() {
        UpdateBulkRequest up = new UpdateBulkRequest();
        addRequest(up);
        return up;
    }

    @Override
    public InsertBulkRequest addInsertBulkReqpest(List<Map<String, Object>> toInsert) {
        InsertBulkRequest in = new InsertBulkRequest(toInsert);
        addRequest(in);
        return in;
    }

    @Override
    public DeleteBulkRequest addDeleteBulkRequest() {
        DeleteBulkRequest del = new DeleteBulkRequest();
        addRequest(del);
        return del;
    }


    @Override
    public Map<String, Object> execute() {
        List<WriteModel<? extends Document>> lst = new ArrayList<>();
        Map<Document, Map<String, Object>> inserts = new HashMap<>();
        int count = 0;
        List<BulkWriteResult> results = new ArrayList<>();

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
            count++;
            if (count >= driver.getMaximums().getMaxWriteBatchSize()) {
                results.add(commitWrite(lst));
                lst.clear();
            }
        }
        if (lst.size() != 0) {
            results.add(commitWrite(lst));
            lst.clear();
        }



        if (inserts.size() != 0) {
            for (Map.Entry<Document, Map<String, Object>> e : inserts.entrySet()) {
                e.getValue().put("_id", e.getKey().get("_id"));
                if (e.getValue().get("_id") == null) {
                    log.fatal("objects were stored, but no _id returned!!!!!!!!!");
                }
            }
        }

        Map<String, Object> res = new HashMap<>();

        int delCount = 0;
        int matchedCount = 0;
        int insertCount = 0;
        int modifiedCount = 0;
        int upsertCount = 0;
        for (BulkWriteResult r : results) {
            delCount += r.getDeletedCount();
            matchedCount += r.getMatchedCount();
            insertCount += r.getInsertedCount();
            modifiedCount += r.getModifiedCount();
            upsertCount += r.getUpserts().size();
        }

        res.put("num_del", delCount);
        res.put("num_matched", matchedCount);
        res.put("num_insert", insertCount);
        res.put("num_modified", modifiedCount);
        res.put("num_upserts", upsertCount);
        return res;
    }

    private BulkWriteResult commitWrite(List<WriteModel<? extends Document>> lst) {
        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        bulkWriteOptions.ordered(ordered);
        return driver.getCollection(driver.getDb(db), collection, ReadPreference.nearest(), wc).bulkWrite(lst, bulkWriteOptions);
    }
}
