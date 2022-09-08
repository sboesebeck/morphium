package de.caluga.morphium.driver.mongodb;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.Doc;
import de.caluga.morphium.driver.ReadPreference;
import de.caluga.morphium.driver.WriteConcern;
import de.caluga.morphium.driver.bulk.*;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Stephan Bösebeck
 * Date: 13.11.15
 * Time: 23:06
 * <p>
 * Bulk context
 */
@SuppressWarnings("WeakerAccess")
public class MongodbBulkContext extends BulkRequestContext {
    private final Logger log = LoggerFactory.getLogger(MongodbBulkContext.class);
    private final MongoDriver driver;
    private final boolean ordered;
    private final String db;
    private final String collection;
    private final WriteConcern wc;

    private final List<BulkRequest> requests;

    public MongodbBulkContext(Morphium m, String db, String collection, MongoDriver driver, boolean ordered, WriteConcern wc) {
        super(m);
        this.driver = driver;
        this.ordered = ordered;
        this.db = db;
        this.collection = collection;
        this.wc = wc;

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
    public InsertBulkRequest addInsertBulkRequest(List<Map<String, Object>> toInsert) {
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
    public Doc execute() {
        List<WriteModel<? extends Document>> lst = new ArrayList<>();
        int count = 0;
        List<BulkWriteResult> results = new ArrayList<>();
        for (BulkRequest br : requests) {
            if (br instanceof InsertBulkRequest) {
                //Insert...
                InsertBulkRequest ib = (InsertBulkRequest) br;
                DriverHelper.replaceMorphiumIdByObjectId(ib.getToInsert());
                for (Map<String, Object> o : ib.getToInsert()) {
                    Document document = new Document(o);
                    lst.add(new InsertOneModel<>(document));
                }

            } else if (br instanceof DeleteBulkRequest) {
                DeleteBulkRequest dbr = (DeleteBulkRequest) br;
                DriverHelper.replaceMorphiumIdByObjectId(((DeleteBulkRequest) br).getQuery());
                if (dbr.isMultiple()) {
                    lst.add(new DeleteManyModel<>(new Document(dbr.getQuery())));
                } else {
                    lst.add(new DeleteOneModel<>(new Document(dbr.getQuery())));
                }
            } else {
                //update
                UpdateBulkRequest up = (UpdateBulkRequest) br;
                UpdateOptions upd = new UpdateOptions();
                upd.upsert(up.isUpsert());
                DriverHelper.replaceMorphiumIdByObjectId(up.getQuery());
                DriverHelper.replaceMorphiumIdByObjectId(up.getCmd());
                if (up.isMultiple()) {
                    UpdateManyModel updateModel = new UpdateManyModel(new Document(up.getQuery()), new Document(up.getCmd()), upd);
                    //noinspection unchecked
                    lst.add(updateModel);
                } else {
                    //noinspection unchecked
                    lst.add(new UpdateOneModel(new Document(up.getQuery()), new Document(up.getCmd()), upd));
                }
            }
            count++;
            if (driver.getMaximums().getMaxWriteBatchSize() != null) {
                if (count >= driver.getMaximums().getMaxWriteBatchSize()) {
                    results.add(commitWrite(lst));
                    lst.clear();
                }
            }
        }
        if (!lst.isEmpty()) {
            results.add(commitWrite(lst));
            lst.clear();
        }


        Doc res = new Doc();

        int delCount = 0;
        int matchedCount = 0;
        int insertCount = 0;
        int modifiedCount = 0;
        int upsertCount = 0;
        for (BulkWriteResult r : results) {
            try {
                delCount += r.getDeletedCount();
                matchedCount += r.getMatchedCount();
                insertCount += r.getInsertedCount();
                modifiedCount += r.getModifiedCount();
                upsertCount += r.getUpserts().size();
            } catch (UnsupportedOperationException e) {
                // If write is unacknowledged and mongo version is < 3.6 BulkWriteResult will throw an UnsupportedOperationException
            }
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
        //return driver.getCollection(driver.getDb(db), collection, ReadPreference.nearest(), wc).bulkWrite(lst, bulkWriteOptions);
        return null;
    }
}
