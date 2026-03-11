package de.caluga.morphium.replicaset;

import de.caluga.morphium.annotations.Embedded;
import de.caluga.morphium.annotations.Property;

import java.util.List;

/**
 * User: Stephan Bösebeck
 * Date: 24.08.12
 * Time: 11:31
 * <p>
 * ReplicasetConf
 */
@SuppressWarnings("UnusedDeclaration")
@Embedded(translateCamelCase = false)
public class ReplicaSetConf {
    //{ "_id" : "hi1", "version" : 15, "members" : [ 	{ 	"_id" : 0, 	"host" : "mongo1.holidayinsider.com:27017", 	"priority" : 5 }, 	{ 	"_id" : 1, 	"host" : "mongo2.holidayinsider.com:27017", 	"priority" : 3 }, 	{ 	"_id" : 3, 	"host" : "mongo3.holidayinsider.com:27017", 	"priority" : 2 }, 	{ 	"_id" : 4, 	"host" : "mongo4.holidayinsider.com:27017", 	"priority" : 0 }, 	{ 	"_id" : 5, 	"host" : "mongo3.holidayinsider.com:27018", 	"arbiterOnly" : true } ] }
    @Property(fieldName = "_id")
    private String id;

    private int version;
    private List<ConfNode> members;

    public List getMemberList() {
        return members;
    }

    public String getId() {
        return id;
    }

    public int getVersion() {
        return version;
    }

    public List<ConfNode> getMembers() {
        return members;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder("[ \n");
        if (members != null) {
            for (ConfNode n : members) {
                stringBuilder.append(n.toString());
                stringBuilder.append(",\n");
            }
        }
        stringBuilder.append(" ]");

        return "ReplicaSetConf{" +
                "id='" + id + '\'' +
                ", version=" + version +
                ", members=" + stringBuilder.toString() +
                '}';
    }
}
