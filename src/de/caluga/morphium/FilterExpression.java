package de.caluga.morphium;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import java.util.List;
import java.util.Vector;

public class FilterExpression {
    private String field;
    private Object value;
    private List<FilterExpression> children;

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public List<FilterExpression> getChildren() {
        return children;
    }

    public void setChildren(List<FilterExpression> children) {
        this.children = children;
    }

    public void addChild(FilterExpression e) {
        if (children == null) {
            children = new Vector<FilterExpression>();
        }
        children.add(e);
    }

    public DBObject dbObject() {
        DBObject o = new BasicDBObject();
        if (children != null) {
            for (FilterExpression flt : children) {
                o.put(field, flt.dbObject());
            }
        } else {
            if (value != null && value.getClass().isEnum()) {
                o.put(field,((Enum)value).name());
            } else {
                o.put(field, value);
            }
        }
        return o;
    }

}