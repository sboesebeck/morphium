package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.annotations.caching.Cache;

import java.util.Date;

/**
 * User: Stephan Bösebeck
 * Date: 26.06.13
 * Time: 08:19
 * <p>
 * TODO: Add documentation here
 */
@Entity
@Cache
public class Person {
    @Id
    private String id;
    private String name;
    private Date birthday;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getBirthday() {
        return birthday;
    }

    public void setBirthday(Date birthday) {
        this.birthday = birthday;
    }

    public enum Fields {id, name, birthday}
}
