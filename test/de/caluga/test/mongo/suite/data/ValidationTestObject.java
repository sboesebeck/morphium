package de.caluga.test.mongo.suite.data;

import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.driver.bson.MorphiumId;
import org.hibernate.validator.constraints.Email;

import javax.validation.constraints.*;
import java.util.Date;
import java.util.List;

/**
 * User: martinstolz
 * Date: 29.08.12
 */
@Entity
public class ValidationTestObject {

    @Id
    private MorphiumId id;

    @Min(3)
    @Max(7)
    private int theInt;

    @NotNull
    private Integer anotherInt;

    @Future
    private Date whenever;

    @Pattern(regexp = "m[ueü]nchen")
    private String whereever;

    @Size(min = 2, max = 5)
    private List friends;

    @Email
    private String email;

    public MorphiumId getId() {
        return id;
    }

    public void setId(MorphiumId id) {
        this.id = id;
    }

    public int getTheInt() {
        return theInt;
    }

    public void setTheInt(int theInt) {
        this.theInt = theInt;
    }

    public Integer getAnotherInt() {
        return anotherInt;
    }

    public void setAnotherInt(Integer anotherInt) {
        this.anotherInt = anotherInt;
    }

    public Date getWhenever() {
        return whenever;
    }

    public void setWhenever(Date whenever) {
        this.whenever = whenever;
    }

    public String getWhereever() {
        return whereever;
    }

    public void setWhereever(String whereever) {
        this.whereever = whereever;
    }

    public List getFriends() {
        return friends;
    }

    public void setFriends(List friends) {
        this.friends = friends;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }
}
