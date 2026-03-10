package de.caluga.morphium.query.geospatial;

import de.caluga.morphium.annotations.Embedded;

@Embedded
public abstract class Geo<T> {

    private GeoType type;
    private T coordinates;

    public Geo(GeoType t) {
        type = t;
    }

    public GeoType getType() {
        return type;
    }

    public void setType(GeoType type) {
        this.type = type;
    }

    public T getCoordinates() {
        return coordinates;
    }

    public void setCoordinates(T coordinates) {
        this.coordinates = coordinates;
    }
}
