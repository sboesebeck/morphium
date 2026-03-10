package de.caluga.morphium.query.geospatial;

import java.util.ArrayList;
import java.util.List;

public class MultiPoint extends Geo<List<List<Double>>> {
    public MultiPoint() {
        super(GeoType.MULTIPOINT);
        setCoordinates(new ArrayList<>());
    }

    public void addPoint(Point p) {
        getCoordinates().add(p.getCoordinates());
    }
}
