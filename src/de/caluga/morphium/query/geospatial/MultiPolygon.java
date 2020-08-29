package de.caluga.morphium.query.geospatial;

import java.util.ArrayList;
import java.util.List;

public class MultiPolygon extends Geo<List<List<List<double[]>>>> {
    public MultiPolygon() {
        super(GeoType.MULTIPOLYGON);
        setCoordinates(new ArrayList<>());
    }

    public void addPolygon(Polygon p) {
        getCoordinates().add(p.getCoordinates());
    }
}
