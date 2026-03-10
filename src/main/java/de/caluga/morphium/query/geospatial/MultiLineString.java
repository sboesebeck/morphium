package de.caluga.morphium.query.geospatial;

import java.util.ArrayList;
import java.util.List;

public class MultiLineString extends Geo<List<List<double[]>>> {
    public MultiLineString() {
        super(GeoType.MULITLINESTRING);
        setCoordinates(new ArrayList<>());
    }

    public void addLine(LineString ls) {
        getCoordinates().add(ls.getCoordinates());
    }
}
