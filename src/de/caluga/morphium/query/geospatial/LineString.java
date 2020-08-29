package de.caluga.morphium.query.geospatial;

import java.util.ArrayList;
import java.util.List;

public class LineString extends Geo<List<double[]>> {
    public LineString() {
        super(GeoType.LINESTRING);
        setCoordinates(new ArrayList<>());
    }
}
