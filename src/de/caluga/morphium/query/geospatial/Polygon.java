package de.caluga.morphium.query.geospatial;

import java.util.ArrayList;
import java.util.List;


// list of polygon rings
// list of coordinates
// list of points
public class Polygon extends Geo<List<List<double[]>>> {
    public Polygon() {
        super(GeoType.POLYGON);
        setCoordinates(new ArrayList<>());
    }
}
