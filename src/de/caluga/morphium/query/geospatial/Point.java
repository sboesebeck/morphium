package de.caluga.morphium.query.geospatial;

import java.util.ArrayList;
import java.util.List;

//array of 2: lng, lat
public class Point extends Geo<List<Double>> {
    public Point() {
        super(GeoType.POINT);
        ArrayList<Double> coordinates = new ArrayList<>(2);
        coordinates.add(0.0);
        coordinates.add(0.0);
        setCoordinates(coordinates);
    }

    public Point(Double lng, Double lat) {
        this();
        setLong(lng);
        setLat(lat);
    }

    public void setLat(Double lat) {
        getCoordinates().set(1, lat);
    }

    public void setLong(Double lng) {
        getCoordinates().set(0, lng);
    }


}
