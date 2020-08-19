package de.caluga.morphium.query.geospatial;

import java.util.List;

//array of 2: lng, lat
public class Point extends Geo<int[]> {
    public Point() {
        super(GeoType.POINT);
        setCoordinates(new int[2]);
    }

    public void setLat(int lat) {
        getCoordinates()[1] = lat;
    }

    public void setLong(int lng) {
        getCoordinates()[0] = lng;
    }

}
