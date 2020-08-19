package de.caluga.morphium.query.geospatial;

public enum GeoType {
    POINT("Point"),
    POLYGON("Polygon"),
    LINESTRING("LineString"),
    MULTIPOINT("MultiPoint"),
    MULITLINESTRING("MultiLineString"),
    MULTIPOLYGON("MultiPoligon");

    String mongoName;

    GeoType(String mongoName) {
        this.mongoName = mongoName;
    }
}
