package de.caluga.morphium.query.geospatial;

public enum GeoType {
    POINT("Point"),
    POLYGON("Polygon"),
    LINESTRING("LineString"),
    MULTIPOINT("MultiPoint"),
    MULITLINESTRING("MultiLineString"),
    MULTIPOLYGON("MultiPoligon");

    @SuppressWarnings("CanBeFinal")
    String mongoName;

    public String getMongoName() {
        return mongoName;
    }

    GeoType(String mongoName) {
        this.mongoName = mongoName;
    }
}
