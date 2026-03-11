package de.caluga.morphium.objectmapping;

import de.caluga.morphium.UtilsMap;
import de.caluga.morphium.query.geospatial.*;

import java.util.List;
import java.util.Map;

public class BsonGeoMapper implements MorphiumTypeMapper<Geo> {

    @Override
    public Object marshall(Geo o) {

        return UtilsMap.of("type", (Object) o.getType().getMongoName(), "coordinates", o.getCoordinates());
    }

    @SuppressWarnings({"ConstantConditions", "unchecked", "rawtypes"})
    @Override
    public Geo unmarshall(Object d) {
        if (!(d instanceof Map)) return null;

        Map data = (Map) d;

        String type = data.get("type").toString();

        GeoType ty = null;
        for (GeoType t : GeoType.values()) {
            if (t.getMongoName().equals(type)) {
                ty = t;
                break;
            }
        }
        Geo ret = null;
        switch (ty) {
            case POINT:
                Point p = new Point();
                //noinspection unchecked
                p.setCoordinates((List<Double>) data.get("coordinates"));
                ret = p;
                break;
            case POLYGON:
                Polygon pl = new Polygon();
                //noinspection unchecked
                pl.setCoordinates((List<List<double[]>>) data.get("coordinates"));
                ret = pl;
                break;
            case LINESTRING:
                LineString ls = new LineString();
                //noinspection unchecked
                ls.setCoordinates((List<double[]>) data.get("coordinates"));
                ret = ls;
                break;
            case MULTIPOINT:
                MultiPoint mp = new MultiPoint();
                //noinspection unchecked
                mp.setCoordinates((List<List<Double>>) data.get("coordinates"));
                ret = mp;
                break;
            case MULTIPOLYGON:
                MultiPolygon mpl = new MultiPolygon();
                //noinspection unchecked
                mpl.setCoordinates((List<List<List<double[]>>>) data.get("coordinates"));
                ret = mpl;
                break;
            case MULITLINESTRING:
                MultiLineString mls = new MultiLineString();
                //noinspection unchecked
                mls.setCoordinates((List<List<double[]>>) data.get("coordinates"));
                ret = mls;
                break;
            default:
                throw new IllegalArgumentException("Unsupported geo type");
        }
        return ret;

    }
}
