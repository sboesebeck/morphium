package de.caluga.morphium.mapping;

import de.caluga.morphium.Utils;
import de.caluga.morphium.query.geospatial.*;

import javax.sound.sampled.Line;
import java.util.List;
import java.util.Map;

public class BsonGeoMapper implements MorphiumTypeMapper<Geo> {

    @Override
    public Object marshall(Geo o) {

        return Utils.getMap("type", (Object) o.getType().getMongoName()).add("coordinates", o.getCoordinates());
    }

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
                p.setCoordinates((List<Double>) data.get("coordinates"));
                ret = p;
                break;
            case POLYGON:
                Polygon pl = new Polygon();
                pl.setCoordinates((List<List<double[]>>) data.get("coordinates"));
                ret = pl;
                break;
            case LINESTRING:
                LineString ls = new LineString();
                ls.setCoordinates((List<double[]>) data.get("coordinates"));
                ret = ls;
                break;
            case MULTIPOINT:
                MultiPoint mp = new MultiPoint();
                mp.setCoordinates((List<List<Double>>) data.get("coordinates"));
                ret = mp;
                break;
            case MULTIPOLYGON:
                MultiPolygon mpl = new MultiPolygon();
                mpl.setCoordinates((List<List<List<double[]>>>) data.get("coordinates"));
                ret = mpl;
                break;
            case MULITLINESTRING:
                MultiLineString mls = new MultiLineString();
                mls.setCoordinates((List<List<double[]>>) data.get("coordinates"));
                ret = mls;
                break;
            default:
                throw new IllegalArgumentException("Unsupported geo type");
        }
        return ret;

    }
}
