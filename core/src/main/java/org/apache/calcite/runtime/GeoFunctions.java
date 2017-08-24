/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.runtime;

import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.util.Util;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import com.esri.core.geometry.MapGeometry;
import com.esri.core.geometry.Point;
import com.esri.core.geometry.Polygon;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.WktExportFlags;
import com.esri.core.geometry.WktImportFlags;

import com.google.common.base.Preconditions;

import java.math.BigDecimal;

/**
 * Helper methods to implement Geo-spatial functions in generated code.
 *
 * <p>Remaining tasks:
 *
 * <ul>
 *   <li>Determine type code for
 *   {@link org.apache.calcite.sql.type.ExtraSqlTypes#GEOMETRY}
 *   <li>Should we create aliases for functions in upper-case?
 *   Without ST_ prefix?
 *   <li>Consider adding spatial literals, e.g. `GEOMETRY 'POINT (30 10)'`
 *   <li>Integer arugments, e.g. SELECT ST_MakePoint(1, 2, 1.5)
 *   <li>Are GEOMETRY values comparable? If so add ORDER BY test
 * </ul>
 */
@SuppressWarnings({"UnnecessaryUnboxing", "WeakerAccess", "unused"})
@Deterministic
@Strict
@Experimental
public class GeoFunctions {
  private static final int NO_SRID = 0;
  private static final SpatialReference SPATIAL_REFERENCE =
      SpatialReference.create(4326);

  private GeoFunctions() {}

  private static UnsupportedOperationException todo() {
    return new UnsupportedOperationException();
  }

  protected static Geom bind(Geometry geometry, int srid) {
    if (geometry == null) {
      return null;
    }
    if (srid == NO_SRID) {
      return new SimpleGeom(geometry);
    }
    return bind(geometry, SpatialReference.create(srid));
  }

  private static MapGeom bind(Geometry geometry, SpatialReference sr) {
    return new MapGeom(new MapGeometry(geometry, sr));
  }

  // Geometry conversion functions (2D and 3D) ================================

  public static String ST_AsText(Geom g) {
    return ST_AsWKT(g);
  }

  public static String ST_AsWKT(Geom g) {
    return GeometryEngine.geometryToWkt(g.g(),
        WktExportFlags.wktExportDefaults);
  }

  public static Geom ST_GeomFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_GeomFromText(String s, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(s,
        WktImportFlags.wktImportDefaults, Geometry.Type.Unknown);
    return bind(g, srid);
  }

  public static Geom ST_LineFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_LineFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Line);
    return bind(g, srid);
  }

  public static Geom ST_MPointFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_MPointFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.MultiPoint);
    return bind(g, srid);
  }

  public static Geom ST_PointFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_PointFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Point);
    return bind(g, srid);
  }

  public static Geom ST_PolyFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_PolyFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Polygon);
    return bind(g, srid);
  }

  public static Geom ST_MLineFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_MLineFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Unknown); // NOTE: there is no Geometry.Type.MultiLine
    return bind(g, srid);
  }

  public static Geom ST_MPolyFromText(String s) {
    return ST_GeomFromText(s, NO_SRID);
  }

  public static Geom ST_MPolyFromText(String wkt, int srid) {
    final Geometry g = GeometryEngine.geometryFromWkt(wkt,
        WktImportFlags.wktImportDefaults,
        Geometry.Type.Unknown); // NOTE: there is no Geometry.Type.MultiPolygon
    return bind(g, srid);
  }

  // Geometry creation functions ==============================================

  /**  Constructs a 2D point from coordinates. */
  public static Geom ST_MakePoint(BigDecimal x, BigDecimal y) {
    // NOTE: Combine the double and BigDecimal variants of this function
    if (x == null || y == null) {
      return null; // TODO: strictness should make this check unnecessary
    }
    return ST_MakePoint(x.doubleValue(), y.doubleValue());
  }

  private static Geom ST_MakePoint(double x, double y) {
    final Geometry g = new Point(x, y);
    return new SimpleGeom(g);
  }

  /**  Constructs a 3D point from coordinates. */
  public static Geom ST_MakePoint(BigDecimal x, BigDecimal y, BigDecimal z) {
    final Geometry g = new Point(x.doubleValue(), y.doubleValue(),
        z.doubleValue());
    return new SimpleGeom(g);
  }

  // Geometry properties (2D and 3D) ==========================================

  /** Returns whether {@code geom} has at least one z-coordinate. */
  public static boolean ST_Is3D(Geom geom) {
    return geom.g().hasZ();
  }

  /** Returns the z-value of the first coordinate of {@code geom}. */
  public static Double ST_Z(Geom geom) {
    return geom.g().getDescription().hasZ() && geom.g() instanceof Point
        ? ((Point) geom.g()).getZ() : null;
  }

  /** Returns the distance between {@code geom1} and {@code geom2}. */
  public static double ST_Distance(Geom geom1, Geom geom2) {
    return GeometryEngine.distance(geom1.g(), geom2.g(), geom1.sr());
  }

  // Geometry predicates ======================================================

  /** Returns whether {@code geom1} and {@code geom2} are within
   * {@code distance} of each other. */
  public static boolean ST_DWithin(Geom geom1, Geom geom2, double distance) {
    final double distance1 =
        GeometryEngine.distance(geom1.g(), geom2.g(), geom1.sr());
    return distance1 <= distance;
  }

  // Geometry operators (2D and 3D) ===========================================

  /** Computes a buffer around {@code geom}. */
  public static Geom ST_Buffer(Geom geom, double distance) {
    final Polygon g = GeometryEngine.buffer(geom.g(), geom.sr(), distance);
    return geom.wrap(g);
  }

  /** Computes a buffer around {@code geom} with . */
  public static Geom ST_Buffer(Geom geom, double distance, int quadSegs) {
    throw todo();
  }

  /** Computes a buffer around {@code geom}. */
  public static Geom ST_Buffer(Geom geom, double bufferSize, String style) {
    int quadSegCount = 8;
    CapStyle endCapStyle = CapStyle.ROUND;
    JoinStyle joinStyle = JoinStyle.ROUND;
    float mitreLimit = 5f;
    int i = 0;
    parse:
    for (;;) {
      int equals = style.indexOf('=', i);
      if (equals < 0) {
        break;
      }
      int space = style.indexOf(' ', equals);
      if (space < 0) {
        space = style.length();
      }
      String name = style.substring(i, equals);
      String value = style.substring(equals + 1, space);
      switch (name) {
      case "quad_segs":
        quadSegCount = Integer.valueOf(value);
        break;
      case "endcap":
        endCapStyle = CapStyle.of(value);
        break;
      case "join":
        joinStyle = JoinStyle.of(value);
        break;
      case "mitre_limit":
      case "miter_limit":
        mitreLimit = Float.parseFloat(value);
        break;
      default:
        // ignore the value
      }
      i = space;
      for (;;) {
        if (i >= style.length()) {
          break parse;
        }
        if (style.charAt(i) != ' ') {
          break;
        }
        ++i;
      }
    }
    return buffer(geom, bufferSize, quadSegCount, endCapStyle, joinStyle,
        mitreLimit);
  }

  private static Geom buffer(Geom geom, double bufferSize,
      int quadSegCount, CapStyle endCapStyle, JoinStyle joinStyle,
      float mitreLimit) {
    Util.discard(endCapStyle + ":" + joinStyle + ":" + mitreLimit
        + ":" + quadSegCount);
    throw todo();
  }

  /** Computes the union of {@code geom1} and {@code geom2}. */
  public static Geom ST_Union(Geom geom1, Geom geom2) {
    SpatialReference sr = geom1.sr();
    final Geometry g =
        GeometryEngine.union(new Geometry[]{geom1.g(), geom2.g()}, sr);
    return bind(g, sr);
  }

  /** Computes the union of the geometries in {@code geomCollection}. */
  @SemiStrict public static Geom ST_Union(Geom geomCollection) {
    SpatialReference sr = geomCollection.sr();
    final Geometry g =
        GeometryEngine.union(new Geometry[] {geomCollection.g()}, sr);
    return bind(g, sr);
  }

  // Geometry projection functions ============================================

  /** Transforms {@code geom} from one coordinate reference
   * system (CRS) to the CRS specified by {@code srid}. */
  public static Geom ST_Transform(Geom geom, int srid) {
    return geom.transform(srid);
  }

  /** Returns a copy of {@code geom} with a new SRID. */
  public static Geom ST_SetSRID(Geom geom, int srid) {
    return geom.transform(srid);
  }

  // Inner classes ============================================================

  /** How the "buffer" command terminates the end of a line. */
  enum CapStyle {
    ROUND, FLAT, SQUARE;

    static CapStyle of(String value) {
      switch (value) {
      case "round":
        return ROUND;
      case "flat":
      case "butt":
        return FLAT;
      case "square":
        return SQUARE;
      default:
        throw new IllegalArgumentException("unknown endcap value: " + value);
      }
    }
  }

  /** How the "buffer" command decorates junctions between line segments. */
  enum JoinStyle {
    ROUND, MITRE, BEVEL;

    static JoinStyle of(String value) {
      switch (value) {
      case "round":
        return ROUND;
      case "mitre":
      case "miter":
        return MITRE;
      case "bevel":
        return BEVEL;
      default:
        throw new IllegalArgumentException("unknown join value: " + value);
      }
    }
  }

  /** Geometry. It may or may not have a spatial reference
   * associated with it. */
  public interface Geom {
    Geometry g();

    SpatialReference sr();

    Geom transform(int srid);

    Geom wrap(Geometry g);
  }

  /** Sub-class of geometry that has no spatial reference. */
  static class SimpleGeom implements Geom {
    final Geometry g;

    SimpleGeom(Geometry g) {
      this.g = Preconditions.checkNotNull(g);
    }

    @Override public String toString() {
      return g.toString();
    }

    public Geometry g() {
      return g;
    }

    public SpatialReference sr() {
      return SPATIAL_REFERENCE;
    }

    public Geom transform(int srid) {
      if (srid == SPATIAL_REFERENCE.getID()) {
        return this;
      }
      return bind(g, srid);
    }

    public Geom wrap(Geometry g) {
      return new SimpleGeom(g);
    }
  }

  /** Sub-class of geometry that has a spatial reference. */
  static class MapGeom implements Geom {
    final MapGeometry mg;

    MapGeom(MapGeometry mg) {
      this.mg = Preconditions.checkNotNull(mg);
    }

    @Override public String toString() {
      return mg.toString();
    }

    public Geometry g() {
      return mg.getGeometry();
    }

    public SpatialReference sr() {
      return mg.getSpatialReference();
    }

    public Geom transform(int srid) {
      if (srid == NO_SRID) {
        return new SimpleGeom(mg.getGeometry());
      }
      if (srid == mg.getSpatialReference().getID()) {
        return this;
      }
      return bind(mg.getGeometry(), srid);
    }

    public Geom wrap(Geometry g) {
      return bind(g, this.mg.getSpatialReference());
    }
  }
}

// End GeoFunctions.java
