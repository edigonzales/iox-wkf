package ch.interlis.ioxwkf.dbtools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import ch.interlis.iox.IoxException;

/** describes a list of dataTypes.
 */
public class AttributeDescriptor {
	private String dbColumnName=null;
	private String iomAttributeName=null;
	private Integer attributeType=null;
	private String attributeTypeName=null;
	private String dbColumnGeomTypeName=null;
	private Integer coordDimension=null;
	private Integer srId=null;
	private Integer precision=null;
	
	/** bool is an additional type name of boolean.
	 * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_BOOL="bool";
	
	// JDBC/DB column type name if java.sql.Types.OTHER
	/** xml is a name of JDBC/DB column type.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_XML="xml";
	/** xml is a name of JDBC/DB column type.<br>
	 * it is defined by 36 characters in format of: ISO 11578.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_UUID="uuid";
	/** geometry is the name of JDBC/DB column type to identify the dataType as geometry.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnTypeName()</li>
	 */
	public final static String DBCOLUMN_TYPENAME_GEOMETRY="geometry";
	// geometry types
	/** multiPolygon is a collection of Polygons.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_MULTIPOLYGON="MULTIPOLYGON";
	/** represents a polygon with linear edges, which includes shell and may includes holes.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_POLYGON="POLYGON";
	/** a MultiLineString is defined by one or more LineStrings.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_MULTILINESTRING="MULTILINESTRING";
	/** lineString is a sequence of line segments.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_LINESTRING="LINESTRING";
	/** an aggregate class containing only instances of Point.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_MULTIPOINT="MULTIPOINT";
	/** a geometric object consisting of one point.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
	 * get type:<br>
	 * <li>getDbColumnGeomTypeName()</li>
	 */
	public final static String GEOMETRYTYPE_POINT="POINT";
	
	private final static String GEOMCOLUMNS_COLUMN_TYPE="type";
	private final static String GEOMCOLUMNS_COLUMN_SRID="srid";
	private final static String GEOMCOLUMNS_COLUMN_DIMENSION="coord_dimension";
	
	/** get the name of column.
	 * @return column name as String.
	 */
	protected String getDbColumnName() {
		return dbColumnName;
	}
	/** set given name as column name in String.
	 * @param attributeName
	 */
	protected void setDbColumnName(String attributeName) {
		this.dbColumnName = attributeName;
	}
	/** get the attribute name of an iomObject.
	 * @return object name in String.
	 */
	protected String getIomAttributeName() {
		return iomAttributeName==null? dbColumnName : iomAttributeName;
	}
	/** set the given attribute name in type String.
	 * @param attributeName
	 */
	protected void setIomAttributeName(String attributeName) {
		this.iomAttributeName = attributeName;
	}
	/** get the type of JDBC/DB column.
	 * <p>
	 * requirements:
     * <li>has to be a type of: java.sql.Types</li>
     * <p>
	 * @return typeNumber in integer.
	 */
	protected Integer getDbColumnType() {
		return attributeType;
	}
	/** set the given attribute type.
	 * <p>
	 * requirements:
     * <li>has to be a type of: java.sql.Types</li>
     * <p>
	 * @param attributeType
	 */
	protected void setDbColumnType(Integer attributeType) {
		this.attributeType = attributeType;
	}
	/** get the name of DBColumn type.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * @return DBColumnTypeName in String.
	 */
	protected String getDbColumnTypeName() {
		return attributeTypeName;
	}
	/** set the given DBColumn type name in String.
	 * <p>
	 * requirements:
     * <li>has to be type of: 'java.sql.Types.OTHER'</li>
     * <p>
	 * @param attributeTypeName
	 */
	protected void setDbColumnTypeName(String attributeTypeName) {
		this.attributeTypeName = attributeTypeName;
	}
	/** get the type of DBColumn geometry name.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
	 */
	protected String getDbColumnGeomTypeName() {
		return dbColumnGeomTypeName;
	}
	/** set the given geometry type name.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
     * @param dbColumnGeomTypeName in String.
	 */
	protected void setDbColumnGeomTypeName(String dbColumnGeomTypeName) {
		this.dbColumnGeomTypeName = dbColumnGeomTypeName;
	}
	/** get the coordinate dimension.<br>
	 * the dimension of the coordinates that define this Geometry, which must be the same as the coordinate dimension of the coordinate reference system for this Geometry.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
     * @return coordDimension in integer.
	 */
	protected Integer getCoordDimension() {
		return coordDimension;
	}
	/** set the given coordinate dimension.<br>
	 * the dimension of the coordinates that define this Geometry, which must be the same as the coordinate dimension of the coordinate reference system for this Geometry.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
     * @param coordDimension in Integer.
	 */
	protected void setCoordDimension(Integer coordDimension) {
		this.coordDimension = coordDimension;
	}
	/** get the SRID.<br>
	 * the srId is an integer value that uniquely identifies the Spatial Referencing System (SRS) within the database.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
     * @return srId in Integer.
	 */
	protected Integer getSrId() {
		return srId;
	}
	/** set the given srId.<br>
	 * the srId is an integer value that uniquely identifies the Spatial Referencing System (SRS) within the database.
	 * <p>
	 * requirements:
     * <li>isGeometry() has to be true</li>
     * <p>
     * @param srId
	 */
	protected void setSrId(Integer srId) {
		this.srId = srId;
	}
	/** get the precision in Integer.<br>
	 * precision is the number of digits in the unscaled value.
	 * <p>
	 * @return precision.
	 */
	protected Integer getPrecision() {
		return precision;
	}
	/** set the given bytePrecision in Integer.<br>
	 * precision is the number of digits in the unscaled value.
	 * <p>
	 * @param precision
	 */
	protected void setPrecision(Integer precision) {
		this.precision = precision;
	}
	/** add geometry data to geometry attribute in given attribute descriptors.
	 * @param schemaName
	 * @param tableName
	 * @param attributeDesc
	 * @param db
	 * @return final list of attribute descriptors
	 * @throws SQLException 
	 */
	protected static List<AttributeDescriptor> addGeomDataToAttributeDescriptors(String schemaName, String tableName, List<AttributeDescriptor> attributeDesc, Connection db) throws SQLException {
		for(AttributeDescriptor attr:attributeDesc) {
			if(attr.getDbColumnTypeName().equals(DBCOLUMN_TYPENAME_GEOMETRY)) {
				ResultSet tableInDb =null;
				StringBuilder queryBuild=new StringBuilder();
				queryBuild.append("SELECT "+GEOMCOLUMNS_COLUMN_DIMENSION+","+GEOMCOLUMNS_COLUMN_SRID+","+ GEOMCOLUMNS_COLUMN_TYPE+" FROM geometry_columns WHERE ");
				if(schemaName!=null) {
					queryBuild.append("f_table_schema='"+schemaName+"' AND ");
				}
				queryBuild.append("f_table_name='"+tableName+"' "); 
				queryBuild.append("AND f_geometry_column='"+attr.getDbColumnName()+"';");
				try {
					Statement stmt = db.createStatement();
					tableInDb=stmt.executeQuery(queryBuild.toString());
				} catch (SQLException e) {
					throw new SQLException(e);
				}
				tableInDb.next();
				attr.setCoordDimension(tableInDb.getInt(GEOMCOLUMNS_COLUMN_DIMENSION));
				attr.setSrId(tableInDb.getInt(GEOMCOLUMNS_COLUMN_SRID));
				attr.setDbColumnGeomTypeName(tableInDb.getString(GEOMCOLUMNS_COLUMN_TYPE));
			}
		}
		return attributeDesc;
	}
	/** create selection to table inside schema, create list of attribute descriptors and get it back.
	 * @param schemaName
	 * @param tableName
	 * @param db
	 * @return list of attribute descriptors.
	 * @throws IoxException
	 */
	protected static List<AttributeDescriptor> getAttributeDescriptors(String schemaName, String tableName, Connection db) throws IoxException {
		List<AttributeDescriptor> attrs=new ArrayList<AttributeDescriptor>();
		ResultSet tableInDb =null;
		StringBuilder queryBuild=new StringBuilder();
		queryBuild.append("SELECT * FROM ");
		if(schemaName!=null) {
			queryBuild.append(schemaName+".");
		}
		queryBuild.append(tableName+" WHERE 1<>1;");
		try {
			Statement stmt = db.createStatement();
			tableInDb=stmt.executeQuery(queryBuild.toString());
			if(tableInDb==null) {
				throw new IoxException("table "+schemaName+"."+tableName+" not found");
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		ResultSetMetaData rsmd;
		try {
			rsmd = tableInDb.getMetaData();
			for(int k=1;k<rsmd.getColumnCount()+1;k++) {
				tableInDb.next();
				// create attr descriptor
				AttributeDescriptor attr=new AttributeDescriptor();
				attr.setPrecision(rsmd.getPrecision(k));
				attr.setDbColumnName(rsmd.getColumnName(k));
				attr.setDbColumnType(rsmd.getColumnType(k));
				attr.setDbColumnTypeName(rsmd.getColumnTypeName(k));
				attrs.add(attr);
			}
		} catch (SQLException e) {
			throw new IoxException(e);
		}
		return attrs;
	}
	
	/** check if this is a geometry type.
     * <p>
	 * get type:
	 * <li>getDbColumnGeomTypeName()</li>
	 * <p>
	 * @return true if datatype is part of geometry, false if not.
	 */
	public boolean isGeometry() {
		return attributeType==Types.OTHER && attributeTypeName!=null && attributeTypeName.equals(AttributeDescriptor.DBCOLUMN_TYPENAME_GEOMETRY);
	}
}