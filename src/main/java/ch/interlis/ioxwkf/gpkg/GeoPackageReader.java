package ch.interlis.ioxwkf.gpkg;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxReader;

/** Read data from a GeoPackage.
 * If the file to read from can not be found, an exception will be thrown.
 */
public class GeoPackageReader implements IoxReader {

    // the name of the geometry columns table in the geopackage database
    private static final String GEOMETRY_COLUMNS_TABLE_NAME = "gpkg_geometry_columns";
    private static final String GEOM_COLUMN_NAME = "column_name";
    private static final String GEOM_TYPE_COLUMN_NAME = "geometry_type_name";

    // state
    private int state;
    private static final int START = 0;
    private static final int INSIDE_TRANSFER = 1;
    private static final int INSIDE_BASKET = 2;
    private static final int INSIDE_OBJECT = 3;
    private static final int END_BASKET = 4;
    private static final int END_TRANSFER = 5;
    private static final int END = 6;

    // geopackage reader
    private Connection conn = null;
    private ResultSet featureSet = null;
    private Statement featureStatement = null;

    // iox
    private TransferDescription td;
    private IoxFactoryCollection factory = new ch.interlis.iox_j.DefaultIoxFactoryCollection();
    private java.io.File inputFile = null;
    private String tableName = null;
    private int nextId = 1;

    // model, topic, class
    private String topicIliQName = "Topic";
    private String classIliQName = null;

    // attributes, as read from the sqlite database
    private Map<String, String> gpkgAttributes=new HashMap<String, String>();

    // Name of the geometry attributes in the geopackage database
    private Map<String, String> theGeomAttrs=new HashMap<String, String>();
    
    // attributes, as returned from this reader (as values of IomObjects).
    // List is in the same order as gpkgAttributes, but case of attribute name might be different.
    private Map<String, String> iliAttributes=null;

//    private SimpleDateFormat xtfDate=new SimpleDateFormat("yyyy-MM-dd");
//    private SimpleDateFormat xtfDateTime=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    /** Creates a new geopackage reader.
     * @param gpkgFile to read from
     * @throws IoxException
     */
    public GeoPackageReader(java.io.File gpkgFile, String tableName) throws IoxException {
        this(gpkgFile, tableName, null);
    }
    
    /** Creates a new geopackage reader.
     * @param gpkgFile to read from
     * @throws IoxException
     */
    public GeoPackageReader(java.io.File gpkgFile, String tableName, Settings settings) throws IoxException{
        state = START;
        td = null;
        inputFile = gpkgFile;
        this.tableName = tableName;
        init(inputFile, settings);
    }
    
    /** Initialize file content.
     * @param gpkgFile
     * @param settings
     * @throws IoxException
     */
    private void init(java.io.File gpkgFile, Settings settings) throws IoxException {
        factory = new ch.interlis.iox_j.DefaultIoxFactoryCollection();
        
    
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + gpkgFile);
//            Statement statement = conn.createStatement();
//            ResultSet rs = statement.executeQuery("SELECT * FROM point;");
//            while(rs.next())
//            {
//                // read the result set
//                System.out.println(rs.getString("foo").toString());
//            }
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                    conn = null;
                } catch (SQLException f) {
                    throw new IoxException(f);
                }
            }
            throw new IoxException(e);
        } 
    }

    /** The optional Interlis model.
     * @param td
     */
    public void setModel(TransferDescription td){
        this.td = td;
    }

    /** read the path of input geopackage file and return the single name of geopackage file.
     * @return file path to read from.
     * @throws IoxException
     */
    private String getNameOfDataFile() throws IoxException {
        // get path of the shp file
        String path=inputFile.getPath();
        if(path!=null) {
            String[] pathParts=path.split("\\\\");
            int partLength=pathParts.length;
            String file=pathParts[partLength-1];
            String[] fileParts=file.split(".gpkg"); // TODO: support more extensions
            file=fileParts[0];
            return file;
        } else {
            throw new IoxException("expected gpkg file");
        }
    }

    @Override
    public IoxEvent read() throws IoxException {
        IomObject iomObj = null;        
        if(state == START){
            state = INSIDE_TRANSFER;
            topicIliQName = null;
            classIliQName = null;
            return new ch.interlis.iox_j.StartTransferEvent();
        }
        if(state==INSIDE_TRANSFER){
            state=INSIDE_BASKET;
        }
        if(state == INSIDE_BASKET) {
            System.out.println("********* INSIDE_BASKET");
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = conn.createStatement();
                rs = stmt.executeQuery("SELECT * FROM " + tableName);
                ResultSetMetaData md = rs.getMetaData();
                for (int i=1; i<=md.getColumnCount(); i++) {
                    gpkgAttributes.put(md.getColumnLabel(i), md.getColumnTypeName(i));
                }
                rs.close();
                
                String sql = "SELECT "+GEOM_COLUMN_NAME+", "+GEOM_TYPE_COLUMN_NAME+" FROM "+GEOMETRY_COLUMNS_TABLE_NAME + " WHERE table_name = "
                        + " '" + tableName + "';";
                rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    theGeomAttrs.put(rs.getObject(GEOM_COLUMN_NAME).toString(), rs.getObject(GEOM_TYPE_COLUMN_NAME).toString());
                }
                rs.close();
            } catch (SQLException e) {
                throw new IoxException(e);
            } finally {
                try { 
                    if (rs != null) {
                        rs.close(); 
                    }
                } catch (Exception e) {
                    throw new IoxException(e);
                };
                try { 
                    if (stmt != null) {
                        stmt.close(); 
                    }
                } catch (Exception e) {
                    throw new IoxException(e);
                };
            }
 
            // result set (iterator) for the features in the table
            try {
                List<String> gpkgAttributeNames = new ArrayList<String>(gpkgAttributes.keySet());
                String attrs = String.join(",", gpkgAttributeNames);
                String sql = "SELECT " + attrs + " FROM " + tableName;
                System.out.println(sql);
                featureStatement = conn.createStatement();
                featureSet = featureStatement.executeQuery(sql);
            } catch (SQLException e) {
                throw new IoxException(e);
            }
  
          
            if (td != null) {
                
            } else {
                topicIliQName=getNameOfDataFile()+".Topic";
                classIliQName=topicIliQName+".Class"+getNextId();
                iliAttributes=new HashMap<String, String>();
                for(String gpkgAttribute:gpkgAttributes.keySet()) {
                    iliAttributes.put(gpkgAttribute, gpkgAttribute);
                }
            }
            String bid="b"+getNextId();
            state=INSIDE_OBJECT;
            return new ch.interlis.iox_j.StartBasketEvent(topicIliQName, bid);
        }
        if(state==INSIDE_OBJECT) {
            System.out.println("********* INSIDE_OBJECT");
            Gpkg2iox gpkg2iox = new Gpkg2iox();
            try {
                while(featureSet.next()) {
                    // feature object
                    iomObj=createIomObject(classIliQName, null);

                    for (Map.Entry<String, String> entry : gpkgAttributes.entrySet()) {
                        IomObject subIomObj=null;
                        
                        // attribute name
                        String gpkgAttrName = entry.getKey();
                        String gpkgAttrType = entry.getValue();
                        String iliAttrName=iliAttributes.get(gpkgAttrName);

                        // attribute value
                        Object gpkgAttrValue = featureSet.getObject(gpkgAttrName);
                        if (gpkgAttrValue!=null) {
                            if (theGeomAttrs.containsKey(gpkgAttrName)) {
                                try {
                                    subIomObj = gpkg2iox.read((byte[])gpkgAttrValue);
                                    iomObj.addattrobj(iliAttrName, subIomObj);
                                } catch (ParseException e) {
                                    throw new IoxException(e);
                                }
                            } else {
                                if (gpkgAttrType.equalsIgnoreCase("BLOB")) {
                                    String s = Base64.getEncoder().encodeToString((byte[])gpkgAttrValue);
                                    iomObj.setattrvalue(iliAttrName, s);
                                } else {
                                    String valueStr=gpkgAttrValue.toString();
                                    if(valueStr!=null && valueStr.length()>0)
                                    iomObj.setattrvalue(iliAttrName, valueStr);
                                }
                            }
                        }
                    }
                    // return each simple feature object.
                    return new ch.interlis.iox_j.ObjectEvent(iomObj);
                }
            } catch (SQLException e) {
                throw new IoxException(e);
            } finally {
                try { 
                    if (featureSet != null) {
                        featureSet.close(); 
                    }
                } catch (Exception e) {
                    throw new IoxException(e);
                };
                try { 
                    if (featureStatement != null) {
                        featureStatement.close(); 
                    }
                } catch (Exception e) {
                    throw new IoxException(e);
                };
            }
            state=END_BASKET;            
        }
        if(state==END_BASKET){
            state=END_TRANSFER;
            return new ch.interlis.iox_j.EndBasketEvent();
        }
        if(state==END_TRANSFER){
            state=END;
            return new ch.interlis.iox_j.EndTransferEvent();
        }
        return null;
    }

    @Override
    public void close() throws IoxException {
        if (conn != null) {
            try {
                conn.close();
                conn = null;
            } catch (SQLException e) {
                throw new IoxException(e);
            }
        }        
    }

    @Override
    public IomObject createIomObject(String type, String oid) throws IoxException {
        if(oid==null) {
            oid="o"+getNextId();
        }
        return factory.createIomObject(type, oid);
    }

    @Override
    public IoxFactoryCollection getFactory() throws IoxException {
        return factory;
    }

    @Override
    public void setFactory(IoxFactoryCollection factory) throws IoxException {
        this.factory=factory;
        
    }
    
    private String getNextId() {
        int count=nextId;
        nextId+=1;
        return String.valueOf(count);
    }
}
