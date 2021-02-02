package ch.interlis.ioxwkf.gpkg;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import net.iharder.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.vividsolutions.jts.io.ParseException;

import ch.ehi.basics.settings.Settings;
import ch.interlis.ili2c.metamodel.Model;
import ch.interlis.ili2c.metamodel.PredefinedModel;
import ch.interlis.ili2c.metamodel.Table;
import ch.interlis.ili2c.metamodel.Topic;
import ch.interlis.ili2c.metamodel.TransferDescription;
import ch.interlis.ili2c.metamodel.Viewable;
import ch.interlis.iom.IomObject;
import ch.interlis.iox.IoxEvent;
import ch.interlis.iox.IoxException;
import ch.interlis.iox.IoxFactoryCollection;
import ch.interlis.iox.IoxReader;
import ch.ehi.ili2gpkg.Gpkg2iox;
import ch.interlis.ioxwkf.dbtools.AttributeDescriptor;

/** Read a table from a GeoPackage database.
 * If the file to read from can not be found, an exception will be thrown.
 * <p>
 * <b>Interlis model</b><p>
 * <ul>
 * <li>If a model is set, make sure that the file contents matches a class in the model. 
 * The attribute names (ignoring case) are used to search the class. 
 * If no class can be found, an exception will be thrown.</li>
 * <li>If no model is set, the table definition will be used to define a reader internal model.</li>
 * </ul>
 * <p>
 * example:<br>
 * File file = new File("file.gpkg");<br>
 * GpkgReader reader = new GpkgReader(file);<br>
 * reader.setModel(td);<br>
 * <p>
 * 
 * <b>IomObject</b><p>
 * IomObject iomObj=createIomObject(type,oid);<br>
 * <br>
 * If the model is set:
 * <li>type==according to found class</li>
 * <li>oid==Start counting by 1</li>
 * <p>
 * If there is no model set:
 * <li>type==modelname.topicname.classname</li>
 * <li>The modelname is the name of the table name.</li>
 * <li>The topicname is 'Topic'</li>
 * <li>The classname is: 'Class1'</li>
 * <li>oid==Start counting by 1</li>
 * <p>
 * 
 * <b>Data type mapping</b><br>
 * If a text attribute from the table is mapped to a non TEXT attribute according to the given Interlis class,
 * it is expected that the value is encoded according to Interlis 2.3 encoding rules.<p>
 * <b>Not supported INTERLIS data types</b><p>
 * <li>StructureType</li>
 * <li>ReferenceType</li>
 * <p>
 * <b>Attachement</b><p>
 * <li><a href="http://www.geopackage.org/spec/">GeoPackage specification</a></li>
 * <li><a href="https://www.ech.ch/vechweb/page?p=dossier&documentNumber=eCH-0031&documentVersion=2.0">Interlis specification</a></li>
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
    private ResultSet featureResultSet = null;
    private Statement featureStatement = null;

	private SimpleDateFormat xtfDate=new SimpleDateFormat("yyyy-MM-dd");
	private SimpleDateFormat xtfDateTime=new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    // iox
    private TransferDescription td;
    private IoxFactoryCollection factory = new ch.interlis.iox_j.DefaultIoxFactoryCollection();
    private File inputFile = null;
    private String tableName = null;
    private int nextId = 1;

    // model, topic, class
    private String topicIliQName = "Topic";
    private String classIliQName = null;

    // attributes, as read from the sqlite database
    private List<AttributeDescriptor> gpkgAttributes = new ArrayList<AttributeDescriptor>();

    // Name of the geometry attributes in the geopackage database
    private List<String> theGeomAttrs = new ArrayList<String>();
    
    // attributes, as returned from this reader (as values of IomObjects).
    private List<String> iliAttributes=null;

    /** Creates a new geopackage reader.
     * @param gpkgFile to read from
     * @throws IoxException
     */
    public GeoPackageReader(File gpkgFile, String tableName) throws IoxException {
        this(gpkgFile, tableName, null);
    }
    
    /** Creates a new geopackage reader.
     * @param gpkgFile to read from
     * @throws IoxException
     */
    public GeoPackageReader(File gpkgFile, String tableName, Settings settings) throws IoxException{
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
    private void init(File gpkgFile, Settings settings) throws IoxException {
        factory = new ch.interlis.iox_j.DefaultIoxFactoryCollection();
        try {
            conn = DriverManager.getConnection("jdbc:sqlite:" + gpkgFile.getAbsolutePath());
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM gpkg_contents;"); // TODO: better handling of GeoPackage check?  
            rs.close();
        } catch (SQLException e) {
            if (conn != null) {
                try {
                    conn.rollback();
                    conn.close();
                    conn = null;
                } catch (SQLException f) {
                    throw new IoxException("expected valid geopackage file");
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
            Statement stmt = null;
            ResultSet rs = null;
            try {
                stmt = conn.createStatement();
                
                // Figure out all geometry attributes in this table.
                String sql = "SELECT "+GEOM_COLUMN_NAME+", "+GEOM_TYPE_COLUMN_NAME+" FROM "+GEOMETRY_COLUMNS_TABLE_NAME + " WHERE table_name = "
                        + " '" + tableName + "';";
                rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    theGeomAttrs.add(rs.getObject(GEOM_COLUMN_NAME).toString().toLowerCase());
                }
                rs.close();

                rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
                ResultSetMetaData md = rs.getMetaData();
                for (int i=1; i<=md.getColumnCount(); i++) {
                    AttributeDescriptor attrDesc = new AttributeDescriptor();
                    attrDesc.setDbColumnName(md.getColumnLabel(i).toLowerCase());
                    attrDesc.setDbColumnTypeName(md.getColumnTypeName(i).toLowerCase());
                    gpkgAttributes.add(attrDesc);
                }
                rs.close();
                
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
                List<String> gpkgAttributeNames = new ArrayList<String>();
                for (AttributeDescriptor attr : gpkgAttributes) {
                    gpkgAttributeNames.add(attr.getDbColumnName());
                }
                String attrs = GeoPackageWriter.StringJoin(",", gpkgAttributeNames);
                String sql = "SELECT " + attrs + " FROM " + tableName;
                featureStatement = conn.createStatement();
                featureResultSet = featureStatement.executeQuery(sql);
            } catch (SQLException e) {
                throw new IoxException(e);
            }
  
          
            if (td != null) {
                iliAttributes=new ArrayList<String>();
                Viewable viewable=getViewableByGpkgAttributes(gpkgAttributes, iliAttributes);
                if(viewable==null){
                    throw new IoxException("attributes '"+getNameList(gpkgAttributes)+"' not found in model: '"+td.getLastModel().getName()+"'.");
                }
                // get model data
                topicIliQName=viewable.getContainer().getScopedName();
                classIliQName=viewable.getScopedName();
            } else {
                // if no model is set, the table name must be equal to the model name
                topicIliQName=tableName+".Topic";
                classIliQName=topicIliQName+".Class"+getNextId();
                iliAttributes=new ArrayList<String>();
                for(AttributeDescriptor gpkgAttribute : gpkgAttributes) {
                    iliAttributes.add(gpkgAttribute.getDbColumnName());
                }
            }
            String bid="b"+getNextId();
            state=INSIDE_OBJECT;
            return new ch.interlis.iox_j.StartBasketEvent(topicIliQName, bid);
        }
        if(state==INSIDE_OBJECT) {
            Gpkg2iox gpkg2iox = new Gpkg2iox(); // TODO: use mapper instead?
            try {
                while(featureResultSet.next()) {
                    // feature object
                    iomObj=createIomObject(classIliQName, null);
                    int attrc=gpkgAttributes.size();
                    for(int attri=0;attri<attrc;attri++) {
                        AttributeDescriptor gpkgAttribute = gpkgAttributes.get(attri);
                        IomObject subIomObj=null;
                        
                        // attribute name
                        String gpkgAttrName = gpkgAttribute.getDbColumnName();
                        String iliAttrName=iliAttributes.get(attri);
                        
                        // attribute type
                        String gpkgAttrType = gpkgAttribute.getDbColumnTypeName();
                        
                        // attribute value
                        // TODO: handle different date datetime stuff
                        // see https://www.sqlite.org/datatype3.html
                        Object gpkgAttrValue = featureResultSet.getObject(gpkgAttrName);
                        
                        if (gpkgAttrValue!=null) {
                            if (theGeomAttrs.contains(gpkgAttrName)) {
                                try {
                                    subIomObj = gpkg2iox.read((byte[])gpkgAttrValue);                                    
                                    if (subIomObj != null) {
                                        iomObj.addattrobj(iliAttrName, subIomObj); 
                                    }
                                } catch (ParseException e) {
                                    throw new IoxException(e);
                                }
                            } else {
                                if (gpkgAttrType.equalsIgnoreCase("BLOB")) {
                                    String s = Base64.encodeBytes((byte[])gpkgAttrValue);
                                    iomObj.setattrvalue(iliAttrName, s);
                                } else if (gpkgAttrType.equalsIgnoreCase("DATETIME")) {
                                	// TODO: timezone conversion needed?
                                	String valueStr=gpkgAttrValue.toString();
                                	if (valueStr != null) {
                                		iomObj.setattrvalue(iliAttrName, valueStr.substring(0, valueStr.length() - 1));
                                	}
                                } else if (gpkgAttrType.equalsIgnoreCase("BOOLEAN")) {
                                    String valueStr=gpkgAttrValue.toString();
                                    if(valueStr!=null && valueStr.length()>0) {
                                        if (valueStr.equals("0")) {
                                            iomObj.setattrvalue(iliAttrName, "false"); 
                                        } else {
                                            iomObj.setattrvalue(iliAttrName, "true"); 
                                        }
                                    }
                                }
                                else {
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
            } 
            try { 
                if (featureResultSet != null) {
                    featureResultSet.close(); 
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

    private String getNameList(List<AttributeDescriptor> attrs) {
        StringBuffer ret=new StringBuffer();
        String sep="";
        for (AttributeDescriptor attr:attrs) {
            ret.append(sep);
            ret.append(attr.getDbColumnName());
            sep=",";
        }
        return ret.toString();
    }

    private Viewable getViewableByGpkgAttributes(List<AttributeDescriptor> gpkgAttrs, List<String> iliAttrs) throws IoxException {
        Viewable viewable=null;
        ArrayList<ArrayList<Viewable>> models=setupNameMapping();
        // first last model file.
        for(int modeli=models.size()-1;modeli>=0;modeli--){
            ArrayList<Viewable> classes=models.get(modeli);
            for(int classi=classes.size()-1;classi>=0;classi--){
                Viewable iliViewable=classes.get(classi);
                Map<String,ch.interlis.ili2c.metamodel.AttributeDef> iliAttrMap=new HashMap<String,ch.interlis.ili2c.metamodel.AttributeDef>();
                Iterator attrIter=iliViewable.getAttributes();
                ArrayList<String> geomAttrs=new ArrayList<String>();
                while(attrIter.hasNext()){
                    ch.interlis.ili2c.metamodel.AttributeDef attribute=(ch.interlis.ili2c.metamodel.AttributeDef) attrIter.next();
                    String attrName=attribute.getName();
                    ch.interlis.ili2c.metamodel.Type type=attribute.getDomainResolvingAliases();
                    if(type instanceof ch.interlis.ili2c.metamodel.CoordType || type instanceof ch.interlis.ili2c.metamodel.LineType) {
                        geomAttrs.add(attrName.toLowerCase());
                    } else {
                        iliAttrMap.put(attrName.toLowerCase(),attribute);
                    }
                }
                // check if ili model attributes are the same as the attributes in the gpkg file
                if(equalAttrs(iliAttrMap, geomAttrs, gpkgAttrs)) {
                    viewable=iliViewable;
                    iliAttrs.clear();
                    for (AttributeDescriptor gpkgAttr : gpkgAttrs) {
                        if (geomAttrs.contains(gpkgAttr.getDbColumnName())) {
                            iliAttrs.add(gpkgAttr.getDbColumnName());
                        } else {
                            iliAttrs.add(iliAttrMap.get(gpkgAttr.getDbColumnName().toLowerCase()).getName());
                        }
                    }
                    return viewable;
                }
            }
        }
        return null;
    }

    private ArrayList<ArrayList<Viewable>> setupNameMapping(){
        ArrayList<ArrayList<Viewable>> models=new ArrayList<ArrayList<Viewable>>();
        Iterator tdIterator = td.iterator();
        while(tdIterator.hasNext()){
            Object modelObj = tdIterator.next();
            if(!(modelObj instanceof Model)){
                continue;
            }
            if(modelObj instanceof PredefinedModel) {
                continue;
            }
            // iliModel
            Model model = (Model) modelObj;
            ArrayList<Viewable> classes=new ArrayList<Viewable>();
            Iterator modelIterator = model.iterator();
            while(modelIterator.hasNext()){
                Object topicObj = modelIterator.next();
                if(!(topicObj instanceof Topic)){
                    continue;
                }
                // iliTopic
                Topic topic = (Topic) topicObj;
                // iliClass
                Iterator classIter=topic.iterator();
                while(classIter.hasNext()){
                    Object classObj=classIter.next();
                    if(!(classObj instanceof Table)){
                        continue;
                    }
                    Table viewable = (Table) classObj;
                    if(viewable.isAbstract() || !viewable.isIdentifiable()) {
                        continue;
                    }
                    classes.add(viewable);
                }
            }
            models.add(classes);
        }
        return models;
    }
 
    private boolean equalAttrs(Map<String, ch.interlis.ili2c.metamodel.AttributeDef> iliAttrs, List<String> geomAttrs, List<AttributeDescriptor> gpkgAttrs) {
        if (iliAttrs.size() + geomAttrs.size() != gpkgAttrs.size()) {
            return false;
        }
        for (AttributeDescriptor gpkgAttr : gpkgAttrs) {
            if (!iliAttrs.containsKey(gpkgAttr.getDbColumnName()) && !geomAttrs.contains(gpkgAttr.getDbColumnName())) {
                return false;
            }
        }
        return true;
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
    
    /** gets the list of all attribute names (incl. geometries) in the read/returned IomObjects.
     * @return list of attribute names.
     */
    public String[] getAttributes() {
        return iliAttributes.toArray(new String[iliAttributes.size()]);
    }
    
    /**
     * gets the list of geometry attributes in the geopackage table.
     * @return map of geometry attribute names.
     */
    public List<String> getGeometryAttributes() {
        return this.theGeomAttrs;
    }

}
