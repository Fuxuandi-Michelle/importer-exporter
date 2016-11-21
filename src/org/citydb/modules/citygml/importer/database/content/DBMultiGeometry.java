package org.citydb.modules.citygml.importer.database.content;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

import org.citydb.config.Config;
import org.citydb.config.internal.Internal;
import org.citydb.database.DatabaseConnectionPool;
import org.citydb.log.Logger;

import org.citydb.modules.citygml.importer.util.LocalTextureCoordinatesResolver;
import org.citydb.modules.citygml.importer.util.RingValidator;
import org.citydb.util.Util;
import org.citygml4j.model.citygml.CityGMLClass;
import org.citygml4j.model.gml.GMLClass;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryArrayProperty;
import org.citygml4j.model.gml.geometry.GeometryProperty;
import org.citygml4j.util.child.ChildInfo;
import org.citygml4j.util.gmlid.DefaultGMLIdManager;
import org.citygml4j.util.walker.GeometryWalker;
import org.citygml4j.model.gml.geometry.aggregates.MultiGeometry;
import org.citygml4j.model.gml.geometry.primitives.LinearRing;

public class DBMultiGeometry implements DBImporter {
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final Config config;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psMultiGeom;
	private PreparedStatement psNextSeqValues;
	private DBDeprecatedMaterialModel materialModelImporter;
	private DBOtherGeometry otherGeometryImporter;
	
	private DBPointLineGeometry ptLnGeometryImporter;
	private DBSurfaceOfMultiGeometry surfaceOfMultiGeomImporter;
	private PrimaryKeyManager pkManager;

	private int dbSrid;
	private boolean replaceGmlId;
	private boolean importAppearance;
	private boolean applyTransformation;
	private boolean isImplicit;
	private int batchCounter;
	private int nullGeometryType;
	private String nullGeometryTypeName;
	private LocalTextureCoordinatesResolver localTexCoordResolver;
	private RingValidator ringValidator;
	
	public DBMultiGeometry(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.config = config;
		this.dbImporterManager = dbImporterManager;

		init();
	}
	
	private void init() throws SQLException {	
		
		replaceGmlId = config.getProject().getImporter().getGmlId().isUUIDModeReplace();
		dbSrid = DatabaseConnectionPool.getInstance().getActiveDatabaseAdapter().getConnectionMetaData().getReferenceSystem().getSrid();
		importAppearance = config.getProject().getImporter().getAppearances().isSetImportAppearance();
		applyTransformation = config.getProject().getImporter().getAffineTransformation().isSetUseAffineTransformation();
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();
		
		String gmlIdCodespace = config.getInternal().getCurrentGmlIdCodespace();
		if (gmlIdCodespace != null && gmlIdCodespace.length() > 0)
			gmlIdCodespace = "'" + gmlIdCodespace + "', ";
		else
			gmlIdCodespace = null;
		
		StringBuilder stmt = new StringBuilder()
				.append("insert into MULTI_GEOMETRY (ID, GMLID, ").append(gmlIdCodespace != null ? "GMLID_CODESPACE, " : "").append("PARENT_ID, ROOT_ID, IS_SURFACE, IS_OTHER, IS_MULTI, IS_XLINK, IS_REVERSE, CITYOBJECT_ID) values ")
				.append("(?, ?, ").append(gmlIdCodespace != null ? gmlIdCodespace : "").append("?, ?, ?, ?, ?, ?, ?, ?)");
		
		
		psMultiGeom = batchConn.prepareStatement(stmt.toString());
		psNextSeqValues = batchConn.prepareStatement(dbImporterManager.getDatabaseAdapter().getSQLAdapter().getNextSequenceValuesQuery(DBSequencerEnum.MULTI_GEOMETRY_ID_SEQ));

		materialModelImporter = (DBDeprecatedMaterialModel)dbImporterManager.getDBImporter(DBImporterEnum.DEPRECATED_MATERIAL_MODEL);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		
		surfaceOfMultiGeomImporter = (DBSurfaceOfMultiGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_OF_MULTI_GEOMETRY);
		ptLnGeometryImporter = (DBPointLineGeometry)dbImporterManager.getDBImporter(DBImporterEnum.POINT_LINE_GEOMETRY);
				
		pkManager = new PrimaryKeyManager();
		localTexCoordResolver = dbImporterManager.getLocalTextureCoordinatesResolver();
		ringValidator = new RingValidator();
		
	}
	
	public boolean isMultiGeometry(AbstractGeometry abstractGeometry) {
		switch(abstractGeometry.getGMLClass()) {
		case MULTI_GEOMETRY:
			return true;
		default:
			return false;
		}
	}
	
	public long insert(AbstractGeometry multiGeometry, long cityObjectId) throws SQLException{
		System.out.println("---------------------start insert(AbstractGeometry multiGeometry, long cityObjectId) ----------------------");
		if(!isMultiGeometry(multiGeometry)) {
			StringBuilder msg = new StringBuilder(Util.getGeometrySignature(
					multiGeometry.getGMLClass(), 
					multiGeometry.getId()));
			msg.append(": Unsupported geometry type.");

			LOG.error(msg.toString());
			return 0;
		}
		System.out.println("---------------------stp2 insert(AbstractGeometry multiGeometry, long cityObjectId) ----------------------");
		boolean success = pkManager.retrieveIds(multiGeometry);
		System.out.println("---------------------stp3 insert(AbstractGeometry multiGeometry, long cityObjectId) ----------------------");
		if (!success) {
			StringBuilder msg = new StringBuilder(Util.getGeometrySignature(
				multiGeometry.getGMLClass(), 
					multiGeometry.getId()));
			msg.append(": Failed to acquire primary key values for surface geometry from database.");

			LOG.error(msg.toString());
			return 0;
		}
		
		System.out.println("---------------------middle insert(AbstractGeometry multiGeometry, long cityObjectId) ----------------------");
		long multiGeometryId = pkManager.nextId();
		insert(multiGeometry, multiGeometryId, 0, multiGeometryId, false, false, false, cityObjectId);
		pkManager.clear();

		return multiGeometryId; 
		
	}
	
	public void insert(AbstractGeometry multi_Geometry, 
			long multiGeometryId, 
			long parentId, 
			long rootId, 
			boolean reverse, 
			boolean isXlink, 
			boolean isCopy,
			long cityObjectId) throws SQLException{
		GMLClass multiGeometryType = multi_Geometry.getGMLClass();
		dbImporterManager.updateGeometryCounter(multiGeometryType);
		
		if (!isCopy)
			isCopy = multi_Geometry.hasLocalProperty(Internal.GEOMETRY_ORIGINAL);

		if (!isXlink)
			isXlink = multi_Geometry.hasLocalProperty(Internal.GEOMETRY_XLINK);
		System.out.println("---------------------start insert(,,,,,) ----------------------");
		// gml:id handling
		String origGmlId, gmlId;
		origGmlId = gmlId = multi_Geometry.getId();

		if (gmlId == null || replaceGmlId) {
			if (!multi_Geometry.hasLocalProperty(Internal.GEOMETRY_ORIGINAL)) {
				if (!multi_Geometry.hasLocalProperty("replaceGmlId")) {
					gmlId = DefaultGMLIdManager.getInstance().generateUUID();					
					multi_Geometry.setId(gmlId);
					multi_Geometry.setLocalProperty("replaceGmlId", true);
				}
			} else {
				AbstractGeometry original = (AbstractGeometry)multi_Geometry.getLocalProperty(Internal.GEOMETRY_ORIGINAL);
				if (!original.hasLocalProperty("replaceGmlId")) {
					gmlId = DefaultGMLIdManager.getInstance().generateUUID();					
					original.setId(gmlId);
					original.setLocalProperty("replaceGmlId", true);
				} else
					gmlId = original.getId();

				multi_Geometry.setId(gmlId);
			}
		}
		
		//handle multigeometry
		if(multiGeometryType == GMLClass.MULTI_GEOMETRY) {
			MultiGeometry multiGeometry = (MultiGeometry)multi_Geometry;
			
			if(origGmlId != null && !isCopy)
				dbImporterManager.putUID(origGmlId, multiGeometryId, rootId, reverse, gmlId, CityGMLClass.ABSTRACT_GML_GEOMETRY);
			
			//set root entry
			psMultiGeom.setLong(1, multiGeometryId);
			psMultiGeom.setString(2, gmlId);
			psMultiGeom.setLong(4, rootId);
			psMultiGeom.setInt(5, 0);
			psMultiGeom.setInt(6, 0);
			psMultiGeom.setInt(7, 1);
			psMultiGeom.setInt(8, isXlink ? 1 : 0);
			psMultiGeom.setInt(9, reverse ? 1 : 0);
			
			if(parentId != 0)
				psMultiGeom.setLong(3, parentId);
			else 
				psMultiGeom.setNull(3, Types.NULL);
			
			if(cityObjectId != 0)
				psMultiGeom.setLong(10,cityObjectId);
			else 
				psMultiGeom.setNull(10, Types.NULL);
			
			addBatch();
			
			//set parentId
			parentId = multiGeometryId;
			System.out.println("---------------------start multigeometry geometry member----------------------");
			//get GeometryMember
			if(multiGeometry.isSetGeometryMember()) {
				for(GeometryProperty geometryProperty : multiGeometry.getGeometryMember()) {
					long geometryId = 0;
					//GeometryObject GeometryObject = null;
					
					if(geometryProperty.isSetGeometry()) {
						AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
						
						if(surfaceOfMultiGeomImporter.isSurfaceGeometry(abstractGeometry)) {
							surfaceOfMultiGeomImporter.insert(abstractGeometry,multiGeometryId, cityObjectId);
						} else if(ptLnGeometryImporter.isPointOrLineGeometry(abstractGeometry)) {
							//DBPointCurverGeometry
							ptLnGeometryImporter.insert(abstractGeometry, multiGeometryId, cityObjectId);
						} else if(isMultiGeometry(abstractGeometry)) {
							//another round
							multiGeometryId = pkManager.nextId();
							insert(abstractGeometry, multiGeometryId, parentId, rootId, reverse, isXlink, isCopy, cityObjectId);
							
						} else {
							LOG.error(abstractGeometry.getGMLClass() + " is not supported as member of a " + GMLClass.MULTI_GEOMETRY);
						}
					} else {
						// xlink
						String href = geometryProperty.getHref();

						if (href != null && href.length() != 0) {
							//linkMultiGeometry
							/*dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
									surfaceGeometryId,
									parentId,
									rootId,
									reverse,
									href,
									cityObjectId));*/
						}
					}
						
				}
			}
			
			System.out.println("---------------------start multigeometry geometry members----------------------");
			
			//get geometryMembers
			if(multiGeometry.isSetGeometryMembers()) {
				GeometryArrayProperty<? extends AbstractGeometry> geometryArrayProperty = multiGeometry.getGeometryMembers();
				
				if(geometryArrayProperty.isSetGeometry()) {
					for(AbstractGeometry abstractGeometry : geometryArrayProperty.getGeometry()) {
						
						if(surfaceOfMultiGeomImporter.isSurfaceGeometry(abstractGeometry)) {
							surfaceOfMultiGeomImporter.insert(abstractGeometry,multiGeometryId, cityObjectId);
						} else if(otherGeometryImporter.isPointOrLineGeometry(abstractGeometry)) {
							
							//DBPointCurverGeometry
						} else if(isMultiGeometry(abstractGeometry)) {
							//another round
							multiGeometryId = pkManager.nextId();
							insert(abstractGeometry, multiGeometryId, parentId, rootId, reverse, isXlink, isCopy, cityObjectId);
							
						} else {
							LOG.error(abstractGeometry.getGMLClass() + " is not supported as member of a " + GMLClass.MULTI_GEOMETRY);
						}
						
					}
				}
			}
				
		}
	}
	
	private void addBatch() throws SQLException {
		psMultiGeom.addBatch();
		System.out.println("---------------------execure psMultiGeom----------------------");
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.MULTI_GEOMETRY);
	}
	
	@Override
	public void executeBatch() throws SQLException {
		psMultiGeom.executeBatch();
		batchCounter = 0;
	}
	@Override
	public void close() throws SQLException {
		psMultiGeom.close();
		psNextSeqValues.close();
		
	}
	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.MULTI_GEOMETRY;
	}
	
	private class PrimaryKeyManager extends GeometryWalker{
		private final ChildInfo info = new ChildInfo();
		private long[] ids;
		private int count;
		private int index;

		@Override
		public void visit(AbstractGeometry geometry) {
			
			switch (geometry.getGMLClass()) {
			case MULTI_GEOMETRY:
				count++;
			default:
				break;
			}
		}

		private void clear() {
			reset();
			ids = null;
			count = 0;
			index = 0;
		}

		private boolean retrieveIds(AbstractGeometry geometry) throws SQLException {
			clear();

			// count number of tuples to be inserted into database
			geometry.accept(this);
			if (count == 0)
				return false;

			// retrieve sequence values
			ResultSet rs = null;
			try {
				psNextSeqValues.setInt(1, count);
				rs = psNextSeqValues.executeQuery();

				ids = new long[count];
				int i = 0;

				while (rs.next())
					ids[i++] = rs.getLong(1);

				return true;
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException e) {
						//
					}
				}
			}
		}

		private long nextId() {
			return ids[index++];
		}
	}
}
