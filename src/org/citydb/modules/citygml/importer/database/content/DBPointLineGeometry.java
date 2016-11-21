package org.citydb.modules.citygml.importer.database.content;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.citydb.api.geometry.GeometryObject;
import org.citydb.config.Config;
import org.citydb.database.DatabaseConnectionPool;
import org.citydb.log.Logger;
import org.citydb.modules.citygml.importer.util.LocalTextureCoordinatesResolver;
import org.citydb.modules.citygml.importer.util.RingValidator;
import org.citydb.util.Util;
import org.citygml4j.model.gml.GMLClass;
import org.citygml4j.model.gml.geometry.AbstractGeometry;

public class DBPointLineGeometry implements DBImporter {
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final Config config;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psPtLnGeom;
	//private PreparedStatement psNextSeqValues;

	private DBOtherGeometry otherGeometryImporter;
	
	private int batchCounter;
	private int nullGeometryType;
	private String nullGeometryTypeName;

	public DBPointLineGeometry(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.config = config;
		this.dbImporterManager = dbImporterManager;

		init();
	}
	
	private void init() throws SQLException {
		
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		String gmlIdCodespace = config.getInternal().getCurrentGmlIdCodespace();
		if (gmlIdCodespace != null && gmlIdCodespace.length() > 0)
			gmlIdCodespace = "'" + gmlIdCodespace + "', ";
		else
			gmlIdCodespace = null;	
		
		StringBuilder stmt = new StringBuilder()
				.append("insert into POINT_CURVE_GEOMETRY (ID, GMLID, ").append(gmlIdCodespace != null ? "GMLID_CODESPACE, " : "").append("PARENT_MULTI_GEOM_ID, ROOT_MULTI_GEOM_ID, GEOMETRY, CITYOBJECT_ID) values ")
				.append("(?, ?, ").append(gmlIdCodespace != null ? gmlIdCodespace : "").append("?, ?, ?, ?");
		
		psPtLnGeom = batchConn.prepareStatement(stmt.toString());
		//psNextSeqValues = batchConn.prepareStatement(dbImporterManager.getDatabaseAdapter().getSQLAdapter().getNextSequenceValuesQuery(DBSequencerEnum.POINT_LINE_GEOMETRY_ID_SEQ));

		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		

		
	}
	
	
	
	public boolean isPointOrLineGeometry(AbstractGeometry abstractGeometry) {
		return otherGeometryImporter.isPointOrLineGeometry(abstractGeometry);
	}
	
	
	public long insert(AbstractGeometry ptLnGeometry, Long rootMultiGeomId, Long cityObjectId) throws SQLException {
		long ptLnGeometryId = dbImporterManager.getDBId(DBSequencerEnum.POINT_LINE_GEOMETRY_ID_SEQ);
		if(!isPointOrLineGeometry(ptLnGeometry)) {
			StringBuilder msg = new StringBuilder(Util.getGeometrySignature(
					ptLnGeometry.getGMLClass(), 
					ptLnGeometry.getId()));
			msg.append(": Unsupported geometry type.");

			LOG.error(msg.toString());
			return 0;
		}
		
		insertPtLnGeometry(ptLnGeometry, ptLnGeometryId, (long)0, rootMultiGeomId, cityObjectId);
		return 0;
	}
	
	public void insertPtLnGeometry(AbstractGeometry ptLnGeometry, 
			long ptLnGeometryId,
			long parentId, 
			long rootMultiGeomId,
			long cityObjectId) throws SQLException {
		GMLClass ptLnGeometryType = ptLnGeometry.getGMLClass();
		dbImporterManager.updateGeometryCounter(ptLnGeometryType);
		
		psPtLnGeom.setLong(1, ptLnGeometryId);
		String gmlId = ptLnGeometry.getId();
		psPtLnGeom.setString(2, gmlId);
		psPtLnGeom.setLong(3, parentId);
		psPtLnGeom.setLong(4,rootMultiGeomId);
		
		GeometryObject geometryObject = null;
		
		if(isPointOrLineGeometry(ptLnGeometry)) {
			geometryObject = otherGeometryImporter.getPointOrCurveGeometry(ptLnGeometry);
		}
		
		if (geometryObject != null)
			psPtLnGeom.setObject(5, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
		else
			psPtLnGeom.setNull(5, nullGeometryType, nullGeometryTypeName);
		
		psPtLnGeom.setLong(6, cityObjectId);
		
		addBatch();
		
	}
	
	
	private void addBatch() throws SQLException {
		psPtLnGeom.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.POINT_LINE_GEOMETRY);
	}

	@Override
	public void executeBatch() throws SQLException {
		psPtLnGeom.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		psPtLnGeom.close();
		//psNextSeqValues.close();
		
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.POINT_LINE_GEOMETRY;
	}
	
	
	
	
}
