package org.citydb.modules.citygml.importer.database.content;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

import org.citydb.api.geometry.GeometryObject;
import org.citydb.config.Config;
import org.citydb.database.TableEnum;
import org.citydb.log.Logger;
import org.citydb.modules.citygml.common.database.xlink.DBXlinkSurfaceGeometry;
import org.citydb.util.Util;
import org.citygml4j.geometry.Matrix;
import org.citygml4j.model.citygml.CityGMLClass;
import org.citygml4j.model.citygml.building.AbstractBoundarySurface;
import org.citygml4j.model.citygml.building.BoundarySurfaceProperty;

import org.citygml4j.model.citygml.buildingExtra.Stair;
import org.citygml4j.model.citygml.core.ImplicitGeometry;
import org.citygml4j.model.citygml.core.ImplicitRepresentationProperty;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryProperty;

public class DBStair implements DBImporter{
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psStair;
	private DBCityObject cityObjectImporter;
	private DBThematicSurface thematicSurfaceImporter;
	private DBSurfaceGeometry surfaceGeometryImporter;
	private DBOtherGeometry otherGeometryImporter;
	private DBImplicitGeometry implicitGeometryImporter;

	//multiGeometry
	private DBMultiGeometry multiGeometryImporter;
	
	private boolean affineTransformation;
	private int batchCounter;
	private int nullGeometryType;
	private String nullGeometryTypeName;
	
	
	public DBStair(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		affineTransformation = config.getProject().getImporter().getAffineTransformation().isSetUseAffineTransformation();
		init();
	}
	
	private void init() throws SQLException {
		
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into STAIR (ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, BUILDING_ID, PODIUM_ID, STOREY_ID, ")
		.append("LOD4_BREP_ID, LOD4_OTHER_GEOM, ")
		.append("LOD4_IMPLICIT_REP_ID, ")
		.append("LOD4_IMPLICIT_REF_POINT, ")
		.append("LOD4_IMPLICIT_TRANSFORMATION, LOD4_MULTI_GEOM_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psStair = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		implicitGeometryImporter = (DBImplicitGeometry)dbImporterManager.getDBImporter(DBImporterEnum.IMPLICIT_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
		
		multiGeometryImporter = (DBMultiGeometry)dbImporterManager.getDBImporter(DBImporterEnum.MULTI_GEOMETRY);
	}
	
	public long insert(Stair stair, CityGMLClass parent, long parentId) throws SQLException {	
		long stairId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		if(stairId == 0)
			return 0;
		
		// CityObject
		cityObjectImporter.insert(stair, stairId);
		
		//Stair
		//ID
		psStair.setLong(1, stairId);
		
		// bldg:class
		if (stair.isSetClazz() && stair.getClazz().isSetValue()) {
			psStair.setString(2, stair.getClazz().getValue());
			psStair.setString(3, stair.getClazz().getCodeSpace());
		} else {
			psStair.setNull(2, Types.VARCHAR);
			psStair.setNull(3, Types.VARCHAR);
		}

		// bldg:function
		if (stair.isSetFunction()) {
			String[] function = Util.codeList2string(stair.getFunction());
			psStair.setString(4, function[0]);
			psStair.setString(5, function[1]);
		} else {
			psStair.setNull(4, Types.VARCHAR);
			psStair.setNull(5, Types.VARCHAR);
		}

		// bldg:usage
		if (stair.isSetUsage()) {
			String[] usage = Util.codeList2string(stair.getUsage());
			psStair.setString(6, usage[0]);
			psStair.setString(7, usage[1]);
		} else {
			psStair.setNull(6, Types.VARCHAR);
			psStair.setNull(7, Types.VARCHAR);
		}
		
		// parentId
		switch (parent) {
		case BUILDING:
		case BUILDING_PART:
			psStair.setLong(8, parentId);
			psStair.setNull(9, Types.NULL);
			psStair.setNull(10, Types.NULL);
			break;
		case PODIUM:
			psStair.setNull(8, Types.NULL);
			psStair.setLong(9, parentId);
			psStair.setNull(10, Types.NULL);
			break;
		case STOREY:
			psStair.setNull(8, Types.NULL);
			psStair.setNull(9, Types.NULL);
			psStair.setLong(10, parentId);
			break;
		default:
			psStair.setNull(8, Types.NULL);
			psStair.setNull(9, Types.NULL);
			psStair.setNull(10, Types.NULL);
		}
		
		//Gemetry
		
		GeometryProperty<? extends AbstractGeometry> geometryProperty = stair.getLod4Geometry();
		long geometryId = 0;
		long multiGeometryId = 0;
		GeometryObject geometryObject = null;
		
		if (geometryProperty != null) {
			System.out.println("---------------------start: import stair geometry----------------------");
			if (geometryProperty.isSetGeometry()) {
				AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
				if (surfaceGeometryImporter.isSurfaceGeometry(abstractGeometry))
					geometryId = surfaceGeometryImporter.insert(abstractGeometry, stairId);
				else if (otherGeometryImporter.isPointOrLineGeometry(abstractGeometry))
					geometryObject = otherGeometryImporter.getPointOrCurveGeometry(abstractGeometry);
				else if (multiGeometryImporter.isMultiGeometry(abstractGeometry))
					multiGeometryId = multiGeometryImporter.insert(abstractGeometry, stairId);

				else {
					StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
							stair.getCityGMLClass(), 
							stair.getId()));
					msg.append(": Unsupported geometry type ");
					msg.append(abstractGeometry.getGMLClass()).append('.');

					LOG.error(msg.toString());
				}

				geometryProperty.unsetGeometry();
			} else {
				// xlink
				String href = geometryProperty.getHref();

				if (href != null && href.length() != 0) {
					dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
							href, 
							stairId, 
							TableEnum.STAIR, 
							"LOD4_BREP_ID"));
				}
			}
			
			System.out.println("---------------------end: import stair geometry----------------------");
			
		}
		
		if (geometryId != 0)
			psStair.setLong(11, geometryId);
		else
			psStair.setNull(11, Types.NULL);

		if (geometryObject != null)
			psStair.setObject(12, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
		else
			psStair.setNull(12, nullGeometryType, nullGeometryTypeName);
		
		if(multiGeometryId != 0)
			psStair.setLong(16, multiGeometryId);
		else 
			psStair.setNull(16, Types.NULL);
		
		//implicit geometry
		
		ImplicitRepresentationProperty implicit = stair.getLod4ImplicitRepresentation();
		GeometryObject pointGeom = null;
		String matrixString = null;
		long implicitId = 0;
		
		if (implicit != null) {
			if (implicit.isSetObject()) {
				ImplicitGeometry geometry = implicit.getObject();

				// reference Point
				if (geometry.isSetReferencePoint())
					pointGeom = otherGeometryImporter.getPoint(geometry.getReferencePoint());

				// transformation matrix
				if (geometry.isSetTransformationMatrix()) {
					Matrix matrix = geometry.getTransformationMatrix().getMatrix();
					if (affineTransformation)
						matrix = dbImporterManager.getAffineTransformer().transformImplicitGeometryTransformationMatrix(matrix);

					matrixString = Util.collection2string(matrix.toRowPackedList(), " ");
				}

				// reference to IMPLICIT_GEOMETRY
				implicitId = implicitGeometryImporter.insert(geometry, stairId);
			}
		}

		if (implicitId != 0)
			psStair.setLong(13, implicitId);
		else
			psStair.setNull(13, Types.NULL);

		if (pointGeom != null)
			psStair.setObject(14, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(pointGeom, batchConn));
		else
			psStair.setNull(14, nullGeometryType, nullGeometryTypeName);

		if (matrixString != null)
			psStair.setString(15, matrixString);
		else
			psStair.setNull(15, Types.VARCHAR);
		
		psStair.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.STAIR);
		
		// BoundarySurfaces
		if (stair.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : stair.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, stair.getCityGMLClass(), stairId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								stair.getCityGMLClass(), 
								stair.getId()));
						msg.append(": Failed to write ");
						msg.append(Util.getFeatureSignature(
								boundarySurface.getCityGMLClass(), 
								gmlId));

						LOG.error(msg.toString());
					}

					// free memory of nested feature
					boundarySurfaceProperty.unsetBoundarySurface();
				} else {
					// xlink
					String href = boundarySurfaceProperty.getHref();

					if (href != null && href.length() != 0) {
						LOG.error("XLink reference '" + href + "' to " + CityGMLClass.ABSTRACT_BUILDING_BOUNDARY_SURFACE + " feature is not supported.");
					}
				}
			}
		}

		// insert local appearance
		cityObjectImporter.insertAppearance(stair, stairId);

		
		
		return stairId;
	}

	@Override
	public void executeBatch() throws SQLException {
		psStair.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		
		psStair.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.STAIR;
	}
	
}
