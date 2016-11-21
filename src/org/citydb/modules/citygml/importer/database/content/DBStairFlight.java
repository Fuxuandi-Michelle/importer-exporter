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

import org.citygml4j.model.citygml.buildingExtra.StairFlight;
import org.citygml4j.model.citygml.core.ImplicitGeometry;
import org.citygml4j.model.citygml.core.ImplicitRepresentationProperty;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryProperty;

public class DBStairFlight implements DBImporter{
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	//multiGeometry
	private DBMultiGeometry multiGeometryImporter;

	private PreparedStatement psStairFlight;
	private DBCityObject cityObjectImporter;
	private DBThematicSurface thematicSurfaceImporter;
	private DBSurfaceGeometry surfaceGeometryImporter;
	private DBOtherGeometry otherGeometryImporter;
	private DBImplicitGeometry implicitGeometryImporter;

	private boolean affineTransformation;
	private int batchCounter;
	private int nullGeometryType;
	private String nullGeometryTypeName;
	
	
	public DBStairFlight(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		affineTransformation = config.getProject().getImporter().getAffineTransformation().isSetUseAffineTransformation();
		init();
	}
	
	private void init() throws SQLException {
		
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into STAIR_FLIGHT (ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, BUILDING_ID, PODIUM_ID, STOREY_ID, ")
		.append("LOD4_BREP_ID, LOD4_OTHER_GEOM, ")
		.append("LOD4_IMPLICIT_REP_ID, ")
		.append("LOD4_IMPLICIT_REF_POINT, ")
		.append("LOD4_IMPLICIT_TRANSFORMATION, LOD4_MULTI_GEOM_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psStairFlight = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		implicitGeometryImporter = (DBImplicitGeometry)dbImporterManager.getDBImporter(DBImporterEnum.IMPLICIT_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
		
		multiGeometryImporter = (DBMultiGeometry)dbImporterManager.getDBImporter(DBImporterEnum.MULTI_GEOMETRY);

	}
	
	public long insert(StairFlight stairFlight, CityGMLClass parent, long parentId) throws SQLException {	
		long stairFlightId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		if(stairFlightId == 0)
			return 0;
		
		// CityObject
		cityObjectImporter.insert(stairFlight, stairFlightId);
		
		//StairFlight
		//ID
		psStairFlight.setLong(1, stairFlightId);
		
		// bldg:class
		if (stairFlight.isSetClazz() && stairFlight.getClazz().isSetValue()) {
			psStairFlight.setString(2, stairFlight.getClazz().getValue());
			psStairFlight.setString(3, stairFlight.getClazz().getCodeSpace());
		} else {
			psStairFlight.setNull(2, Types.VARCHAR);
			psStairFlight.setNull(3, Types.VARCHAR);
		}

		// bldg:function
		if (stairFlight.isSetFunction()) {
			String[] function = Util.codeList2string(stairFlight.getFunction());
			psStairFlight.setString(4, function[0]);
			psStairFlight.setString(5, function[1]);
		} else {
			psStairFlight.setNull(4, Types.VARCHAR);
			psStairFlight.setNull(5, Types.VARCHAR);
		}

		// bldg:usage
		if (stairFlight.isSetUsage()) {
			String[] usage = Util.codeList2string(stairFlight.getUsage());
			psStairFlight.setString(6, usage[0]);
			psStairFlight.setString(7, usage[1]);
		} else {
			psStairFlight.setNull(6, Types.VARCHAR);
			psStairFlight.setNull(7, Types.VARCHAR);
		}
		
		// parentId
		switch (parent) {
		case BUILDING:
		case BUILDING_PART:
			psStairFlight.setLong(8, parentId);
			psStairFlight.setNull(9, Types.NULL);
			psStairFlight.setNull(10, Types.NULL);
			break;
		case PODIUM:
			psStairFlight.setNull(8, Types.NULL);
			psStairFlight.setLong(9, parentId);
			psStairFlight.setNull(10, Types.NULL);
			break;
		case STOREY:
			psStairFlight.setNull(8, Types.NULL);
			psStairFlight.setNull(9, Types.NULL);
			psStairFlight.setLong(10, parentId);
			break;
		default:
			psStairFlight.setNull(8, Types.NULL);
			psStairFlight.setNull(9, Types.NULL);
			psStairFlight.setNull(10, Types.NULL);
		}
		
		//Gemetry
		
		GeometryProperty<? extends AbstractGeometry> geometryProperty = stairFlight.getLod4Geometry();
		long geometryId = 0;
		long multiGeometryId = 0;
		GeometryObject geometryObject = null;
		
		if (geometryProperty != null) {
			if (geometryProperty.isSetGeometry()) {
				AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
				if (surfaceGeometryImporter.isSurfaceGeometry(abstractGeometry))
					geometryId = surfaceGeometryImporter.insert(abstractGeometry, stairFlightId);
				else if (otherGeometryImporter.isPointOrLineGeometry(abstractGeometry))
					geometryObject = otherGeometryImporter.getPointOrCurveGeometry(abstractGeometry);
				else if (multiGeometryImporter.isMultiGeometry(abstractGeometry))
					multiGeometryId = multiGeometryImporter.insert(abstractGeometry, stairFlightId);
				else {
					StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
							stairFlight.getCityGMLClass(), 
							stairFlight.getId()));
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
							stairFlightId, 
							TableEnum.STAIR_FLIGHT, 
							"LOD4_BREP_ID"));
				}
			}
		}
		
		if (geometryId != 0)
			psStairFlight.setLong(11, geometryId);
		else
			psStairFlight.setNull(11, Types.NULL);

		if (geometryObject != null)
			psStairFlight.setObject(12, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
		else
			psStairFlight.setNull(12, nullGeometryType, nullGeometryTypeName);

		if(multiGeometryId != 0)
			psStairFlight.setLong(16, multiGeometryId);
		else 
			psStairFlight.setNull(16, Types.NULL);
		
		//implicit geometry
		
		ImplicitRepresentationProperty implicit = stairFlight.getLod4ImplicitRepresentation();
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
				implicitId = implicitGeometryImporter.insert(geometry, stairFlightId);
			}
		}

		if (implicitId != 0)
			psStairFlight.setLong(13, implicitId);
		else
			psStairFlight.setNull(13, Types.NULL);

		if (pointGeom != null)
			psStairFlight.setObject(14, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(pointGeom, batchConn));
		else
			psStairFlight.setNull(14, nullGeometryType, nullGeometryTypeName);

		if (matrixString != null)
			psStairFlight.setString(15, matrixString);
		else
			psStairFlight.setNull(15, Types.VARCHAR);
		
		psStairFlight.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.STAIR_FLIGHT);
		
		// BoundarySurfaces
		if (stairFlight.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : stairFlight.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, stairFlight.getCityGMLClass(), stairFlightId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								stairFlight.getCityGMLClass(), 
								stairFlight.getId()));
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
		cityObjectImporter.insertAppearance(stairFlight, stairFlightId);

		
		
		return stairFlightId;
	}

	@Override
	public void executeBatch() throws SQLException {
		psStairFlight.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		
		psStairFlight.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.STAIR_FLIGHT;
	}
	
}
