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

import org.citygml4j.model.citygml.buildingExtra.RampFlight;
import org.citygml4j.model.citygml.core.ImplicitGeometry;
import org.citygml4j.model.citygml.core.ImplicitRepresentationProperty;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryProperty;

public class DBRampFlight implements DBImporter{
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psRampFlight;
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
	
	
	public DBRampFlight(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		affineTransformation = config.getProject().getImporter().getAffineTransformation().isSetUseAffineTransformation();
		init();
	}
	
	private void init() throws SQLException {
		
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into RAMP_FLIGHT (ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, BUILDING_ID, PODIUM_ID, STOREY_ID, ")
		.append("LOD4_BREP_ID, LOD4_OTHER_GEOM, ")
		.append("LOD4_IMPLICIT_REP_ID, ")
		.append("LOD4_IMPLICIT_REF_POINT, ")
		.append("LOD4_IMPLICIT_TRANSFORMATION, LOD4_MULTI_GEOM_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psRampFlight = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		implicitGeometryImporter = (DBImplicitGeometry)dbImporterManager.getDBImporter(DBImporterEnum.IMPLICIT_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
		
		multiGeometryImporter = (DBMultiGeometry)dbImporterManager.getDBImporter(DBImporterEnum.MULTI_GEOMETRY);
	}
	
	public long insert(RampFlight rampFlight, CityGMLClass parent, long parentId) throws SQLException {	
		long rampFlightId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		if(rampFlightId == 0)
			return 0;
		
		// CityObject
		cityObjectImporter.insert(rampFlight, rampFlightId);
		
		//RampFlight
		//ID
		psRampFlight.setLong(1, rampFlightId);
		
		// bldg:class
		if (rampFlight.isSetClazz() && rampFlight.getClazz().isSetValue()) {
			psRampFlight.setString(2, rampFlight.getClazz().getValue());
			psRampFlight.setString(3, rampFlight.getClazz().getCodeSpace());
		} else {
			psRampFlight.setNull(2, Types.VARCHAR);
			psRampFlight.setNull(3, Types.VARCHAR);
		}

		// bldg:function
		if (rampFlight.isSetFunction()) {
			String[] function = Util.codeList2string(rampFlight.getFunction());
			psRampFlight.setString(4, function[0]);
			psRampFlight.setString(5, function[1]);
		} else {
			psRampFlight.setNull(4, Types.VARCHAR);
			psRampFlight.setNull(5, Types.VARCHAR);
		}

		// bldg:usage
		if (rampFlight.isSetUsage()) {
			String[] usage = Util.codeList2string(rampFlight.getUsage());
			psRampFlight.setString(6, usage[0]);
			psRampFlight.setString(7, usage[1]);
		} else {
			psRampFlight.setNull(6, Types.VARCHAR);
			psRampFlight.setNull(7, Types.VARCHAR);
		}
		
		// parentId
		switch (parent) {
		case BUILDING:
		case BUILDING_PART:
			psRampFlight.setLong(8, parentId);
			psRampFlight.setNull(9, Types.NULL);
			psRampFlight.setNull(10, Types.NULL);
			break;
		case PODIUM:
			psRampFlight.setNull(8, Types.NULL);
			psRampFlight.setLong(9, parentId);
			psRampFlight.setNull(10, Types.NULL);
			break;
		case STOREY:
			psRampFlight.setNull(8, Types.NULL);
			psRampFlight.setNull(9, Types.NULL);
			psRampFlight.setLong(10, parentId);
			break;
		default:
			psRampFlight.setNull(8, Types.NULL);
			psRampFlight.setNull(9, Types.NULL);
			psRampFlight.setNull(10, Types.NULL);
		}
		
		//Gemetry
		
		GeometryProperty<? extends AbstractGeometry> geometryProperty = rampFlight.getLod4Geometry();
		long geometryId = 0;
		long multiGeometryId = 0;	
		GeometryObject geometryObject = null;
		
		if (geometryProperty != null) {
			if (geometryProperty.isSetGeometry()) {
				AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
				if (surfaceGeometryImporter.isSurfaceGeometry(abstractGeometry))
					geometryId = surfaceGeometryImporter.insert(abstractGeometry, rampFlightId);
				else if (otherGeometryImporter.isPointOrLineGeometry(abstractGeometry))
					geometryObject = otherGeometryImporter.getPointOrCurveGeometry(abstractGeometry);
				else if (multiGeometryImporter.isMultiGeometry(abstractGeometry))
					multiGeometryId = multiGeometryImporter.insert(abstractGeometry, rampFlightId);
				else {
					StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
							rampFlight.getCityGMLClass(), 
							rampFlight.getId()));
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
							rampFlightId, 
							TableEnum.RAMP_FLIGHT, 
							"LOD4_BREP_ID"));
				}
			}
		}
		
		if (geometryId != 0)
			psRampFlight.setLong(11, geometryId);
		else
			psRampFlight.setNull(11, Types.NULL);

		if (geometryObject != null)
			psRampFlight.setObject(12, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
		else
			psRampFlight.setNull(12, nullGeometryType, nullGeometryTypeName);

		if(multiGeometryId != 0)
			psRampFlight.setLong(16, multiGeometryId);
		else 
			psRampFlight.setNull(16, Types.NULL);

		//implicit geometry
		
		ImplicitRepresentationProperty implicit = rampFlight.getLod4ImplicitRepresentation();
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
				implicitId = implicitGeometryImporter.insert(geometry, rampFlightId);
			}
		}

		if (implicitId != 0)
			psRampFlight.setLong(13, implicitId);
		else
			psRampFlight.setNull(13, Types.NULL);

		if (pointGeom != null)
			psRampFlight.setObject(14, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(pointGeom, batchConn));
		else
			psRampFlight.setNull(14, nullGeometryType, nullGeometryTypeName);

		if (matrixString != null)
			psRampFlight.setString(15, matrixString);
		else
			psRampFlight.setNull(15, Types.VARCHAR);
		
		
		psRampFlight.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.RAMP_FLIGHT);
		
		// BoundarySurfaces
		if (rampFlight.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : rampFlight.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, rampFlight.getCityGMLClass(), rampFlightId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								rampFlight.getCityGMLClass(), 
								rampFlight.getId()));
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
		cityObjectImporter.insertAppearance(rampFlight, rampFlightId);

		
		
		return rampFlightId;
	}

	@Override
	public void executeBatch() throws SQLException {
		psRampFlight.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		
		psRampFlight.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.RAMP_FLIGHT;
	}
	
}
