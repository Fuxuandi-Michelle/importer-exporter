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
import org.citygml4j.model.citygml.buildingExtra.Beam;
import org.citygml4j.model.citygml.core.ImplicitGeometry;
import org.citygml4j.model.citygml.core.ImplicitRepresentationProperty;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryProperty;

public class DBBeam implements DBImporter{
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psBeam;
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
	
	
	public DBBeam(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		affineTransformation = config.getProject().getImporter().getAffineTransformation().isSetUseAffineTransformation();
		init();
	}
	
	private void init() throws SQLException {
		
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into BEAM (ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, BUILDING_ID, PODIUM_ID, STOREY_ID, ")
		.append("LOD4_BREP_ID, LOD4_OTHER_GEOM, ")
		.append("LOD4_IMPLICIT_REP_ID, ")
		.append("LOD4_IMPLICIT_REF_POINT, ")
		.append("LOD4_IMPLICIT_TRANSFORMATION, LOD4_MULTI_GEOM_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psBeam = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		implicitGeometryImporter = (DBImplicitGeometry)dbImporterManager.getDBImporter(DBImporterEnum.IMPLICIT_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
		
		multiGeometryImporter = (DBMultiGeometry)dbImporterManager.getDBImporter(DBImporterEnum.MULTI_GEOMETRY);
		
	}
	
	public long insert(Beam beam, CityGMLClass parent, long parentId) throws SQLException {	
		long beamId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		if(beamId == 0)
			return 0;
		
		// CityObject
		cityObjectImporter.insert(beam, beamId);
		
		//Beam
		//ID
		psBeam.setLong(1, beamId);
		
		// bldg:class
		if (beam.isSetClazz() && beam.getClazz().isSetValue()) {
			psBeam.setString(2, beam.getClazz().getValue());
			psBeam.setString(3, beam.getClazz().getCodeSpace());
		} else {
			psBeam.setNull(2, Types.VARCHAR);
			psBeam.setNull(3, Types.VARCHAR);
		}

		// bldg:function
		if (beam.isSetFunction()) {
			String[] function = Util.codeList2string(beam.getFunction());
			psBeam.setString(4, function[0]);
			psBeam.setString(5, function[1]);
		} else {
			psBeam.setNull(4, Types.VARCHAR);
			psBeam.setNull(5, Types.VARCHAR);
		}

		// bldg:usage
		if (beam.isSetUsage()) {
			String[] usage = Util.codeList2string(beam.getUsage());
			psBeam.setString(6, usage[0]);
			psBeam.setString(7, usage[1]);
		} else {
			psBeam.setNull(6, Types.VARCHAR);
			psBeam.setNull(7, Types.VARCHAR);
		}
		
		// parentId
		switch (parent) {
		case BUILDING:
		case BUILDING_PART:
			psBeam.setLong(8, parentId);
			psBeam.setNull(9, Types.NULL);
			psBeam.setNull(10, Types.NULL);
			break;
		case PODIUM:
			psBeam.setNull(8, Types.NULL);
			psBeam.setLong(9, parentId);
			psBeam.setNull(10, Types.NULL);
			break;
		case STOREY:
			psBeam.setNull(8, Types.NULL);
			psBeam.setNull(9, Types.NULL);
			psBeam.setLong(10, parentId);
			break;
		default:
			psBeam.setNull(8, Types.NULL);
			psBeam.setNull(9, Types.NULL);
			psBeam.setNull(10, Types.NULL);
		}
		
		//Gemetry
		
		GeometryProperty<? extends AbstractGeometry> geometryProperty = beam.getLod4Geometry();
		long geometryId = 0;
		long multiGeometryId = 0;
		GeometryObject geometryObject = null;
		
		if (geometryProperty != null) {
			if (geometryProperty.isSetGeometry()) {
				AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
				if (surfaceGeometryImporter.isSurfaceGeometry(abstractGeometry))
					geometryId = surfaceGeometryImporter.insert(abstractGeometry, beamId);
				else if (otherGeometryImporter.isPointOrLineGeometry(abstractGeometry))
					geometryObject = otherGeometryImporter.getPointOrCurveGeometry(abstractGeometry);
				else if (multiGeometryImporter.isMultiGeometry(abstractGeometry))
					multiGeometryId = multiGeometryImporter.insert(abstractGeometry, beamId);
				else {
					StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
							beam.getCityGMLClass(), 
							beam.getId()));
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
							beamId, 
							TableEnum.BEAM, 
							"LOD4_BREP_ID"));
				}
			}
		}
		
		if (geometryId != 0)
			psBeam.setLong(11, geometryId);
		else
			psBeam.setNull(11, Types.NULL);
		
		if (geometryObject != null)
			psBeam.setObject(12, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
		else
			psBeam.setNull(12, nullGeometryType, nullGeometryTypeName);
		 
		if(multiGeometryId != 0)
			psBeam.setLong(16, multiGeometryId);
		else 
			psBeam.setNull(16, Types.NULL);
		
		//implicit geometry
		
		ImplicitRepresentationProperty implicit = beam.getLod4ImplicitRepresentation();
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
				implicitId = implicitGeometryImporter.insert(geometry, beamId);
			}
		}

		if (implicitId != 0)
			psBeam.setLong(13, implicitId);
		else
			psBeam.setNull(13, Types.NULL);

		if (pointGeom != null)
			psBeam.setObject(14, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(pointGeom, batchConn));
		else
			psBeam.setNull(14, nullGeometryType, nullGeometryTypeName);

		if (matrixString != null)
			psBeam.setString(15, matrixString);
		else
			psBeam.setNull(15, Types.VARCHAR);
		
		
		psBeam.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.BEAM);
		
		// BoundarySurfaces
		if (beam.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : beam.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, beam.getCityGMLClass(), beamId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								beam.getCityGMLClass(), 
								beam.getId()));
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
		cityObjectImporter.insertAppearance(beam, beamId);

		
		
		return beamId;
	}

	@Override
	public void executeBatch() throws SQLException {
		psBeam.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		
		psBeam.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.BEAM;
	}
	
}
