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
import org.citygml4j.model.citygml.buildingExtra.Column;
import org.citygml4j.model.citygml.core.ImplicitGeometry;
import org.citygml4j.model.citygml.core.ImplicitRepresentationProperty;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryProperty;

public class DBBuildingColumn implements DBImporter{
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psBuildingColumn;
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
	
	public DBBuildingColumn(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		affineTransformation = config.getProject().getImporter().getAffineTransformation().isSetUseAffineTransformation();
		init();
	}
	
	private void init() throws SQLException {
		
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into BUILDING_COLUMN (ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, BUILDING_ID, PODIUM_ID, STOREY_ID, ")
		.append("LOD4_BREP_ID, LOD4_OTHER_GEOM, ")
		.append("LOD4_IMPLICIT_REP_ID, ")
		.append("LOD4_IMPLICIT_REF_POINT, ")
		.append("LOD4_IMPLICIT_TRANSFORMATION, LOD4_MULTI_GEOM_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psBuildingColumn = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		implicitGeometryImporter = (DBImplicitGeometry)dbImporterManager.getDBImporter(DBImporterEnum.IMPLICIT_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
		
		multiGeometryImporter = (DBMultiGeometry)dbImporterManager.getDBImporter(DBImporterEnum.MULTI_GEOMETRY);

	}
	
	public long insert(Column buildingColumn, CityGMLClass parent, long parentId) throws SQLException {	
		long buildingColumnId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		if(buildingColumnId == 0)
			return 0;
		
		// CityObject
		cityObjectImporter.insert(buildingColumn, buildingColumnId);
		
		//Beam
		//ID
		psBuildingColumn.setLong(1, buildingColumnId);
		
		// bldg:class
		if (buildingColumn.isSetClazz() && buildingColumn.getClazz().isSetValue()) {
			psBuildingColumn.setString(2, buildingColumn.getClazz().getValue());
			psBuildingColumn.setString(3, buildingColumn.getClazz().getCodeSpace());
		} else {
			psBuildingColumn.setNull(2, Types.VARCHAR);
			psBuildingColumn.setNull(3, Types.VARCHAR);
		}

		// bldg:function
		if (buildingColumn.isSetFunction()) {
			String[] function = Util.codeList2string(buildingColumn.getFunction());
			psBuildingColumn.setString(4, function[0]);
			psBuildingColumn.setString(5, function[1]);
		} else {
			psBuildingColumn.setNull(4, Types.VARCHAR);
			psBuildingColumn.setNull(5, Types.VARCHAR);
		}

		// bldg:usage
		if (buildingColumn.isSetUsage()) {
			String[] usage = Util.codeList2string(buildingColumn.getUsage());
			psBuildingColumn.setString(6, usage[0]);
			psBuildingColumn.setString(7, usage[1]);
		} else {
			psBuildingColumn.setNull(6, Types.VARCHAR);
			psBuildingColumn.setNull(7, Types.VARCHAR);
		}
		
		// parentId
		switch (parent) {
		case BUILDING:
		case BUILDING_PART:
			psBuildingColumn.setLong(8, parentId);
			psBuildingColumn.setNull(9, Types.NULL);
			psBuildingColumn.setNull(10, Types.NULL);
			break;
		case PODIUM:
			psBuildingColumn.setNull(8, Types.NULL);
			psBuildingColumn.setLong(9, parentId);
			psBuildingColumn.setNull(10, Types.NULL);
			break;
		case STOREY:
			psBuildingColumn.setNull(8, Types.NULL);
			psBuildingColumn.setNull(9, Types.NULL);
			psBuildingColumn.setLong(10, parentId);
			break;
		default:
			psBuildingColumn.setNull(8, Types.NULL);
			psBuildingColumn.setNull(9, Types.NULL);
			psBuildingColumn.setNull(10, Types.NULL);
		}
		
		//Gemetry
		
		GeometryProperty<? extends AbstractGeometry> geometryProperty = buildingColumn.getLod4Geometry();
		long geometryId = 0;
		long multiGeometryId = 0;
		
		GeometryObject geometryObject = null;
		
		if (geometryProperty != null) {
			if (geometryProperty.isSetGeometry()) {
				AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
				if (surfaceGeometryImporter.isSurfaceGeometry(abstractGeometry))
					geometryId = surfaceGeometryImporter.insert(abstractGeometry, buildingColumnId);
				else if (otherGeometryImporter.isPointOrLineGeometry(abstractGeometry))
					geometryObject = otherGeometryImporter.getPointOrCurveGeometry(abstractGeometry);
				else if (multiGeometryImporter.isMultiGeometry(abstractGeometry))
					multiGeometryId = multiGeometryImporter.insert(abstractGeometry, buildingColumnId);
				else {
					StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
							buildingColumn.getCityGMLClass(), 
							buildingColumn.getId()));
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
							buildingColumnId, 
							TableEnum.BUILDING_COLUMN, 
							"LOD4_BREP_ID"));
				}
			}
		}
		
		if (geometryId != 0)
			psBuildingColumn.setLong(11, geometryId);
		else
			psBuildingColumn.setNull(11, Types.NULL);

		if (geometryObject != null)
			psBuildingColumn.setObject(12, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
		else
			psBuildingColumn.setNull(12, nullGeometryType, nullGeometryTypeName);
		
		if(multiGeometryId != 0)
			psBuildingColumn.setLong(16, multiGeometryId);
		else 
			psBuildingColumn.setNull(16, Types.NULL);

		
		//implicit geometry
		
		ImplicitRepresentationProperty implicit = buildingColumn.getLod4ImplicitRepresentation();
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
				implicitId = implicitGeometryImporter.insert(geometry, buildingColumnId);
			}
		}

		if (implicitId != 0)
			psBuildingColumn.setLong(13, implicitId);
		else
			psBuildingColumn.setNull(13, Types.NULL);

		if (pointGeom != null)
			psBuildingColumn.setObject(14, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(pointGeom, batchConn));
		else
			psBuildingColumn.setNull(14, nullGeometryType, nullGeometryTypeName);

		if (matrixString != null)
			psBuildingColumn.setString(15, matrixString);
		else
			psBuildingColumn.setNull(15, Types.VARCHAR);
		
		
		psBuildingColumn.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.BUILDING_COLUMN);
		
		// BoundarySurfaces
		if (buildingColumn.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : buildingColumn.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, buildingColumn.getCityGMLClass(), buildingColumnId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								buildingColumn.getCityGMLClass(), 
								buildingColumn.getId()));
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
		cityObjectImporter.insertAppearance(buildingColumn, buildingColumnId);

		
		
		return buildingColumnId;
	}

	@Override
	public void executeBatch() throws SQLException {
		psBuildingColumn.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		
		psBuildingColumn.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.BUILDING_COLUMN;
	}
	
}

