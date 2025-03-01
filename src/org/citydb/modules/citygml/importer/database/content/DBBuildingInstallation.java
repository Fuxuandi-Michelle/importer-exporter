/*
 * 3D City Database - The Open Source CityGML Database
 * http://www.3dcitydb.org/
 * 
 * Copyright 2013 - 2016
 * Chair of Geoinformatics
 * Technical University of Munich, Germany
 * https://www.gis.bgu.tum.de/
 * 
 * The 3D City Database is jointly developed with the following
 * cooperation partners:
 * 
 * virtualcitySYSTEMS GmbH, Berlin <http://www.virtualcitysystems.de/>
 * M.O.S.S. Computer Grafik Systeme GmbH, Taufkirchen <http://www.moss.de/>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import org.citygml4j.model.citygml.building.BuildingInstallation;
import org.citygml4j.model.citygml.building.IntBuildingInstallation;
import org.citygml4j.model.citygml.core.ImplicitGeometry;
import org.citygml4j.model.citygml.core.ImplicitRepresentationProperty;
import org.citygml4j.model.gml.geometry.AbstractGeometry;
import org.citygml4j.model.gml.geometry.GeometryProperty;

public class DBBuildingInstallation implements DBImporter {
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psBuildingInstallation;
	private DBCityObject cityObjectImporter;
	private DBThematicSurface thematicSurfaceImporter;
	private DBSurfaceGeometry surfaceGeometryImporter;
	private DBOtherGeometry otherGeometryImporter;
	private DBImplicitGeometry implicitGeometryImporter;
	
	//multiGeomtry
	private DBMultiGeometry multiGeometryImporter;
	
	private boolean affineTransformation;
	private int batchCounter;
	private int nullGeometryType;
	private String nullGeometryTypeName;

	public DBBuildingInstallation(Connection batchConn, Config config, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		affineTransformation = config.getProject().getImporter().getAffineTransformation().isSetUseAffineTransformation();
		init();
	}

	private void init() throws SQLException {
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into BUILDING_INSTALLATION (ID, OBJECTCLASS_ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, BUILDING_ID, ROOM_ID, ")
		.append("LOD2_BREP_ID, LOD3_BREP_ID, LOD4_BREP_ID, LOD2_OTHER_GEOM, LOD3_OTHER_GEOM, LOD4_OTHER_GEOM, ")
		.append("LOD2_IMPLICIT_REP_ID, LOD3_IMPLICIT_REP_ID, LOD4_IMPLICIT_REP_ID, ")
		.append("LOD2_IMPLICIT_REF_POINT, LOD3_IMPLICIT_REF_POINT, LOD4_IMPLICIT_REF_POINT, ")
		.append("LOD2_IMPLICIT_TRANSFORMATION, LOD3_IMPLICIT_TRANSFORMATION, LOD4_IMPLICIT_TRANSFORMATION, STOREY_ID, PODIUM_ID, LOD2_MULTI_GEOM_ID,LOD3_MULTI_GEOM_ID, LOD4_MULTI_GEOM_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psBuildingInstallation = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		implicitGeometryImporter = (DBImplicitGeometry)dbImporterManager.getDBImporter(DBImporterEnum.IMPLICIT_GEOMETRY);
		multiGeometryImporter = (DBMultiGeometry)dbImporterManager.getDBImporter(DBImporterEnum.MULTI_GEOMETRY);
				
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
	}

	public long insert(BuildingInstallation buildingInstallation, CityGMLClass parent, long parentId) throws SQLException {
		long buildingInstallationId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		if (buildingInstallationId == 0)
			return 0;

		// CityObject
		cityObjectImporter.insert(buildingInstallation, buildingInstallationId);

		// BuildingInstallation
		// ID
		psBuildingInstallation.setLong(1, buildingInstallationId);

		// OBJECTCLASS_ID
		psBuildingInstallation.setLong(2, Util.cityObject2classId(buildingInstallation.getCityGMLClass()));

		// bldg:class
		if (buildingInstallation.isSetClazz() && buildingInstallation.getClazz().isSetValue()) {
			psBuildingInstallation.setString(3, buildingInstallation.getClazz().getValue());
			psBuildingInstallation.setString(4, buildingInstallation.getClazz().getCodeSpace());
		} else {
			psBuildingInstallation.setNull(3, Types.VARCHAR);
			psBuildingInstallation.setNull(4, Types.VARCHAR);
		}

		// bldg:function
		if (buildingInstallation.isSetFunction()) {
			String[] function = Util.codeList2string(buildingInstallation.getFunction());
			psBuildingInstallation.setString(5, function[0]);
			psBuildingInstallation.setString(6, function[1]);
		} else {
			psBuildingInstallation.setNull(5, Types.VARCHAR);
			psBuildingInstallation.setNull(6, Types.VARCHAR);
		}

		// bldg:usage
		if (buildingInstallation.isSetUsage()) {
			String[] usage = Util.codeList2string(buildingInstallation.getUsage());
			psBuildingInstallation.setString(7, usage[0]);
			psBuildingInstallation.setString(8, usage[1]);
		} else {
			psBuildingInstallation.setNull(7, Types.VARCHAR);
			psBuildingInstallation.setNull(8, Types.VARCHAR);
		}

		// parentId
		switch (parent) {
		case BUILDING:
		case BUILDING_PART:
			psBuildingInstallation.setLong(9, parentId);
			psBuildingInstallation.setNull(10, Types.NULL);
			psBuildingInstallation.setNull(26, Types.NULL);
			psBuildingInstallation.setNull(27, Types.NULL);
			break;
		case BUILDING_ROOM:
			psBuildingInstallation.setNull(9, Types.NULL);
			psBuildingInstallation.setLong(10, parentId);
			psBuildingInstallation.setNull(26, Types.NULL);
			psBuildingInstallation.setNull(27, Types.NULL);
			break;
		case STOREY:
			psBuildingInstallation.setNull(9, Types.NULL);
			psBuildingInstallation.setNull(10, Types.NULL);
			psBuildingInstallation.setLong(26, parentId);
			psBuildingInstallation.setNull(27, Types.NULL);
			break;
		case PODIUM:
			psBuildingInstallation.setNull(9, Types.NULL);
			psBuildingInstallation.setNull(10, Types.NULL);
			psBuildingInstallation.setNull(26, Types.NULL);
			psBuildingInstallation.setLong(27, parentId);
			break;
		default:
			psBuildingInstallation.setNull(9, Types.NULL);
			psBuildingInstallation.setNull(10, Types.NULL);
			psBuildingInstallation.setNull(26, Types.NULL);
			psBuildingInstallation.setNull(27, Types.NULL);
		}

		// Geometry
		for (int i = 0; i < 3; i++) {
			GeometryProperty<? extends AbstractGeometry> geometryProperty = null;
			long geometryId = 0;
			long multiGeometryId = 0;
			GeometryObject geometryObject = null;

			switch (i) {
			case 0:
				geometryProperty = buildingInstallation.getLod2Geometry();
				break;
			case 1:
				geometryProperty = buildingInstallation.getLod3Geometry();
				break;
			case 2:
				geometryProperty = buildingInstallation.getLod4Geometry();
				break;
			}

			if (geometryProperty != null) {
				if (geometryProperty.isSetGeometry()) {
					AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
					if (surfaceGeometryImporter.isSurfaceGeometry(abstractGeometry))
						geometryId = surfaceGeometryImporter.insert(abstractGeometry, buildingInstallationId);
					else if (otherGeometryImporter.isPointOrLineGeometry(abstractGeometry))
						geometryObject = otherGeometryImporter.getPointOrCurveGeometry(abstractGeometry);
					else if (multiGeometryImporter.isMultiGeometry(abstractGeometry))
						multiGeometryId = multiGeometryImporter.insert(abstractGeometry, buildingInstallationId);
					else {
						System.out.println("---------------------not surface,point,line geometry----------------------");
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								buildingInstallation.getCityGMLClass(), 
								buildingInstallation.getId()));
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
								buildingInstallationId, 
								TableEnum.BUILDING_INSTALLATION, 
								"LOD" + (i + 2) + "_BREP_ID"));
					}
				}
			}

			if (geometryId != 0)
				psBuildingInstallation.setLong(11 + i, geometryId);
			else
				psBuildingInstallation.setNull(11 + i, Types.NULL);

			if (geometryObject != null)
				psBuildingInstallation.setObject(14 + i, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
			else
				psBuildingInstallation.setNull(14 + i, nullGeometryType, nullGeometryTypeName);
			
			if(multiGeometryId != 0)
				psBuildingInstallation.setLong(28 + i, multiGeometryId);
			else 
				psBuildingInstallation.setNull(28 + i, Types.NULL);
		}

		// implicit geometry
		for (int i = 0; i < 3; i++) {
			ImplicitRepresentationProperty implicit = null;
			GeometryObject pointGeom = null;
			String matrixString = null;
			long implicitId = 0;

			switch (i) {
			case 0:
				implicit = buildingInstallation.getLod2ImplicitRepresentation();
				break;
			case 1:
				implicit = buildingInstallation.getLod3ImplicitRepresentation();
				break;
			case 2:
				implicit = buildingInstallation.getLod4ImplicitRepresentation();
				break;
			}

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
					implicitId = implicitGeometryImporter.insert(geometry, buildingInstallationId);
				}
			}

			if (implicitId != 0)
				psBuildingInstallation.setLong(17 + i, implicitId);
			else
				psBuildingInstallation.setNull(17 + i, Types.NULL);

			if (pointGeom != null)
				psBuildingInstallation.setObject(20 + i, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(pointGeom, batchConn));
			else
				psBuildingInstallation.setNull(20 + i, nullGeometryType, nullGeometryTypeName);

			if (matrixString != null)
				psBuildingInstallation.setString(23 + i, matrixString);
			else
				psBuildingInstallation.setNull(23 + i, Types.VARCHAR);
		}

		psBuildingInstallation.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.BUILDING_INSTALLATION);

		// BoundarySurfaces
		if (buildingInstallation.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : buildingInstallation.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, buildingInstallation.getCityGMLClass(), buildingInstallationId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								buildingInstallation.getCityGMLClass(), 
								buildingInstallation.getId()));
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
		cityObjectImporter.insertAppearance(buildingInstallation, buildingInstallationId);

		return buildingInstallationId;
	}

	public long insert(IntBuildingInstallation intBuildingInstallation, CityGMLClass parent, long parentId) throws SQLException {
		long intBuildingInstallationId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		if (intBuildingInstallationId == 0)
			return 0;

		// CityObject
		cityObjectImporter.insert(intBuildingInstallation, intBuildingInstallationId);

		// IntBuildingInstallation
		// ID
		psBuildingInstallation.setLong(1, intBuildingInstallationId);

		// OBJECTCLASS_ID
		psBuildingInstallation.setLong(2, Util.cityObject2classId(intBuildingInstallation.getCityGMLClass()));

		// bldg:class
		if (intBuildingInstallation.isSetClazz() && intBuildingInstallation.getClazz().isSetValue()) {
			psBuildingInstallation.setString(3, intBuildingInstallation.getClazz().getValue());
			psBuildingInstallation.setString(4, intBuildingInstallation.getClazz().getCodeSpace());
		} else {
			psBuildingInstallation.setNull(3, Types.VARCHAR);
			psBuildingInstallation.setNull(4, Types.VARCHAR);
		}

		// bldg:function
		if (intBuildingInstallation.isSetFunction()) {
			String[] function = Util.codeList2string(intBuildingInstallation.getFunction());
			psBuildingInstallation.setString(5, function[0]);
			psBuildingInstallation.setString(6, function[1]);
		} else {
			psBuildingInstallation.setNull(5, Types.VARCHAR);
			psBuildingInstallation.setNull(6, Types.VARCHAR);
		}

		// bldg:usage
		if (intBuildingInstallation.isSetUsage()) {
			String[] usage = Util.codeList2string(intBuildingInstallation.getUsage());
			psBuildingInstallation.setString(7, usage[0]);
			psBuildingInstallation.setString(8, usage[1]);
		} else {
			psBuildingInstallation.setNull(7, Types.VARCHAR);
			psBuildingInstallation.setNull(8, Types.VARCHAR);
		}

		// parentId
		switch (parent) {
		case BUILDING:
		case BUILDING_PART:
			psBuildingInstallation.setLong(9, parentId);
			psBuildingInstallation.setNull(10, Types.NULL);
			break;
		case BUILDING_ROOM:
			psBuildingInstallation.setNull(9, Types.NULL);
			psBuildingInstallation.setLong(10, parentId);
			break;
		default:
			psBuildingInstallation.setNull(9, Types.NULL);
			psBuildingInstallation.setNull(10, Types.NULL);
		}	

		// Geometry
		psBuildingInstallation.setNull(11, Types.NULL);
		psBuildingInstallation.setNull(12, Types.NULL);
		psBuildingInstallation.setNull(14, nullGeometryType, nullGeometryTypeName);
		psBuildingInstallation.setNull(15, nullGeometryType, nullGeometryTypeName);

		long geometryId = 0;
		long multiGeometryId = 0;
		GeometryObject geometryObject = null;

		if (intBuildingInstallation.isSetLod4Geometry()) {
			GeometryProperty<? extends AbstractGeometry> geometryProperty = intBuildingInstallation.getLod4Geometry();

			if (geometryProperty.isSetGeometry()) {
				AbstractGeometry abstractGeometry = geometryProperty.getGeometry();
				if (surfaceGeometryImporter.isSurfaceGeometry(abstractGeometry))
					geometryId = surfaceGeometryImporter.insert(abstractGeometry, intBuildingInstallationId);
				else if (otherGeometryImporter.isPointOrLineGeometry(abstractGeometry))
					geometryObject = otherGeometryImporter.getPointOrCurveGeometry(abstractGeometry);
				else if (multiGeometryImporter.isMultiGeometry(abstractGeometry))
					multiGeometryId = multiGeometryImporter.insert(abstractGeometry, intBuildingInstallationId);
				else {
					StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
							intBuildingInstallation.getCityGMLClass(), 
							intBuildingInstallation.getId()));
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
							intBuildingInstallationId, 
							TableEnum.BUILDING_INSTALLATION, 
							"LOD4_BREP_ID"));
				}
			}
		}

		if (geometryId != 0)
			psBuildingInstallation.setLong(13, geometryId);
		else
			psBuildingInstallation.setNull(13, Types.NULL);

		if (geometryObject != null)
			psBuildingInstallation.setObject(16, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(geometryObject, batchConn));
		else
			psBuildingInstallation.setNull(16, nullGeometryType, nullGeometryTypeName);
		
		if(multiGeometryId != 0)
			psBuildingInstallation.setLong(30, multiGeometryId);
		else 
			psBuildingInstallation.setNull(30, Types.NULL);
		
		
		// implicit geometry
		psBuildingInstallation.setNull(17, Types.NULL);
		psBuildingInstallation.setNull(18, Types.NULL);
		psBuildingInstallation.setNull(20, nullGeometryType, nullGeometryTypeName);
		psBuildingInstallation.setNull(21, nullGeometryType, nullGeometryTypeName);
		psBuildingInstallation.setNull(23, Types.VARCHAR);
		psBuildingInstallation.setNull(24, Types.VARCHAR);

		GeometryObject pointGeom = null;
		String matrixString = null;
		long implicitId = 0;

		if (intBuildingInstallation.isSetLod4ImplicitRepresentation()) {
			ImplicitRepresentationProperty implicit = intBuildingInstallation.getLod4ImplicitRepresentation();

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
				implicitId = implicitGeometryImporter.insert(geometry, intBuildingInstallationId);
			}
		}

		if (implicitId != 0)
			psBuildingInstallation.setLong(19, implicitId);
		else
			psBuildingInstallation.setNull(19, Types.NULL);

		if (pointGeom != null)
			psBuildingInstallation.setObject(22, dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(pointGeom, batchConn));
		else
			psBuildingInstallation.setNull(22, nullGeometryType, nullGeometryTypeName);

		if (matrixString != null)
			psBuildingInstallation.setString(25, matrixString);
		else
			psBuildingInstallation.setNull(25, Types.VARCHAR);

		psBuildingInstallation.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.BUILDING_INSTALLATION);

		// BoundarySurfaces
		if (intBuildingInstallation.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : intBuildingInstallation.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, intBuildingInstallation.getCityGMLClass(), intBuildingInstallationId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								intBuildingInstallation.getCityGMLClass(), 
								intBuildingInstallation.getId()));
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
		cityObjectImporter.insertAppearance(intBuildingInstallation, intBuildingInstallationId);

		return intBuildingInstallationId;
	}

	@Override
	public void executeBatch() throws SQLException {
		psBuildingInstallation.executeBatch();
		batchCounter = 0;
	}

	@Override
	public void close() throws SQLException {
		psBuildingInstallation.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		return DBImporterEnum.BUILDING_INSTALLATION;
	}

}
