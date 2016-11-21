package org.citydb.modules.citygml.importer.database.content;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.citydb.api.geometry.GeometryObject;
import org.citydb.database.TableEnum;
import org.citydb.log.Logger;
import org.citydb.modules.citygml.common.database.xlink.DBXlinkBasic;
import org.citydb.modules.citygml.common.database.xlink.DBXlinkSurfaceGeometry;
import org.citydb.util.Util;
import org.citygml4j.model.citygml.CityGMLClass;
import org.citygml4j.model.citygml.building.AbstractBoundarySurface;
import org.citygml4j.model.citygml.building.AbstractBuilding;
import org.citygml4j.model.citygml.building.BoundarySurfaceProperty;
import org.citygml4j.model.citygml.building.BuildingFurniture;
import org.citygml4j.model.citygml.building.BuildingInstallation;
import org.citygml4j.model.citygml.building.BuildingInstallationProperty;
import org.citygml4j.model.citygml.building.BuildingPart;
import org.citygml4j.model.citygml.building.BuildingPartProperty;
import org.citygml4j.model.citygml.building.IntBuildingInstallation;
import org.citygml4j.model.citygml.building.IntBuildingInstallationProperty;
import org.citygml4j.model.citygml.building.InteriorFurnitureProperty;
import org.citygml4j.model.citygml.building.InteriorRoomProperty;
import org.citygml4j.model.citygml.building.Room;
import org.citygml4j.model.citygml.buildingExtra.Beam;
import org.citygml4j.model.citygml.buildingExtra.Column;
import org.citygml4j.model.citygml.buildingExtra.Covering;
import org.citygml4j.model.citygml.buildingExtra.FlowTerminal;
import org.citygml4j.model.citygml.buildingExtra.Railing;
import org.citygml4j.model.citygml.buildingExtra.Ramp;
import org.citygml4j.model.citygml.buildingExtra.RampFlight;
import org.citygml4j.model.citygml.buildingExtra.Slab;
import org.citygml4j.model.citygml.buildingExtra.Stair;
import org.citygml4j.model.citygml.buildingExtra.StairFlight;
import org.citygml4j.model.citygml.buildingExtra.Storey;
import org.citygml4j.model.citygml.core.Address;
import org.citygml4j.model.citygml.core.AddressProperty;
import org.citygml4j.model.gml.basicTypes.DoubleOrNull;
import org.citygml4j.model.gml.basicTypes.MeasureOrNullList;
import org.citygml4j.model.gml.geometry.aggregates.MultiCurveProperty;
import org.citygml4j.model.gml.geometry.aggregates.MultiSurfaceProperty;
import org.citygml4j.model.gml.geometry.primitives.SolidProperty;

public class DBStorey implements DBImporter {
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psStorey;
	private DBCityObject cityObjectImporter;
	private DBSurfaceGeometry surfaceGeometryImporter;
	private DBThematicSurface thematicSurfaceImporter;
	private DBBuildingInstallation buildingInstallationImporter;
	private DBRoom roomImporter;
	private DBBuildingFurniture buildingFurnitureImporter;
	private DBAddress addressImporter;
	private DBOtherGeometry otherGeometryImporter;
	
	//buildingExtra Importer
	
	private DBBeam beamImporter;
	private DBBuildingColumn buildingColumnImporter;
	private DBCovering coveringImporter;
	private DBFlowTerminal flowTerminalImporter;
	private DBRailing railingImporter;
	private DBRamp rampImporter;
	private DBRampFlight rampFlightImporter;
	private DBSlab slabImporter;
	private DBStair stairImporter;
	private DBStairFlight stairFlightImporter;
	
	
	private int batchCounter;
	private int nullGeometryType;
	private String nullGeometryTypeName;
	
	
	public DBStorey(Connection batchConn, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		init();
	}

	private void init() throws SQLException {
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
		.append("insert into STOREY (ID, BUILDING_ID, PODIUM_ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE,  ")
		.append("LOD2_MULTI_CURVE, LOD3_MULTI_CURVE, LOD4_MULTI_CURVE, ")
		.append("LOD2_MULTI_SURFACE_ID, LOD3_MULTI_SURFACE_ID, LOD4_MULTI_SURFACE_ID, ")
		.append("LOD2_SOLID_ID, LOD3_SOLID_ID, LOD4_SOLID_ID) values ")
		.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		psStorey = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
		buildingInstallationImporter = (DBBuildingInstallation)dbImporterManager.getDBImporter(DBImporterEnum.BUILDING_INSTALLATION);
		roomImporter = (DBRoom)dbImporterManager.getDBImporter(DBImporterEnum.ROOM);
		buildingFurnitureImporter = (DBBuildingFurniture)dbImporterManager.getDBImporter(DBImporterEnum.BUILDING_FURNITURE);
		addressImporter = (DBAddress)dbImporterManager.getDBImporter(DBImporterEnum.ADDRESS);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
		
		//buildingExtra Importer Initialize
	
		beamImporter = (DBBeam)dbImporterManager.getDBImporter(DBImporterEnum.BEAM);
		buildingColumnImporter = (DBBuildingColumn)dbImporterManager.getDBImporter(DBImporterEnum.BUILDING_COLUMN);
		coveringImporter = (DBCovering)dbImporterManager.getDBImporter(DBImporterEnum.COVERING);
		flowTerminalImporter = (DBFlowTerminal)dbImporterManager.getDBImporter(DBImporterEnum.FLOW_TERMINAL);
		railingImporter = (DBRailing)dbImporterManager.getDBImporter(DBImporterEnum.RAILING);
		rampImporter = (DBRamp)dbImporterManager.getDBImporter(DBImporterEnum.RAMP);
		rampFlightImporter = (DBRampFlight)dbImporterManager.getDBImporter(DBImporterEnum.RAMP_FLIGHT);
		slabImporter = (DBSlab)dbImporterManager.getDBImporter(DBImporterEnum.SLAB);
		stairImporter = (DBStair)dbImporterManager.getDBImporter(DBImporterEnum.STAIR);
		stairFlightImporter = (DBStairFlight)dbImporterManager.getDBImporter(DBImporterEnum.STAIR_FLIGHT);
				
	}
		//other building installations
		
	public long insert(Storey storey,CityGMLClass parent,long parentId) throws SQLException {
			//String origGmlId = building.getId();
			
			long storeyId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
			if (storeyId == 0)
				return 0;
			/*
			1 id integer NOT NULL,
			2 building_id integer,
			3 podium_id integer,
			4 class character varying(256),
			5 class_codespace character varying(4000),
			6 function character varying(1000),
			7 function_codespace character varying(4000),
			8 usage character varying(1000),
			9 usage_codespace character varying(4000),
			10 lod2_multi_curve geometry(MULTILINESTRINGZ),
			11 lod3_multi_curve geometry(MULTILINESTRINGZ),
			12 lod4_multi_curve geometry(MULTILINESTRINGZ),
			13 lod2_multi_surface_id integer,
			14 lod3_multi_surface_id integer,
			15 lod4_multi_surface_id integer,
			16 lod2_solid_id integer,
			17 lod3_solid_id integer,
			18 lod4_solid_id integer,
			CONSTRAINT storey_pk PRIMARY KEY (id)
			 WITH (FILLFACTOR = 100)
			 */

			// CityObject
			cityObjectImporter.insert(storey, storeyId);

			// Storey
			// ID
			psStorey.setLong(1, storeyId);
			
			//parent id
			switch (parent) {
				case BUILDING:
				case BUILDING_PART:
					psStorey.setLong(2, parentId);
					psStorey.setNull(3, Types.NULL);
					break;
				case PODIUM:
					psStorey.setNull(2, Types.NULL);
					psStorey.setLong(3, parentId);
					break;
				default:
					psStorey.setNull(2, Types.NULL);
					psStorey.setNull(3, Types.NULL);
				}
			
			// BUILDING_PARENT_ID
			/*if (parentId != 0)
				psBuilding.setLong(2, parentId);
			else
				psBuilding.setNull(2, Types.NULL);

			// BUILDING_ROOT_ID
			psStorey.setLong(3, rootId);*/

			// bldg:class
			if (storey.isSetClazz() && storey.getClazz().isSetValue()) {
				psStorey.setString(4, storey.getClazz().getValue());
				psStorey.setString(5, storey.getClazz().getCodeSpace());
			} else {
				psStorey.setNull(4, Types.VARCHAR);
				psStorey.setNull(5, Types.VARCHAR);
			}

			// bldg:function
			if (storey.isSetFunction()) {
				String[] function = Util.codeList2string(storey.getFunction());
				psStorey.setString(6, function[0]);
				psStorey.setString(7, function[1]);
			} else {
				psStorey.setNull(6, Types.VARCHAR);
				psStorey.setNull(7, Types.VARCHAR);
			}

			// bldg:usage
			if (storey.isSetUsage()) {
				String[] usage = Util.codeList2string(storey.getUsage());
				psStorey.setString(8, usage[0]);
				psStorey.setString(9, usage[1]);
			} else {
				psStorey.setNull(8, Types.VARCHAR);
				psStorey.setNull(9, Types.VARCHAR);
			}
			
			// Geometry

			// lodXMultiCurve
			for (int i = 0; i < 3; i++) {
				MultiCurveProperty multiCurveProperty = null;
				GeometryObject multiLine = null;

				switch (i) {
				case 0:
					multiCurveProperty = storey.getLod2MultiCurve();
					break;
				case 1:
					multiCurveProperty = storey.getLod3MultiCurve();
					break;
				case 2:
					multiCurveProperty = storey.getLod4MultiCurve();
					break;
				}

				if (multiCurveProperty != null) {
					multiLine = otherGeometryImporter.getMultiCurve(multiCurveProperty);
					multiCurveProperty.unsetMultiCurve();
				}

				if (multiLine != null) {
					Object multiLineObj = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(multiLine, batchConn);
					psStorey.setObject(10 + i, multiLineObj);
				} else
					psStorey.setNull(10 + i, nullGeometryType, nullGeometryTypeName);
			}

			// lodXMultiSurface
			for (int i = 0; i < 3; i++) {
				MultiSurfaceProperty multiSurfaceProperty = null;
				long multiGeometryId = 0;

				switch (i) {
				case 0:
					multiSurfaceProperty = storey.getLod2MultiSurface();
					break;
				case 1:
					multiSurfaceProperty = storey.getLod3MultiSurface();
					break;
				case 2:
					multiSurfaceProperty = storey.getLod4MultiSurface();
					break;
				}

				if (multiSurfaceProperty != null) {
					if (multiSurfaceProperty.isSetMultiSurface()) {
						multiGeometryId = surfaceGeometryImporter.insert(multiSurfaceProperty.getMultiSurface(), storeyId);
						multiSurfaceProperty.unsetMultiSurface();
					} else {
						// xlink
						String href = multiSurfaceProperty.getHref();

						if (href != null && href.length() != 0) {
							dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
									href, 
									storeyId, 
									TableEnum.STOREY, 
									"LOD" + (i + 2) + "_MULTI_SURFACE_ID"));
						}
					}
				}

				if (multiGeometryId != 0)
					psStorey.setLong(13 + i, multiGeometryId);
				else
					psStorey.setNull(13 + i, Types.NULL);
			}

			// lodXSolid
			for (int i = 0; i < 3; i++) {
				SolidProperty solidProperty = null;
				long solidGeometryId = 0;

				switch (i) {
				case 0:
					solidProperty = storey.getLod2Solid();
					break;
				case 1:
					solidProperty = storey.getLod3Solid();
					break;
				case 2:
					solidProperty = storey.getLod4Solid();
					break;
				}

				if (solidProperty != null) {
					if (solidProperty.isSetSolid()) {
						solidGeometryId = surfaceGeometryImporter.insert(solidProperty.getSolid(), storeyId);
						solidProperty.unsetSolid();
					} else {
						// xlink
						String href = solidProperty.getHref();
						if (href != null && href.length() != 0) {
							dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
									href, 
									storeyId, 
									TableEnum.STOREY, 
									"LOD" + (i + 2) + "_SOLID_ID"));
						}
					}
				}

				if (solidGeometryId != 0)
					psStorey.setLong(16 + i, solidGeometryId);
				else
					psStorey.setNull(16 + i, Types.NULL);
			}

			psStorey.addBatch();
			if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
				dbImporterManager.executeBatch(DBImporterEnum.STOREY);

			// BoundarySurfaces
			if (storey.isSetBoundedBySurface()) {
				for (BoundarySurfaceProperty boundarySurfaceProperty : storey.getBoundedBySurface()) {
					AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

					if (boundarySurface != null) {
						String gmlId = boundarySurface.getId();
						long id = thematicSurfaceImporter.insert(boundarySurface, storey.getCityGMLClass(), storeyId);

						if (id == 0) {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									storey.getCityGMLClass(), 
									storey.getId()));
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

			// BuildingInstallation
			if (storey.isSetOuterBuildingInstallation()) {
				System.out.println("---------------------import storey building installation----------------------");
				for (BuildingInstallationProperty buildingInstProperty : storey.getOuterBuildingInstallation()) {
					BuildingInstallation buildingInst = buildingInstProperty.getBuildingInstallation();

					if (buildingInst != null) {
						String gmlId = buildingInst.getId();
						//long id = buildingInstallationImporter.insert(buildingInst, storey.getCityGMLClass(), storeyId);
						
						//test class of buildingInstallation
						long id = 0;
						
						if(buildingInst instanceof Beam)
							id = beamImporter.insert((Beam) buildingInst, storey.getCityGMLClass(), storeyId);
						else if(buildingInst instanceof Column)
							id = buildingColumnImporter.insert((Column)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof Covering)
							id = coveringImporter.insert((Covering)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof FlowTerminal)
							id = flowTerminalImporter.insert((FlowTerminal)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof Railing)
							id = railingImporter.insert((Railing)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof Ramp)
							id = rampImporter.insert((Ramp)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof RampFlight)
							id = rampFlightImporter.insert((RampFlight)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof Slab)
							id = slabImporter.insert((Slab)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof Stair)
							id = stairImporter.insert((Stair)buildingInst,storey.getCityGMLClass(),storeyId);
						else if(buildingInst instanceof StairFlight)
							id = stairFlightImporter.insert((StairFlight)buildingInst,storey.getCityGMLClass(),storeyId);
						else
							id = buildingInstallationImporter.insert(buildingInst, storey.getCityGMLClass(), storeyId);
						
						if (id == 0) {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									storey.getCityGMLClass(), 
									storey.getId()));
							msg.append(": Failed to write ");
							msg.append(Util.getFeatureSignature(
									CityGMLClass.BUILDING_INSTALLATION, 
									gmlId));

							LOG.error(msg.toString());
						}

						// free memory of nested feature
						buildingInstProperty.unsetBuildingInstallation();
					} else {
						// xlink
						String href = buildingInstProperty.getHref();

						if (href != null && href.length() != 0) {
							LOG.error("XLink reference '" + href + "' to " + CityGMLClass.BUILDING_INSTALLATION + " feature is not supported.");
						}
					}
				}
			}

			// IntBuildingInstallation
			if (storey.isSetInteriorBuildingInstallation()) {
				for (IntBuildingInstallationProperty intBuildingInstProperty : storey.getInteriorBuildingInstallation()) {
					IntBuildingInstallation intBuildingInst = intBuildingInstProperty.getIntBuildingInstallation();

					if (intBuildingInst != null) {
						String gmlId = intBuildingInst.getId();
						long id = buildingInstallationImporter.insert(intBuildingInst, storey.getCityGMLClass(), storeyId);

						if (id == 0) {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									storey.getCityGMLClass(), 
									storey.getId()));
							msg.append(": Failed to write ");
							msg.append(Util.getFeatureSignature(
									CityGMLClass.INT_BUILDING_INSTALLATION, 
									gmlId));

							LOG.error(msg.toString());
						}

						// free memory of nested feature
						intBuildingInstProperty.unsetIntBuildingInstallation();
					} else {
						// xlink
						String href = intBuildingInstProperty.getHref();

						if (href != null && href.length() != 0) {
							LOG.error("XLink reference '" + href + "' to " + CityGMLClass.INT_BUILDING_INSTALLATION + " feature is not supported.");
						}
					}
				}
			}

			// Room
			if (storey.isSetInteriorRoom()) {
				System.out.println("---------------------import storey room----------------------");
				for (InteriorRoomProperty roomProperty : storey.getInteriorRoom()) {
					Room room = roomProperty.getRoom();

					if (room != null) {
						String gmlId = room.getId();
						long id = roomImporter.insert(room, storey.getCityGMLClass(), storeyId);

						if (id == 0) {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									storey.getCityGMLClass(), 
									storey.getId()));
							msg.append(": Failed to write ");
							msg.append(Util.getFeatureSignature(
									CityGMLClass.BUILDING_ROOM, 
									gmlId));

							LOG.error(msg.toString());
						}

						// free memory of nested feature
						roomProperty.unsetRoom();
					} else {
						// xlink
						String href = roomProperty.getHref();

						if (href != null && href.length() != 0) {
							LOG.error("XLink reference '" + href + "' to " + CityGMLClass.BUILDING_ROOM + " feature is not supported.");
						}
					}
				}
			}
			
			
			// BuildingFurniture
			if (storey.isSetInteriorFurniture()) {
				System.out.println("---------------------import storey furniture----------------------");
				for (InteriorFurnitureProperty intFurnitureProperty : storey.getInteriorFurniture()) {
					BuildingFurniture furniture = intFurnitureProperty.getObject();

					if (furniture != null) {
						String gmlId = furniture.getId();
						long id = buildingFurnitureImporter.insert(furniture, storey.getCityGMLClass(), storeyId);

						if (id == 0) {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									storey.getCityGMLClass(), 
									storey.getId()));
							msg.append(": Failed to write ");
							msg.append(Util.getFeatureSignature(
									furniture.getCityGMLClass(), 
									gmlId));

							LOG.error(msg.toString());
						}

						// free memory of nested feature
						intFurnitureProperty.unsetBuildingFurniture();
					} else {
						// xlink
						String href = intFurnitureProperty.getHref();

						if (href != null && href.length() != 0) {
							LOG.error("XLink reference '" + href + "' to " + CityGMLClass.BUILDING_FURNITURE + " feature is not supported.");
						}
					}
				}
			}
			
			
			/*// BuildingPart
			if (storey.isSetConsistsOfBuildingPart()) {
				for (BuildingPartProperty storeyPartProperty : storey.getConsistsOfBuildingPart()) {
					BuildingPart storeyPart = storeyPartProperty.getBuildingPart();

					if (storeyPart != null) {
						long id = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);

						if (id != 0)
							insert(storeyPart, id, storeyId, rootId);
						else {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									storey.getCityGMLClass(), 
									origGmlId));
							msg.append(": Failed to write ");
							msg.append(Util.getFeatureSignature(
									CityGMLClass.BUILDING_PART, 
									storeyPart.getId()));

							LOG.error(msg.toString());
						}

						// free memory of nested feature
						storeyPartProperty.unsetBuildingPart();
					} else {
						// xlink
						String href = buildingPartProperty.getHref();

						if (href != null && href.length() != 0) {
							LOG.error("XLink reference '" + href + "' to " + CityGMLClass.BUILDING_PART + " feature is not supported.");
						}
					}
				}
			}

			// Address
			if (building.isSetAddress()) {
				for (AddressProperty addressProperty : building.getAddress()) {
					Address address = addressProperty.getAddress();

					if (address != null) {
						String gmlId = address.getId();
						long id = addressImporter.insertBuildingAddress(address, buildingId);

						if (id == 0) {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									building.getCityGMLClass(), 
									origGmlId));
							msg.append(": Failed to write ");
							msg.append(Util.getFeatureSignature(
									CityGMLClass.ADDRESS, 
									gmlId));

							LOG.error(msg.toString());
						}

						// free memory of nested feature
						addressProperty.unsetAddress();
					} else {
						// xlink
						String href = addressProperty.getHref();

						if (href != null && href.length() != 0) {
							dbImporterManager.propagateXlink(new DBXlinkBasic(
									buildingId,
									TableEnum.BUILDING,
									href,
									TableEnum.ADDRESS
									));
						}
					}
				}
			}

			// insert local appearance
			cityObjectImporter.insertAppearance(building, buildingId);
			 */
			return storeyId;
		}

	@Override
	public void executeBatch() throws SQLException {
		
		psStorey.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		
		psStorey.close();
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		
		return DBImporterEnum.STOREY;
	}
	
	
	
}
