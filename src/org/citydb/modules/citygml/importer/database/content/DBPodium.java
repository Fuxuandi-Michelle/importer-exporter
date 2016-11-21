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
import org.citygml4j.model.citygml.ade.ADEComponent;
import org.citygml4j.model.citygml.building.AbstractBoundarySurface;
import org.citygml4j.model.citygml.building.AbstractBuilding;
import org.citygml4j.model.citygml.building.BoundarySurfaceProperty;
import org.citygml4j.model.citygml.building.BuildingInstallation;
import org.citygml4j.model.citygml.building.BuildingInstallationProperty;
import org.citygml4j.model.citygml.building.BuildingPart;
import org.citygml4j.model.citygml.building.BuildingPartProperty;
import org.citygml4j.model.citygml.building.IntBuildingInstallation;
import org.citygml4j.model.citygml.building.IntBuildingInstallationProperty;
import org.citygml4j.model.citygml.building.InteriorRoomProperty;
import org.citygml4j.model.citygml.building.Room;
import org.citygml4j.model.citygml.buildingExtra.Beam;
import org.citygml4j.model.citygml.buildingExtra.Column;
import org.citygml4j.model.citygml.buildingExtra.Covering;
import org.citygml4j.model.citygml.buildingExtra.FlowTerminal;
import org.citygml4j.model.citygml.buildingExtra.Podium;
import org.citygml4j.model.citygml.buildingExtra.Railing;
import org.citygml4j.model.citygml.buildingExtra.Ramp;
import org.citygml4j.model.citygml.buildingExtra.RampFlight;
import org.citygml4j.model.citygml.buildingExtra.Slab;
import org.citygml4j.model.citygml.buildingExtra.Stair;
import org.citygml4j.model.citygml.buildingExtra.StairFlight;
import org.citygml4j.model.citygml.buildingExtra.Storey;
import org.citygml4j.model.citygml.buildingExtra.StoreyProperty;
import org.citygml4j.model.citygml.core.Address;
import org.citygml4j.model.citygml.core.AddressProperty;
import org.citygml4j.model.gml.basicTypes.DoubleOrNull;
import org.citygml4j.model.gml.basicTypes.MeasureOrNullList;
import org.citygml4j.model.gml.geometry.aggregates.MultiCurveProperty;
import org.citygml4j.model.gml.geometry.aggregates.MultiSurfaceProperty;
import org.citygml4j.model.gml.geometry.primitives.SolidProperty;

public class DBPodium implements DBImporter {
	
	private final Logger LOG = Logger.getInstance();

	private final Connection batchConn;
	private final DBImporterManager dbImporterManager;

	private PreparedStatement psPodium;
	private DBCityObject cityObjectImporter;
	private DBSurfaceGeometry surfaceGeometryImporter;
	private DBThematicSurface thematicSurfaceImporter;
	private DBBuildingInstallation buildingInstallationImporter;
	private DBRoom roomImporter;
	private DBAddress addressImporter;
	private DBOtherGeometry otherGeometryImporter;
	
	//buildingExtra Importer
	
	private DBStorey storeyImporter;
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
	
	public DBPodium(Connection batchConn, DBImporterManager dbImporterManager) throws SQLException {
		this.batchConn = batchConn;
		this.dbImporterManager = dbImporterManager;

		init();
	}
	
	
	private void init() throws SQLException {
		nullGeometryType = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryType();
		nullGeometryTypeName = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getNullGeometryTypeName();

		StringBuilder stmt = new StringBuilder()
				.append("insert into PODIUM (ID, PODIUM_PARENT_ID, CLASS, CLASS_CODESPACE, FUNCTION, FUNCTION_CODESPACE, USAGE, USAGE_CODESPACE, YEAR_OF_CONSTRUCTION, YEAR_OF_DEMOLITION, ")
				.append("ROOF_TYPE, ROOF_TYPE_CODESPACE, MEASURED_HEIGHT, MEASURED_HEIGHT_UNIT, STOREYS_ABOVE_GROUND, STOREYS_BELOW_GROUND, STOREY_HEIGHTS_ABOVE_GROUND, STOREY_HEIGHTS_AG_UNIT, STOREY_HEIGHTS_BELOW_GROUND, STOREY_HEIGHTS_BG_UNIT, ")
				.append("LOD1_TERRAIN_INTERSECTION, LOD2_TERRAIN_INTERSECTION, LOD3_TERRAIN_INTERSECTION, LOD4_TERRAIN_INTERSECTION, LOD2_MULTI_CURVE, LOD3_MULTI_CURVE, LOD4_MULTI_CURVE, ")
				.append("LOD0_FOOTPRINT_ID, LOD0_ROOFPRINT_ID, LOD1_MULTI_SURFACE_ID, LOD2_MULTI_SURFACE_ID, LOD3_MULTI_SURFACE_ID, LOD4_MULTI_SURFACE_ID, ")
				.append("LOD1_SOLID_ID, LOD2_SOLID_ID, LOD3_SOLID_ID, LOD4_SOLID_ID) values ")
				.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
				psPodium = batchConn.prepareStatement(stmt.toString());
		psPodium = batchConn.prepareStatement(stmt.toString());

		surfaceGeometryImporter = (DBSurfaceGeometry)dbImporterManager.getDBImporter(DBImporterEnum.SURFACE_GEOMETRY);
		cityObjectImporter = (DBCityObject)dbImporterManager.getDBImporter(DBImporterEnum.CITYOBJECT);
		thematicSurfaceImporter = (DBThematicSurface)dbImporterManager.getDBImporter(DBImporterEnum.THEMATIC_SURFACE);
		buildingInstallationImporter = (DBBuildingInstallation)dbImporterManager.getDBImporter(DBImporterEnum.BUILDING_INSTALLATION);
		roomImporter = (DBRoom)dbImporterManager.getDBImporter(DBImporterEnum.ROOM);
		addressImporter = (DBAddress)dbImporterManager.getDBImporter(DBImporterEnum.ADDRESS);
		otherGeometryImporter = (DBOtherGeometry)dbImporterManager.getDBImporter(DBImporterEnum.OTHER_GEOMETRY);
	
		//buildingExtra Importer Initialize
		
		storeyImporter = (DBStorey)dbImporterManager.getDBImporter(DBImporterEnum.STOREY);
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
	
	public long insert(Podium podium) throws SQLException {
		long podiumId = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);
		boolean success = false;

		if (podiumId != 0)
			success = insert(podium, podiumId, 0);

		if (success)
			return podiumId;
		else
			return 0;
	}
	
	public boolean insert(Podium podium, long podiumId, long parentId) throws SQLException {
		
		String origGmlId = podium.getId();
		
		if (podiumId == 0)
			return false;
		
		//CityObject
		long cityObjectId = cityObjectImporter.insert(podium, podiumId,parentId == 0);
		
		if (cityObjectId == 0)
			return false;
		
		// Podium
		// ID
		psPodium.setLong(1, podiumId);
		
		//PODIUM_PARENT_ID
		if (parentId != 0)
			psPodium.setLong(2, parentId);
		else
			psPodium.setNull(2, Types.NULL);
		
		// bldg:class
		if (podium.isSetClazz() && podium.getClazz().isSetValue()) {
			psPodium.setString(3, podium.getClazz().getValue());
			psPodium.setString(4, podium.getClazz().getCodeSpace());
		} else {
			psPodium.setNull(3, Types.VARCHAR);
			psPodium.setNull(4, Types.VARCHAR);
		}
		
		// bldg:function
		if (podium.isSetFunction()) {
			String[] function = Util.codeList2string(podium.getFunction());
			psPodium.setString(5, function[0]);
			psPodium.setString(6, function[1]);
		} else {
			psPodium.setNull(5, Types.VARCHAR);
			psPodium.setNull(6, Types.VARCHAR);
		}

		// bldg:usage
		if (podium.isSetUsage()) {
			String[] usage = Util.codeList2string(podium.getUsage());
			psPodium.setString(7, usage[0]);
			psPodium.setString(8, usage[1]);
		} else {
			psPodium.setNull(7, Types.VARCHAR);
			psPodium.setNull(8, Types.VARCHAR);
		}
		
		// bldg:yearOfConstruction
		if (podium.isSetYearOfConstruction()) {
			psPodium.setDate(9, new Date(podium.getYearOfConstruction().getTime().getTime()));
		} else {
			psPodium.setNull(9, Types.DATE);
		}

		// bldg:yearOfDemolition
		if (podium.isSetYearOfDemolition()) {
			psPodium.setDate(10, new Date(podium.getYearOfDemolition().getTime().getTime()));
		} else {
			psPodium.setNull(10, Types.DATE);
		}

		// bldg:roofType
		if (podium.isSetRoofType() && podium.getRoofType().isSetValue()) {
			psPodium.setString(11, podium.getRoofType().getValue());
			psPodium.setString(12, podium.getRoofType().getCodeSpace());
		} else {
			psPodium.setNull(11, Types.VARCHAR);
			psPodium.setNull(12, Types.VARCHAR);
		}

		// bldg:measuredHeight
		if (podium.isSetMeasuredHeight() && podium.getMeasuredHeight().isSetValue()) {
			psPodium.setDouble(13, podium.getMeasuredHeight().getValue());
			psPodium.setString(14, podium.getMeasuredHeight().getUom());
		} else {
			psPodium.setNull(13, Types.DOUBLE);
			psPodium.setNull(14, Types.VARCHAR);
		}

		// bldg:storeysAboveGround
		if (podium.isSetStoreysAboveGround()) {
			psPodium.setInt(15, podium.getStoreysAboveGround());
		} else {
			psPodium.setNull(15, Types.INTEGER);
		}

		// bldg:storeysBelowGround
		if (podium.isSetStoreysBelowGround()) {
			psPodium.setInt(16, podium.getStoreysBelowGround());
		} else {
			psPodium.setNull(16, Types.INTEGER);
		}

		// bldg:storeyHeightsAboveGround
		String heights = null;
		if (podium.isSetStoreyHeightsAboveGround()) {
			MeasureOrNullList measureOrNullList = podium.getStoreyHeightsAboveGround();
			if (measureOrNullList.isSetDoubleOrNull()) {
				List<String> values = new ArrayList<String>();				
				for (DoubleOrNull doubleOrNull : measureOrNullList.getDoubleOrNull()) {
					if (doubleOrNull.isSetDouble())
						values.add(String.valueOf(doubleOrNull.getDouble()));
					else
						doubleOrNull.getNull().getValue();			
				}

				heights = Util.collection2string(values, " ");
			} 
		}

		if (heights != null) {
			psPodium.setString(17, heights);
			psPodium.setString(18, podium.getStoreyHeightsAboveGround().getUom());
		} else {
			psPodium.setNull(17, Types.VARCHAR);
			psPodium.setNull(18, Types.VARCHAR);
		}

		// bldg:storeyHeightsBelowGround
		heights = null;
		if (podium.isSetStoreyHeightsBelowGround()) {
			MeasureOrNullList measureOrNullList = podium.getStoreyHeightsBelowGround();
			if (measureOrNullList.isSetDoubleOrNull()) {
				List<String> values = new ArrayList<String>();				
				for (DoubleOrNull doubleOrNull : measureOrNullList.getDoubleOrNull()) {
					if (doubleOrNull.isSetDouble())
						values.add(String.valueOf(doubleOrNull.getDouble()));
					else
						doubleOrNull.getNull().getValue();			
				}

				heights = Util.collection2string(values, " ");
			} 
		}

		if (heights != null) {
			psPodium.setString(19, heights);
			psPodium.setString(20, podium.getStoreyHeightsBelowGround().getUom());
		} else {
			psPodium.setNull(19, Types.VARCHAR);
			psPodium.setNull(20, Types.VARCHAR);
		}

		// Geometry
		// lodXTerrainIntersectionCurve
		for (int i = 0; i < 4; i++) {
			MultiCurveProperty multiCurveProperty = null;
			GeometryObject multiLine = null;

			switch (i) {
			case 0:
				multiCurveProperty = podium.getLod1TerrainIntersection();
				break;
			case 1:
				multiCurveProperty = podium.getLod2TerrainIntersection();
				break;
			case 2:
				multiCurveProperty = podium.getLod3TerrainIntersection();
				break;
			case 3:
				multiCurveProperty = podium.getLod4TerrainIntersection();
				break;
			}

			if (multiCurveProperty != null) {
				multiLine = otherGeometryImporter.getMultiCurve(multiCurveProperty);
				multiCurveProperty.unsetMultiCurve();
			}

			if (multiLine != null) {
				Object multiLineObj = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(multiLine, batchConn);
				psPodium.setObject(21 + i, multiLineObj);
			} else
				psPodium.setNull(21 + i, nullGeometryType, nullGeometryTypeName);
		}

		// lodXMultiCurve
		for (int i = 0; i < 3; i++) {
			MultiCurveProperty multiCurveProperty = null;
			GeometryObject multiLine = null;

			switch (i) {
			case 0:
				multiCurveProperty = podium.getLod2MultiCurve();
				break;
			case 1:
				multiCurveProperty = podium.getLod3MultiCurve();
				break;
			case 2:
				multiCurveProperty = podium.getLod4MultiCurve();
				break;
			}

			if (multiCurveProperty != null) {
				multiLine = otherGeometryImporter.getMultiCurve(multiCurveProperty);
				multiCurveProperty.unsetMultiCurve();
			}

			if (multiLine != null) {
				Object multiLineObj = dbImporterManager.getDatabaseAdapter().getGeometryConverter().getDatabaseObject(multiLine, batchConn);
				psPodium.setObject(25 + i, multiLineObj);
			} else
				psPodium.setNull(25 + i, nullGeometryType, nullGeometryTypeName);
		}

		// lod0FootPrint and lod0RoofEdge
		for (int i = 0; i < 2; i++) {
			MultiSurfaceProperty multiSurfaceProperty = null;
			long multiSurfaceId = 0;

			switch (i) {
			case 0:
				multiSurfaceProperty = podium.getLod0FootPrint();
				break;
			case 1:
				multiSurfaceProperty = podium.getLod0RoofEdge();
				break;			
			}

			if (multiSurfaceProperty != null) {
				if (multiSurfaceProperty.isSetMultiSurface()) {
					multiSurfaceId = surfaceGeometryImporter.insert(multiSurfaceProperty.getMultiSurface(), podiumId);
					multiSurfaceProperty.unsetMultiSurface();
				} else {
					// xlink
					String href = multiSurfaceProperty.getHref();

					if (href != null && href.length() != 0) {
						dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
								href, 
								podiumId, 
								TableEnum.PODIUM, 
								i == 0 ? "LOD0_FOOTPRINT_ID" : "LOD0_ROOFPRINT_ID"));
					}
				}
			}

			if (multiSurfaceId != 0)
				psPodium.setLong(28 + i, multiSurfaceId);
			else
				psPodium.setNull(28 + i, Types.NULL);
		}

		// lodXMultiSurface
		for (int i = 0; i < 4; i++) {
			MultiSurfaceProperty multiSurfaceProperty = null;
			long multiGeometryId = 0;

			switch (i) {
			case 0:
				multiSurfaceProperty = podium.getLod1MultiSurface();
				break;
			case 1:
				multiSurfaceProperty = podium.getLod2MultiSurface();
				break;
			case 2:
				multiSurfaceProperty = podium.getLod3MultiSurface();
				break;
			case 3:
				multiSurfaceProperty = podium.getLod4MultiSurface();
				break;
			}

			if (multiSurfaceProperty != null) {
				if (multiSurfaceProperty.isSetMultiSurface()) {
					multiGeometryId = surfaceGeometryImporter.insert(multiSurfaceProperty.getMultiSurface(), podiumId);
					multiSurfaceProperty.unsetMultiSurface();
				} else {
					// xlink
					String href = multiSurfaceProperty.getHref();

					if (href != null && href.length() != 0) {
						dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
								href, 
								podiumId, 
								TableEnum.PODIUM, 
								"LOD" + (i + 1) + "_MULTI_SURFACE_ID"));
					}
				}
			}

			if (multiGeometryId != 0)
				psPodium.setLong(30 + i, multiGeometryId);
			else
				psPodium.setNull(30 + i, Types.NULL);
		}

		// lodXSolid
		for (int i = 0; i < 4; i++) {
			SolidProperty solidProperty = null;
			long solidGeometryId = 0;

			switch (i) {
			case 0:
				solidProperty = podium.getLod1Solid();
				break;
			case 1:
				solidProperty = podium.getLod2Solid();
				break;
			case 2:
				solidProperty = podium.getLod3Solid();
				break;
			case 3:
				solidProperty = podium.getLod4Solid();
				break;
			}

			if (solidProperty != null) {
				if (solidProperty.isSetSolid()) {
					solidGeometryId = surfaceGeometryImporter.insert(solidProperty.getSolid(), podiumId);
					solidProperty.unsetSolid();
				} else {
					// xlink
					String href = solidProperty.getHref();
					if (href != null && href.length() != 0) {
						dbImporterManager.propagateXlink(new DBXlinkSurfaceGeometry(
								href, 
								podiumId, 
								TableEnum.PODIUM, 
								"LOD" + (i + 1) + "_SOLID_ID"));
					}
				}
			}

			if (solidGeometryId != 0)
				psPodium.setLong(34 + i, solidGeometryId);
			else
				psPodium.setNull(34 + i, Types.NULL);
		}

		psPodium.addBatch();
		if (++batchCounter == dbImporterManager.getDatabaseAdapter().getMaxBatchSize())
			dbImporterManager.executeBatch(DBImporterEnum.PODIUM);
		
		//containStorey
		if(podium.isSetGenericApplicationPropertyOfAbstractBuilding()) {
			for(ADEComponent adeComponent : podium.getGenericApplicationPropertyOfAbstractBuilding()) {
				if(adeComponent instanceof StoreyProperty) {
					Storey storey = ((StoreyProperty)adeComponent).getStorey();
					if(storey != null) {
						String gmlId = storey.getId();
						long id = storeyImporter.insert(storey, podium.getCityGMLClass(), podiumId);
						
						if(id == 0) {
							StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
									podium.getCityGMLClass(), 
									origGmlId));
							msg.append(": Failed to write");
							msg.append(Util.getFeatureSignature(
									storey.getCityGMLClass(),
									gmlId));
							LOG.error(msg.toString());
							
						}
						((StoreyProperty)adeComponent).unsetStorey();
					} else {
						String href = ((StoreyProperty)adeComponent).getHref();
						
						if(href != null && href.length() != 0) {
							LOG.error("XLink reference '" + href + "' to " + CityGMLClass.STOREY + " feature is not supported.");
						}
					}
					
				}
			}
		}
				
		// BoundarySurfaces
		if (podium.isSetBoundedBySurface()) {
			for (BoundarySurfaceProperty boundarySurfaceProperty : podium.getBoundedBySurface()) {
				AbstractBoundarySurface boundarySurface = boundarySurfaceProperty.getBoundarySurface();

				if (boundarySurface != null) {
					String gmlId = boundarySurface.getId();
					long id = thematicSurfaceImporter.insert(boundarySurface, podium.getCityGMLClass(), podiumId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								podium.getCityGMLClass(), 
								origGmlId));
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
		if (podium.isSetOuterBuildingInstallation()) {
			for (BuildingInstallationProperty buildingInstProperty : podium.getOuterBuildingInstallation()) {
				BuildingInstallation buildingInst = buildingInstProperty.getBuildingInstallation();

				if (buildingInst != null) {
					String gmlId = buildingInst.getId();
					//long id = buildingInstallationImporter.insert(buildingInst, podium.getCityGMLClass(), podiumId);
					
					//test class of buildingInstallation
					long id = 0;
					
					if(buildingInst instanceof Beam)
						id = beamImporter.insert((Beam) buildingInst, podium.getCityGMLClass(), podiumId);
					else if(buildingInst instanceof Column)
						id = buildingColumnImporter.insert((Column)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof Covering)
						id = coveringImporter.insert((Covering)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof FlowTerminal)
						id = flowTerminalImporter.insert((FlowTerminal)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof Railing)
						id = railingImporter.insert((Railing)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof Ramp)
						id = rampImporter.insert((Ramp)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof RampFlight)
						id = rampFlightImporter.insert((RampFlight)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof Slab)
						id = slabImporter.insert((Slab)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof Stair)
						id = stairImporter.insert((Stair)buildingInst,podium.getCityGMLClass(),podiumId);
					else if(buildingInst instanceof StairFlight)
						id = stairFlightImporter.insert((StairFlight)buildingInst,podium.getCityGMLClass(),podiumId);
					else
						id = buildingInstallationImporter.insert(buildingInst, podium.getCityGMLClass(), podiumId);
					
					

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								podium.getCityGMLClass(), 
								origGmlId));
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
		if (podium.isSetInteriorBuildingInstallation()) {
			for (IntBuildingInstallationProperty intBuildingInstProperty : podium.getInteriorBuildingInstallation()) {
				IntBuildingInstallation intBuildingInst = intBuildingInstProperty.getIntBuildingInstallation();

				if (intBuildingInst != null) {
					String gmlId = intBuildingInst.getId();
					long id = buildingInstallationImporter.insert(intBuildingInst, podium.getCityGMLClass(), podiumId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								podium.getCityGMLClass(), 
								origGmlId));
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
		if (podium.isSetInteriorRoom()) {
			for (InteriorRoomProperty roomProperty : podium.getInteriorRoom()) {
				Room room = roomProperty.getRoom();

				if (room != null) {
					String gmlId = room.getId();
					long id = roomImporter.insert(room, podium.getCityGMLClass(), podiumId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								podium.getCityGMLClass(), 
								origGmlId));
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

		// BuildingPart
		/*if (podium.isSetConsistsOfBuildingPart()) {
			for (BuildingPartProperty buildingPartProperty : podium.getConsistsOfBuildingPart()) {
				BuildingPart buildingPart = buildingPartProperty.getBuildingPart();

				if (buildingPart != null) {
					long id = dbImporterManager.getDBId(DBSequencerEnum.CITYOBJECT_ID_SEQ);

					if (id != 0)
						insert(buildingPart, id, podiumId, rootId);
					else {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								podium.getCityGMLClass(), 
								origGmlId));
						msg.append(": Failed to write ");
						msg.append(Util.getFeatureSignature(
								CityGMLClass.BUILDING_PART, 
								buildingPart.getId()));

						LOG.error(msg.toString());
					}

					// free memory of nested feature
					buildingPartProperty.unsetBuildingPart();
				} else {
					// xlink
					String href = buildingPartProperty.getHref();

					if (href != null && href.length() != 0) {
						LOG.error("XLink reference '" + href + "' to " + CityGMLClass.BUILDING_PART + " feature is not supported.");
					}
				}
			}
		}*/

		// Address
		if (podium.isSetAddress()) {
			for (AddressProperty addressProperty : podium.getAddress()) {
				Address address = addressProperty.getAddress();

				if (address != null) {
					String gmlId = address.getId();
					long id = addressImporter.insertBuildingAddress(address, podiumId);

					if (id == 0) {
						StringBuilder msg = new StringBuilder(Util.getFeatureSignature(
								podium.getCityGMLClass(), 
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
								podiumId,
								TableEnum.PODIUM,
								href,
								TableEnum.ADDRESS
								));
					}
				}
			}
		}

		// insert local appearance
		cityObjectImporter.insertAppearance(podium, podiumId);

		return true;
		
		
	}
	
	@Override
	public void executeBatch() throws SQLException {
		psPodium.executeBatch();
		batchCounter = 0;
		
	}

	@Override
	public void close() throws SQLException {
		psPodium.close();
		
	}

	@Override
	public DBImporterEnum getDBImporterType() {
		
		return DBImporterEnum.PODIUM;
	}
	
}
