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
package org.citydb.modules.citygml.importer.database.uid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

import org.citydb.modules.citygml.common.database.cache.BranchCacheTable;
import org.citydb.modules.citygml.common.database.cache.CacheTable;
import org.citydb.modules.citygml.common.database.cache.CacheTableManager;
import org.citydb.modules.citygml.common.database.cache.model.CacheTableModelEnum;
import org.citydb.modules.citygml.common.database.uid.UIDCacheEntry;
import org.citydb.modules.citygml.common.database.uid.UIDCachingModel;
import org.citygml4j.model.citygml.CityGMLClass;

public class FeatureGmlIdCache implements UIDCachingModel {
	private final int partitions;
	private final CacheTableModelEnum cacheTableModel;
	private final CacheTableManager cacheTableManager;

	private final ReentrantLock mainLock = new ReentrantLock(true);
	private BranchCacheTable branchTable;

	private CacheTable[] backUpTables;
	private PreparedStatement[] psLookupIds;
	private PreparedStatement[] psDrains;
	private ReentrantLock[] locks;
	private boolean[] isIndexed;
	private int[] batchCounters;

	private int batchSize;

	public FeatureGmlIdCache(CacheTableManager cacheTableManager, int partitions, int batchSize) throws SQLException {
		this.cacheTableManager = cacheTableManager;
		this.partitions = partitions;
		this.batchSize = batchSize;

		cacheTableModel = CacheTableModelEnum.GMLID_FEATURE;
		backUpTables = new CacheTable[partitions];
		psLookupIds = new PreparedStatement[partitions];
		psDrains = new PreparedStatement[partitions];
		locks = new ReentrantLock[partitions];
		isIndexed = new boolean[partitions];
		batchCounters = new int[partitions];

		for (int i = 0; i < partitions; i++)
			locks[i] = new ReentrantLock(true);
	}

	@Override
	public void drainToDB(ConcurrentHashMap<String, UIDCacheEntry> map, int drain) throws SQLException {
		int drainCounter = 0;	

		// firstly, try and write those entries which have not been requested so far
		Iterator<Map.Entry<String, UIDCacheEntry>> iter = map.entrySet().iterator();
		while (drainCounter <= drain && iter.hasNext()) {
			Map.Entry<String, UIDCacheEntry> entry = iter.next();
			if (!entry.getValue().isRequested()) {
				String gmlId = entry.getKey();

				// determine partition for gml:id
				int partition = Math.abs(gmlId.hashCode() % partitions);
				initializePartition(partition);

				// get corresponding prepared statement
				PreparedStatement psDrain = psDrains[partition];

				psDrain.setString(1, gmlId);
				psDrain.setLong(2, entry.getValue().getId());
				psDrain.setString(3, entry.getValue().getMapping());
				psDrain.setInt(4, entry.getValue().getType().ordinal());

				psDrain.addBatch();
				if (++batchCounters[partition] == batchSize) {
					psDrain.executeBatch();
					batchCounters[partition] = 0;
				}

				iter.remove();
				++drainCounter;
			}
		}

		// secondly, drain remaining entries until drain limit
		iter = map.entrySet().iterator();
		while (drainCounter <= drain && iter.hasNext()) {
			Map.Entry<String, UIDCacheEntry> entry = iter.next();
			String gmlId = entry.getKey();

			// determine partition for gml:id
			int partition = Math.abs(gmlId.hashCode() % partitions);
			initializePartition(partition);

			// get corresponding prepared statement
			PreparedStatement psDrain = psDrains[partition];

			psDrain.setString(1, gmlId);
			psDrain.setLong(2, entry.getValue().getId());
			psDrain.setString(3, entry.getValue().getMapping());
			psDrain.setInt(4, entry.getValue().getType().ordinal());

			psDrain.addBatch();
			if (++batchCounters[partition] == batchSize) {
				psDrain.executeBatch();
				batchCounters[partition] = 0;
			}

			iter.remove();
			++drainCounter;
		}

		// finally execute batches
		for (int i = 0; i < psDrains.length; i++)
			if (psDrains[i] != null && batchCounters[i] > 0)
				psDrains[i].executeBatch();
	}

	@Override
	public UIDCacheEntry lookupDB(String key) throws SQLException {
		// determine partition for gml:id
		int partition = Math.abs(key.hashCode() % partitions);
		initializePartition(partition);

		// enable indexes upon first lookup
		if (!isIndexed[partition])
			enableIndexesOnCacheTable(partition);

		// lock partition
		final ReentrantLock tableLock = this.locks[partition];
		tableLock.lock();

		try {
			ResultSet rs = null;

			try {
				psLookupIds[partition].setString(1, key);
				rs = psLookupIds[partition].executeQuery();

				if (rs.next()) {
					long id = rs.getLong(1);
					String mapping = rs.getString(2);
					int type = rs.getInt(3);

					return new UIDCacheEntry(id, 0, false, mapping, CityGMLClass.fromInt(type));
				}

				return null;
			} finally {
				if (rs != null) {
					try {
						rs.close();
					} catch (SQLException sqlEx) {
						//
					}

					rs = null;
				}
			}
		} finally {
			tableLock.unlock();
		}
	}

	@Override
	public String lookupDB(long id, CityGMLClass type) throws SQLException {
		// nothing to do here 
		return null;
	}

	@Override
	public void close() throws SQLException {
		for (PreparedStatement ps : psDrains)
			if (ps != null)
				ps.close();
	}

	@Override
	public String getType() {
		return "feature";
	}

	private void enableIndexesOnCacheTable(int partition) throws SQLException {
		final ReentrantLock lock = this.mainLock;
		lock.lock();

		try {
			if (!isIndexed[partition]) {
				backUpTables[partition].createIndexes();
				isIndexed[partition] = true;
			}
		} finally {
			lock.unlock();
		}
	}

	private void initializePartition(int partition) throws SQLException {
		if (branchTable == null) {
			mainLock.lock();

			try {
				if (branchTable == null)
					branchTable = cacheTableManager.createBranchCacheTable(cacheTableModel);
			} finally {
				mainLock.unlock();
			}
		}

		if (backUpTables[partition] == null) {
			final ReentrantLock tableLock = locks[partition];
			tableLock.lock();

			try {
				if (backUpTables[partition] == null) {
					CacheTable tempTable = partition == 0 ? branchTable.getMainTable() : branchTable.branch();

					Connection conn = tempTable.getConnection();
					String tableName = tempTable.getTableName();

					backUpTables[partition] = tempTable;
					psLookupIds[partition] = conn.prepareStatement("select ID, MAPPING, TYPE from " + backUpTables[partition].getTableName() + " where GMLID=?");
					psDrains[partition] = conn.prepareStatement("insert into " + tableName + " (GMLID, ID, MAPPING, TYPE) values (?, ?, ?, ?)");
				}
			} finally {
				tableLock.unlock();
			}
		}
	}
	
}
