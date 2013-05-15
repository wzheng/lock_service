/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.tpcc;

/*
 * Copyright (C) 2004-2006, Denis Lussier
 *
 * LoadData - Load Sample Data directly into database tables or create CSV files for
 *            each table that can then be bulk loaded (again & again & again ...)  :-)
 *
 *    Two optional parameter sets for the command line:
 *
 *                 numWarehouses=9999
 *
 *                 fileLocation=c:/temp/csv/
 *
 *    "numWarehouses" defaults to "1" and when "fileLocation" is omitted the generated
 *    data is loaded into the database tables directly.
 *
 */

import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configCommitCount;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configCustPerDist;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configDistPerWhse;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configItemCount;
import static com.oltpbenchmark.benchmarks.tpcc.jTPCCConfig.configWhseCount;

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.benchmarks.tpcc.jdbc.jdbcIO;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpcc.pojo.District;
import com.oltpbenchmark.benchmarks.tpcc.pojo.History;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Item;
import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Stock;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Warehouse;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import com.oltpbenchmark.DBConnect;
import java.util.ArrayList;
import java.util.HashMap;

public class TPCCLoader extends Loader{
    private static final Logger LOG = Logger.getLogger(TPCCLoader.class);

    public TPCCLoader(TPCCBenchmark benchmark, DBConnect c) {
	super(benchmark, c);
        numWarehouses = (int)Math.round(configWhseCount * this.scaleFactor);
        if (numWarehouses == 0) {
            //where would be fun in that?
            numWarehouses = 1;
        }
        outputFiles= false;
    }

    static boolean fastLoad;
    static String fastLoaderBaseDir;

    // ********** general vars **********************************
    private static java.util.Date now = null;
    private static java.util.Date startDate = null;
    private static java.util.Date endDate = null;

    private static Random gen;
    private static int numWarehouses = 0;
    private static String fileLocation = "";
    private static boolean outputFiles = false;
    private static PrintWriter out = null;
    private static long lastTimeMS = 0;

    private static final int FIRST_UNPROCESSED_O_ID = 2101;
	
    private String getInsertStatement(String tableName) throws SQLException {
    	Table catalog_tbl = this.getTableCatalog(tableName);
    	assert(catalog_tbl != null);
    	String sql = SQLUtil.getInsertSQL(catalog_tbl);
	return sql;
    }

    protected void transRollback() {
	if (outputFiles == false) {
	    try {
	    conn.rollback();
	    } catch (SQLException se) {
	     	LOG.debug(se.getMessage());
	    }
	} else {
	    out.close();
	}
    }

    protected void transCommit() {
	if (outputFiles == false) {
	    try {
		conn.commit();
	    } catch (SQLException se) {
	     	LOG.debug(se.getMessage());
	     	transRollback();
	    }
	} else {
	    out.close();
	}
    }

    protected void truncateTable(String strTable) {

	LOG.debug("Truncating '" + strTable + "' ...");
	//try {
	//this.conn.createStatement().execute("TRUNCATE TABLE " + strTable);
	//this.conn.executeQuery("TRUNCATE TABLE " + strTable);
	//transCommit();
	// } catch (SQLException se) {
	//     LOG.debug(se.getMessage());
	//     transRollback();
	// }

    }

    protected int loadItem(int itemKount) {

	int k = 0;
	int t = 0;
	int randPct = 0;
	int len = 0;
	int startORIGINAL = 0;

	ArrayList<String> statements = new ArrayList<String>();

	HashMap<String, HashMap<String, String> > writes = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> inserts = new HashMap<String, String>();
	writes.put(TPCCConstants.TABLENAME_ITEM, inserts);

	try {
	    String baseStmt = getInsertStatement(TPCCConstants.TABLENAME_ITEM);
	    String itemPrepStmt = baseStmt;


	    now = new java.util.Date();
	    t = itemKount;
	    LOG.debug("\nStart Item Load for " + t + " Items @ " + now
		      + " ...");

	    if (outputFiles == true) {
		out = new PrintWriter(new FileOutputStream(fileLocation
							   + "item.csv"));
		LOG.debug("\nWriting Item file to: " + fileLocation
			  + "item.csv");
	    }

	    Item item = new Item();

	    for (int i = 1; i <= itemKount; i++) {

		item.i_id = i;
		item.i_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24,
								       gen));
		item.i_price = (float) (TPCCUtil.randomNumber(100, 10000, gen) / 100.0);

		// i_data
		randPct = TPCCUtil.randomNumber(1, 100, gen);
		len = TPCCUtil.randomNumber(26, 50, gen);
		if (randPct > 10) {
		    // 90% of time i_data isa random string of length [26 .. 50]
		    item.i_data = TPCCUtil.randomStr(len);
		} else {
		    // 10% of time i_data has "ORIGINAL" crammed somewhere in
		    // middle
		    startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), gen);
		    item.i_data = TPCCUtil.randomStr(startORIGINAL - 1)
			+ "ORIGINAL"
			+ TPCCUtil.randomStr(len - startORIGINAL - 9);
		}

		item.i_im_id = TPCCUtil.randomNumber(1, 10000, gen);

		k++;

		if (outputFiles == false) {
		    // Primary key: i_id
		    String key = Integer.toString(item.i_id);
		    String value = new String();
		    value += item.i_name + Float.toString(item.i_price) + item.i_data + Integer.toString(item.i_im_id);
		    // itemPrepStmt = itemPrepStmt.replaceFirst("\\?", Integer.toString(item.i_id));
		    // itemPrepStmt = itemPrepStmt.replaceFirst("\\?", item.i_name);
		    // itemPrepStmt = itemPrepStmt.replaceFirst("\\?", Float.toString(item.i_price));
		    // itemPrepStmt = itemPrepStmt.replaceFirst("\\?",  item.i_data);
		    // itemPrepStmt = itemPrepStmt.replaceFirst("\\?",  Integer.toString(item.i_im_id));
		    inserts.put(key, value);

		    if ((k % configCommitCount) == 0) {
			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
			    + ((tmpTime - lastTimeMS) / 1000.000)
			    + "                    ";
			LOG.debug(etStr.substring(0, 30) + "  Writing record " + k + " of " + t);
			lastTimeMS = tmpTime;
			this.conn.executeBatch(writes);
			this.conn.commit();
			inserts.clear();
		    }
		} else {
		    String str = "";
		    str = str + item.i_id + ",";
		    str = str + item.i_name + ",";
		    str = str + item.i_price + ",";
		    str = str + item.i_data + ",";
		    str = str + item.i_im_id;
		    out.println(str);

		    if ((k % configCommitCount) == 0) {
			long tmpTime = new java.util.Date().getTime();
			String etStr = "  Elasped Time(ms): "
			    + ((tmpTime - lastTimeMS) / 1000.000)
			    + "                    ";
			LOG.debug(etStr.substring(0, 30)
				  + "  Writing record " + k + " of " + t);
			lastTimeMS = tmpTime;
		    }
		}

	    } // end for

	    long tmpTime = new java.util.Date().getTime();
	    String etStr = "  Elasped Time(ms): "
		+ ((tmpTime - lastTimeMS) / 1000.000)
		+ "                    ";
	    LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
		      + " of " + t);
	    lastTimeMS = tmpTime;

	    if (outputFiles == false) {
		this.conn.executeBatch(writes);
		this.conn.commit();
	    }

	    now = new java.util.Date();
	    LOG.debug("End Item Load @  " + now);

	} catch (SQLException se) {
	    LOG.debug(se.getMessage());
	    transRollback();
	} catch (Exception e) {
	    e.printStackTrace();
	    transRollback();
	}

	return (k);

    } // end loadItem()

    protected int loadWhse(int whseKount) {

	HashMap<String, HashMap<String, String> > writes = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> inserts = new HashMap<String, String>();
	writes.put(TPCCConstants.TABLENAME_WAREHOUSE, inserts);

	try {
		    
	    String whsePrepStmt = getInsertStatement(TPCCConstants.TABLENAME_WAREHOUSE);
	    

	    now = new java.util.Date();
	    LOG.debug("\nStart Whse Load for " + whseKount
		      + " Whses @ " + now + " ...");

	    if (outputFiles == true) {
		out = new PrintWriter(new FileOutputStream(fileLocation
							   + "warehouse.csv"));
		LOG.debug("\nWriting Warehouse file to: "
			  + fileLocation + "warehouse.csv");
	    }

	    Warehouse warehouse = new Warehouse();
	    for (int i = 1; i <= whseKount; i++) {

		warehouse.w_id = i;
		warehouse.w_ytd = 300000;

		// random within [0.0000 .. 0.2000]
		warehouse.w_tax = (float) ((TPCCUtil.randomNumber(0, 2000, gen)) / 10000.0);

		warehouse.w_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(6,
									    10, gen));
		warehouse.w_street_1 = TPCCUtil.randomStr(TPCCUtil
							  .randomNumber(10, 20, gen));
		warehouse.w_street_2 = TPCCUtil.randomStr(TPCCUtil
							  .randomNumber(10, 20, gen));
		warehouse.w_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(10,
									    20, gen));
		warehouse.w_state = TPCCUtil.randomStr(3).toUpperCase();
		warehouse.w_zip = "123456789";

		if (outputFiles == false) {
		    // Primary key: w_id
		    String key = Integer.toString(warehouse.w_id);
		    String value = Float.toString(warehouse.w_ytd) + Float.toString(warehouse.w_tax);
		    value += warehouse.w_name + warehouse.w_street_1 + warehouse.w_street_2 + warehouse.w_city;
		    value += warehouse.w_state + warehouse.w_zip;
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", Integer.toString(warehouse.w_id));
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", Float.toString(warehouse.w_ytd));
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", Float.toString(warehouse.w_tax));
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?",  warehouse.w_name);
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", warehouse.w_street_1);
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", warehouse.w_street_2);
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", warehouse.w_city);
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", warehouse.w_state);
		    // whsePrepStmt = whsePrepStmt.replaceFirst("\\?", warehouse.w_zip);
		    inserts.put(key, value);
		    this.conn.executeQuery(writes);
		    inserts.clear();
		} else {
		    String str = "";
		    str = str + warehouse.w_id + ",";
		    str = str + warehouse.w_ytd + ",";
		    str = str + warehouse.w_tax + ",";
		    str = str + warehouse.w_name + ",";
		    str = str + warehouse.w_street_1 + ",";
		    str = str + warehouse.w_street_2 + ",";
		    str = str + warehouse.w_city + ",";
		    str = str + warehouse.w_state + ",";
		    str = str + warehouse.w_zip;
		    out.println(str);
		}

	    } // end for
	    
	    this.conn.commit();
	    now = new java.util.Date();

	    long tmpTime = new java.util.Date().getTime();
	    LOG.debug("Elasped Time(ms): "
		      + ((tmpTime - lastTimeMS) / 1000.000));
	    lastTimeMS = tmpTime;
	    LOG.debug("End Whse Load @  " + now);

	} catch (SQLException se) {
	    LOG.debug(se.getMessage());
	    transRollback();
	} catch (Exception e) {
	    e.printStackTrace();
	    transRollback();
	}

	return (whseKount);

    } // end loadWhse()

    protected int loadStock(int whseKount, int itemKount) {

	int k = 0;
	int t = 0;
	int randPct = 0;
	int len = 0;
	int startORIGINAL = 0;

	ArrayList<String> statements = new ArrayList<String>();
	HashMap<String, HashMap<String, String> > writes = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> inserts = new HashMap<String, String>();
	writes.put(TPCCConstants.TABLENAME_STOCK, inserts);

	try {
		    
	    String baseStmt = getInsertStatement(TPCCConstants.TABLENAME_STOCK);
	    String stckPrepStmt = baseStmt;

	    now = new java.util.Date();
	    t = (whseKount * itemKount);
	    LOG.debug("\nStart Stock Load for " + t + " units @ "
		      + now + " ...");

	    if (outputFiles == true) {
		out = new PrintWriter(new FileOutputStream(fileLocation
							   + "stock.csv"));
		LOG.debug("\nWriting Stock file to: " + fileLocation
			  + "stock.csv");
	    }

	    Stock stock = new Stock();

	    for (int i = 1; i <= itemKount; i++) {

		for (int w = 1; w <= whseKount; w++) {

		    stock.s_i_id = i;
		    stock.s_w_id = w;
		    stock.s_quantity = TPCCUtil.randomNumber(10, 100, gen);
		    stock.s_ytd = 0;
		    stock.s_order_cnt = 0;
		    stock.s_remote_cnt = 0;

		    // s_data
		    randPct = TPCCUtil.randomNumber(1, 100, gen);
		    len = TPCCUtil.randomNumber(26, 50, gen);
		    if (randPct > 10) {
			// 90% of time i_data isa random string of length [26 ..
			// 50]
			stock.s_data = TPCCUtil.randomStr(len);
		    } else {
			// 10% of time i_data has "ORIGINAL" crammed somewhere
			// in middle
			startORIGINAL = TPCCUtil
			    .randomNumber(2, (len - 8), gen);
			stock.s_data = TPCCUtil.randomStr(startORIGINAL - 1)
			    + "ORIGINAL"
			    + TPCCUtil.randomStr(len - startORIGINAL - 9);
		    }

		    stock.s_dist_01 = TPCCUtil.randomStr(24);
		    stock.s_dist_02 = TPCCUtil.randomStr(24);
		    stock.s_dist_03 = TPCCUtil.randomStr(24);
		    stock.s_dist_04 = TPCCUtil.randomStr(24);
		    stock.s_dist_05 = TPCCUtil.randomStr(24);
		    stock.s_dist_06 = TPCCUtil.randomStr(24);
		    stock.s_dist_07 = TPCCUtil.randomStr(24);
		    stock.s_dist_08 = TPCCUtil.randomStr(24);
		    stock.s_dist_09 = TPCCUtil.randomStr(24);
		    stock.s_dist_10 = TPCCUtil.randomStr(24);

		    k++;
		    if (outputFiles == false) {
			//Primary key: s_w_id + s_i_id
			String key = Integer.toString(stock.s_w_id) + Integer.toString(stock.s_i_id);
			String value = Integer.toString(stock.s_quantity);
			value += Float.toString(stock.s_ytd) + Integer.toString(stock.s_order_cnt) + Integer.toString(stock.s_remote_cnt);
			value += stock.s_dist_01;
			value += stock.s_dist_02;
			value += stock.s_dist_03;
			value += stock.s_dist_04;
			value += stock.s_dist_05;
			value += stock.s_dist_06;
			value += stock.s_dist_07;
			value += stock.s_dist_08;
			value += stock.s_dist_09;
			value += stock.s_dist_10;
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", Integer.toString(stock.s_w_id));
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", Integer.toString(stock.s_i_id));
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", Integer.toString(stock.s_quantity));
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", Float.toString(stock.s_ytd));
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", Integer.toString(stock.s_order_cnt));
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", Integer.toString(stock.s_remote_cnt));
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_data);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_01);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_02);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_03);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_04);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_05);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_06);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_07);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_08);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_09);
			// stckPrepStmt = stckPrepStmt.replaceFirst("\\?", stock.s_dist_10);
			//statements.add(stckPrepStmt);
			inserts.put(key, value);
			//stckPrepStmt = baseStmt;
			if ((k % configCommitCount) == 0) {
			    long tmpTime = new java.util.Date().getTime();
			    String etStr = "  Elasped Time(ms): "
				+ ((tmpTime - lastTimeMS) / 1000.000)
				+ "                    ";
			    LOG.debug(etStr.substring(0, 30)
				      + "  Writing record " + k + " of " + t);
			    lastTimeMS = tmpTime;
			    this.conn.executeBatch(writes);
			    inserts.clear();
			    this.conn.commit();
			}
		    } else {
			String str = "";
			str = str + stock.s_i_id + ",";
			str = str + stock.s_w_id + ",";
			str = str + stock.s_quantity + ",";
			str = str + stock.s_ytd + ",";
			str = str + stock.s_order_cnt + ",";
			str = str + stock.s_remote_cnt + ",";
			str = str + stock.s_data + ",";
			str = str + stock.s_dist_01 + ",";
			str = str + stock.s_dist_02 + ",";
			str = str + stock.s_dist_03 + ",";
			str = str + stock.s_dist_04 + ",";
			str = str + stock.s_dist_05 + ",";
			str = str + stock.s_dist_06 + ",";
			str = str + stock.s_dist_07 + ",";
			str = str + stock.s_dist_08 + ",";
			str = str + stock.s_dist_09 + ",";
			str = str + stock.s_dist_10;
			out.println(str);

			if ((k % configCommitCount) == 0) {
			    long tmpTime = new java.util.Date().getTime();
			    String etStr = "  Elasped Time(ms): "
				+ ((tmpTime - lastTimeMS) / 1000.000)
				+ "                    ";
			    LOG.debug(etStr.substring(0, 30)
				      + "  Writing record " + k + " of " + t);
			    lastTimeMS = tmpTime;
			}
		    }

		} // end for [w]

	    } // end for [i]

	    long tmpTime = new java.util.Date().getTime();
	    String etStr = "  Elasped Time(ms): "
		+ ((tmpTime - lastTimeMS) / 1000.000)
		+ "                    ";
	    LOG.debug(etStr.substring(0, 30)
		      + "  Writing final records " + k + " of " + t);
	    lastTimeMS = tmpTime;
	    if (outputFiles == false) {
		this.conn.executeBatch(writes);
		this.conn.commit();
	    }
	    //transCommit();

	    now = new java.util.Date();
	    LOG.debug("End Stock Load @  " + now);

	} catch (SQLException se) {
	    LOG.debug(se.getMessage());
	    transRollback();

	} catch (Exception e) {
	    e.printStackTrace();
	    transRollback();
	}

	return (k);

    } // end loadStock()

    protected int loadDist(int whseKount, int distWhseKount) {

	int k = 0;
	int t = 0;

	HashMap<String, HashMap<String, String> > writes = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> inserts = new HashMap<String, String>();
	writes.put(TPCCConstants.TABLENAME_DISTRICT, inserts);

	try {

	    String sql = SQLUtil.getInsertSQL(this.getTableCatalog(TPCCConstants.TABLENAME_DISTRICT));
	    //PreparedStatement distPrepStmt = this.conn.prepareStatement(sql);
		    
	    now = new java.util.Date();

	    if (outputFiles == true) {
		out = new PrintWriter(new FileOutputStream(fileLocation
							   + "district.csv"));
		LOG.debug("\nWriting District file to: "
			  + fileLocation + "district.csv");
	    }

	    District district = new District();

	    t = (whseKount * distWhseKount);
	    LOG.debug("\nStart District Data for " + t + " Dists @ "
		      + now + " ...");

	    for (int w = 1; w <= whseKount; w++) {

		for (int d = 1; d <= distWhseKount; d++) {
				    
				    
		    district.d_id = d;
		    district.d_w_id = w;
		    district.d_ytd = 30000;

		    // random within [0.0000 .. 0.2000]
		    district.d_tax = (float) ((TPCCUtil.randomNumber(0, 2000,
								     gen)) / 10000.0);

		    district.d_next_o_id = 3001;
		    district.d_name = TPCCUtil.randomStr(TPCCUtil.randomNumber(
									       6, 10, gen));
		    district.d_street_1 = TPCCUtil.randomStr(TPCCUtil
							     .randomNumber(10, 20, gen));
		    district.d_street_2 = TPCCUtil.randomStr(TPCCUtil
							     .randomNumber(10, 20, gen));
		    district.d_city = TPCCUtil.randomStr(TPCCUtil.randomNumber(
									       10, 20, gen));
		    district.d_state = TPCCUtil.randomStr(3).toUpperCase();
		    district.d_zip = "123456789";

		    k++;

		    if (outputFiles == false) {
			// Primary key: d_w_id + d_id
			String key = Integer.toString(district.d_w_id) + Integer.toString(district.d_id);
			String value = Float.toString(district.d_ytd) + Float.toString(district.d_tax);
			value += Integer.toString(district.d_next_o_id);
			value += district.d_name + district.d_street_1 + district.d_street_2;
			value += district.d_city + district.d_state + district.d_zip;
			// sql = sql.replaceFirst("\\?", Integer.toString(district.d_w_id));
		    	// sql = sql.replaceFirst("\\?", Integer.toString(district.d_id));
		    	// sql = sql.replaceFirst("\\?", Float.toString(district.d_ytd));
		    	// sql = sql.replaceFirst("\\?", Float.toString(district.d_tax));
		    	// sql = sql.replaceFirst("\\?", Integer.toString(district.d_next_o_id));
		    	// sql = sql.replaceFirst("\\?", district.d_name);
		    	// sql = sql.replaceFirst("\\?", district.d_street_1);
		    	// sql = sql.replaceFirst("\\?", district.d_street_2);
		    	// sql = sql.replaceFirst("\\?", district.d_city);
		    	// sql = sql.replaceFirst("\\?", district.d_state);
		    	// sql = sql.replaceFirst("\\?", district.d_zip);
			inserts.put(key, value);
			this.conn.executeQuery(writes);
			inserts.clear();
		    } else {
		    	String str = "";
		    	str = str + district.d_id + ",";
		    	str = str + district.d_w_id + ",";
		    	str = str + district.d_ytd + ",";
		    	str = str + district.d_tax + ",";
		    	str = str + district.d_next_o_id + ",";
		    	str = str + district.d_name + ",";
		    	str = str + district.d_street_1 + ",";
		    	str = str + district.d_street_2 + ",";
		    	str = str + district.d_city + ",";
		    	str = str + district.d_state + ",";
		    	str = str + district.d_zip;
		    	out.println(str);
		    }

		} // end for [d]

	    } // end for [w]

	    long tmpTime = new java.util.Date().getTime();
	    String etStr = "  Elasped Time(ms): "
		+ ((tmpTime - lastTimeMS) / 1000.000)
		+ "                    ";
	    LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
		      + " of " + t);
	    lastTimeMS = tmpTime;
	    this.conn.commit();
	    //transCommit();
	    now = new java.util.Date();
	    LOG.debug("End District Load @  " + now);

	// } catch (SQLException se) {
	//     LOG.debug(se.getMessage());
	//     transRollback();
	} catch (Exception e) {
	    e.printStackTrace();
	    transRollback();
	}

	return (k);

    } // end loadDist()

    protected int loadCust(int whseKount, int distWhseKount, int custDistKount) {

	int k = 0;
	int t = 0;

	Customer customer = new Customer();
	History history = new History();
	PrintWriter outHist = null;

	ArrayList<String> customerStatements = new ArrayList<String>();
	ArrayList<String> historyStatements = new ArrayList<String>();	

	HashMap<String, HashMap<String, String> > customerWrites = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> customerInserts = new HashMap<String, String>();
	customerWrites.put(TPCCConstants.TABLENAME_CUSTOMER, customerInserts);

	HashMap<String, HashMap<String, String> > historyWrites = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> historyInserts = new HashMap<String, String>();
	historyWrites.put(TPCCConstants.TABLENAME_HISTORY, historyInserts);

	int historyNum = 0;


	try {
	    String custBaseStmt = getInsertStatement(TPCCConstants.TABLENAME_CUSTOMER);
	    String histBaseStmt = getInsertStatement(TPCCConstants.TABLENAME_HISTORY);
	    String custPrepStmt = custBaseStmt;
	    String histPrepStmt = histBaseStmt;

	    now = new java.util.Date();

	    if (outputFiles == true) {
		out = new PrintWriter(new FileOutputStream(fileLocation
							   + "customer.csv"));
		LOG.debug("\nWriting Customer file to: "
			  + fileLocation + "customer.csv");
		outHist = new PrintWriter(new FileOutputStream(fileLocation
							       + "cust-hist.csv"));
		LOG.debug("\nWriting Customer History file to: "
			  + fileLocation + "cust-hist.csv");
	    }

	    t = (whseKount * distWhseKount * custDistKount * 2);
	    LOG.debug("\nStart Cust-Hist Load for " + t
		      + " Cust-Hists @ " + now + " ...");

	    for (int w = 1; w <= whseKount; w++) {

		for (int d = 1; d <= distWhseKount; d++) {

		    for (int c = 1; c <= custDistKount; c++) {

			Timestamp sysdate = new java.sql.Timestamp(System.currentTimeMillis());

			customer.c_id = c;
			customer.c_d_id = d;
			customer.c_w_id = w;

			// discount is random between [0.0000 ... 0.5000]
			customer.c_discount = (float) (TPCCUtil.randomNumber(1, 5000, gen) / 10000.0);

			if (TPCCUtil.randomNumber(1, 100, gen) <= 10) {
			    customer.c_credit = "BC"; // 10% Bad Credit
			} else {
			    customer.c_credit = "GC"; // 90% Good Credit
			}
			if (c <= 1000) {
			    customer.c_last = TPCCUtil.getLastName(c - 1);
			} else {
			    customer.c_last = TPCCUtil
				.getNonUniformRandomLastNameForLoad(gen);
			}
			customer.c_first = TPCCUtil.randomStr(TPCCUtil
							      .randomNumber(8, 16, gen));
			customer.c_credit_lim = 50000;

			customer.c_balance = -10;
			customer.c_ytd_payment = 10;
			customer.c_payment_cnt = 1;
			customer.c_delivery_cnt = 0;

			customer.c_street_1 = TPCCUtil.randomStr(TPCCUtil
								 .randomNumber(10, 20, gen));
			customer.c_street_2 = TPCCUtil.randomStr(TPCCUtil
								 .randomNumber(10, 20, gen));
			customer.c_city = TPCCUtil.randomStr(TPCCUtil
							     .randomNumber(10, 20, gen));
			customer.c_state = TPCCUtil.randomStr(3).toUpperCase();
			// TPC-C 4.3.2.7: 4 random digits + "11111"
			customer.c_zip = TPCCUtil.randomNStr(4) + "11111";

			customer.c_phone = TPCCUtil.randomNStr(16);

			customer.c_since = sysdate;
			customer.c_middle = "OE";
			customer.c_data = TPCCUtil.randomStr(TPCCUtil
							     .randomNumber(300, 500, gen));

			history.h_c_id = c;
			history.h_c_d_id = d;
			history.h_c_w_id = w;
			history.h_d_id = d;
			history.h_w_id = w;
			history.h_date = sysdate;
			history.h_amount = 10;
			history.h_data = TPCCUtil.randomStr(TPCCUtil
							    .randomNumber(10, 24, gen));

			k = k + 2;
			if (outputFiles == false) {
			    // Customer table - Primary key: c_w_id + c_d_id + c_id
			    String key =  Integer.toString(customer.c_w_id) +  Integer.toString(customer.c_d_id) + Integer.toString(customer.c_id);
			    String value = Float.toString(customer.c_discount) + customer.c_credit;
			    value += customer.c_last +  customer.c_first + Float.toString(customer.c_credit_lim);
			    value += Float.toString(customer.c_balance) + Float.toString(customer.c_ytd_payment);
			    value += Integer.toString(customer.c_payment_cnt) + Integer.toString(customer.c_delivery_cnt);
			    value += customer.c_street_1 + customer.c_street_2;
			    value += customer.c_city + customer.c_state + customer.c_zip;
			    value += customer.c_phone + customer.c_since.toString();
			    value += customer.c_middle + customer.c_data;

			    // custPrepStmt = custPrepStmt.replace("\\?", Integer.toString(customer.c_w_id));
			    // custPrepStmt = custPrepStmt.replace("\\?", Integer.toString(customer.c_d_id));
			    // custPrepStmt = custPrepStmt.replace("\\?", Integer.toString(customer.c_id));
			    // custPrepStmt = custPrepStmt.replace("\\?", Float.toString(customer.c_discount));
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_credit);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_last);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_first);
			    // custPrepStmt = custPrepStmt.replace("\\?", Float.toString(customer.c_credit_lim));
			    // custPrepStmt = custPrepStmt.replace("\\?", Float.toString(customer.c_balance));
			    // custPrepStmt = custPrepStmt.replace("\\?", Float.toString(customer.c_ytd_payment));
			    // custPrepStmt = custPrepStmt.replace("\\?", Integer.toString(customer.c_payment_cnt));
			    // custPrepStmt = custPrepStmt.replace("\\?", Integer.toString(customer.c_delivery_cnt));
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_street_1);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_street_2);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_city);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_state);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_zip);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_phone);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_since.toString());
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_middle);
			    // custPrepStmt = custPrepStmt.replace("\\?", customer.c_data);

			    // customerStatements.add(custPrepStmt);
			    // custPrepStmt = custBaseStmt;
			    customerInserts.put(key, value);
			    
			    String histKey = Integer.toString(historyNum);
			    String histValue = Integer.toString(history.h_c_id) + Integer.toString(history.h_c_d_id);
			    histValue += Integer.toString(history.h_c_w_id) + Integer.toString(history.h_d_id);
			    histValue += Integer.toString(history.h_w_id) + history.h_date.toString();
			    histValue += Float.toString(history.h_amount) + history.h_data;

			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", Integer.toString(history.h_c_id));
			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", Integer.toString(history.h_c_d_id));
			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", Integer.toString(history.h_c_w_id));

			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", Integer.toString(history.h_d_id));
			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", Integer.toString(history.h_w_id));
			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", history.h_date.toString());
			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", Float.toString(history.h_amount));
			    // histPrepStmt = histPrepStmt.replaceFirst("\\?", history.h_data);

			    historyInserts.put(histKey, histValue);
			    customerInserts.put(key, value);
			    historyNum++;

			    if ((k % configCommitCount) == 0) {
				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
				    + ((tmpTime - lastTimeMS) / 1000.000)
				    + "                    ";
				LOG.debug(etStr.substring(0, 30)
					  + "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;


				this.conn.executeBatch(customerWrites);
				this.conn.executeBatch(historyWrites);
				this.conn.commit();
				customerInserts.clear();
				historyInserts.clear();
			    }
			} else {
			    String str = "";
			    str = str + customer.c_id + ",";
			    str = str + customer.c_d_id + ",";
			    str = str + customer.c_w_id + ",";
			    str = str + customer.c_discount + ",";
			    str = str + customer.c_credit + ",";
			    str = str + customer.c_last + ",";
			    str = str + customer.c_first + ",";
			    str = str + customer.c_credit_lim + ",";
			    str = str + customer.c_balance + ",";
			    str = str + customer.c_ytd_payment + ",";
			    str = str + customer.c_payment_cnt + ",";
			    str = str + customer.c_delivery_cnt + ",";
			    str = str + customer.c_street_1 + ",";
			    str = str + customer.c_street_2 + ",";
			    str = str + customer.c_city + ",";
			    str = str + customer.c_state + ",";
			    str = str + customer.c_zip + ",";
			    str = str + customer.c_phone;
			    out.println(str);

			    str = "";
			    str = str + history.h_c_id + ",";
			    str = str + history.h_c_d_id + ",";
			    str = str + history.h_c_w_id + ",";
			    str = str + history.h_d_id + ",";
			    str = str + history.h_w_id + ",";
			    str = str + history.h_date + ",";
			    str = str + history.h_amount + ",";
			    str = str + history.h_data;
			    outHist.println(str);

			    if ((k % configCommitCount) == 0) {
				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
				    + ((tmpTime - lastTimeMS) / 1000.000)
				    + "                    ";
				LOG.debug(etStr.substring(0, 30)
					  + "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;

			    }
			}

		    } // end for [c]

		} // end for [d]

	    } // end for [w]

	    long tmpTime = new java.util.Date().getTime();
	    String etStr = "  Elasped Time(ms): "
		+ ((tmpTime - lastTimeMS) / 1000.000)
		+ "                    ";
	    LOG.debug(etStr.substring(0, 30) + "  Writing record " + k
		      + " of " + t);
	    lastTimeMS = tmpTime;
	    this.conn.executeBatch(customerWrites);
	    this.conn.executeBatch(historyWrites);
	    this.conn.commit();
	    customerInserts.clear();
	    historyInserts.clear();

	    now = new java.util.Date();
	    if (outputFiles == true) {
		outHist.close();
	    }
	    LOG.debug("End Cust-Hist Data Load @  " + now);

	} catch (SQLException se) {
	    LOG.debug(se.getMessage());
	    transRollback();
	    if (outputFiles == true) {
		outHist.close();
	    }
	} catch (Exception e) {
	    e.printStackTrace();
	    transRollback();
	    if (outputFiles == true) {
		outHist.close();
	    }
	}

	return (k);

    } // end loadCust()

    protected int loadOrder(int whseKount, int distWhseKount, int custDistKount) {

	int k = 0;
	int t = 0;
	PrintWriter outLine = null;
	PrintWriter outNewOrder = null;
	ArrayList<String> ordrStatements = new ArrayList<String>();
	ArrayList<String> nworStatements = new ArrayList<String>();
	ArrayList<String> orlnStatements = new ArrayList<String>();

	HashMap<String, HashMap<String, String> > ooWrites = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> ooInserts = new HashMap<String, String>();
	ooWrites.put(TPCCConstants.TABLENAME_OPENORDER, ooInserts);

	HashMap<String, HashMap<String, String> > noWrites = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> noInserts = new HashMap<String, String>();
	noWrites.put(TPCCConstants.TABLENAME_NEWORDER, noInserts);

	HashMap<String, HashMap<String, String> > olWrites = new HashMap<String, HashMap<String, String> >();
	HashMap<String, String> olInserts = new HashMap<String, String>();
	olWrites.put(TPCCConstants.TABLENAME_ORDERLINE, olInserts);

	try {
	    
	    String ordrBaseStmt = getInsertStatement(TPCCConstants.TABLENAME_OPENORDER);
	    String nworBaseStmt = getInsertStatement(TPCCConstants.TABLENAME_NEWORDER);
	    String orlnBaseStmt = getInsertStatement(TPCCConstants.TABLENAME_ORDERLINE);

	    String ordrPrepStmt = ordrBaseStmt;
	    String nworPrepStmt = nworBaseStmt;
	    String orlnPrepStmt = orlnBaseStmt;

	    if (outputFiles == true) {
		out = new PrintWriter(new FileOutputStream(fileLocation
							   + "order.csv"));
		LOG.debug("\nWriting Order file to: " + fileLocation
			  + "order.csv");
		outLine = new PrintWriter(new FileOutputStream(fileLocation
							       + "order-line.csv"));
		LOG.debug("\nWriting Order Line file to: "
			  + fileLocation + "order-line.csv");
		outNewOrder = new PrintWriter(new FileOutputStream(fileLocation
								   + "new-order.csv"));
		LOG.debug("\nWriting New Order file to: "
			  + fileLocation + "new-order.csv");
	    }

	    now = new java.util.Date();
	    Oorder oorder = new Oorder();
	    NewOrder new_order = new NewOrder();
	    OrderLine order_line = new OrderLine();
	    jdbcIO myJdbcIO = new jdbcIO();

	    t = (whseKount * distWhseKount * custDistKount);
	    t = (t * 11) + (t / 3);
	    LOG.debug("whse=" + whseKount + ", dist=" + distWhseKount
		      + ", cust=" + custDistKount);
	    LOG.debug("\nStart Order-Line-New Load for approx " + t
		      + " rows @ " + now + " ...");

	    for (int w = 1; w <= whseKount; w++) {

		for (int d = 1; d <= distWhseKount; d++) {
		    // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
		    int[] c_ids = new int[custDistKount];
		    for (int i = 0; i < custDistKount; ++i) {
			c_ids[i] = i + 1;
		    }
		    // Collections.shuffle exists, but there is no
		    // Arrays.shuffle
		    for (int i = 0; i < c_ids.length - 1; ++i) {
			int remaining = c_ids.length - i - 1;
			int swapIndex = gen.nextInt(remaining) + i + 1;
			assert i < swapIndex;
			int temp = c_ids[swapIndex];
			c_ids[swapIndex] = c_ids[i];
			c_ids[i] = temp;
		    }

		    for (int c = 1; c <= custDistKount; c++) {

			oorder.o_id = c;
			oorder.o_w_id = w;
			oorder.o_d_id = d;
			oorder.o_c_id = c_ids[c - 1];
			// o_carrier_id is set *only* for orders with ids < 2101
			// [4.3.3.1]
			if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
			    oorder.o_carrier_id = TPCCUtil.randomNumber(1, 10,
									gen);
			} else {
			    oorder.o_carrier_id = null;
			}
			oorder.o_ol_cnt = TPCCUtil.randomNumber(5, 15, gen);
			oorder.o_all_local = 1;
			oorder.o_entry_d = System.currentTimeMillis();

			k++;
			if (outputFiles == false) {
			    //myJdbcIO.insertOrder(ordrStatements, ordrPrepStmt, oorder);
			    // Primary: o_w_id + o_d_id + o_id
			    String key = "";
			    key = key + oorder.o_w_id + oorder.o_d_id + oorder.o_id;
			    String value = "";
			    value = value + oorder.o_c_id + oorder.o_carrier_id + oorder.o_ol_cnt + oorder.o_all_local;
			    ooInserts.put(key, value);
			} else {
			    String str = "";
			    str = str + oorder.o_id + ",";
			    str = str + oorder.o_w_id + ",";
			    str = str + oorder.o_d_id + ",";
			    str = str + oorder.o_c_id + ",";
			    str = str + oorder.o_carrier_id + ",";
			    str = str + oorder.o_ol_cnt + ",";
			    str = str + oorder.o_all_local + ",";
			    Timestamp entry_d = new java.sql.Timestamp(
								       oorder.o_entry_d);
			    str = str + entry_d;
			    out.println(str);
			}

			// 900 rows in the NEW-ORDER table corresponding to the
			// last
			// 900 rows in the ORDER table for that district (i.e.,
			// with
			// NO_O_ID between 2,101 and 3,000)

			if (c >= FIRST_UNPROCESSED_O_ID) {

			    new_order.no_w_id = w;
			    new_order.no_d_id = d;
			    new_order.no_o_id = c;

			    k++;
			    if (outputFiles == false) {
				//myJdbcIO.insertNewOrder(nworStatements, nworPrepStmt, new_order);
				String key = "";
				key = key + new_order.no_w_id + new_order.no_d_id + new_order.no_o_id;
				String value = key;
				noInserts.put(key, value);
			    } else {
				String str = "";
				str = str + new_order.no_w_id + ",";
				str = str + new_order.no_d_id + ",";
				str = str + new_order.no_o_id;
				outNewOrder.println(str);
			    }

			} // end new order

			for (int l = 1; l <= oorder.o_ol_cnt; l++) {
			    order_line.ol_w_id = w;
			    order_line.ol_d_id = d;
			    order_line.ol_o_id = c;
			    order_line.ol_number = l; // ol_number
			    order_line.ol_i_id = TPCCUtil.randomNumber(1,
								       100000, gen);
			    if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
				order_line.ol_delivery_d = oorder.o_entry_d;
				order_line.ol_amount = 0;
			    } else {
				order_line.ol_delivery_d = null;
				// random within [0.01 .. 9,999.99]
				order_line.ol_amount = (float) (TPCCUtil
								.randomNumber(1, 999999, gen) / 100.0);
			    }

			    order_line.ol_supply_w_id = order_line.ol_w_id;
			    order_line.ol_quantity = 5;
			    order_line.ol_dist_info = TPCCUtil.randomStr(24);

			    k++;
			    if (outputFiles == false) {
				//myJdbcIO.insertOrderLine(orlnStatements, orlnPrepStmt, order_line);
				// Primary key: ol_w_id + ol_d_id + ol_o_id + ol_number
				String key = "";
				key += order_line.ol_w_id + order_line.ol_d_id + order_line.ol_o_id + order_line.ol_number;
				String value = "";
				value += order_line.ol_i_id;
				Timestamp delivery_d = new Timestamp(order_line.ol_delivery_d);
				value += delivery_d;
				value += order_line.ol_amount;
				value += order_line.ol_supply_w_id;
				value += order_line.ol_quantity;
				value += order_line.ol_dist_info;
				olInserts.put(key, value);
			    } else {
				String str = "";
				str = str + order_line.ol_w_id + ",";
				str = str + order_line.ol_d_id + ",";
				str = str + order_line.ol_o_id + ",";
				str = str + order_line.ol_number + ",";
				str = str + order_line.ol_i_id + ",";
				Timestamp delivery_d = new Timestamp(order_line.ol_delivery_d);
				str = str + delivery_d + ",";
				str = str + order_line.ol_amount + ",";
				str = str + order_line.ol_supply_w_id + ",";
				str = str + order_line.ol_quantity + ",";
				str = str + order_line.ol_dist_info;
				outLine.println(str);
			    }

			    if ((k % configCommitCount) == 0) {
				long tmpTime = new java.util.Date().getTime();
				String etStr = "  Elasped Time(ms): "
				    + ((tmpTime - lastTimeMS) / 1000.000)
				    + "                    ";
				LOG.debug(etStr.substring(0, 30)
					  + "  Writing record " + k + " of " + t);
				lastTimeMS = tmpTime;
				if (outputFiles == false) {

				    this.conn.executeBatch(ooWrites);
				    this.conn.executeBatch(noWrites);
				    this.conn.executeBatch(olWrites);
				    ooInserts.clear();
				    noInserts.clear();
				    olInserts.clear();
				    this.conn.commit();
				}
			    }

			} // end for [l]

		    } // end for [c]

		} // end for [d]

	    } // end for [w]

	    LOG.debug("  Writing final records " + k + " of " + t);
	    if (outputFiles == false) {
		this.conn.executeBatch(ooWrites);
		this.conn.executeBatch(noWrites);
		this.conn.executeBatch(olWrites);
		ooInserts.clear();
		noInserts.clear();
		olInserts.clear();
	    } else {
		outLine.close();
		outNewOrder.close();
	    }
	    //transCommit();
	    this.conn.commit();
	    now = new java.util.Date();
	    LOG.debug("End Orders Load @  " + now);

	} catch (Exception e) {
	    e.printStackTrace();
	    transRollback();
	    if (outputFiles == true) {
		outLine.close();
		outNewOrder.close();
	    }
	}

	return (k);

    } // end loadOrder()

    // This originally used org.apache.commons.lang.NotImplementedException
    // but I don't get why...
    public static final class NotImplementedException extends UnsupportedOperationException {
        private static final long serialVersionUID = 1958656852398867984L;
    }

    @Override
    public void load() throws SQLException {
	
	// if (outputFiles == false) {
	//     // Clearout the tables
	//     truncateTable(TPCCConstants.TABLENAME_ITEM);
	//     truncateTable(TPCCConstants.TABLENAME_WAREHOUSE);
	//     truncateTable(TPCCConstants.TABLENAME_STOCK);
	//     truncateTable(TPCCConstants.TABLENAME_DISTRICT);
	//     truncateTable(TPCCConstants.TABLENAME_CUSTOMER);
	//     truncateTable(TPCCConstants.TABLENAME_HISTORY);
	//     truncateTable(TPCCConstants.TABLENAME_OPENORDER);
	//     truncateTable(TPCCConstants.TABLENAME_ORDERLINE);
	//     truncateTable(TPCCConstants.TABLENAME_NEWORDER);
	// }

	// seed the random number generator
	gen = new Random(System.currentTimeMillis());

	// ######################### MAINLINE
	// ######################################
	startDate = new java.util.Date();
	LOG.debug("------------- LoadData Start Date = " + startDate
		  + "-------------");

	long startTimeMS = new java.util.Date().getTime();
	lastTimeMS = startTimeMS;

	long totalRows = loadWhse(numWarehouses);
	totalRows += loadItem(configItemCount);
	totalRows += loadStock(numWarehouses, configItemCount);
	totalRows += loadDist(numWarehouses, configDistPerWhse);
	totalRows += loadCust(numWarehouses, configDistPerWhse,
			      configCustPerDist);
	totalRows += loadOrder(numWarehouses, configDistPerWhse,
			       configCustPerDist);

	long runTimeMS = (new java.util.Date().getTime()) + 1 - startTimeMS;
	endDate = new java.util.Date();
	LOG.debug("");
	LOG.debug("------------- LoadJDBC Statistics --------------------");
	LOG.debug("     Start Time = " + startDate);
	LOG.debug("       End Time = " + endDate);
	LOG.debug("       Run Time = " + (int) runTimeMS / 1000 + " Seconds");
	LOG.debug("    Rows Loaded = " + totalRows + " Rows");
	LOG.debug("Rows Per Second = " + (totalRows / (runTimeMS / 1000)) + " Rows/Sec");
	LOG.debug("------------------------------------------------------");
	
    }
} // end LoadData Class
