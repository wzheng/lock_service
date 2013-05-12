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
package com.oltpbenchmark.benchmarks.tpcc.jdbc;

/*
 * jdbcIO - execute JDBC statements
 *
 * Copyright (C) 2004-2006, Denis Lussier
 *
 */

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

import com.oltpbenchmark.benchmarks.tpcc.pojo.NewOrder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;

import java.util.ArrayList;

public class jdbcIO {

    public void insertOrder(ArrayList<String> statements, String ordrPrepStmt, Oorder oorder) {

	//try {
		    
	    ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", Integer.toString(oorder.o_w_id));
	    ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", Integer.toString(oorder.o_d_id));
	    ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", Integer.toString(oorder.o_id));
	    ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", Integer.toString(oorder.o_c_id));
	    if (oorder.o_carrier_id != null) {
		ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", Integer.toString(oorder.o_carrier_id));
	    } else {
		// TODO: is this right??
		ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", "NULL");
	    }
	    ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", Integer.toString(oorder.o_ol_cnt));
	    ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", Integer.toString(oorder.o_all_local));
	    Timestamp entry_d = new java.sql.Timestamp(oorder.o_entry_d);
	    ordrPrepStmt = ordrPrepStmt.replaceFirst("\\?", entry_d.toString());

	    statements.add(ordrPrepStmt);
			
	//} catch (SQLException se) {
	//    throw new RuntimeException(se);
	//}

    } // end insertOrder()

    public void insertNewOrder(ArrayList<String> statements, String nworPrepStmt, NewOrder new_order) {

	//try {
	    nworPrepStmt = nworPrepStmt.replaceFirst("\\?", Integer.toString(new_order.no_w_id));
	    nworPrepStmt = nworPrepStmt.replaceFirst("\\?", Integer.toString(new_order.no_d_id));
	    nworPrepStmt = nworPrepStmt.replaceFirst("\\?", Integer.toString(new_order.no_o_id));

	    statements.add(nworPrepStmt);

	//} catch (SQLException se) {
	//    throw new RuntimeException(se);
	//}

    } // end insertNewOrder()

    public void insertOrderLine(ArrayList<String> statements, String orlnPrepStmt, OrderLine order_line) {

	//try {
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Integer.toString(order_line.ol_w_id));
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Integer.toString(order_line.ol_d_id));
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Integer.toString(order_line.ol_o_id));
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Integer.toString(order_line.ol_number));
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Integer.toString(order_line.ol_i_id));

	    Timestamp delivery_d = null;
	    if (order_line.ol_delivery_d != null)
		delivery_d = new Timestamp(order_line.ol_delivery_d);
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", delivery_d.toString());

	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Float.toString(order_line.ol_amount));
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Integer.toString(order_line.ol_supply_w_id));
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", Float.toString(order_line.ol_quantity));
	    orlnPrepStmt = orlnPrepStmt.replaceFirst("\\?", order_line.ol_dist_info);

	    statements.add(orlnPrepStmt);
	//} catch (SQLException se) {
	//    throw new RuntimeException(se);
	//}

    } // end insertOrderLine()

} // end class jdbcIO()
