/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.auctionmark.util;

import com.oltpbenchmark.util.CompositeId;

public class UserId extends CompositeId {
    
    private static final int COMPOSITE_BITS[] = {
        24, // ITEM_COUNT
        24, // OFFSET
    };
    private static final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);
    
    /**
     * The size index is the position in the histogram for the number
     * of users per items size
     */
    private int itemCount;
    
    /**
     * The offset is based on the number of users that exist at a given size index
     */
    private int offset;
    
    public UserId() {
        // For serialization
    }

    /**
     * Constructor
     * @param itemCount
     * @param offset
     */
    public UserId(int itemCount, int offset) {
        this.itemCount = itemCount;
        this.offset = offset;
    }
    
    /**
     * Constructor
     * Converts a composite id generated by encode() into the full object
     * @param composite_id
     */
    public UserId(long composite_id) {
        this.decode(composite_id);
    }
    
    @Override
    public long encode() {
        return (this.encode(COMPOSITE_BITS, COMPOSITE_POWS));
    }
    @Override
    public void decode(long composite_id) {
        long values[] = super.decode(composite_id, COMPOSITE_BITS, COMPOSITE_POWS);
        this.offset = (int)values[0];
        this.itemCount = (int)values[1];
    }
    @Override
    public long[] toArray() {
        return (new long[]{ this.offset, this.itemCount });
    }
    
    public int getItemCount() {
        return this.itemCount;
    }
    public int getOffset() {
        return this.offset;
    }
    
    @Override
    public String toString() {
        return String.format("UserId<itemCount=%d,offset=%d>",
                             this.itemCount, this.offset);
    }
    
    public static String toString(long userId) {
        return new UserId(userId).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        
        if (!(obj instanceof UserId) || obj == null)
            return false;
                
        UserId o = (UserId)obj;
        return (this.itemCount == o.itemCount 
              && this.offset == o.offset);       
    }
    
    @Override
    public int hashCode() {        
        int prime = 11;
        int result = 1;
        result = prime * result + itemCount;
        result = prime * result + offset;
        return result;
    }
}
