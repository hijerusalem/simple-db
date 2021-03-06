package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<String, Integer> map;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.map = new HashMap<>();
    }

    private String getAggregateKey(Tuple tup) {
        if (gbfieldtype == null)
            return "default";
        switch (gbfieldtype) {
            case INT_TYPE:
                IntField f = (IntField) tup.getField(gbfield);
                return String.valueOf(f.getValue());
            case STRING_TYPE:
                StringField sf = (StringField) tup.getField(gbfield);
                return sf.getValue();
        }
        return null;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String key = this.getAggregateKey(tup);
        Integer currentAggregateVal = map.get(key);
        switch (what) {
            case COUNT:
                if (currentAggregateVal != null)
                    map.put(key, currentAggregateVal + 1);
                else
                    map.put(key, 1);
                break;
            default:
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpItr();
    }

    private class OpItr implements OpIterator {
        private Boolean open = false;
        private Iterator<Map.Entry<String, Integer>> it;
        public void open() {
            open = true;
            it = map.entrySet().iterator();
        }
        public boolean hasNext() {
            if (!open)
                return false;
            return it.hasNext();
        }

        public TupleDesc getTupleDesc() {
            TupleDesc tupleDesc;
            if (gbfieldtype != null) {
                Type[] types = {gbfieldtype, Type.INT_TYPE};
                tupleDesc = new TupleDesc(types);
            }
            else {
                Type[] types = {Type.INT_TYPE};
                tupleDesc = new TupleDesc(types);
            }
            return tupleDesc;
        }

        public Tuple next() {
            if (!open)
                return null;
            Map.Entry<String, Integer> entry  = it.next();

            IntField aggregateField = new IntField(entry.getValue());
            Tuple t = new Tuple(getTupleDesc());
            if (gbfieldtype != null) {
                switch (gbfieldtype) {
                    case INT_TYPE:
                        t.setField(0, new IntField(Integer.valueOf(entry.getKey())));
                        break;
                    case STRING_TYPE:
                        t.setField(0, new StringField(entry.getKey(), 100));
                        break;
                }
                t.setField(1, aggregateField);
            }
            else
                t.setField(0, aggregateField);
            return t;

        }

        public void rewind() {
            it = map.entrySet().iterator();
        }

        public void close() {
            open = false;
        }
    }

}

