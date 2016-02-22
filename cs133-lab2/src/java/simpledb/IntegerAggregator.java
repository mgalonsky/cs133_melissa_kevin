package simpledb;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Computes some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
       
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;
    private Type gbfieldtype;
    private int field;
    private Op what;
    private boolean isSingleAgg;
   
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	
    	if(gbfield == NO_GROUPING || gbfieldtype == null){
    		isSingleAgg = true;
    	} else {
    		isSingleAgg = false;
    	}
    	
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.field = afield;
        this.what = what;
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor. See Aggregator.java for more.
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    
    private HashMap<Field, Integer[]> groups;
    private int runningAggregate;
    private int count;
    
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	runningAggregate = 0;
    	count = 0;
    	
    	if(groups.containsKey(tup.getField(gbfield))){
    		runningAggregate = groups.get(tup.getField(gbfield))[0];
    		count =  groups.get(tup.getField(gbfield))[1];
    	} 
    	
    	int newVal = 0;
    	Field newFieldVal = tup.getField(field);
    	if(newFieldVal instanceof IntField){
    		newVal = ((IntField) newFieldVal).getValue();
    	}
    	
    	if(what==Op.MIN){
    		mergeMin(newVal);
    	} else if(what==Op.MAX) {
    		mergeMax(newVal);
    	} else if(what==Op.SUM) {
    		mergeSum(newVal);
    	} else if(what==Op.AVG) {
    		mergeAvg(newVal);
    	} else if(what==Op.COUNT){
    		mergeCount(newVal);
    	}
    	
    	Integer[] vals = new Integer[2];
    	vals[0] = runningAggregate;
    	vals[1] = count;
    	
    	groups.put(tup.getField(gbfield), vals);    	
    	
    }
    
    // MIN, MAX, SUM, AVG, COUNT
    
    
    private void mergeMin(int val){
    	if(val < runningAggregate) {
    		runningAggregate = val;
    	}
    	count+=1;
    }
    private void mergeMax(int val) {
    	if(val > runningAggregate){
    		runningAggregate = val;
    	}
    	count+=1;
    }
    private void mergeSum(int val) {
    	runningAggregate+=val;
    	count+=1;
    }
    private void mergeAvg(int val) {
    	runningAggregate+=val;
    	count+=1;
    }
    private void mergeCount(int val) {
    	count+=1;
    }
    

    /**
     * Returns a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
    	Tuple[] tuples = new Tuple[groups.size()];
    	
    	Type[] gbtype = new Type[2];
    	gbtype[0] = gbfieldtype;
    	gbtype[1] = Type.INT_TYPE;
    	
    	
    	
    	TupleDesc desc = new TupleDesc(gbtype);
    	
    	Iterator<Field> iter = this.groups.keySet().iterator();
    	
    	int i = 0;
    	while(iter.hasNext()) {
    		Field nextField = iter.next();
    		Tuple tup = new Tuple(desc);
    		
    		tup.setField(0, nextField);
    		Integer[] vals = groups.get(nextField);
    		
    		
    		if(what==Op.MIN){
    			tup.setField(1, new IntField(vals[0]));
        	} else if(what==Op.MAX) {
        		tup.setField(1, new IntField(vals[0]));
        	} else if(what==Op.SUM) {
        		tup.setField(1, new IntField(vals[0]));
        	} else if(what==Op.AVG) {
        		tup.setField(1, new IntField(vals[0]/vals[1]));
        	} else if(what==Op.COUNT){
        		tup.setField(1, new IntField(vals[1]));
        	}
    		tuples[i] = tup;
    		i+=1;
    		
    	}
    	
    	class IntAggIter implements DbIterator {
    		private boolean open;
    		private TupleDesc desc;
    		private HashMap<Field, Integer[]> groups;
    		private int currentLoc;
    		private Iterator<Field> keyIter;
    		private Op op;
    		
    		IntAggIter(HashMap<Field, Integer[]> groups, Type gbFieldType, Op op) {
    			this.groups = groups;
    			open = false;
    			currentLoc = 0;
    			Type[] ty = new Type[2];
    			ty[0] = gbFieldType;
    			ty[1] = Type.INT_TYPE;
    			desc = new TupleDesc(ty);
    			keyIter = this.groups.keySet().iterator();
    			this.op = op;
    		}

			@Override
			public void open() throws DbException, TransactionAbortedException {
				open = true;	
			}

			@Override
			public boolean hasNext() throws DbException,
					TransactionAbortedException {
				if (!open) {
					throw new TransactionAbortedException();
				}
				return keyIter.hasNext();
			}

			@Override
			public Tuple next() throws DbException,
					TransactionAbortedException, NoSuchElementException {
				if (!open) {
					throw new NoSuchElementException();
				}
				Tuple tup = new Tuple(desc);
				Field nextKey = keyIter.next();
				tup.setField(0, nextKey);
				switch (op) {
				case AVG:
					tup.setField(1, new IntField(groups.get(nextKey)[0]/groups.get(nextKey)[1]));
					break;
				case COUNT:
					tup.setField(1,  new IntField(groups.get(nextKey)[1]));
					break;
				default:
					tup.setField(1, new IntField(groups.get(nextKey)[0]));
				}
				return tup;
			}

			@Override
			public void rewind() throws DbException,
					TransactionAbortedException {
				if (!open) {
					throw new TransactionAbortedException();
				}
				keyIter = this.groups.keySet().iterator();
			}

			@Override
			public TupleDesc getTupleDesc() {
				return desc;
			}

			@Override
			public void close() {
				open = false;
			}
    		
    	}
    }
    
    
}
