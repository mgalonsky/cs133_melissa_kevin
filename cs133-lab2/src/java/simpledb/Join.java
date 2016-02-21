package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    
    private JoinPredicate p;
    private DbIterator child1;
    private DbIterator child2;
    
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    public JoinPredicate getJoinPredicate() {
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name. Can be taken from the appropriate child's TupleDesc.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1.getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name. Can be taken from the appropriate child's TupleDesc.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2.getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * Should return a TupleDesc that represents the schema for the joined tuples. 
     *@see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        child1.open();
        child2.open();
    }

    public void close() {
    	super.close();
        child1.close();
        child2.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator later on if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    private Tuple currentChild;
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        while(true) {
        	if(currentChild == null || !child2.hasNext()) {
        		if(!child1.hasNext()) {
        			return null;
        		}
        		currentChild = child1.next();
        		child2.rewind();
        	}
        	while (child2.hasNext()) {
        		Tuple nextChild2 = child2.next();
        		if (p.filter(currentChild, nextChild2)) {
        			TupleDesc newTDesc = TupleDesc.merge(currentChild.getTupleDesc(), nextChild2.getTupleDesc());
        			Tuple newTuple = new Tuple(newTDesc);
        			for (int i = 0; i<newTDesc.numFields(); i++) {
        				if (i < currentChild.getTupleDesc().numFields()) {
        					newTuple.setField(i, currentChild.getField(i));
        				} else {
        					newTuple.setField(i, nextChild2.getField(i-currentChild.getTupleDesc().numFields()));
        				}
        			}
        			return newTuple;
        		}
        	}
        }
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
    	DbIterator children[] = new DbIterator[2];
        children[0] = this.child1;
        children[1] = this.child2;
        return children;
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
