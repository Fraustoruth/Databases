package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    protected Comparator<Record> comparator;
    private TransactionContext transaction;
    private Run sortedRecords;
    private int numBuffers;
    private int sortColumnIndex;
    private String sortColumnName;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int N = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(N / (double)numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * N * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() { return true; }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) this.sortedRecords = sort();
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        List<Record> listOfRecords = new ArrayList<>();
        while (records.hasNext()) {
            listOfRecords.add(records.next());
        }
        listOfRecords.sort(this.comparator);
        Run runToReturn = makeRun(listOfRecords);
        return runToReturn;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be added to the output run
     * next.
     *
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        assert (runs.size() <= this.numBuffers - 1);

        List<Record> sortedList = new ArrayList<>();
        PriorityQueue<Pair<Record, Integer>> PQ = new PriorityQueue<>(new RecordPairComparator());
        List<Iterator<Record>> iteratorList = new ArrayList<>();

        //traverse the runs and create an iterator for each run (add the iterators to the
        // iterator list,
        // and add the first record of each run to the priority queue
        //as a Pair<Record, i>
        for (int i = 0; i < runs.size(); i++) {
            Iterator<Record> runI = runs.get(i).iterator();
            iteratorList.add(runI);
            if (runI.hasNext()) {
                Pair<Record, Integer> firstRecord = new Pair<>(runI.next(), i);
                PQ.add(firstRecord);
            }
        }
        //pop the first element in the queue, get the first item and added to the sorted run list
        //get the second element to know which iterator to access and which record add to the queue
        while (!PQ.isEmpty()) {
            Pair<Record, Integer> temp = PQ.poll();
            Record sortedRecord = temp.getFirst();
            int iteratorIndex = temp.getSecond();
            sortedList.add(sortedRecord);

            //if there are more items in the iterator we just dequeued, keep yielding.
            if (iteratorList.get(iteratorIndex).hasNext()) { //selecting the iterator from which we need to add to the queue
                PQ.add(new Pair<>(iteratorList.get(iteratorIndex).next(), iteratorIndex));//yielding record from iterator and adding it as a pair to the queue
            }
        }

        //create a run of the sortedList
        Run sortedRun = makeRun();
        sortedRun.addAll(sortedList);
        return sortedRun;
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {

        List<Run> sortedRuns = new ArrayList<>();
        int buffersAvailable = (this.numBuffers - 1);
        int numPartitions = (int) Math.floor(runs.size() / buffersAvailable);
        int leftOver = (int) runs.size() % buffersAvailable;
        int lowerBound = 0;
        int upperBound = buffersAvailable;

        //evenly partitioned runs
        for (int i = 0; i < numPartitions; i++) {
            List<Run> runGroup = runs.subList(lowerBound, upperBound); //+1 bc it is exclusive
            lowerBound+=buffersAvailable;
            upperBound+=buffersAvailable;
            Run sortedGroup = mergeSortedRuns(runGroup);
            sortedRuns.add(sortedGroup);
        }
        //possibly uneven partition
        for (int i = 0; i < leftOver; i++) {
            List<Run> runGroup = runs.subList(lowerBound, upperBound - 1);
            lowerBound+=buffersAvailable;
            upperBound+=buffersAvailable;
            sortedRuns.add(mergeSortedRuns(runGroup));
        }
        return sortedRuns;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        BacktrackingIterator<Record> recordIterator;
        List<Run> sortedRuns = new ArrayList<>();
        int numPages = getSource().estimateStats().getNumPages();

        //phase 1: create sorted runs
        while (sourceIterator.hasNext()) {
            recordIterator = getBlockIterator(sourceIterator, getSource().getSchema(), this.numBuffers);
            Run sortedRun = sortRun(recordIterator);
            sortedRuns.add(sortedRun);
        }

        //phase 2: merge sorted runs
        while (sortedRuns.size() > 1) {
            sortedRuns = mergePass(sortedRuns);
        }
        return sortedRuns.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }
}
