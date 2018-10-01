package io.kdatalog.iris;

import org.deri.iris.api.basics.ITuple;
import org.deri.iris.storage.IRelation;
import org.deri.iris.utils.UniqueList;

import java.util.List;

public class SimplePDoPRelation implements IRelation {
    public SimplePDoPRelation() {
        mTuples = new UniqueList<>();
    }

    public boolean add(ITuple tuple) {
        assert mTuples.isEmpty() || (mTuples.get(0).size() == tuple.size());
        return mTuples.add(tuple);
    }

    public boolean addAll(IRelation relation) {
        boolean added = false;
        for (int i = 0; i < relation.size(); i++) {
            if (add(relation.get(i))) {
                added = true;
            }
        }
        return added;
    }

    public ITuple get(int index) {
        return mTuples.get(index);
    }

    public int size() {
        return mTuples.size();
    }

    public boolean contains(ITuple tuple) {
        return mTuples.contains(tuple);
    }

    @Override
    public String toString() {
        return mTuples.toString();
    }

    private final List<ITuple> mTuples;
}


