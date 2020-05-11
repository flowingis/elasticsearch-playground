package it.flowing.complex.service;

import com.google.common.base.Preconditions;

public class Searcher {

    public void search(String indexName) {
        Preconditions.checkNotNull(indexName);
        Preconditions.checkArgument(!indexName.isEmpty());
    }

}
