package it.flowing.complex.model;

public enum SearchType {
    MATCH_ALL_QUERY,
    TERM_QUERY,
    TERMS_QUERY,
    EXISTS_QUERY,
    FUZZ_QUERY,
    RANGE_QUERY,
    BOOL_QUERY,
    NESTED_QUERY
}
