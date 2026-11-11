package com.jobscheduler.job;

import java.util.Set;
import java.util.TreeSet;

public class UnknownJobTypeException extends RuntimeException {

    public UnknownJobTypeException(String type, Set<String> known) {
        super("Unknown job type '" + type + "'. Known types: " + new TreeSet<>(known));
    }
}
