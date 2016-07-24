package com.deduper.wrapper;

import java.io.Serializable;

/**
 * authored by @rahulanishetty on 7/24/16.
 */
public class StringWrapper implements Comparable<StringWrapper>, Serializable {
    private String actual;

    public StringWrapper(String actual) {
        this.actual = actual;
    }


    public String getActual() {
        return actual;
    }

    public void setActual(String actual) {
        this.actual = actual;
    }

    @Override
    public int compareTo(StringWrapper o) {
        return actual.compareTo(o.actual);
    }

    @Override
    public String toString() {
        return "StringWrapper{" +
                "actual='" + actual + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StringWrapper that = (StringWrapper) o;

        return actual.equals(that.actual);

    }

    @Override
    public int hashCode() {
        return actual.hashCode();
    }
}
