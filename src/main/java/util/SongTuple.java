package util;

/**
 * Ethan Petuchowski 2/27/15
 */
public class SongTuple implements Comparable<SongTuple> {
    String name;
    String url;

    public SongTuple(String name, String url) {
        this.name = name;
        this.url = url;
    }

    /**
     * two songs are equal iff their names are the same
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SongTuple)) return false;
        SongTuple tuple = (SongTuple) o;
        if (!name.equals(tuple.name)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @return a negative integer, zero, or a positive integer as this object is less than, equal
     * to, or greater than the specified object.
     */
    @Override public int compareTo(SongTuple o) {
        return name.compareTo(o.name);
    }

    public String toLogString() {
        return name+"\n"+url;
    }
}
