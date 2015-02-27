package util;

/**
 * Ethan Petuchowski 2/27/15
 */
public class SongTuple {
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
}
