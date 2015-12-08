package net.michaelho.db.mongo;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by mike on 12/11/14.
 */
public class AppendableHashSet<E> extends HashSet<E> {

    @SafeVarargs
    public AppendableHashSet (E... objects) {
        super();
        Collections.addAll(this, objects);
    }

    public AppendableHashSet<E> append (E object) {
        this.add(object);
        return this;
    }
}
