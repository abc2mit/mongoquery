package net.michaelho.db.mongo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by mike on 12/11/14.
 */
public class AppendableArrayList<E> extends ArrayList<E> {

    @SafeVarargs
    public AppendableArrayList (E... objects) {
        super();
        Collections.addAll(this, objects);
    }

    public AppendableArrayList<E> append (E object) {
        this.add(object);
        return this;
    }
}
