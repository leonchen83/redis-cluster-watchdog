package com.moilioncircle.redis.cluster.watchdog.util.type;

import java.util.function.Function;

/**
 * Created by Baoyi Chen on 2017/7/20.
 */
public class Tuple2<T1, T2> {
    private final T1 v1;
    private final T2 v2;

    public Tuple2(T1 v1, T2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public Tuple2(Tuple2<T1, T2> rhs) {
        this.v1 = rhs.getV1();
        this.v2 = rhs.getV2();
    }

    public T1 getV1() {
        return v1;
    }

    public T2 getV2() {
        return v2;
    }

    public <V1, V2> Tuple2<V1, V2> map(Function<Tuple2<T1, T2>, Tuple2<V1, V2>> function) {
        return function.apply(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) o;

        if (v1 != null ? !v1.equals(tuple2.v1) : tuple2.v1 != null) return false;
        return v2 != null ? v2.equals(tuple2.v2) : tuple2.v2 == null;
    }

    @Override
    public int hashCode() {
        int result = v1 != null ? v1.hashCode() : 0;
        result = 31 * result + (v2 != null ? v2.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "[" + v1 + ", " + v2 + "]";
    }
}
