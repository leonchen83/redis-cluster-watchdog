package com.moilioncircle.redis.cluster.watchdog.util.type;

import java.util.function.Function;

/**
 * Created by Baoyi Chen on 2017/7/20.
 */
public class Tuple5<T1, T2, T3, T4, T5> {
    private final T1 v1;
    private final T2 v2;
    private final T3 v3;
    private final T4 v4;
    private final T5 v5;

    public Tuple5(T1 v1, T2 v2, T3 v3, T4 v4, T5 v5) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
        this.v4 = v4;
        this.v5 = v5;
    }

    public Tuple5(Tuple5<T1, T2, T3, T4, T5> rhs) {
        this.v1 = rhs.getV1();
        this.v2 = rhs.getV2();
        this.v3 = rhs.getV3();
        this.v4 = rhs.getV4();
        this.v5 = rhs.getV5();
    }

    public T1 getV1() {
        return v1;
    }

    public T2 getV2() {
        return v2;
    }

    public T3 getV3() {
        return v3;
    }

    public T4 getV4() {
        return v4;
    }

    public T5 getV5() {
        return v5;
    }

    public <V1, V2, V3, V4, V5> Tuple5<V1, V2, V3, V4, V5> map(Function<Tuple5<T1, T2, T3, T4, T5>, Tuple5<V1, V2, V3, V4, V5>> function) {
        return function.apply(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple5<?, ?, ?, ?, ?> tuple5 = (Tuple5<?, ?, ?, ?, ?>) o;

        if (v1 != null ? !v1.equals(tuple5.v1) : tuple5.v1 != null) return false;
        if (v2 != null ? !v2.equals(tuple5.v2) : tuple5.v2 != null) return false;
        if (v3 != null ? !v3.equals(tuple5.v3) : tuple5.v3 != null) return false;
        if (v4 != null ? !v4.equals(tuple5.v4) : tuple5.v4 != null) return false;
        return v5 != null ? v5.equals(tuple5.v5) : tuple5.v5 == null;
    }

    @Override
    public int hashCode() {
        int result = v1 != null ? v1.hashCode() : 0;
        result = 31 * result + (v2 != null ? v2.hashCode() : 0);
        result = 31 * result + (v3 != null ? v3.hashCode() : 0);
        result = 31 * result + (v4 != null ? v4.hashCode() : 0);
        result = 31 * result + (v5 != null ? v5.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "[" + v1 + ", " + v2 + ", " + v3 + ", " + v4 + ", " + v5 + "]";
    }
}
