package com.moilioncircle.redis.cluster.watchdog.util.type;

import java.util.function.Function;

/**
 * @author Leon Chen
 * @since 1.0.0
 */
public class Tuple4<T1, T2, T3, T4> {
    private final T1 v1;
    private final T2 v2;
    private final T3 v3;
    private final T4 v4;

    public Tuple4(T1 v1, T2 v2, T3 v3, T4 v4) {
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
        this.v4 = v4;
    }

    public Tuple4(Tuple4<T1, T2, T3, T4> rhs) {
        this.v1 = rhs.getV1();
        this.v2 = rhs.getV2();
        this.v3 = rhs.getV3();
        this.v4 = rhs.getV4();
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

    public <V1, V2, V3, V4> Tuple4<V1, V2, V3, V4> map(Function<Tuple4<T1, T2, T3, T4>, Tuple4<V1, V2, V3, V4>> function) {
        return function.apply(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Tuple4<?, ?, ?, ?> tuple4 = (Tuple4<?, ?, ?, ?>) o;

        if (v1 != null ? !v1.equals(tuple4.v1) : tuple4.v1 != null) return false;
        if (v2 != null ? !v2.equals(tuple4.v2) : tuple4.v2 != null) return false;
        if (v3 != null ? !v3.equals(tuple4.v3) : tuple4.v3 != null) return false;
        return v4 != null ? v4.equals(tuple4.v4) : tuple4.v4 == null;
    }

    @Override
    public int hashCode() {
        int result = v1 != null ? v1.hashCode() : 0;
        result = 31 * result + (v2 != null ? v2.hashCode() : 0);
        result = 31 * result + (v3 != null ? v3.hashCode() : 0);
        result = 31 * result + (v4 != null ? v4.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "[" + v1 + ", " + v2 + ", " + v3 + ", " + v4 + "]";
    }
}
