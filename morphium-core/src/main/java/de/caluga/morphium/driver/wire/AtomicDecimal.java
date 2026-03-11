package de.caluga.morphium.driver.wire;

import java.util.concurrent.atomic.AtomicInteger;

public final class AtomicDecimal extends Number {
    //Zähler
    private AtomicInteger numerator;
    //Nenner
    private AtomicInteger denominator;

    public AtomicDecimal(int numerator) {
        this.numerator = new AtomicInteger(numerator);
        this.denominator = new AtomicInteger(1);
    }

    public AtomicDecimal(int numerator, int denominator) {
        this.numerator = new AtomicInteger(numerator);
        this.denominator = new AtomicInteger(denominator);
    }

    public double get() {
        return doubleValue();
    }

    public AtomicDecimal setDenominator(int den) {
        denominator.set(den);
        return this;
    }

    public AtomicDecimal setNumerator(int num) {
        numerator.set(num);
        return this;
    }

    public double incrementAndGet() {
        numerator.incrementAndGet();
        return doubleValue();
    }

    public double decrementAndGet() {
        numerator.decrementAndGet();
        return doubleValue();
    }

    @Override
    public int intValue() {
        return (int) (numerator.get() / denominator.get());
    }

    @Override
    public long longValue() {
        return (int) (numerator.get() / denominator.get());

    }

    @Override
    public float floatValue() {
        return (float) ((float) numerator.get() / (float) denominator.get());
    }

    @Override
    public double doubleValue() {
        return (double) ((double) numerator.get() / (double) denominator.get());

    }
}
