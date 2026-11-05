package org.prats.utilities;

public class CircularBuffer<T> {

    private final Object[] buffer;
    private int nextIndex;
    private int size;
    private int offloadThreshold;

    public CircularBuffer(
            int capacity,
            int offloadThreshold)
    {
        nextIndex                   = 0;
        size                        = 0;
        this.offloadThreshold       = offloadThreshold;
        buffer                      = new Object[capacity];
    }

    public void add(T element) {
        buffer[nextIndex] = element;

        nextIndex = (nextIndex + 1) % buffer.length;

        if (size < buffer.length) {
            size++;
        }
    }

    private synchronized void offloadBuffer() {

    }

    private void logAndOffloadBuffer() {

    }

    private void clearBuffer() {
        for (int i = 0; i < size; ++i) {
            buffer[i] = 0;
        }
        nextIndex = 0;
    }

    public int size() {
        return size;
    }

    public int capacity() {
        return buffer.length;
    }
}
