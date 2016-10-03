/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltcore.utils;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.voltcore.logging.VoltLogger;
/**
 * A pool of {@link java.nio.ByteBuffer ByteBuffers} that are
 * allocated with
 * {@link java.nio.ByteBuffer#allocate(int) * ByteBuffer.allocate}.
 * Buffers are stored in Arenas that are powers of 2. The smallest arena is 16 bytes.
 */
public final class HBBPool {
    private static final VoltLogger TRACE = new VoltLogger("HBBPOOL");
    private static final VoltLogger HOST = new VoltLogger("HBBPOOL");

//    static {
//        TRACE.setLevel(Level.TRACE);
//    }

    /**
     * Number of bytes allocated globally by DBBPools
     */
    private static AtomicLong bytesAllocatedGlobally = new AtomicLong(0);
    static long getBytesAllocatedGlobally()
    {
        return bytesAllocatedGlobally.get();
    }
    private static final NonBlockingHashMap<Integer, ConcurrentLinkedQueue<BufferWrapper>> m_pooledBuffers =
            new NonBlockingHashMap<Integer, ConcurrentLinkedQueue<BufferWrapper>>();
    /**
     * Find the closest power of 2 that's larger than or equal to the requested capacity.
     * @return 0 if the requested capacity is 0, the requested capacity itself if the
     * next power of 2 overflows, or the next power of 2.
     */
    static int roundToClosestPowerOf2(int capacity) {
        if (capacity == 0) return 0;
        else if (capacity == 1) return 2;
        final int result = Integer.highestOneBit(capacity - 1) << 1;
        return result < 0 ? capacity : result;
    }
    /**
     * Allocate a HeapBuffer from a global lock free pool. The allocated buffer may
     * have a capacity larger than the requested size. The limit will be set to the requested
     * size.
     */
    private static SharedBBContainer internalAllocateHeapAndPool(final Integer capacity, boolean logging) {
        final int bucket = roundToClosestPowerOf2(capacity);
        ConcurrentLinkedQueue<BufferWrapper> pooledBuffers = m_pooledBuffers.get(bucket);
        if (pooledBuffers == null) {
            pooledBuffers = new ConcurrentLinkedQueue<BufferWrapper>();
            if (m_pooledBuffers.putIfAbsent(bucket, pooledBuffers) != null) {
                pooledBuffers = m_pooledBuffers.get(bucket);
            }
        }
        BufferWrapper container = pooledBuffers.poll();
        SharedBBContainer result;
        if (container == null) {
            result = allocateHeap(bucket, logging);
        }
        else {
            container.m_wrapperRefCount.set(1);
            result = new SharedBBContainer(container, false, logging);
        }
        result.b().limit(capacity);
        return result;
    }
    public static SharedBBContainer allocateHeapAndPool(final Integer capacity) {
        return internalAllocateHeapAndPool(capacity, true);
    }
    public static SharedBBContainer allocateHeapAndPoolNoLogging(final Integer capacity) {
        return internalAllocateHeapAndPool(capacity, false);
    }
    //In OOM conditions try clearing the pool
    private static void clear() {
        long startingBytes = bytesAllocatedGlobally.get();
        for (Entry<Integer, ConcurrentLinkedQueue<BufferWrapper>> poolEntry : m_pooledBuffers.entrySet()) {
            ConcurrentLinkedQueue<BufferWrapper> pool = poolEntry.getValue();
            int capacity = poolEntry.getKey();
            while (pool.poll() != null) {
                bytesAllocatedGlobally.getAndAdd(-capacity);
            }
        }
        new VoltLogger("HOST").warn(
                "Attempted to resolve DirectByteBuffer OOM by freeing pooled buffers. " +
                "Starting bytes was " + startingBytes + " after clearing " +
                 bytesAllocatedGlobally.get() + " change " + (startingBytes - bytesAllocatedGlobally.get()));
    }
    private static void logAllocation(SharedBBContainer container, int count) {
        if (TRACE.isTraceEnabled()) {
            BufferWrapper bufWrapper = container.m_bufWrapper;
            int wrapperCount = bufWrapper.m_wrapperRefCount.get();
            String message = (count==1 && wrapperCount == 1?"Allocated":("Duplicated (" +
                    wrapperCount + "/" + count + ")")) +
                    " BufferWrapper " + Integer.toHexString(bufWrapper.hashCode()) +
                    " Container " + Integer.toHexString(container.hashCode()) +
                    " with HBB capacity " + bufWrapper.m_buffer.length +
                    " total allocated " + bytesAllocatedGlobally.get() +
                    " from " + CoreUtils.throwableToString(new Throwable());
            TRACE.trace(message);
        }
    }
    private static void logDeallocation(SharedBBContainer container, int count) {
        if (TRACE.isTraceEnabled()) {
            BufferWrapper bufWrapper = container.m_bufWrapper;
            int wrapperCount = bufWrapper.m_wrapperRefCount.get();
            String message = (wrapperCount==0?"Deallocated":("Dereferenced (" +
                    wrapperCount + "/" + count + ")")) +
                    " BufferWrapper " + Integer.toHexString(bufWrapper.hashCode()) +
                    " Container " + Integer.toHexString(container.hashCode()) +
                    " with HBB capacity " + bufWrapper.m_buffer.length +
                    " total allocated " + bytesAllocatedGlobally.get() +
                    " from " + CoreUtils.throwableToString(new Throwable());
            TRACE.trace(message);
        }
    }
    /*
     * The only reason to not retrieve the address is that network code shared
     * with the java client shouldn't have a dependency on the native library
     */
    private static SharedBBContainer allocateHeap(final int capacity, final boolean logging) {
        SharedBBContainer retval = null;
        try {
            retval = new SharedBBContainer(capacity, false, logging);
        } catch (OutOfMemoryError e) {
            if (e.getMessage().contains("Direct buffer memory")) {
                clear();
                retval = new SharedBBContainer(capacity, false, logging);
            } else {
                throw new Error(e);
            }
        }
        bytesAllocatedGlobally.getAndAdd(capacity);
        return retval;
    }

    private static class BufferWrapper {
        final private AtomicInteger m_wrapperRefCount = new AtomicInteger(1);
        final private byte[] m_buffer;

        private BufferWrapper(final byte[] buff) {
            m_buffer = buff;
        }
    }

    public static class SharedBBContainer {
        final private AtomicInteger m_containerRefCount = new AtomicInteger(1);
        private final BufferWrapper m_bufWrapper;
        private final ByteBuffer b;
        private volatile Throwable m_freeThrowable;
        private Throwable m_allocationThrowable;

        private SharedBBContainer(final int capacity, final boolean readOnly, final boolean logging) {
            final byte[] buffer = new byte[capacity];
            m_bufWrapper = new BufferWrapper(buffer);
            if (readOnly) {
                b = ByteBuffer.wrap(buffer).asReadOnlyBuffer();
            }
            else {
                b = ByteBuffer.wrap(buffer);
            }
            if (logging)
                logAllocation(this, 1);
            trackAllocation();
        }

        private SharedBBContainer(BufferWrapper singleContainer, final boolean readOnly, final boolean logging) {
            m_bufWrapper = singleContainer;
            if (readOnly) {
                b = ByteBuffer.wrap(m_bufWrapper.m_buffer).asReadOnlyBuffer();
            }
            else {
                b = ByteBuffer.wrap(m_bufWrapper.m_buffer);
            }
            if (logging)
                logAllocation(this, 1);
            trackAllocation();
        }

        private SharedBBContainer(final SharedBBContainer container, final boolean slice, final boolean readOnly) {
            container.checkUseAfterFree();
            m_bufWrapper = container.m_bufWrapper;
            m_bufWrapper.m_wrapperRefCount.incrementAndGet();
            if (slice) {
                if (readOnly) {
                    b = container.b.slice().asReadOnlyBuffer();
                }
                else {
                    b = container.b.slice();
                }
            }
            else {
                if (readOnly) {
                    b = container.b.duplicate().asReadOnlyBuffer();
                }
                else {
                    b = container.b.duplicate();
                }
            }
            logAllocation(this, 1);
        }

        // Use when the same object has multiple references
        public void implicitReference() {
            checkUseAfterFree();
            int count = m_containerRefCount.incrementAndGet();
            logAllocation(this, count);
            trackAllocation();
        }

        private void internalDiscard(final boolean logging) {
            int count = m_containerRefCount.decrementAndGet();
            if (count == 0) {
                checkDoubleFree();
                if (m_bufWrapper.m_wrapperRefCount.decrementAndGet() == 0) {
                    if (logging) {
                        logDeallocation(this, count);
                    }
                    try {
                        int capacity = m_bufWrapper.m_buffer.length;
                        m_pooledBuffers.get(capacity).offer(m_bufWrapper);
                    } catch (Throwable e) {
                        crash("Failed to deallocate shared byte buffer", false, e);
                    }
                }
                else {
                    if (logging)
                        logDeallocation(this, count);
                }
            }
            else {
                checkUseAfterFree();
                if (logging)
                    logDeallocation(this, count);
            }
        }

        public void discard() {
            internalDiscard(true);
        }

        public void discardNoLogging() {
            internalDiscard(false);
        }

        public ByteBuffer b() {
            checkUseAfterFree();
            return b;
        }

        public SharedBBContainer slice() {
            return new SharedBBContainer(this, true, false);
        }

        public SharedBBContainer sliceReadOnly() {
            return new SharedBBContainer(this, true, true);
        }

        public SharedBBContainer duplicate() {
            return new SharedBBContainer(this, false, false);
        }

        public SharedBBContainer duplicateReadOnly() {
            return new SharedBBContainer(this, false, true);
        }

        private void trackAllocation() {
            m_allocationThrowable = new Throwable("\"" + Thread.currentThread().getName() + "\" at " + System.currentTimeMillis());
        }

        final protected void checkUseAfterFree() {
            if (m_freeThrowable != null) {
                System.err.println("Use of BufferWrapper " + Integer.toHexString(m_bufWrapper.hashCode()) +
                        " Container " + Integer.toHexString(hashCode()) +
                        " with HBB capacity " + m_bufWrapper.m_buffer.length + " after free in HBBPool");
                System.err.println("Free was by:");
                m_freeThrowable.printStackTrace();
                System.err.println("Use was by:");
                Throwable t = new Throwable("\"" + Thread.currentThread().getName() + "\" at " + System.currentTimeMillis());
                t.printStackTrace();
                HOST.fatal("Use after free in HBBPool");
                HOST.fatal("Free was by:", m_freeThrowable);
                HOST.fatal("Use was by:", t);
                System.exit(-1);
            }
        }

        final protected void checkDoubleFree() {
            synchronized (this) {
                if (m_freeThrowable != null) {
                    System.err.println("Double free of BufferWrapper " + Integer.toHexString(m_bufWrapper.hashCode()) +
                            " Container " + Integer.toHexString(hashCode()) +
                            " with HBB capacity " + m_bufWrapper.m_buffer.length + " in HBBPool");
                    System.err.println("Original free was by:");
                    m_freeThrowable.printStackTrace();
                    System.err.println("Current free was by:");
                    Throwable t = new Throwable("\"" + Thread.currentThread().getName() + "\" at " + System.currentTimeMillis());
                    t.printStackTrace();
                    HOST.fatal("Double free in HBBPool");
                    HOST.fatal("Original free was by:", m_freeThrowable);
                    HOST.fatal("Current free was by:", t);
                    System.exit(-1);
                }
                m_freeThrowable = new Throwable("\"" + Thread.currentThread().getName() + "\" at " + System.currentTimeMillis());
            }
        }

        @Override
        public void finalize() {
            if (m_freeThrowable == null) {
                String errMsg = "BufferWrapper " + Integer.toHexString(m_bufWrapper.hashCode()) +
                        " Container " + Integer.toHexString(hashCode()) + " was never discarded (" +
                        m_bufWrapper.m_wrapperRefCount.get() + ") allocated by:";
                System.err.println(errMsg);
                m_allocationThrowable.printStackTrace();
                HOST.fatal(errMsg, m_allocationThrowable);
                System.exit(-1);
            }
        }
    }

    private static void crash(String msg, boolean stackTrace, Throwable e) {
        // The client code doesn't want to link to the VoltDB class, so this hack was born.
        // It should be temporary as the goal is to remove client code dependency on
        // HBBPool in the medium term.
        try {
            Class<?> vdbClz = Class.forName("org.voltdb.VoltDB");
            Method m = vdbClz.getMethod("crashLocalVoltDB", String.class, boolean.class, Throwable.class);
            m.invoke(null, msg, stackTrace, e);
        } catch (Exception ignored) {
            HOST.fatal(msg, ignored);
            System.err.println(msg);
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
