package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.utils.HBBPool;
import org.voltcore.utils.HBBPool.SharedBBContainer;

public class SPIfromSerializedContainer extends SPIfromSerialization {

    /*
     * This ByteBuffer is accessed from multiple threads concurrently.
     * Always duplicate it before reading
     */
    private SharedBBContainer serializedParams = null;

    @Override
    public ByteBuffer GetUnsafeSerializedBBParams() {
        // This will not bump the refcount of the container so the container could be reused
        // while holding this copy of the bytebuffer if we are not careful
        return serializedParams.b().duplicate();
    }

    public void setSerializedParams(SharedBBContainer serializedParams) {
        assert(serializedParams.b().position() == 0);
        this.serializedParamSize = serializedParams.b().limit();
        this.serializedParams = serializedParams;
    }

    protected void initFromParameterSet(ParameterSet params) throws IOException {
        SharedBBContainer newSerialization = HBBPool.allocateHeapAndPool(serializedParamSize);
        params.flattenToBuffer(newSerialization.b());
        newSerialization.b().flip();
        discard();
        setSerializedParams(newSerialization);
    }

    @Override
    public StoredProcedureInvocation getShallowCopy()
    {
        SPIfromSerializedContainer copy = new SPIfromSerializedContainer();
        commonShallowCopy(copy);
        copy.serializedParams = serializedParams.duplicate();
        copy.serializedParamSize = serializedParamSize;

        return copy;
    }

    public void initFromContainer(SharedBBContainer container) throws IOException
    {
        ByteBuffer buf = container.b();
        genericInit(buf);
        // do not deserialize parameters in ClientInterface context
        setSerializedParams(container.slice());
    }

    /*
     * Store a copy of the parameters to the procedure in serialized form.
     * In a cluster there is no reason to throw away the serialized bytes
     * because it will be forwarded in most cases and there is no need to repeat the work.
     * Command logging also takes advantage of this to avoid reserializing the parameters.
     * In some cases the params will never have been serialized (null) because
     * the SPI is generated internally. A duplicate view of the buffer is returned
     * to make access thread safe. Can't return a read only view because ByteBuffer.array()
     * is invoked by the command log.
     */
    public SharedBBContainer getSerializedParams() {
        if (serializedParams != null) {
            return serializedParams.duplicate();
        }
        return null;
    }

    @Override
    public void implicitReference() {
        serializedParams.implicitReference();
    }

    @Override
    public void discard() {
        if (serializedParams != null) {
            serializedParams.discard();
        }
    }
}
