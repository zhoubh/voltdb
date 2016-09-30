package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SPIfromSerializedBuffer extends SPIfromSerialization {

    /*
     * This ByteBuffer is accessed from multiple threads concurrently.
     * Always duplicate it before reading
     */
    private ByteBuffer serializedParams = null;

    @Override
    public ByteBuffer GetUnsafeSerializedBBParams() {
        return serializedParams.duplicate();
    }

    public void setSerializedParams(ByteBuffer serializedParams) {
        assert(serializedParams.position() == 0);
        this.serializedParamSize = serializedParams.limit();
        this.serializedParams = serializedParams;
    }

    @Override
    public StoredProcedureInvocation getShallowCopy() {
        SPIfromSerializedBuffer copy = new SPIfromSerializedBuffer();
        commonShallowCopy(copy);
        copy.serializedParams = serializedParams.duplicate();
        copy.serializedParamSize = serializedParamSize;

        return copy;
    }

    public void initFromByteBuffer(ByteBuffer buf) throws IOException {
        genericInit(buf);
        // do not deserialize parameters in ClientInterface context
        serializedParams = buf.duplicate();
    }

    protected void initFromParameterSet(ParameterSet params) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(serializedParamSize);
        params.flattenToBuffer(buf);
        serializedParams = buf;
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
    public ByteBuffer getSerializedParams() {
        if (serializedParams != null) {
            return serializedParams.duplicate();
        }
        return null;
    }

    @Override
    public void implicitReference() {}

    @Override
    public void discard() {}
}
