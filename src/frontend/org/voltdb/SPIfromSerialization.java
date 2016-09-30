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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.client.BatchTimeoutOverrideType;
import org.voltdb.client.ProcedureInvocationExtensions;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.utils.SerializationHelper;

public abstract class SPIfromSerialization extends StoredProcedureInvocation {

    public abstract ByteBuffer GetUnsafeSerializedBBParams();

    /**
     * Serialize and then deserialize an invocation so that it has serializedParams set for command logging if the
     * invocation is sent to a local site.
     * @return The round-tripped version of the invocation
     * @throws IOException
     */
    @Override
    public SPIfromSerialization roundTripForCL() throws IOException
    {
        return this;
    }

    @Override
    Object getParameterAtIndex(int partitionIndex) {
        try {
            ByteBuffer copy = GetUnsafeSerializedBBParams();
            Object rslt = ParameterSet.getParameterAtIndex(partitionIndex, copy);
            return rslt;
        }
        catch (Exception ex) {
            throw new RuntimeException("Invalid partitionIndex: " + partitionIndex, ex);
        }
    }

    @Override
    public final void flattenToBuffer(ByteBuffer buf) throws IOException
    {
        // for self-check assertion
        int startPosition = buf.position();

        commonFlattenToBuffer(buf);
        ByteBuffer genericSerializedParms = GetUnsafeSerializedBBParams();
        assert(genericSerializedParms != null);
        // if position can be non-zero, then the dup/rewind logic below
        // would be wrong?
        assert(genericSerializedParms.position() == 0);
        buf.put(genericSerializedParms.array(),
                genericSerializedParms.position() + genericSerializedParms.arrayOffset(),
                genericSerializedParms.remaining());

        int len = buf.position() - startPosition;
        assert(len == getSerializedSize());
    }

    public void genericInit(ByteBuffer buf) throws IOException
    {
        byte version = buf.get();// version number also embeds the type
        // this will throw for an unexpected type, like the DRv1 type, for example
        type = ProcedureInvocationType.typeFromByte(version);
        m_procNameBytes = null;

        switch (type) {
            case ORIGINAL:
                initOriginalFromBuffer(buf);
                break;
            case VERSION1:
                initVersion1FromBuffer(buf);
                break;
            case VERSION2:
                initVersion2FromBuffer(buf);
                break;
        }

        // ensure extension count is correct
        setBatchTimeout(batchTimeout);
    }

    private void initOriginalFromBuffer(ByteBuffer buf) throws IOException {
        byte[] procNameBytes = SerializationHelper.getVarbinary(buf);
        if (procNameBytes == null) {
            throw new IOException("Procedure name cannot be null in invocation deserialization.");
        }
        if (procNameBytes.length == 0) {
            throw new IOException("Procedure name cannot be length zero in invocation deserialization.");
        }
        setProcName(procNameBytes);
        clientHandle = buf.getLong();
    }

    private void initVersion1FromBuffer(ByteBuffer buf) throws IOException {
        BatchTimeoutOverrideType batchTimeoutType = BatchTimeoutOverrideType.typeFromByte(buf.get());
        if (batchTimeoutType == BatchTimeoutOverrideType.NO_OVERRIDE_FOR_BATCH_TIMEOUT) {
            batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
        } else {
            batchTimeout = buf.getInt();
            // Client side have already checked the batchTimeout value, but,
            // on server side, we should check non-negative batchTimeout value again
            // in case of someone is using a non-standard client.
            if (batchTimeout < 0) {
                throw new IllegalArgumentException("Timeout value can't be negative." );
            }
        }

        // the rest of the format is the same as the original
        initOriginalFromBuffer(buf);
    }

    private void initVersion2FromBuffer(ByteBuffer buf) throws IOException {
        byte[] procNameBytes = SerializationHelper.getVarbinary(buf);
        if (procNameBytes == null) {
            throw new IOException("Procedure name cannot be null in invocation deserialization.");
        }
        if (procNameBytes.length == 0) {
            throw new IOException("Procedure name cannot be length zero in invocation deserialization.");
        }
        setProcName(procNameBytes);

        clientHandle = buf.getLong();

        // default values for extensions
        batchTimeout = BatchTimeoutOverrideType.NO_TIMEOUT;
        // read any invocation extensions and skip any we don't recognize
        int extensionCount = buf.get();

        // this limits things a bit, but feels worth it in terms of being a possible way
        // to stumble on a bug
        if (extensionCount < 0) {
            throw new IOException("SPI extension count was < 0: possible corrupt network data.");
        }
        if (extensionCount > 30) {
            throw new IOException("SPI extension count was > 30: possible corrupt network data.");
        }

        for (int i = 0; i < extensionCount; ++i) {
            final byte type = ProcedureInvocationExtensions.readNextType(buf);
            switch (type) {
            case ProcedureInvocationExtensions.BATCH_TIMEOUT:
                batchTimeout = ProcedureInvocationExtensions.readBatchTimeout(buf);
                break;
            default:
                ProcedureInvocationExtensions.skipUnknownExtension(buf);
                break;
            }
        }
    }

    abstract protected void initFromParameterSet(ParameterSet params) throws IOException;

    public void setParams(final Object... parameters) throws IOException {
        // convert the params to the correct flattened version
        ParameterSet params = ParameterSet.fromArrayNoCopy(parameters);
        serializedParamSize = params.getSerializedSize();
        initFromParameterSet(params);
    }

    @Override
    public ParameterSet getParams() {
        try {
            return ParameterSet.fromByteBuffer(GetUnsafeSerializedBBParams());
        }
        catch (IOException e) {
            // Don't rethrow Errors as RuntimeExceptions because we will eat their
            // delicious goodness later
            if (e.getCause() != null && e.getCause() instanceof Error) {
                throw (Error)e.getCause();
            }
            throw new RuntimeException(e);
        }
    }
}
