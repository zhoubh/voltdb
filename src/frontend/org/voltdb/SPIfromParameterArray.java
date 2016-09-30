package org.voltdb;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

import org.voltcore.utils.HBBPool;
import org.voltcore.utils.HBBPool.SharedBBContainer;

public class SPIfromParameterArray extends StoredProcedureInvocation {

    FutureTask<ParameterSet> paramSet;
    Object[] rawParams;


    public void setSafeParams(final Object... parameters) {
        // convert the params to the expected types
        rawParams = parameters;
        paramSet = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() {
                ParameterSet params = ParameterSet.fromArrayWithCopy(rawParams);
                serializedParamSize = params.getSerializedSize();
                return params;
            }
        });
    }

    public void setParams(final Object... parameters) {
        // convert the params to the expected types
        rawParams = parameters;
        paramSet = new FutureTask<ParameterSet>(new Callable<ParameterSet>() {
            @Override
            public ParameterSet call() {
                ParameterSet params = ParameterSet.fromArrayNoCopy(rawParams);
                serializedParamSize = params.getSerializedSize();
                return params;
            }
        });
    }

    /**
     * Serialize and then deserialize an invocation so that it has serializedParams set for command logging if the
     * invocation is sent to a local site.
     * @return The round-tripped version of the invocation
     * @throws IOException
     */
    @Override
    public SPIfromSerialization roundTripForCL() throws IOException {
        // Ensure that the paramset has been built so we know the serializedSize
        getParams();
        SharedBBContainer bbContainer = HBBPool.allocateHeapAndPool(getSerializedSize());
        flattenToBuffer(bbContainer.b());
        bbContainer.b().flip();

        SPIfromSerializedContainer rti = new SPIfromSerializedContainer();
        rti.initFromContainer(bbContainer);
        bbContainer.discard();
        return rti;
    }

    @Override
    public StoredProcedureInvocation getShallowCopy() {
        SPIfromParameterArray copy = new SPIfromParameterArray();
        commonShallowCopy(copy);
        copy.rawParams = rawParams;
        copy.paramSet = paramSet;
        copy.serializedParamSize = serializedParamSize;

        return copy;
    }


    @Override
    Object getParameterAtIndex(int partitionIndex) {
        try {
            return rawParams[partitionIndex];
        }
        catch (Exception ex) {
            throw new RuntimeException("Invalid partitionIndex: " + partitionIndex, ex);
        }
    }

    @Override
    public ParameterSet getParams() {
        paramSet.run();
        try {
            return paramSet.get();
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Interrupted while deserializing a parameter set", false, e);
        } catch (ExecutionException e) {
            // Don't rethrow Errors as RuntimeExceptions because we will eat their
            // delicious goodness later
            if (e.getCause() != null && e.getCause() instanceof Error) {
                throw (Error)e.getCause();
            }
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public final void flattenToBuffer(ByteBuffer buf) throws IOException {
        // for self-check assertion
        int startPosition = buf.position();

        commonFlattenToBuffer(buf);
        assert(rawParams != null);
        try {
            getParams().flattenToBuffer(buf);
        }
        catch (BufferOverflowException e) {
            hostLog.info("SP \"" + procName + "\" has thrown BufferOverflowException");
            hostLog.info(toString());
            throw e;
        }

        int len = buf.position() - startPosition;
        assert(len == getSerializedSize());
    }

    @Override
    public void implicitReference() {}

    @Override
    public void discard() {}
}
