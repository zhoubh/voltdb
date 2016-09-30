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

package org.voltcore.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.utils.HBBPool;
import org.voltcore.utils.HBBPool.SharedBBContainer;

public abstract class VoltProtocolHandler implements InputHandler {
    /** VoltProtocolPorts each have a unique id */
    private static AtomicLong m_globalConnectionCounter = new AtomicLong(0);

    /** The distinct exception class allows better logging of these unexpected errors. */
    class BadMessageLength extends IOException {
        private static final long serialVersionUID = 8547352379044459911L;
        public BadMessageLength(String string) {
            super(string);
        }
    }

    /** messages read by this connection */
    private int m_sequenceId;
    /** serial number of this VoltPort */
    private final long m_connectionId;
    private int m_nextLength;

    private static int MAX_MESSAGE_LENGTH = 52428800;

    public VoltProtocolHandler() {
        m_sequenceId = 0;
        m_connectionId = m_globalConnectionCounter.incrementAndGet();
    }

    public static long getNextConnectionId() {
        return m_globalConnectionCounter.incrementAndGet();
    }

    @Override
    public boolean nextMessageReady(final NIOReadStream inputStream) throws BadMessageLength {

        /*
         * Note that access to the read stream is not synchronized. In this application
         * the VoltPort will invoke this input handler to interact with the read stream guaranteeing
         * thread safety. That said the Connection interface does allow other parts of the application
         * access to the read stream.
         */

        if (m_nextLength == 0 && inputStream.dataAvailable() > (Integer.SIZE/8)) {
            m_nextLength = inputStream.getInt();
            if (m_nextLength < 1) {
                throw new BadMessageLength(
                        "Next message length is " + m_nextLength + " which is less than 1 and is nonsense");
            }
            if (m_nextLength > MAX_MESSAGE_LENGTH) {
                throw new BadMessageLength(
                        "Next message length is " + m_nextLength + " which is greater then the hard coded " +
                        "max of " + MAX_MESSAGE_LENGTH + ". Break up the work into smaller chunks (2 megabytes is reasonable) " +
                        "and send as multiple messages or stored procedure invocations");
            }
            assert m_nextLength > 0;
        }
        if (m_nextLength > 0 && inputStream.dataAvailable() >= m_nextLength) {
            return true;
        }
        return false;
    }

    public byte getNextMessageByte(final NIOReadStream inputStream) {
        m_nextLength--;
        return inputStream.getByte();
    }

    public short getNextMessageShort(final NIOReadStream inputStream) {
        m_nextLength -= 2;
        return inputStream.getShort();
    }

    public int getNextMessageInt(final NIOReadStream inputStream) {
        m_nextLength -= 4;
        return inputStream.getInt();
    }

    public long getNextMessageLong(final NIOReadStream inputStream) {
        m_nextLength -= 8;
        return inputStream.getLong();
    }

    public void getNextMessageByteArray(final NIOReadStream inputStream, byte[] dest, final int byteCount) {
        m_nextLength -= byteCount;
        inputStream.getCountedBytes(dest, byteCount);
    }

    public ByteBuffer getNextBBMessage(final NIOReadStream inputStream) {
        ByteBuffer result = ByteBuffer.allocate(m_nextLength);
        inputStream.getBytes(result.array());
        m_nextLength = 0;
        m_sequenceId++;
        return result;
    }

    public SharedBBContainer getNextHBBMessage(final NIOReadStream inputStream) {
        SharedBBContainer result = HBBPool.allocateHeapAndPool(m_nextLength);
        inputStream.getCountedBytes(result.b().array(), m_nextLength);
        m_nextLength = 0;
        m_sequenceId++;
        return result;
    }

    public SharedBBContainer getNextHBBMessageNoLogging(final NIOReadStream inputStream) {
        SharedBBContainer result = HBBPool.allocateHeapAndPoolNoLogging(m_nextLength);
        inputStream.getCountedBytes(result.b().array(), m_nextLength);
        m_nextLength = 0;
        m_sequenceId++;
        return result;
    }

    @Override
    public void started(Connection c) {
    }

    @Override
    public void starting(Connection c) {
    }

    @Override
    public void stopped(Connection c) {
    }

    @Override
    public void stopping(Connection c) {
    }

    @Override
    public long connectionId() {
        return m_connectionId;
    }

    public int sequenceId() {
        return m_sequenceId;
    }

    protected int getNextMessageLength() {
        return m_nextLength;
    }

}
