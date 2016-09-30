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

package org.voltcore.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltcore.network.NIOReadStream;
import org.voltcore.network.VoltProtocolHandler;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.HBBPool.SharedBBContainer;

public class HeartbeatMessage extends TransactionInfoBaseMessage {

    long m_lastSafeTxnId; // this is the largest txn acked by all partitions running the java for it

    HeartbeatMessage() {
        super();
    }

    public HeartbeatMessage(long initiatorHSId, long txnId, long lastSafeTxnId) {
        super(initiatorHSId, -1, txnId, txnId, true, false);
        m_lastSafeTxnId = lastSafeTxnId;
    }

    public long getLastSafeTxnId() {
        return m_lastSafeTxnId;
    }

    @Override
    public int getSerializedSize() {
        int msgsize = super.getSerializedSize() +  8;
        return msgsize;
    }

    @Override
    public void flattenToBuffer(ByteBuffer buf) throws IOException {
        buf.put(VoltMessageFactory.HEARTBEAT_ID);
        super.flattenToBuffer(buf);

        buf.putLong(m_lastSafeTxnId);

        assert(buf.limit() == buf.position());
        buf.limit(buf.position());
    }

    @Override
    public void initFromContainer(SharedBBContainer container) throws IOException {
        super.initFromContainer(container);
        ByteBuffer buf = container.b();
        m_lastSafeTxnId = buf.getLong();

        assert(buf.limit() == buf.position());
        container.discardNoLogging();
    }

    @Override
    public void initFromInputHandler(VoltProtocolHandler handler, NIOReadStream inputStream) throws IOException {
        initFromContainer(handler.getNextHBBMessageNoLogging(inputStream));
    }

    @Override
    public void discard() {}

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("HEARTBEAT (FROM ");
        sb.append(CoreUtils.hsIdToString(m_sourceHSId));
        sb.append(") FOR TXN ");
        sb.append(m_txnId);
        sb.append(" and LAST SAFE ");
        sb.append(m_lastSafeTxnId);

        return sb.toString();
    }
}
