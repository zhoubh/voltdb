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

package org.voltdb.client;

/**
 * A utility callback that can be instantiated for asynchronous invocations where the result including success/failure
 * is going to be ignored.
 */
public final class NullCallback implements ProcedureCallback {
    private static final NullCallback INSTANCE = new NullCallback();

    // NullCallback is stateless -- avoid short-lived memory allocation
    // by calling instance() to reuse the long-lived instance
    // -- the only one ever required.
    // TODO: transition to a deprecated public constructor
    // then to a private constructor to force use of instance().
    public NullCallback() { };

    public static NullCallback instance() {
        return INSTANCE;
    }

    @Override
    public void clientCallback(ClientResponse clientResponse) {
    }

}
