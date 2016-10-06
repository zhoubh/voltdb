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


#ifndef HSTOREINDEXCOUNTEXECUTOR_H
#define HSTOREINDEXCOUNTEXECUTOR_H

#include "executors/abstractexecutor.h"

#include "boost/shared_array.hpp"

namespace voltdb {
class AbstractExpression;

class IndexCountExecutor : public AbstractExecutor {
public:
    IndexCountExecutor(VoltDBEngine* engine, AbstractPlanNode* abstractNode)
        : AbstractExecutor(engine, abstractNode)
        , m_searchKeyBackingStore(NULL)
        , m_endKeyBackingStore(NULL)
    { }
    ~IndexCountExecutor();

private:
    bool p_init(AbstractPlanNode*, TempTableLimits*);
    bool p_execute(const NValueArray &params);

    int m_numOfSearchKeys;
    IndexLookupType m_lookupType;
    boost::shared_array<AbstractExpression*> m_searchKeys;
    char* m_searchKeyBackingStore;

    int m_numOfEndKeys;
    IndexLookupType m_endType;
    boost::shared_array<AbstractExpression*> m_endKeys;
    char* m_endKeyBackingStore;
};

} // namespace voltdb

#endif // HSTOREINDEXCOUNTEXECUTOR_H
