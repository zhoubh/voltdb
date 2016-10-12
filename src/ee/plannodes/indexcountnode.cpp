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

#include "indexcountnode.h"

#include "boost/foreach.hpp"

#include <sstream>

namespace voltdb {

IndexCountPlanNode::~IndexCountPlanNode() { }

PlanNodeType IndexCountPlanNode::getPlanNodeType() const { return PLAN_NODE_TYPE_INDEXCOUNT; }

std::string IndexCountPlanNode::debugInfo(const std::string &spacer) const
{
    std::ostringstream buffer;
    buffer << AbstractScanPlanNode::debugInfo(spacer)
           << spacer << "TargetIndexName[" << m_targetIndexName << "]\n"
           << spacer << "IndexLookupType["
           << indexLookupToString(m_lookupType) << "]\n"
           << spacer << "SearchKey Expressions:\n";
    BOOST_FOREACH(auto searchKey, m_searchKeyExpressions) {
        buffer << searchKey->debug(spacer);
    }

    buffer << spacer << "EndKey Expressions:\n";
    BOOST_FOREACH(auto endKey, m_endKeyExpressions) {
        buffer << endKey->debug(spacer);
    }

    buffer << spacer << "Skip Null Expression: ";
    if (m_skipNullPredicate != NULL) {
        buffer << "\n" << m_skipNullPredicate->debug(spacer);
    }
    else {
        buffer << "<NULL>\n";
    }

    return buffer.str();
}

void IndexCountPlanNode::loadFromJSONObject(PlannerDomValue obj)
{
    AbstractScanPlanNode::loadFromJSONObject(obj);
    assert(getPredicate() == NULL);

    std::string endTypeString = obj.valueForKey("END_TYPE").asStr();
    m_endType = stringToIndexLookup(endTypeString);

    std::string lookupTypeString = obj.valueForKey("LOOKUP_TYPE").asStr();
    m_lookupType = stringToIndexLookup(lookupTypeString);

    m_targetIndexName = obj.valueForKey("TARGET_INDEX_NAME").asStr();

    m_searchKeyExpressions.loadExpressionArrayFromJSONObject("SEARCHKEY_EXPRESSIONS", obj);
#ifndef NDEBUG
    BOOST_FOREACH(auto searchKey, m_searchKeyExpressions) {
        assert(searchKey);
    }
#endif

    m_endKeyExpressions.loadExpressionArrayFromJSONObject("ENDKEY_EXPRESSIONS", obj);
#ifndef NDEBUG
    BOOST_FOREACH(auto endKey, m_endkeyExpressions) {
        assert(endKey);
    }
#endif

    m_skipNullPredicate.reset(loadExpressionFromJSONObject("SKIP_NULL_PREDICATE", obj));
}

}// namespace voltdb
