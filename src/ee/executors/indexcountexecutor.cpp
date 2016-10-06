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

#include "indexcountexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/ValueFactory.hpp"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"
#include "plannodes/indexcountnode.h"
#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

static long countNulls(TableIndex * tableIndex, AbstractExpression * countNULLExpr,
        IndexCursor& indexCursor) {
    if (countNULLExpr == NULL) {
        return 0;
    }
    long numNULLs = 0;
    TableTuple tuple;
    while ( ! (tuple = tableIndex->nextValue(indexCursor)).isNullTuple()) {
         if ( ! countNULLExpr->eval(&tuple, NULL).isTrue()) {
             break;
         }
        numNULLs++;
    }
    return numNULLs;
}


bool IndexCountExecutor::p_init(AbstractPlanNode *, TempTableLimits* limits) {
    VOLT_DEBUG("init IndexCount Executor");
    // Create output table based on output schema from the plan
    setTempOutputTable(limits);
    assert(m_tmpOutputTable);
    assert(m_tmpOutputTable->columnCount() == 1);

    IndexCountPlanNode* node = dynamic_cast<IndexCountPlanNode*>(m_abstractNode);
    assert(node);

    // The target table should be a persistent table
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(node->getTargetTable());
    assert(targetTable);

    // Grab the Index from the table. Throw an error if the index is missing.
    TableIndex *tableIndex = targetTable->index(node->getTargetIndexName());
    assert (tableIndex != NULL);

    // This index should have a true countable flag
    assert(tableIndex->isCountableIndex());

    // Miscellanous Information
    m_lookupType = INDEX_LOOKUP_TYPE_INVALID;

    const std::vector<AbstractExpression*>& searchKeyExpressions = node->getSearchKeyExpressions();
    m_numOfSearchKeys = (int)searchKeyExpressions.size();
    if (m_numOfSearchKeys != 0) {
        m_lookupType = node->getLookupType();
        m_searchKeyBackingStore = new char[tableIndex->getKeySchema()->tupleLength()];

        AbstractExpression** searchKeyExprs = new AbstractExpression*[m_numOfSearchKeys];
        for (int ctr = 0; ctr < m_numOfSearchKeys; ++ctr) {
            searchKeyExprs[ctr] = searchKeyExpressions[ctr];
        }
        // The executor owns the expression array -- the plan node owns the expressions.
        m_searchKeys = boost::shared_array<AbstractExpression*>(searchKeyExprs);
    }

    const std::vector<AbstractExpression*>& endKeyExpressions = node->getEndKeyExpressions();
    m_numOfEndKeys = (int)endKeyExpressions.size();
    if (m_numOfEndKeys != 0) {
        m_endType = node->getEndType();
        m_endKeyBackingStore = new char[tableIndex->getKeySchema()->tupleLength()];

        AbstractExpression** endKeyExprs = new AbstractExpression*[m_numOfEndKeys];
        for (int ctr = 0; ctr < m_numOfEndKeys; ++ctr) {
            endKeyExprs[ctr] = endKeyExpressions[ctr];
        }
        // The executor owns the expression array -- the plan node owns the expressions.
        m_endKeys = boost::shared_array<AbstractExpression*>(endKeyExprs);
    }

    VOLT_DEBUG("IndexCount: %s.%s\n", targetTable->name().c_str(),
            tableIndex->getName().c_str());
    return true;
}

bool IndexCountExecutor::p_execute(const NValueArray &params) {
    IndexCountPlanNode* node = static_cast<IndexCountPlanNode*>(m_abstractNode);
    assert(node);
    assert(node == dynamic_cast<IndexCountPlanNode*>(m_abstractNode));

    // update local target table with its most recent reference
    // The target table should be a persistent table.
    PersistentTable* targetTable = static_cast<PersistentTable*>(node->getTargetTable());
    assert(targetTable);
    assert(targetTable == dynamic_cast<PersistentTable*>(node->getTargetTable()));
    TableIndex* tableIndex = targetTable->index(node->getTargetIndexName());
    assert(tableIndex);
    IndexCursor indexCursor(tableIndex->getTupleSchema());

    TableTuple searchKey(tableIndex->getKeySchema());
    if (m_numOfSearchKeys != 0) {
        searchKey.moveNoHeader(m_searchKeyBackingStore);
    }
    TableTuple endKey(tableIndex->getKeySchema());
    if (m_numOfEndKeys != 0) {
        endKey.moveNoHeader(m_endKeyBackingStore);
    }

    // Need to move GTE to find (x,_) when doing a partial covering search.
    // The planner sometimes used to lie in this case: index_lookup_type_eq is incorrect.
    // Index_lookup_type_gte is necessary.
    assert(m_lookupType != INDEX_LOOKUP_TYPE_EQ ||
            searchKey.getSchema()->columnCount() == m_numOfSearchKeys ||
            searchKey.getSchema()->columnCount() == m_numOfEndKeys);

    int activeNumOfSearchKeys = m_numOfSearchKeys;
    IndexLookupType localLookupType = m_lookupType;
    bool searchKeyUnderflow = false;
    bool endKeyOverflow = false;
    // Overflow cases that can return early without accessing the index need this
    // default 0 count as their result.
    TableTuple& tmptup = m_tmpOutputTable->tempTuple();
    tmptup.setNValue(0, ValueFactory::getBigIntValue( 0 ));

    //
    // SEARCH KEY
    //
    if (m_numOfSearchKeys != 0) {
        bool earlyReturnForSearchKeyOutOfRange = false;
        searchKey.setAllNulls();
        VOLT_DEBUG("<Index Count>Initial (all null) search key: '%s'", searchKey.debugNoHeader().c_str());
        AbstractExpression** searchKeyExprArray = m_searchKeys.get();
        for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
            NValue candidateValue = searchKeyExprArray[ctr]->eval(NULL, NULL);
            if (candidateValue.isNull()) {
                // when any part of the search key is NULL, the result is false when it compares to anything.
                // do early return optimization, our index comparator may not handle null comparison correctly.
                earlyReturnForSearchKeyOutOfRange = true;
                break;
            }

            try {
                searchKey.setNValue(ctr, candidateValue);
            }
            catch (const SQLException &e) {
                // This next bit of logic handles underflow and overflow while
                // setting up the search keys.
                // e.g. TINYINT > 200 or INT <= 6000000000

                // re-throw if not an overflow or underflow
                // currently, it's expected to always be an overflow or underflow
                if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW |
                                             SQLException::TYPE_UNDERFLOW)) == 0) {
                    throw e;
                }

                // handle the case where this is a comparison, rather than equality match
                // comparison is the only place where the executor might return matching tuples
                // e.g. TINYINT < 1000 should return all values

                if ((localLookupType != INDEX_LOOKUP_TYPE_EQ) &&
                        (ctr == (activeNumOfSearchKeys - 1))) {
                    assert (localLookupType == INDEX_LOOKUP_TYPE_GT ||
                            localLookupType == INDEX_LOOKUP_TYPE_GTE);

                    if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        earlyReturnForSearchKeyOutOfRange = true;
                        break;
                    }

                    if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        searchKeyUnderflow = true;
                        break;
                    }

                    throw e;
                }

                // if a EQ comparision is out of range, then return no tuples
                earlyReturnForSearchKeyOutOfRange = true;
                break;
            }
        }
        VOLT_TRACE("Search key after substitutions: '%s'", searchKey.debugNoHeader().c_str());

        if (earlyReturnForSearchKeyOutOfRange) {
            m_tmpOutputTable->insertTempTuple(tmptup);
            return true;
        }
    }

    //
    // END KEY
    //
    if (m_numOfEndKeys != 0) {
        bool earlyReturnForEndKeyOutOfRange = false;
        endKey.setAllNulls();
        VOLT_DEBUG("Initial (all null) end key: '%s'", endKey.debugNoHeader().c_str());
        AbstractExpression** endKeyExprArray = m_endKeys.get();
        for (int ctr = 0; ctr < m_numOfEndKeys; ctr++) {
            NValue endKeyValue = endKeyExprArray[ctr]->eval(NULL, NULL);
            if (endKeyValue.isNull()) {
                // when any part of the search key is NULL, the result is false when it compares to anything.
                // do early return optimization, our index comparator may not handle null comparison correctly.
                earlyReturnForEndKeyOutOfRange = true;
                break;
            }

            try {
                endKey.setNValue(ctr, endKeyValue);
            }
            catch (const SQLException &e) {
                // This next bit of logic handles underflow and overflow while
                // setting up the search keys.
                // e.g. TINYINT > 200 or INT <= 6000000000

                // re-throw if not an overflow or underflow
                // currently, it's expected to always be an overflow or underflow
                if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW | SQLException::TYPE_UNDERFLOW)) == 0) {
                    throw e;
                }

                if (ctr == (m_numOfEndKeys - 1)) {
                    assert (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE);
                    if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                        earlyReturnForEndKeyOutOfRange = true;
                        break;
                    }

                    if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                        endKeyOverflow = true;
                        const ValueType type = endKey.getSchema()->columnType(ctr);
                        NValue tmpEndKeyValue = ValueFactory::getBigIntValue(getMaxTypeValue(type));
                        endKey.setNValue(ctr, tmpEndKeyValue);

                        VOLT_DEBUG("<Index count> end key out of range, MAX value: %ld...\n", (long)getMaxTypeValue(type));
                        break;
                    }

                    throw e;
                }

                // if a EQ comparision is out of range, then return no tuples
                earlyReturnForEndKeyOutOfRange = true;
                break;
            }
        }
        VOLT_TRACE("End key after substitutions: '%s'", endKey.debugNoHeader().c_str());

        if (earlyReturnForEndKeyOutOfRange) {
            m_tmpOutputTable->insertTempTuple(tmptup);
            return true;
        }
    }

    // POST EXPRESSION
    assert (node->getPredicate() == NULL);

    //
    // COUNT NULL EXPRESSION
    //
    AbstractExpression* countNULLExpr = node->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (countNULLExpr != NULL) {
        VOLT_DEBUG("COUNT NULL Expression:\n%s", countNULLExpr->debug(true).c_str());
    }

    bool reverseScanNullEdgeCase = false;
    bool reverseScanMovedIndexToScan = false;
    if (m_numOfSearchKeys < m_numOfEndKeys &&
            (m_endType == INDEX_LOOKUP_TYPE_LT || m_endType == INDEX_LOOKUP_TYPE_LTE)) {
        reverseScanNullEdgeCase = true;
        VOLT_DEBUG("Index count: reverse scan edge null case." );
    }


    // An index count has two cases: unique and non-unique
    int64_t rkStart = 0, rkEnd = 0, rkRes = 0;
    int leftIncluded = 0, rightIncluded = 0;

    if (m_numOfSearchKeys != 0) {
        // Deal with multi-map
        VOLT_DEBUG("INDEX_LOOKUP_TYPE(%d) m_numSearchKeys(%d) key:%s",
                   localLookupType, activeNumOfSearchKeys, searchKey.debugNoHeader().c_str());
        if (searchKeyUnderflow == false) {
            if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
                rkStart = tableIndex->getCounterLET(&searchKey, true, indexCursor);
            }
            else {
                // handle start inclusive cases.
                if (tableIndex->hasKey(&searchKey)) {
                    leftIncluded = 1;
                    rkStart = tableIndex->getCounterLET(&searchKey, false, indexCursor);

                    if (reverseScanNullEdgeCase) {
                        tableIndex->moveToKeyOrGreater(&searchKey, indexCursor);
                        reverseScanMovedIndexToScan = true;
                    }
                }
                else {
                    rkStart = tableIndex->getCounterLET(&searchKey, true, indexCursor);
                }
            }
        }
        else {
            // Do not count null row or columns
            tableIndex->moveToKeyOrGreater(&searchKey, indexCursor);
            assert(countNULLExpr);
            long numNULLs = countNulls(tableIndex, countNULLExpr, indexCursor);
            rkStart += numNULLs;
            VOLT_DEBUG("Index count[underflow case]: "
                    "find out %ld null rows or columns are not counted in.", numNULLs);

        }
    }

    if (reverseScanNullEdgeCase) {
        // reverse scan case
        if (!reverseScanMovedIndexToScan && localLookupType != INDEX_LOOKUP_TYPE_GT) {
            tableIndex->moveToEnd(true, indexCursor);
        }
        assert(countNULLExpr);
        long numNULLs = countNulls(tableIndex, countNULLExpr, indexCursor);
        rkStart += numNULLs;
        VOLT_DEBUG("Index count[reverse case]: "
                "find out %ld null rows or columns are not counted in.", numNULLs);
    }

    if (m_numOfEndKeys != 0) {
        if (endKeyOverflow) {
            rkEnd = tableIndex->getCounterGET(&endKey, true, indexCursor);
        }
        else {
            IndexLookupType localEndType = m_endType;
            if (localEndType == INDEX_LOOKUP_TYPE_LT) {
                rkEnd = tableIndex->getCounterGET(&endKey, false, indexCursor);
            }
            else {
                if (tableIndex->hasKey(&endKey)) {
                    rightIncluded = 1;
                    rkEnd = tableIndex->getCounterGET(&endKey, true, indexCursor);
                }
                else {
                    rkEnd = tableIndex->getCounterGET(&endKey, false, indexCursor);
                }
            }
        }
    }
    else {
        rkEnd = tableIndex->getSize();
        rightIncluded = 1;
    }
    rkRes = rkEnd - rkStart - 1 + leftIncluded + rightIncluded;
    VOLT_DEBUG("Index Count ANSWER %ld = %ld - %ld - 1 + %d + %d\n",
            (long)rkRes, (long)rkEnd, (long)rkStart, leftIncluded, rightIncluded);
    tmptup.setNValue(0, ValueFactory::getBigIntValue( rkRes ));
    m_tmpOutputTable->insertTempTuple(tmptup);

    VOLT_DEBUG ("Index Count :\n %s", m_tmpOutputTable->debug().c_str());
    return true;
}

IndexCountExecutor::~IndexCountExecutor() {
    delete [] m_searchKeyBackingStore;
    delete [] m_endKeyBackingStore;
}
