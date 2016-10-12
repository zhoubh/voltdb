/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include "indexscanexecutor.h"

#include "common/debuglog.h"
#include "common/common.h"
#include "common/tabletuple.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "executors/aggregateexecutor.h"
#include "executors/executorutil.h"
#include "execution/ProgressMonitorProxy.h"
#include "expressions/abstractexpression.h"
#include "expressions/expressionutil.h"
#include "indexes/tableindex.h"

// Inline PlanNodes
#include "plannodes/indexscannode.h"
#include "plannodes/projectionnode.h"
#include "plannodes/limitnode.h"
#include "plannodes/aggregatenode.h"

#include "storage/table.h"
#include "storage/tableiterator.h"
#include "storage/temptable.h"
#include "storage/persistenttable.h"

using namespace voltdb;

bool IndexScanExecutor::p_init(AbstractPlanNode *, TempTableLimits* limits) {
    VOLT_TRACE("init IndexScan Executor");
    IndexScanPlanNode* node = dynamic_cast<IndexScanPlanNode*>(m_abstractNode);
    assert(node);

    // Create output table based on output schema from the plan
    setTempOutputTable(limits, node->getTargetTable()->name());
    assert(m_tmpOutputTable);


    // The target table should be a persistent table
    PersistentTable* targetTable = dynamic_cast<PersistentTable*>(node->getTargetTable());
    assert(targetTable);

    // Grab the index from the table. Throw an error if the index is missing.
    TableIndex *tableIndex = targetTable->index(node->getTargetIndexName());
    assert (tableIndex != NULL);

    //
    // INLINE PROJECTION
    //
    m_projectionNode =
        static_cast<ProjectionPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_PROJECTION));

    if (m_projectionNode != NULL) {
        m_projector = OptimizedProjector(m_projectionNode->getOutputColumnExpressions());
        m_projector.optimize(m_projectionNode->getOutputTable()->schema(),
                             node->getTargetTable()->schema());
    }

    // Inline aggregation can be serial, partial or hash
    m_aggExec = getInlineAggregateExecutor(m_abstractNode);

    //
    // Miscellanous Information
    //
    m_lookupType = node->getLookupType();
    m_sortDirection = node->getSortDirection();

    const std::vector<AbstractExpression*>& searchKeyExpressions = node->getSearchKeyExpressions();
    m_numOfSearchKeys = (int)searchKeyExpressions.size();
    m_searchKeyBackingStore = new char[tableIndex->getKeySchema()->tupleLength()];

    AbstractExpression** searchKeyExprs = new AbstractExpression*[m_numOfSearchKeys];
    for (int ctr = 0; ctr < m_numOfSearchKeys; ++ctr) {
        searchKeyExprs[ctr] = searchKeyExpressions[ctr];
    }
    // The executor owns the expression array -- the plan node owns the expressions.
    m_searchKeys = boost::shared_array<AbstractExpression*>(searchKeyExprs);

    VOLT_TRACE("Index key schema: '%s'", tableIndex->getKeySchema()->debug().c_str());
    VOLT_DEBUG("IndexScan: %s.%s\n", targetTable->name().c_str(), tableIndex->getName().c_str());

    return true;
}

bool IndexScanExecutor::p_execute(const NValueArray &params) {
    IndexScanPlanNode* node = static_cast<IndexScanPlanNode*>(m_abstractNode);
    assert(node);
    assert(node == dynamic_cast<IndexScanPlanNode*>(m_abstractNode));

    // Short-circuit an empty scan
    if (node->isEmptyScan()) {
        VOLT_DEBUG ("Empty Index Scan :\n %s", m_tmpOutputTable->debug().c_str());
        if (m_aggExec != NULL) {
            m_aggExec->p_execute_finish();
        }
        return true;
    }

    //
    // INLINE LIMIT
    //
    LimitPlanNode* limit_node =
            dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limit_node != NULL) {
        limit_node->getLimitAndOffsetByReference(params, limit, offset);
    }

    //
    // POST EXPRESSION
    //
    AbstractExpression* post_expression = node->getPredicate();
    if (post_expression != NULL) {
        VOLT_DEBUG("Post Expression:\n%s", post_expression->debug(true).c_str());
    }

    // Initialize the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, post_expression, limit, offset);

    // update local target table with its most recent reference
    // The target table should be a persistent table.
    PersistentTable* targetTable = static_cast<PersistentTable*>(node->getTargetTable());
    assert(targetTable);
    assert(targetTable == dynamic_cast<PersistentTable*>(node->getTargetTable()));
    TableIndex* tableIndex = targetTable->index(node->getTargetIndexName());
    assert(tableIndex);
    IndexCursor indexCursor(tableIndex->getTupleSchema());

    TableTuple tempTuple;
    ProgressMonitorProxy pmp(m_engine, this);
    if (m_aggExec != NULL) {
        const TupleSchema * inputSchema = (m_projectionNode ?
                                           m_projectionNode->getOutputTable()->schema() :
                                           tableIndex->getTupleSchema());
        tempTuple = m_aggExec->p_execute_init(params, &pmp, inputSchema,
                                              m_tmpOutputTable, &postfilter);
    }
    else {
        tempTuple = m_tmpOutputTable->tempTuple();
    }

    //
    // SEARCH KEY
    //
    int activeNumOfSearchKeys = m_numOfSearchKeys;
    IndexLookupType localLookupType = m_lookupType;
    SortDirectionType localSortDirection = m_sortDirection;
    bool earlyReturnForSearchKeyOutOfRange = false;
    TableTuple searchKey(tableIndex->getKeySchema());
    assert(m_lookupType != INDEX_LOOKUP_TYPE_EQ ||
            searchKey.getSchema()->columnCount() == m_numOfSearchKeys);
    searchKey.moveNoHeader(m_searchKeyBackingStore);
    searchKey.setAllNulls();
    VOLT_TRACE("Initial (all null) search key: '%s'", searchKey.debugNoHeader().c_str());
    AbstractExpression** searchKeyExprArray = m_searchKeys.get();
    for (int ctr = 0; ctr < activeNumOfSearchKeys; ctr++) {
        NValue candidateValue = searchKeyExprArray[ctr]->eval(NULL, NULL);
        if (candidateValue.isNull()) {
            // When any part of the search key is NULL, the result is false
            // when it compares to anything.
            // Do an early return optimization.
            earlyReturnForSearchKeyOutOfRange = true;
            break;
        }

        try {
            searchKey.setNValue(ctr, candidateValue);
        }
        catch (const SQLException &e) {
            // This next bit of logic handles underflow, overflow and search key length
            // exceeding variable length column size (variable length mismatch) when
            // setting up the search keys.
            // e.g. TINYINT > 200 or INT <= 6000000000
            // VarChar(3 bytes) < "abcd" or VarChar(3) > "abbd"

            // re-throw if not an overflow, underflow or variable length mismatch
            // currently, it's expected to always be an overflow or underflow
            if ((e.getInternalFlags() & (SQLException::TYPE_OVERFLOW |
                                         SQLException::TYPE_UNDERFLOW |
                                         SQLException::TYPE_VAR_LENGTH_MISMATCH)) == 0) {
                throw e;
            }

            // handle the case where this is a comparison, rather than equality match
            // comparison is the only place where the executor might return matching tuples
            // e.g. TINYINT < 1000 should return all values
            if ((localLookupType != INDEX_LOOKUP_TYPE_EQ) &&
                    (ctr == (activeNumOfSearchKeys - 1))) {

                if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_GT) ||
                            (localLookupType == INDEX_LOOKUP_TYPE_GTE)) {
                        // gt or gte when key overflows returns nothing except inline agg
                        earlyReturnForSearchKeyOutOfRange = true;
                        break;
                    }
                    else {
                        // for overflow on reverse scan, we need to
                        // do a forward scan to find the correct start
                        // point, which is exactly what LTE would do.
                        // so, set the lookupType to LTE and the missing
                        // searchkey will be handled by extra post filters
                        localLookupType = INDEX_LOOKUP_TYPE_LTE;
                    }
                }
                else if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                    if ((localLookupType == INDEX_LOOKUP_TYPE_LT) ||
                            (localLookupType == INDEX_LOOKUP_TYPE_LTE)) {

                        // lt or lte when key underflows returns nothing except inline agg
                        earlyReturnForSearchKeyOutOfRange = true;
                        break;
                    }
                    // don't allow GTE because it breaks null handling
                    localLookupType = INDEX_LOOKUP_TYPE_GT;
                }
                else if (e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH) {
                    // shrink the search key and add the updated key to search key table tuple
                    searchKey.shrinkAndSetNValue(ctr, candidateValue);
                    // search will be performed on shrinked key,
                    // so update the lookup operation to account for it
                    switch (localLookupType) {
                    case INDEX_LOOKUP_TYPE_LT:
                    case INDEX_LOOKUP_TYPE_LTE:
                        localLookupType = INDEX_LOOKUP_TYPE_LTE;
                        break;
                    case INDEX_LOOKUP_TYPE_GT:
                    case INDEX_LOOKUP_TYPE_GTE:
                        localLookupType = INDEX_LOOKUP_TYPE_GT;
                        break;
                    default:
                        assert(!"IndexScanExecutor::p_execute - can't index on not equals");
                        return false;
                    }
                }

                // If here, all tuples with the previous searchkey
                // columns need to be scanned. Note, if there is only one column,
                // then all tuples will be scanned. The only exception to this
                // case is when setting the search key in the search tuple exceeded
                // the length restriction of the search column.
                if (!(e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH)) {
                    // for variable length mismatch error, the search key
                    // needed to perform the search has been generated and
                    // added to the search tuple. So no need to decrement
                    // activeNumOfSearchKeys
                    activeNumOfSearchKeys--;
                }
                if (localSortDirection == SORT_DIRECTION_TYPE_INVALID) {
                    localSortDirection = SORT_DIRECTION_TYPE_ASC;
                }
            }
            // if a EQ comparison is out of range, then return no tuples
            else {
                earlyReturnForSearchKeyOutOfRange = true;
            }
            break;
        }
    }

    if (earlyReturnForSearchKeyOutOfRange) {
        if (m_aggExec != NULL) {
            m_aggExec->p_execute_finish();
        }
        return true;
    }

    assert((activeNumOfSearchKeys == 0) || (searchKey.getSchema()->columnCount() > 0));
    VOLT_TRACE("Search key after substitutions: '%s', # of active search keys: %d", searchKey.debugNoHeader().c_str(), activeNumOfSearchKeys);

    //
    // END EXPRESSION
    //
    AbstractExpression* endExpression = node->getEndExpression();
    if (endExpression != NULL) {
        VOLT_DEBUG("End Expression:\n%s", endExpression->debug(true).c_str());
    }

    // INITIAL EXPRESSION
    AbstractExpression* initialExpression = node->getInitialExpression();
    if (initialExpression != NULL) {
        VOLT_DEBUG("Initial Expression:\n%s", initialExpression->debug(true).c_str());
    }

    //
    // SKIP NULL EXPRESSION
    //
    AbstractExpression* skipNullExpr = node->getSkipNullPredicate();
    // For reverse scan edge case NULL values and forward scan underflow case.
    if (skipNullExpr != NULL) {
        VOLT_DEBUG("COUNT NULL Expression:\n%s", skipNullExpr->debug(true).c_str());
    }

    //
    // An index scan has three parts:
    //  (1) Lookup tuples using the search key
    //  (2) For each tuple that comes back, check whether the
    //  end_expression is false.
    //  If it is, then we stop scanning. Otherwise...
    //  (3) Check whether the tuple satisfies the post expression.
    //      If it does, then add it to the output table
    //
    // Use our search key to prime the index iterator
    // Now loop through each tuple given to us by the iterator
    //

    TableTuple tuple;
    if (activeNumOfSearchKeys > 0) {
        VOLT_TRACE("INDEX_LOOKUP_TYPE(%d) m_numSearchkeys(%d) key:%s",
                localLookupType, activeNumOfSearchKeys, searchKey.debugNoHeader().c_str());

        if (localLookupType == INDEX_LOOKUP_TYPE_EQ) {
            tableIndex->moveToKey(&searchKey, indexCursor);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GT) {
            tableIndex->moveToGreaterThanKey(&searchKey, indexCursor);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GTE) {
            tableIndex->moveToKeyOrGreater(&searchKey, indexCursor);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_LT) {
            tableIndex->moveToLessThanKey(&searchKey, indexCursor);
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_LTE) {
            // find the entry whose key is greater than search key,
            // do a forward scan using initialExpr to find the correct
            // start point to do reverse scan
            bool isEnd = tableIndex->moveToGreaterThanKey(&searchKey, indexCursor);
            if (isEnd) {
                tableIndex->moveToEnd(false, indexCursor);
            }
            else {
                while (!(tuple = tableIndex->nextValue(indexCursor)).isNullTuple()) {
                    pmp.countdownProgress();
                    if (initialExpression != NULL && !initialExpression->eval(&tuple, NULL).isTrue()) {
                        // just passed the first failed entry, so move 2 backward
                        tableIndex->moveToBeforePriorEntry(indexCursor);
                        break;
                    }
                }
                if (tuple.isNullTuple()) {
                    tableIndex->moveToEnd(false, indexCursor);
                }
            }
        }
        else if (localLookupType == INDEX_LOOKUP_TYPE_GEO_CONTAINS) {
            tableIndex->moveToCoveringCell(&searchKey, indexCursor);
        }
        else {
            return false;
        }
    }
    else {
        bool toStartActually = (localSortDirection != SORT_DIRECTION_TYPE_DESC);
        tableIndex->moveToEnd(toStartActually, indexCursor);
    }

    while (postfilter.isUnderLimit() &&
           getNextTuple(localLookupType,
                        &tuple,
                        tableIndex,
                        &indexCursor,
                        activeNumOfSearchKeys)) {
        if (tuple.isPendingDelete()) {
            continue;
        }
        VOLT_TRACE("LOOPING in indexscan: tuple: '%s'\n",
                tuple.debug("tablename").c_str());

        pmp.countdownProgress();
        //
        // First check to eliminate the null-valued index rows
        // for the UNDERFLOW case only
        //
        if (skipNullExpr != NULL) {
            if (skipNullExpr->eval(&tuple, NULL).isTrue()) {
                VOLT_DEBUG("Index scan: find out null rows or columns.");
                continue;
            }
            skipNullExpr = NULL;
        }
        //
        // First check whether the end_expression is now false
        //
        if (endExpression != NULL && ! endExpression->eval(&tuple, NULL).isTrue()) {
            VOLT_TRACE("End Expression evaluated to false, stopping scan");
            break;
        }

        //
        // Then apply our post-predicate and LIMIT/OFFSET to do further filtering
        //
        if (postfilter.eval(&tuple, NULL)) {
            if (m_projector.numSteps() > 0) {
                m_projector.exec(tempTuple, tuple);
                outputTuple(postfilter, tempTuple);
            }
            else {
                outputTuple(postfilter, tuple);
            }
            pmp.countdownProgress();
        }
    }

    if (m_aggExec) {
        m_aggExec->p_execute_finish();
    }

    VOLT_DEBUG ("Index Scanned :\n %s", m_tmpOutputTable->debug().c_str());
    return true;
}

void IndexScanExecutor::outputTuple(CountingPostfilter& postfilter, TableTuple& tuple) {
    if (m_aggExec) {
        m_aggExec->p_execute_tuple(tuple);
        return;
    }
    //
    // Insert the tuple into our output table
    //
    assert(m_tmpOutputTable);
    m_tmpOutputTable->insertTempTuple(tuple);
}

IndexScanExecutor::~IndexScanExecutor() {
    delete [] m_searchKeyBackingStore;
}
