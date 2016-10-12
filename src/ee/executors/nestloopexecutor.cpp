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
#include "nestloopexecutor.h"

#include "executors/aggregateexecutor.h"

#include "execution/ProgressMonitorProxy.h"

#include "expressions/abstractexpression.h"

#include "plannodes/aggregatenode.h"
#include "plannodes/limitnode.h"
#include "plannodes/nestloopnode.h"

#include "storage/tableiterator.h"
#include "storage/tabletuplefilter.h"

#ifdef VOLT_DEBUG_ENABLED
#include <ctime>
#include <sys/times.h>
#include <unistd.h>
#endif

using namespace std;
using namespace voltdb;

const static int8_t UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
const static int8_t MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);

bool NestLoopExecutor::p_init(AbstractPlanNode*, TempTableLimits* limits) {
    VOLT_TRACE("init NLJ Executor");
    assert(limits);

    // Init parent first
    AbstractJoinExecutor::p_init(m_abstractNode, limits);

    NestLoopPlanNode* node = static_cast<NestLoopPlanNode*>(m_abstractNode);
    assert(node);
    assert(node == dynamic_cast<NestLoopPlanNode*>(m_abstractNode));

    // NULL tuples for left and full joins
    initNullTuples(node->getInputTable(), node->getInputTable(1));

    return true;
}

bool NestLoopExecutor::p_execute(const NValueArray &params) {
    VOLT_TRACE("executing NestLoop...");
    NestLoopPlanNode* node = static_cast<NestLoopPlanNode*>(m_abstractNode);
    assert(node);
    assert(node == dynamic_cast<NestLoopPlanNode*>(m_abstractNode));

    // output table must be a temp table
    assert(m_tmpOutputTable);

    assert(node->getInputTableCount() == 2);

    Table* outerTable = node->getInputTable();
    assert(outerTable);

    Table* innerTable = node->getInputTable(1);
    assert(innerTable);

    VOLT_TRACE("Execute %s", m_abstractNode->debug().c_str());
    VOLT_TRACE ("input table left:\n %s", outerTable->debug().c_str());
    VOLT_TRACE ("input table right:\n %s", innerTable->debug().c_str());

    AbstractExpression *preJoinPredicate = node->getPreJoinPredicate();
    AbstractExpression *joinPredicate = node->getJoinPredicate();
    AbstractExpression *wherePredicate = node->getWherePredicate();

    LimitPlanNode* limitNode = static_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    assert(limitNode == dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT)));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limitNode) {
        limitNode->getLimitAndOffsetByReference(params, limit, offset);
    }

    int numOfOuterCols = outerTable->columnCount();
    int numOfInnerCols = innerTable->columnCount();
    TableTuple outerTuple(node->getInputTable(0)->schema());
    TableTuple innerTuple(node->getInputTable(1)->schema());
    const TableTuple& nullInnerTuple = m_nullInnerTuple.tuple();

    TableIterator iterator0 = outerTable->iteratorDeletingAsWeGo();
    // Init the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, wherePredicate, limit, offset);

    // The table filter to keep track of inner tuples that don't match any of outer tuples for FULL joins
    TableTupleFilter innerTableFilter;
    if (m_joinType == JOIN_TYPE_FULL) {
        // Prepopulate the view with all inner tuples
        innerTableFilter.init(innerTable);
    }

    TableTuple joinTuple;
    ProgressMonitorProxy pmp(m_engine, this);
    if (m_aggExec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        const TupleSchema * aggInputSchema = node->getTupleSchemaPreAgg();
        joinTuple = m_aggExec->p_execute_init(params, &pmp, aggInputSchema, m_tmpOutputTable, &postfilter);
    }
    else {
        joinTuple = m_tmpOutputTable->tempTuple();
    }

    while (postfilter.isUnderLimit() && iterator0.next(outerTuple)) {
        pmp.countdownProgress();

        // populate output table's temp tuple with outer table's values
        // probably have to do this at least once - avoid doing it many
        // times per outer tuple
        joinTuple.setNValues(0, outerTuple, 0, numOfOuterCols);

        // did this loop body find at least one match for this tuple?
        bool outerMatch = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any of inner tuples
        if (preJoinPredicate == NULL || preJoinPredicate->eval(&outerTuple, NULL).isTrue()) {

            // By default, the delete as we go flag is false.
            TableIterator iterator1 = innerTable->iterator();
            while (postfilter.isUnderLimit() && iterator1.next(innerTuple)) {
                pmp.countdownProgress();
                // Apply join filter to produce matches for each outer that has them,
                // then pad unmatched outers, then filter them all
                if (joinPredicate == NULL || joinPredicate->eval(&outerTuple, &innerTuple).isTrue()) {
                    outerMatch = true;
                    // The inner tuple passed the join predicate
                    if (m_joinType == JOIN_TYPE_FULL) {
                        // Mark it as matched
                        innerTableFilter.updateTuple(innerTuple, MATCHED_TUPLE);
                    }
                    // Filter the joined tuple
                    if (postfilter.eval(&outerTuple, &innerTuple)) {
                        // Matched! Complete the joined tuple with the inner column values.
                        joinTuple.setNValues(numOfOuterCols, innerTuple, 0, numOfInnerCols);
                        outputTuple(postfilter, joinTuple, pmp);
                    }
                }
            }// END INNER WHILE LOOP
        }// END IF PRE JOIN CONDITION

        //
        // Left Outer Join
        //
        if (m_joinType != JOIN_TYPE_INNER && !outerMatch && postfilter.isUnderLimit()) {
            // Still needs to pass the filter
            if (postfilter.eval(&outerTuple, &nullInnerTuple)) {
                // Matched! Complete the joined tuple with the inner column values.
                joinTuple.setNValues(numOfOuterCols, nullInnerTuple, 0, numOfInnerCols);
                outputTuple(postfilter, joinTuple, pmp);
            }
        }// END IF LEFT OUTER JOIN
    }// END OUTER WHILE LOOP

    //
    // FULL Outer Join. Iterate over the unmatched inner tuples
    //
    if (m_joinType == JOIN_TYPE_FULL && postfilter.isUnderLimit()) {
        // Preset outer columns to null
        const TableTuple& nullOuterTuple = m_nullOuterTuple.tuple();
        joinTuple.setNValues(0, nullOuterTuple, 0, numOfOuterCols);

        TableTupleFilter_iter<UNMATCHED_TUPLE> endItr = innerTableFilter.end<UNMATCHED_TUPLE>();
        for (TableTupleFilter_iter<UNMATCHED_TUPLE> itr = innerTableFilter.begin<UNMATCHED_TUPLE>();
                itr != endItr && postfilter.isUnderLimit(); ++itr) {
            // Restore the tuple value
            uint64_t tupleAddr = innerTableFilter.getTupleAddress(*itr);
            innerTuple.move((char *)tupleAddr);
            // Still needs to pass the filter
            assert(innerTuple.isActive());
            if (postfilter.eval(&nullOuterTuple, &innerTuple)) {
                // Passed! Complete the joined tuple with the inner column values.
                joinTuple.setNValues(numOfOuterCols, innerTuple, 0, numOfInnerCols);
                outputTuple(postfilter, joinTuple, pmp);
            }
        }
    }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    cleanupInputTempTable(innerTable);
    cleanupInputTempTable(outerTable);

    return true;
}
