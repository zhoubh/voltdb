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

#include "nestloopindexexecutor.h"

#include "executors/aggregateexecutor.h"
#include "executors/indexscanexecutor.h"

#include "execution/ProgressMonitorProxy.h"

#include "expressions/abstractexpression.h"

#include "indexes/tableindex.h"

#include "plannodes/aggregatenode.h"
#include "plannodes/indexscannode.h"
#include "plannodes/limitnode.h"
#include "plannodes/nestloopindexnode.h"

#include "storage/persistenttable.h"
#include "storage/tabletuplefilter.h"
#include "storage/tableiterator.h"

using namespace std;
using namespace voltdb;

const static int8_t UNMATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE);
const static int8_t MATCHED_TUPLE(TableTupleFilter::ACTIVE_TUPLE + 1);

static void throwForFailedInit(IndexScanPlanNode* indexNode) {
    char msg[1024 * 10];
    snprintf(msg, sizeof(msg),
             "Failed to retrieve index from inner table for internal PlanNode '%s'",
             indexNode->debug().c_str());
    VOLT_ERROR("%s", msg);
    throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, msg);
}

bool NestLoopIndexExecutor::p_init(AbstractPlanNode*, TempTableLimits* limits) {
    VOLT_TRACE("init NLIJ Executor");
    assert(limits);

    // Init parent first
    AbstractJoinExecutor::p_init(m_abstractNode, limits);

    NestLoopIndexPlanNode* node = static_cast<NestLoopIndexPlanNode*>(m_abstractNode);
    assert(node);
    assert(node == dynamic_cast<NestLoopIndexPlanNode*>(m_abstractNode));
    m_indexNode =
        static_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN));
    assert(m_indexNode);
    assert(m_indexNode ==
        dynamic_cast<IndexScanPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_INDEXSCAN)));
    VOLT_TRACE("<NestLoopIndexPlanNode> %s, <IndexScanPlanNode> %s",
               node->debug().c_str(), m_indexNode->debug().c_str());

    m_lookupType = m_indexNode->getLookupType();
    m_sortDirection = m_indexNode->getSortDirection();

    // We need exactly one input table and a target table
    assert(node->getInputTableCount() == 1);

    Table* inputTable = node->getInputTable();
    assert(inputTable);

    node->getOutputColumnExpressions(m_outputExpressions);

    // Make sure that we actually have search keys
    assert(m_indexNode->getSearchKeyExpressions().size());

    PersistentTable* innerTable = static_cast<PersistentTable*>(m_indexNode->getTargetTable());
    assert(innerTable);
    assert(innerTable == dynamic_cast<PersistentTable*>(m_indexNode->getTargetTable()));

    // Grab the Index from our inner table
    // We'll throw an error if the index is missing
    TableIndex* index = innerTable->index(m_indexNode->getTargetIndexName());
    if (index == NULL) {
        throwForFailedInit(m_indexNode);
    }

    // NULL tuples for left and full joins
    initNullTuples(inputTable, innerTable);

    m_indexKeyValues.init(index->getKeySchema());
    return true;
}

inline static bool setIndexKeyValues(TableTuple& outerTuple,
                                     const TableTuple& indexKeyValues,
                                     int numOfSearchKeys,
                                     const std::vector<AbstractExpression*>& searchKeyExprs,
                                     IndexLookupType* pLookupType,
                                     SortDirectionType* pSortDirection) {
    indexKeyValues.setAllNulls();
    for (int ctr = 0; ctr < numOfSearchKeys; ctr++) {
        NValue candidateValue = searchKeyExprs[ctr]->eval(&outerTuple, NULL);
        if (candidateValue.isNull()) {
            // When any part of the search key is NULL,
            // the result is false when it compares to anything.
            // Do an early return optimization.
            return false;
        }
        try {
            indexKeyValues.setNValue(ctr, candidateValue);
        }
        catch (const SQLException &e) {
            // This next bit of logic handles underflow and overflow while
            // setting up the search keys.
            // e.g. TINYINT > 200 or INT <= 6000000000

            // Re-throw if not some kind of out of bound value exception.
            // Currently, it's expected to always be an overflow or underflow.
            const int OUT_OF_BOUND_VALUE_OF_ANY_KIND =
                SQLException::TYPE_OVERFLOW |
                SQLException::TYPE_UNDERFLOW |
                SQLException::TYPE_VAR_LENGTH_MISMATCH;
            if (0 == (e.getInternalFlags() & OUT_OF_BOUND_VALUE_OF_ANY_KIND)) {
                throw e;
            }

            assert(*pLookupType != INDEX_LOOKUP_TYPE_INVALID);
            assert(*pLookupType != INDEX_LOOKUP_TYPE_GEO_CONTAINS);

            // key prefix comparison or EQ comparison of the last key
            // component with an out of range key value
            // returns no matches --- except for left/full-outer joins
            if ((*pLookupType == INDEX_LOOKUP_TYPE_EQ) ||
                (ctr < numOfSearchKeys - 1)) {
                return false;
            }

            // handle the case where this is a non-EQ comparison,
            // the only condition when an out of range key may
            // yet have matching tuples
            // e.g. TINYINT < 1000 should return all values
            if (e.getInternalFlags() & SQLException::TYPE_OVERFLOW) {
                if ((*pLookupType == INDEX_LOOKUP_TYPE_GT) ||
                    (*pLookupType == INDEX_LOOKUP_TYPE_GTE)) {
                    // GT or GTE an overflow key breaks out and
                    // only gets results for left/full-outer joins.
                    return false;
                }

                // LT or LTE an overflow key is treated as a prefix LTE
                // to issue an "initial" forward scan
                *pLookupType = INDEX_LOOKUP_TYPE_LTE;
                return true;
            }

            if (e.getInternalFlags() & SQLException::TYPE_UNDERFLOW) {
                if ((*pLookupType == INDEX_LOOKUP_TYPE_LT) ||
                    (*pLookupType == INDEX_LOOKUP_TYPE_LTE)) {
                    // LT or LTE an underflow key should be treated
                    // as LTE NULL which will fail after an "initial"
                    // forward scan past NULLs.
                    *pLookupType = INDEX_LOOKUP_TYPE_LTE;
                }
                else {
                    // GTE an underflow key should be treated as GT NULL
                    // because GTE NULL would match nulls.
                    *pLookupType = INDEX_LOOKUP_TYPE_GT;
                }
                return true;
            }

            assert(e.getInternalFlags() & SQLException::TYPE_VAR_LENGTH_MISMATCH);
            // truncate the search key before adding to the search key tuple
            indexKeyValues.shrinkAndSetNValue(ctr, candidateValue);
            // adjust the lookup handling of the "edge" value
            // to account for the truncation, e.g.
            // WHERE TWO_CHAR_COL < 'abcd' --> WHERE TWO_CHAR_COL <= 'ab'
            // WHERE TWO_CHAR_COL >= 'abcd' --> WHERE TWO_CHAR_COL > 'ab'
            switch (*pLookupType) {
            case INDEX_LOOKUP_TYPE_LT:
                *pLookupType = INDEX_LOOKUP_TYPE_LTE;
                return true;
            case INDEX_LOOKUP_TYPE_GTE:
                *pLookupType = INDEX_LOOKUP_TYPE_GT;
                return true;
            default:
                return true;
            }
        }// End catch block for out of bounds index key
    }// End for each search key component
    return true;
}

inline static bool joinedTupleQualifies(voltdb::TableTuple& joinTuple,
                                        const voltdb::TableTuple& outerTuple,
                                        int numOfOuterCols,
                                        const voltdb::TableTuple& innerTuple,
                                        voltdb::CountingPostfilter& postfilter,
                                        vector<voltdb::AbstractExpression*> outputExprs) {
    // Still needs to pass the filter
    if ( ! postfilter.eval(&outerTuple, &innerTuple)) {
        return false;
    }
    // Passed! Complete the joined tuple with the inner column values.
    for (int colCtr = numOfOuterCols;
         colCtr < joinTuple.sizeInValues();
         ++colCtr) {
        joinTuple.setNValue(colCtr,
                            outputExprs[colCtr]->eval(&outerTuple,
                                                      &innerTuple));
    }
    return true;
}

bool NestLoopIndexExecutor::p_execute(const NValueArray &params) {
    VOLT_TRACE("executing NestLoopIndex...");
    NestLoopIndexPlanNode* node = static_cast<NestLoopIndexPlanNode*>(m_abstractNode);
    assert(node);
    assert(node == dynamic_cast<NestLoopIndexPlanNode*>(m_abstractNode));

    // output table must be a temp table
    assert(m_tmpOutputTable);

    // target table is a persistent table
    assert(m_indexNode);

    VOLT_TRACE("Execute %s, <IndexScanPlanNode> %s",
               m_abstractNode->debug().c_str(), m_indexNode->debug().c_str());

    PersistentTable* innerTable = static_cast<PersistentTable*>(m_indexNode->getTargetTable());
    assert(innerTable);
    assert(innerTable == dynamic_cast<PersistentTable*>(m_indexNode->getTargetTable()));

    TableIndex* index = innerTable->index(m_indexNode->getTargetIndexName());
    assert(index);
    IndexCursor indexCursor(index->getTupleSchema());

    // The outerTable is the input table that has tuples to be iterated
    assert(node->getInputTableCount() == 1);
    Table* outerTable = node->getInputTable();
    assert(outerTable);
    VOLT_TRACE("executing NestLoopIndex with outer table: %s, inner table: %s",
               outerTable->debug().c_str(), innerTable->debug().c_str());

    AbstractExpression* endExpr = m_indexNode->getEndExpression();
    AbstractExpression* postExpr = m_indexNode->getPredicate();
    AbstractExpression* initialExpr = m_indexNode->getInitialExpression();
    // For reverse scan edge case NULL values and forward scan underflow case.
    AbstractExpression* skipNullExpr = m_indexNode->getSkipNullPredicate();
    AbstractExpression* prejoinPredicate = node->getPreJoinPredicate();
    AbstractExpression* wherePredicate = node->getWherePredicate();
    LimitPlanNode* limitNode = static_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT));
    assert(limitNode == dynamic_cast<LimitPlanNode*>(node->getInlinePlanNode(PLAN_NODE_TYPE_LIMIT)));
    int limit = CountingPostfilter::NO_LIMIT;
    int offset = CountingPostfilter::NO_OFFSET;
    if (limitNode) {
        limitNode->getLimitAndOffsetByReference(params, limit, offset);
    }

    //
    // OUTER TABLE ITERATION
    //
    int numOfOuterCols = outerTable->columnCount();
    TableTuple outerTuple(outerTable->schema());
    TableTuple innerTuple(innerTable->schema());
    const TableTuple &nullInnerTuple = m_nullInnerTuple.tuple();

    TableIterator outerIterator = outerTable->iteratorDeletingAsWeGo();
    assert (outerTuple.sizeInValues() == numOfOuterCols);
    assert (innerTuple.sizeInValues() == innerTable->columnCount());

    // Init the postfilter
    CountingPostfilter postfilter(m_tmpOutputTable, wherePredicate, limit, offset);

    // The table filter to keep track of inner tuples that don't match any of outer tuples for FULL joins
    TableTupleFilter innerTableFilter;
    if (m_joinType == JOIN_TYPE_FULL) {
        // Prepopulate the set with all inner tuples
        innerTableFilter.init(innerTable);
    }

    TableTuple joinTuple;
    ProgressMonitorProxy pmp(m_engine, this);
    // It's not immediately obvious here, so there's some subtlety to
    // note with respect to the schema of the joinTuple.
    //
    // The innerTuple is used to represent the values from the inner
    // table in the case of the join predicate passing, and for left
    // outer joins, the nullTuple is used if there is no match.  Both
    // of these tuples include the complete schema of the table being
    // scanned.  The inner table is being scanned via an inlined scan
    // node, so there is no temp table corresponding to it.
    //
    // Predicates that are evaluated against the inner table should
    // therefore use the complete schema of the table being scanned.
    //
    // The joinTuple is the tuple that contains the values that we
    // actually want to put in the output of the join (or to aggregate
    // if there is an inlined agg plan node).  This tuple needs to
    // omit the unused columns from the inner table.  The inlined
    // index scan itself has an inlined project node that defines the
    // columns that should be output by the join, and omits those that
    // are not needed.  So the joinTuple contains the columns we're
    // using from the outer table, followed by the "projected" schema
    // for the inlined scan of the inner table.
    if (m_aggExec != NULL) {
        VOLT_TRACE("Init inline aggregate...");
        const TupleSchema * aggInputSchema = node->getTupleSchemaPreAgg();
        joinTuple = m_aggExec->p_execute_init(params, &pmp, aggInputSchema,
                                               m_tmpOutputTable, &postfilter);
    }
    else {
        joinTuple = m_tmpOutputTable->tempTuple();
    }

    VOLT_TRACE("<numOfOuterCols>: %d\n", numOfOuterCols);
    while (postfilter.isUnderLimit() && outerIterator.next(outerTuple)) {
        VOLT_TRACE("outerTuple:%s",
                   outerTuple.debug(outerTable->name()).c_str());
        pmp.countdownProgress();

        // Set the join tuple columns that originate solely from the outer tuple.
        // Must be outside the inner loop in case of the empty inner table.
        joinTuple.setNValues(0, outerTuple, 0, numOfOuterCols);

        // did this loop body find at least one match for this tuple?
        bool outerMatch = false;
        // For outer joins if outer tuple fails pre-join predicate
        // (join expression based on the outer table only)
        // it can't match any inner tuples
        if (prejoinPredicate == NULL ||
            prejoinPredicate->eval(&outerTuple, NULL).isTrue()) {
            const std::vector<AbstractExpression*>& searchKeyExprs =
                m_indexNode->getSearchKeyExpressions();
            int numOfSearchKeys = searchKeyExprs.size();
            VOLT_TRACE("<Nested Loop Index exec, WHILE-LOOP...>\n");
            IndexLookupType effectiveLookupType = m_lookupType;
            SortDirectionType effectiveSortDirection = m_sortDirection;
            VOLT_TRACE("Lookup type: %d\n", m_lookupType);
            VOLT_TRACE("SortDirectionType: %d\n", m_sortDirection);

            //
            // Now use the outer table tuple to construct the search key
            // against the inner table
            //

            const TableTuple& indexKeyValues = m_indexKeyValues.tuple();
            // did setting the search key fail (usually due to overflow)
            bool keyIsValid = setIndexKeyValues(outerTuple,
                                                indexKeyValues,
                                                numOfSearchKeys,
                                                searchKeyExprs,
                                                &effectiveLookupType,
                                                &effectiveSortDirection);

            VOLT_TRACE("Searching %s", indexKeyValues.debug("").c_str());

            // if a search value didn't fit into the targeted index key, skip this key
            if (keyIsValid) {
                //
                // Our index scan on the inner table is going to have three parts:
                //  (1) Lookup tuples using the search key
                //
                //  (2) For each tuple that comes back, check whether the
                //      endExpr is false.  If it is, then we stop
                //      scanning. Otherwise...
                //
                //  (3) Check whether the tuple satisfies the post expression.
                //      If it does, then add it to the output table
                //
                // Use our search key to prime the index iterator
                // The loop through each tuple given to us by the iterator
                //
                // Essentially cut and pasted this if ladder from
                // index scan executor
                if (numOfSearchKeys > 0) {
                    if (effectiveLookupType == INDEX_LOOKUP_TYPE_EQ) {
                        index->moveToKey(&indexKeyValues, indexCursor);
                    }
                    else if (effectiveLookupType == INDEX_LOOKUP_TYPE_GT) {
                        index->moveToGreaterThanKey(&indexKeyValues, indexCursor);
                    }
                    else if (effectiveLookupType == INDEX_LOOKUP_TYPE_GTE) {
                        index->moveToKeyOrGreater(&indexKeyValues, indexCursor);
                    }
                    else if (effectiveLookupType == INDEX_LOOKUP_TYPE_LT) {
                        index->moveToLessThanKey(&indexKeyValues, indexCursor);
                    }
                    else if (effectiveLookupType == INDEX_LOOKUP_TYPE_LTE) {
                        // find the entry whose key is greater than search key,
                        // do a forward scan using initialExpr to find the correct
                        // start point to do reverse scan
                        bool isEnd = index->moveToGreaterThanKey(&indexKeyValues, indexCursor);
                        if (isEnd) {
                            index->moveToEnd(false, indexCursor);
                        }
                        else {
                            while (!(innerTuple = index->nextValue(indexCursor)).isNullTuple()) {
                                pmp.countdownProgress();
                                if (initialExpr != NULL && !initialExpr->eval(&outerTuple, &innerTuple).isTrue()) {
                                    // just passed the first failed entry, so move 2 backward
                                    index->moveToBeforePriorEntry(indexCursor);
                                    break;
                                }
                            }
                            if (innerTuple.isNullTuple()) {
                                index->moveToEnd(false, indexCursor);
                            }
                        }
                    }
                    else if (effectiveLookupType == INDEX_LOOKUP_TYPE_GEO_CONTAINS) {
                        index->moveToCoveringCell(&indexKeyValues, indexCursor);
                    }
                    else {
                        return false;
                    }
                }
                else {
                    bool toStartActually = (effectiveSortDirection != SORT_DIRECTION_TYPE_DESC);
                    index->moveToEnd(toStartActually, indexCursor);
                }

                AbstractExpression* skipNullExprIteration = skipNullExpr;

                while (postfilter.isUnderLimit() &&
                       IndexScanExecutor::getNextTuple(effectiveLookupType,
                                                       &innerTuple,
                                                       index,
                                                       &indexCursor,
                                                       numOfSearchKeys)) {
                    if (innerTuple.isPendingDelete()) {
                        continue;
                    }
                    VOLT_TRACE("innerTuple:%s",
                               innerTuple.debug(innerTable->name()).c_str());
                    pmp.countdownProgress();

                    //
                    // First check to eliminate the null index rows for UNDERFLOW case only
                    //
                    if (skipNullExprIteration != NULL) {
                        if (skipNullExprIteration->eval(&outerTuple, &innerTuple).isTrue()) {
                            VOLT_DEBUG("Index scan: find out null rows or columns.");
                            continue;
                        }
                        skipNullExprIteration = NULL;
                    }

                    //
                    // First check whether the endExpr is now false
                    //
                    if (endExpr != NULL &&
                        !endExpr->eval(&outerTuple, &innerTuple).isTrue()) {
                        VOLT_TRACE("End Expression evaluated to false, stopping scan\n");
                        break;
                    }
                    //
                    // Then apply our post-predicate to do further filtering
                    //
                    if (postExpr == NULL ||
                        postExpr->eval(&outerTuple, &innerTuple).isTrue()) {
                        outerMatch = true;
                        // The inner tuple passed the join conditions
                        if (m_joinType == JOIN_TYPE_FULL) {
                            // Mark inner tuple as matched
                            innerTableFilter.updateTuple(innerTuple, MATCHED_TUPLE);
                        }
                        // Still need to pass where filtering
                        if (joinedTupleQualifies(joinTuple,
                                                outerTuple,
                                                numOfOuterCols,
                                                innerTuple,
                                                postfilter,
                                                m_outputExpressions)) {
                            outputTuple(postfilter, joinTuple, pmp);
                        }
                    }
                }// END INNER WHILE LOOP
            }// END IF INDEX KEY IS VALID
        }// END IF PRE JOIN CONDITION

        //
        // Left/Full Outer Join
        //
        if (m_joinType != JOIN_TYPE_INNER &&
            !outerMatch &&
            postfilter.isUnderLimit() &&
            joinedTupleQualifies(joinTuple,
                                outerTuple,
                                numOfOuterCols,
                                nullInnerTuple,
                                postfilter,
                                m_outputExpressions)) {
            outputTuple(postfilter, joinTuple, pmp);
        }
    }// END OUTER WHILE LOOP

    // For FULL outer join, left-pad and output the unmatched inner tuples
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
            assert(innerTuple.isActive());
            if (joinedTupleQualifies(joinTuple,
                                    nullOuterTuple,
                                    numOfOuterCols,
                                    innerTuple,
                                    postfilter,
                                    m_outputExpressions)) {
                // The unmatched tuple qualified the post-filter
                // and has been merged into the join tuple.
                outputTuple(postfilter, joinTuple, pmp);
            }
        }
    }

    if (m_aggExec != NULL) {
        m_aggExec->p_execute_finish();
    }

    VOLT_TRACE("result table:\n %s", m_tmpOutputTable->debug().c_str());
    VOLT_TRACE("Finished NestLoopIndex");

    cleanupInputTempTable(innerTable);
    cleanupInputTempTable(outerTable);

    return true;
}

NestLoopIndexExecutor::~NestLoopIndexExecutor() { }
