/*-------------------------------------------------------------------------
 *
 * nodeNestloop.c
 *	  routines to support nest-loop joins
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeNestloop.c
 *
 *-------------------------------------------------------------------------
 */
/*
 *	 INTERFACE ROUTINES
 *		ExecNestLoop	 - process a nestloop join of two plans
 *		ExecInitNestLoop - initialize the join
 *		ExecEndNestLoop  - shut down the join
 */

#include "postgres.h"

#include <math.h>
#include <stdlib.h>

#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "storage/bufmgr.h"

/* ----------------------------------------------------------------
 *		ExecNestLoop(node)
 *
 * old comments
 *		Returns the tuple joined from inner and outer tuples which
 *		satisfies the qualification clause.
 *
 *		It scans the inner relation to join with current outer tuple.
 *
 *		If none is found, next tuple from the outer relation is retrieved
 *		and the inner relation is scanned from the beginning again to join
 *		with the outer tuple.
 *
 *		NULL is returned if all the remaining outer tuples are tried and
 *		all fail to join with the inner tuples.
 *
 *		NULL is also returned if there is no tuple from inner relation.
 *
 *		Conditions:
 *		  -- outerTuple contains current tuple from outer relation and
 *			 the right son(inner relation) maintains "cursor" at the tuple
 *			 returned previously.
 *				This is achieved by maintaining a scan position on the outer * relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */

#define MAX(a,b) ((a) > (b) ? (a) : (b))

static RelationPage* CreateRelationPage() {
	int i;
	RelationPage *relationPage = palloc(sizeof(RelationPage));
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	for (i = 0; i < PAGE_SIZE; i++) {
		relationPage->tuples[i] = NULL;
	}
	return relationPage;
}

static void RemoveRelationPage(RelationPage **relationPageAdr) {
	int i;
	RelationPage *relationPage;
	relationPage = *relationPageAdr;
	if (relationPage == NULL) {
		return;
	}
	for (i = 0; i < PAGE_SIZE; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	pfree(relationPage);
	(*relationPageAdr) = NULL;
}

static int LoadNextPage(PlanState *planState, RelationPage *relationPage) {
	int i;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextPage: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	// Remove the old stored tuples
	for (i = 0; i < PAGE_SIZE; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	for (i = 0; i < PAGE_SIZE; i++) {
		TupleTableSlot *tts = ExecProcNode(planState);
		if (TupIsNull(tts)) {
			relationPage->tuples[i] = NULL;
			break;
		} else {
			relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
			ExecCopySlot(relationPage->tuples[i], tts);
			relationPage->tupleCount++;
		}
	}
	return relationPage->tupleCount;
}

//static int LoadNextOuterPage(PlanState *outerPlan, RelationPage *relationPage, ScanKey xidScanKey, int fromIndex) {
//	int i, j = 0;
//	int size;
//	TupleTableSlot *tts;
//	if (relationPage == NULL) {
//		elog(ERROR, "LoadNextOuterPage: null page");
//	}
//	relationPage->index = 0;
//	relationPage->tupleCount = 0;
//	size = PAGE_SIZE;
//	// Remove the old stored tuples
//	for (i = 0; i < size; i++) {
//		if (!TupIsNull(relationPage->tuples[i])) {
//			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
//			relationPage->tuples[i] = NULL;
//		}
//	}
//	for (i = 0; i < size; i++) {
//		while (true) {
//			ScanKeyEntryInitialize(xidScanKey, //TODO is it fine to init ScanKey once?
//					0, // flags
//					1, /* attribute number to scan */
//					3, /* op's strategy */
//					23, /* strategy subtype */
//					0, // ((OpExpr *) clause)->inputcollid,	/* collation */
//					65, /* reg proc to use */
//					fromIndex + j); /* constant */
//			j++;
//			if (IsA(outerPlan, IndexScanState)) {
//				((IndexScanState*) outerPlan)->iss_NumScanKeys = 1;
//				((IndexScanState*) outerPlan)->iss_ScanKeys = xidScanKey;
//			} else if (IsA(outerPlan, IndexOnlyScanState)) {
//				((IndexOnlyScanState*) outerPlan)->ioss_NumScanKeys = 1;
//				((IndexOnlyScanState*) outerPlan)->ioss_ScanKeys = xidScanKey;
//			} else {
//				elog(ERROR, "Outer plan type is not index scan");
//			}
//			ExecReScan(outerPlan);
//			tts = ExecProcNode(outerPlan);
//			if (TupIsNull(tts)) {
//				continue;
//				// relationPage->tuples[i] = NULL;
//				// break;
//			} else {
//				relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
//				ExecCopySlot(relationPage->tuples[i], tts);
//				relationPage->tupleCount++;
//			}
//			break;
//		}
//
//	}
////	return relationPage->tupleCount;
//	return j;
//}

static void LoadPageWithTIDs(PlanState* outerPlan, struct tupleRewards* tids, RelationPage* relationPage, int index, Relation heapRelation, TupleTableSlot* tup) {
    Buffer tempBuf = InvalidBuffer;
    int i;
    int size;
    TupleTableSlot* tts = tup;
    if(relationPage == NULL) {
        elog(ERROR, "LoadNextOuterPage: null page");
    }
    relationPage->index = 0;

    relationPage->tupleCount = 0;
    size = tids[index].size;
    // Remove the old stored tuples
    for (i = 0; i < size; i++) {
        if (!TupIsNull(relationPage->tuples[i])) {
                ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
                relationPage->tuples[i] = NULL;
        }
    }

    for ( i = 0; i < size; i++) {
        while(true) {
            if(heap_fetch(heapRelation, outerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false, NULL)) {
                ExecStoreTuple(&tids[index].tuples[i],
                        tts,
                        tempBuf,
                        false);
                if (TupIsNull(tts)) {
                    ReleaseBuffer(tempBuf);
                    continue;
                } else {
                    relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
                    ExecCopySlot(relationPage->tuples[i], tts);
                    relationPage->tupleCount++;
                    ReleaseBuffer(tempBuf);
                }
                break;
            }
        }
    }
}


//static void LoadPageWithTids(ScanState *ss, PlanState *ps, RelationPage *relationPage, int pageIndex,
//		struct RewardTuples *tuples) {
//	int i;
//	Buffer tempBuf = InvalidBuffer;
//	bool isNull;
//	TupleTableSlot *slot;
//
//	if (relationPage == NULL)
//		elog(ERROR, "LoadPageWithTids: null page");
//
//	relationPage->index = 0;
//	relationPage->tupleCount = 0;
//	// Remove the old stored tuples
//	for (i = 0; i < PAGE_SIZE; i++) {
//		if (!TupIsNull(relationPage->tuples[i])) {
//			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
//			relationPage->tuples[i] = NULL;
//		}
//	}
//
//	slot = ss->ss_ScanTupleSlot;
//
//	//elog(INFO, "pos id: %d", tuples[pageIndex].htds[5].t_self.ip_posid);
//
//	for (i = 0; i < PAGE_SIZE; i++) {
//
//		if (heap_fetch(ss->ss_currentRelation, ps->state->es_snapshot, &tuples[pageIndex].htds[i], &tempBuf, false,
//				NULL)) {
//			//elog(INFO, "heap_fetch with TID done");
//			/*
//			 * store the scanned tuple in the scan tuple slot of the scan
//			 * state.  Eventually we will only do this and not return a tuple.
//			 * Note: we pass 'false' because tuples returned by amgetnext are
//			 * pointers onto disk pages and were not created with palloc() and
//			 * so should not be pfree()'d.
//			 */
//
//			ExecStoreTuple(&tuples[pageIndex].htds[i], /* tuple to store */
//			slot, /* slot to store in */
//			tempBuf, /* buffer associated with tuple  */
//			false); /* don't pfree */
//
//			//if (BufferIsValid(tempBuf)) {
//			ReleaseBuffer(tempBuf);
//			//}
//
//			//elog(INFO, "Tuple stored");
//			//elog(INFO, "l_orderkey: attr1 %d", DatumGetInt32(slot_getattr(slot, 1, &isNull)));
//			//elog(INFO, "l_partkey: attr2 %d", DatumGetInt32(slot_getattr(slot, 2, &isNull)));
//			//elog(INFO, "l_suppkey: attr3 %s", DatumGetInt32(slot_getattr(slot, 3, &isNull)));
//			//elog(INFO, "l_linenumber: attr4 %f", DatumGetInt32(slot_getattr(slot, 4, &isNull)));
//
//
//			if (!TupIsNull(slot)) {
//				relationPage->tuples[i] = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor);
//				ExecCopySlot(relationPage->tuples[i], slot);
//				relationPage->tupleCount++;
//			}
//		}
//	}
//}

//static void copyHeapTuples(HeapTupleData *htds1, HeapTupleData *htds2) {
//	int i;
//	for (i = 0; i < PAGE_SIZE; i++) {
//		*htds1 = *htds2;
//	}
//	return;
//}

static int popBestTidPageIndex(NestLoopState *node) {
	int i;
	int bestOuterPageIdx = 0;
	int bestInnerPageIdx = 0;
	int bestPageIndex = 0;

	//elog(INFO, "popbest");
	for (i = 0; i < node->activeOuterRelnPages; i++) {
		if (node->outerRewardTuples[i].reward > node->outerRewardTuples[bestOuterPageIdx].reward) {
			bestOuterPageIdx = i;
		}
	}

	for (i = 0; i < node->activeInnerRelnPages; i++) {
		if (node->innerRewardTuples[i].reward > node->innerRewardTuples[bestInnerPageIdx].reward) {
			bestInnerPageIdx = i;
		}
	}

	if (node->innerRewardTuples[bestInnerPageIdx].reward == 0 && node->outerRewardTuples[bestOuterPageIdx].reward == 0){
		if (rand() % 2 == 0){
//			elog(INFO,"pick best inner while reward = 0 !!!!!");
//			bestPageIndex = bestInnerPageIdx;
			bestPageIndex = rand() % node->activeInnerRelnPages;
			node->isBestPageOuter = false;
		}else{
//			elog(INFO,"pick best ountter while reward = 0 #####");
//			bestPageIndex = bestOuterPageIdx;
			bestPageIndex = rand() % node->activeOuterRelnPages;
			node->isBestPageOuter = true;
		}
		return bestPageIndex;
	}


	if (node->activeInnerRelnPages != 0 && node->activeOuterRelnPages != 0
			&& node->innerRewardTuples[bestInnerPageIdx].reward > node->outerRewardTuples[bestOuterPageIdx].reward) {
//		if (node->innerRewardTuples[bestInnerPageIdx].reward > 0)
//			elog(INFO,"best is inner and reward is %d",node->innerRewardTuples[bestInnerPageIdx].reward);
		bestPageIndex = bestInnerPageIdx;
		node->isBestPageOuter = false;
	} else {
//			if (node->outerRewardTuples[bestOuterPageIdx].reward > 0)
//				elog(INFO,"best is outter and reward is %d",node->outerRewardTuples[bestOuterPageIdx].reward);
		bestPageIndex = bestOuterPageIdx;
		node->isBestPageOuter = true;
	}


	//elog(INFO, "bestPageIdx: %d", bestPageIndex);
	return bestPageIndex;
}

static void storeTIDs(RelationPage* relationPage, struct tupleRewards* tids, int index, int reward) {
    int i = 0;
    tids[index].reward = reward;
    tids[index].size = relationPage->tupleCount;
    for(i = 0; i < relationPage->tupleCount; i++) {
        tids[index].tuples[i] = *relationPage->tuples[i]->tts_tuple;
    }
}

//static void storeTidsWithReward(RelationPage *relationPage, RewardTuples *rewardTuples, int pageNo, int reward) {
//	int i = 0;
//	rewardTuples[pageNo].reward = reward;
//	//elog(INFO, "reward =%d", rewardTuples[pageNo].reward);
//	//elog(INFO, "reward =%d", reward);
//	for (i = 0; i < relationPage->tupleCount; i++) {
//		rewardTuples[pageNo].htds[i] = *relationPage->tuples[i]->tts_tuple;
//		//elog(INFO, "lo =%d", rewardTuples[pageNo].htds[i].t_self.ip_blkid.bi_lo);
//		//elog(INFO, "hi =%d", rewardTuples[pageNo].htds[i].t_self.ip_blkid.bi_hi);
//		//elog(INFO, "pos =%d", rewardTuples[pageNo].htds[i].t_self.ip_posid);
//	}
//}

//static int popBestPageXid(NestLoopState *node) {
//	int i;
//	int bestPageIndex;
//	int bestXid;
//
//	bestPageIndex = 0;
//	for (i = 0; i < node->activeOuterRelnPages; i++) {
//		if (node->rewards[i] > node->rewards[bestPageIndex]) {
//			bestPageIndex = i;
//		}
//	}
//	bestXid = node->xids[bestPageIndex];
//	node->xids[bestPageIndex] = node->xids[node->activeOuterRelnPages - 1];
//	node->rewards[bestPageIndex] = node->rewards[node->activeOuterRelnPages - 1];
//	node->activeOuterRelnPages--;
//	return bestXid;
//}

static void PrintNodeCounters(NestLoopState *node) {
	elog(INFO, "Read outer pages: %d", node->outerPageCounter);
	elog(INFO, "Read inner pages: %d", node->innerPageCounter);
	elog(INFO, "Read outer tuples: %ld", node->outerTupleCounter);
	elog(INFO, "Read inner tuples: %ld", node->innerTupleCounter);
	elog(INFO, "Generated joins: %d", node->generatedJoins);
	elog(INFO, "Rescan Count: %d", node->rescanCount);
	elog(INFO, "Current XidPage: %d", node->pageIndex);
	elog(INFO, "Active Outer Relations: %d", node->activeOuterRelnPages);
	elog(INFO, "Active Inner Relations: %d", node->activeInnerRelnPages);
	elog(INFO, "Total page reads: %d", (node->outerPageCounter + node->innerPageCounterTotal));
}

static TupleTableSlot* ExecRightBanditJoin(PlanState *pstate) {
//	NestLoopState *node = castNode(NestLoopState, pstate);
//	NestLoop *nl;
//	PlanState *innerPlan;
//	PlanState *outerPlan;
//	TupleTableSlot *outerTupleSlot;
//	TupleTableSlot *innerTupleSlot;
//	ExprState *joinqual;
//	ExprState *otherqual;
//	ExprContext *econtext;
//	ListCell *lc;
//
//	CHECK_FOR_INTERRUPTS();
//
//	/*
//	 * get information from the node
//	 */
//	ENL1_printf("getting info from node");
//
//	nl = (NestLoop*) node->js.ps.plan;
//	joinqual = node->js.joinqual;
//	otherqual = node->js.ps.qual;
//	outerPlan = innerPlanState(node);
//	innerPlan = outerPlanState(node);
//	econtext = node->js.ps.ps_ExprContext;
//
//	/*
//	 * Reset per-tuple memory context to free any expression evaluation
//	 * storage allocated in the previous tuple cycle.
//	 */
//	ResetExprContext(econtext);
//
//	/*
//	 * Ok, everything is setup for the join so now loop until we return a
//	 * qualifying join tuple.
//	 */
//	ENL1_printf("entering main loop");
//
//	// if (nl->join.inner_unique)
//	// elog(WARNING, "inner relation is detected as unique");
//	//
//	if (node->innerTupleCounter == 0)
//		ExecReScan(outerPlan);
//
//	for (;;) {
//		if (node->needOuterPage) {
//			if (!node->reachedEndOfOuter && node->activeOuterRelnPages < node->sqrtOfInnerPages) {
//				// explore
//				node->isExploringOuter = true;
//				node->pageIndex++;
//				node->pageIndex = MAX(node->pageIndex, node->lastPageIndex);
//				LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
//				if (node->outerPage->tupleCount < PAGE_SIZE) {
//					elog(INFO, "Reached end of outer");
//					node->reachedEndOfOuter = true;
//					if (node->outerPage->tupleCount == 0)
//						continue;
//				}
//				node->outerTupleCounter += node->outerPage->tupleCount;
//				node->outerPageCounter++;
//				node->lastReward = 0;
//				node->exploreOuterStepCounter = 1;
//			} else if ((!node->reachedEndOfOuter && node->activeOuterRelnPages == node->sqrtOfInnerPages)
//					|| (node->reachedEndOfOuter && node->activeOuterRelnPages > 0)) {
//				// exploit
//				node->outerPage->index = 0;
//				node->isExploringOuter = false;
//				node->exploitOuterStepCounter = 0;
//				node->lastPageIndex = MAX(node->pageIndex, node->lastPageIndex);
//				node->pageIndex = popBestPageXid(node);
//				LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
//			} else {
//				// join is done
//				elog(INFO, "Join finished normally");
//				return NULL;
//
//			}
//			node->needOuterPage = false;
//			node->needInnerPage = true;
//		}
//		if (node->needInnerPage) {
//			if (node->reachedEndOfInner) {
//				// Getting ready for rescan
//				foreach(lc, nl->nestParams)
//				{
//					NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
//					int paramno = nlp->paramno;
//					ParamExecData *prm;
//
//					prm = &(econtext->ecxt_param_exec_vals[paramno]);
//					// Param value should be an OUTER_VAR var
//					Assert(IsA(nlp->paramval, Var));
//					Assert(nlp->paramval->varno == OUTER_VAR);
//					Assert(nlp->paramval->varattno > 0);
//					// prm->value = slot_getattr(outerTupleSlot,
//					prm->value = slot_getattr(node->outerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
//					// Flag parameter value as changed
//					innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
//				}
//				node->innerPageCounter = 0;
//				ExecReScan(innerPlan);
//				node->rescanCount++;
//				node->reachedEndOfInner = false;
//			}
//			LoadNextPage(innerPlan, node->innerPage);
//			if (node->innerPage->tupleCount < PAGE_SIZE) {
//				node->reachedEndOfInner = true;
//				if (node->innerPage->tupleCount == 0)
//					continue;
//			}
//			node->innerTupleCounter += node->innerPage->tupleCount;
//			node->innerPageCounter++;
//			node->innerPageCounterTotal++;
//			node->needInnerPage = false;
//		}
//		if (node->innerPage->index == node->innerPage->tupleCount) {
//			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
//				node->outerPage->index++;
//				node->innerPage->index = 0;
//			} else {
//				node->needInnerPage = true;
//				if (node->isExploringOuter && node->lastReward > 0
//						&& node->exploreOuterStepCounter < node->innerPageNumber) { //stay with current
//					node->outerPage->index = 0;
//					node->reward += node->lastReward;
//					node->lastReward = 0;
//					node->exploreOuterStepCounter++;
//				} else if (node->isExploringOuter && node->exploreOuterStepCounter == node->innerPageNumber) {
//					// we have generated all possible joins for the current output page
//					// while exploring, no need to store it
//					node->needOuterPage = true;
//				} else if (node->isExploringOuter && node->lastReward == 0) {
//					//push the current explored page
//					node->xids[node->activeOuterRelnPages] = node->pageIndex;
//					node->rewards[node->activeOuterRelnPages] = node->reward;
//					node->activeOuterRelnPages++;
//					node->needOuterPage = true;
//				} else if (!node->isExploringOuter && node->exploreOuterStepCounter < node->innerPageNumber) {
//					node->outerPage->index = 0;
//					node->exploitOuterStepCounter++;
//				} else if (!node->isExploringOuter && node->exploreOuterStepCounter == node->innerPageNumber) {
//					// Done with this outer page forever
//					node->needOuterPage = true;
//				} else {
//					elog(ERROR, "Khiarlikh...");
//				}
//				continue;
//			}
//		}
//
//		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
//		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
//		econtext->ecxt_outertuple = innerTupleSlot;
//		econtext->ecxt_innertuple = outerTupleSlot;
//		node->innerPage->index++;
//		if (TupIsNull(innerTupleSlot)) {
//			elog(WARNING, "inner tuple is null");
//			return NULL;
//		}
//		if (TupIsNull(outerTupleSlot)) {
//			if (node->activeOuterRelnPages > 0) { // still has pages in stack
//				// elog(WARNING, "Finishing join while there are active pages");
//				elog(INFO, "Null outer detected");
//				node->needOuterPage = true;
//				continue;
//			}
//			return NULL;
//		}
//
//		ENL1_printf("testing qualification");
//		if (ExecQual(joinqual, econtext)) {
//
//			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
//				ENL1_printf("qualification succeeded, projecting tuple");
//				node->lastReward++;
//				node->generatedJoins++;
//				if (node->pageIndex >= node->outerPageNumber) {
//					elog(WARNING, "pageIndex > outerPageNumber!?");
//					return NULL;
//				}
//				//TODO do this check earlier in the algorithm
////				if (list_member_int(node->pageIdJoinIdLists[node->pageIndex], node->innerPageCounter)) {
////					continue;
////				}
////				// Add current xid-innerPageCounter to result sets
////				lcons_int(node->innerPageCounter, node->pageIdJoinIdLists[node->pageIndex]);
//				return ExecProject(node->js.ps.ps_ProjInfo);
//			} else
//				InstrCountFiltered2(node, 1);
//		} else
//			InstrCountFiltered1(node, 1);
//
//		ResetExprContext(econtext);ENL1_printf("qualification failed, looping");
//	}
}

static TupleTableSlot* ExecBanditJoin(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);

	econtext = node->js.ps.ps_ExprContext;
	node->outerss = (ScanState*)outerPlan;
	node->innerss = (ScanState*)innerPlan;


	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");

	// if (nl->join.inner_unique)
	// elog(WARNING, "inner relation is detected as unique");

	for (;;) {
		if (node->needOuterPage) {
			if (node->outerTurn) {
				//elog(INFO, "NeedOuter: OuterTurn");
				if (!node->greedyExploit && !node->reachedEndOfOuter && node->activeOuterRelnPages < node->sqrtOfInnerPages) {
					// explore
					node->isExploringOuter = true;
					node->outerPageCounter++;
					LoadNextPage(outerPlan, node->outerPage);
					if (node->outerPageCounter >= node->outerPageNumber) {
						elog(INFO, "Reached end of outer");
						node->reachedEndOfOuter = true;
						if (node->outerPage->tupleCount == 0)
							continue;
					}
					node->outerTupleCounter += node->outerPage->tupleCount;
					node->lastReward = 0;
					node->exploreOuterStepCounter = 1;
					node->needOuterPage = false;
					node->needInnerPage = true;
					continue;
				} else if (node->greedyExploit || (!node->reachedEndOfOuter && node->activeOuterRelnPages == node->sqrtOfInnerPages)
						|| (node->reachedEndOfOuter && node->activeOuterRelnPages > 0)) {
					// exploit
					node->greedyExploit = false;
					node->outerPage->index = 0;
					node->isExploringOuter = false;
					node->exploitOuterStepCounter = 0;
					node->pageIndex = popBestTidPageIndex(node);
					if (node->isBestPageOuter) {
						//elog(INFO, "Best PAge Outer to be loaded");
//						LoadPageWithTids(ssOuter, outerPlan, node->outerPage, node->pageIndex, node->outerRewardTuples);
						LoadPageWithTIDs(outerPlan, node->outerRewardTuples ,node->outerPage, node->pageIndex, node->outerss->ss_currentRelation, node->outerss->ss_ScanTupleSlot);

						node->outerRewardTuples[node->pageIndex] = node->outerRewardTuples[node->activeOuterRelnPages - 1];
						node->activeOuterRelnPages--;

						node->needOuterPage = false;
						node->needInnerPage = true;
						continue;
					} else {
						//elog(INFO, "Best PAge Inner to be loaded");
						//LoadPageWithTids(ssInner, innerPlan, node->innerPage, node->pageIndex, node->innerRewardTuples);
						LoadPageWithTIDs(innerPlan, node->innerRewardTuples ,node->innerPage, node->pageIndex, node->innerss->ss_currentRelation, node->innerss->ss_ScanTupleSlot);

						node->innerRewardTuples[node->pageIndex] = node->innerRewardTuples[node->activeInnerRelnPages - 1];
						node->activeInnerRelnPages--;

						node->outerTurn = false;
						node->needOuterPage = true;
						node->needInnerPage = false;
						continue;
					}
				} else {
					// join is done
					elog(INFO, "Join finished normally");
					return NULL;
				}
			} else {
				//elog(INFO, "NeedOuter: InnerTurn");
				if (node->reachedEndOfOuter) {
					// Getting ready for rescan
					foreach(lc, nl->nestParams)
					{
						NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
						int paramno = nlp->paramno;
						ParamExecData *prm;

						prm = &(econtext->ecxt_param_exec_vals[paramno]);
						// Param value should be an INNER_VAR var
						Assert(IsA(nlp->paramval, Var));
						Assert(nlp->paramval->varno == INNER_VAR);
						Assert(nlp->paramval->varattno > 0);
						// prm->value = slot_getattr(outerTupleSlot,
						prm->value = slot_getattr(node->innerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
						// Flag parameter value as changed
						innerPlan->chgParam = bms_add_member(outerPlan->chgParam, paramno);
					}
					//node->outerPageCounter = 0;
					ExecReScan(outerPlan);
					node->rescanCount++;
					node->reachedEndOfOuter = false;
				}
				LoadNextPage(outerPlan, node->outerPage);
				if (node->outerPage->tupleCount < PAGE_SIZE) {
					node->reachedEndOfOuter = true;
					if (node->outerPage->tupleCount == 0)
						continue;
				}
//				node->outerTupleCounter += node->outerPage->tupleCount;
//				node->outerPageCounter++;
//				node->outerPageCounterTotal++;
				node->needOuterPage = false;
			}
		}
		if (node->needInnerPage) {
			if (node->outerTurn) {
				//elog(INFO, "NeedInner: OuterTurn");
				if (node->reachedEndOfInner) {
					// Getting ready for rescan
					foreach(lc, nl->nestParams)
					{
						NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
						int paramno = nlp->paramno;
						ParamExecData *prm;

						prm = &(econtext->ecxt_param_exec_vals[paramno]);
						// Param value should be an OUTER_VAR var
						Assert(IsA(nlp->paramval, Var));
						Assert(nlp->paramval->varno == OUTER_VAR);
						Assert(nlp->paramval->varattno > 0);
						// prm->value = slot_getattr(outerTupleSlot,
						prm->value = slot_getattr(node->outerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
						// Flag parameter value as changed
						innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
					}
					//node->innerPageCounter = 0;
					ExecReScan(innerPlan);
					node->rescanCount++;
					node->reachedEndOfInner = false;
				}
				LoadNextPage(innerPlan, node->innerPage);
				if (node->innerPage->tupleCount < PAGE_SIZE) {
					node->reachedEndOfInner = true;
					if (node->innerPage->tupleCount == 0)
						continue;
				}
				//node->innerTupleCounter += node->innerPage->tupleCount;
				//node->innerPageCounter++;
				//node->innerPageCounterTotal++;
				node->needInnerPage = false;
			} else {
				//elog(INFO, "NeedInner: InnerTurn");
				if (!node->greedyExploit && !node->reachedEndOfInner && node->activeInnerRelnPages < node->sqrtOfOuterPages) {
					// explore
					node->isExploringInner = true;
					node->innerPageCounter++;
					// node->pageIndex = MAX(node->pageIndex, node->lastPageIndex);
					LoadNextPage(innerPlan, node->innerPage);
					if (node->innerPageCounter >= node->innerPageNumber) {
						elog(INFO, "Reached end of outer");
						node->reachedEndOfInner = true;
						if (node->innerPage->tupleCount == 0)
							continue;
					}
					node->innerTupleCounter += node->innerPage->tupleCount;
					node->lastReward = 0;
					node->exploreInnerStepCounter = 1;
					node->needInnerPage = false;
					node->needOuterPage = true;
					continue;
				} else if (node->greedyExploit || (!node->reachedEndOfInner && node->activeInnerRelnPages == node->sqrtOfOuterPages)
						|| (node->reachedEndOfInner && node->activeInnerRelnPages > 0)) {
					// exploit
					node->greedyExploit = false;
					node->innerPage->index = 0;
					node->isExploringInner = false;
					node->exploitInnerStepCounter = 0;
					node->pageIndex = popBestTidPageIndex(node);

					if (node->isBestPageOuter) {
						//LoadPageWithTids(ssOuter, outerPlan, node->outerPage, node->pageIndex, node->outerRewardTuples);
						LoadPageWithTIDs(outerPlan, node->outerRewardTuples ,node->outerPage, node->pageIndex, node->outerss->ss_currentRelation, node->outerss->ss_ScanTupleSlot);

						node->outerRewardTuples[node->pageIndex] = node->outerRewardTuples[node->activeOuterRelnPages - 1];
						node->activeOuterRelnPages--;

						node->needOuterPage = false;
						node->needInnerPage = true;
						node->outerTurn = true;
						continue;
					} else {
						//LoadPageWithTids(ssInner, innerPlan, node->innerPage, node->pageIndex, node->innerRewardTuples);
						LoadPageWithTIDs(innerPlan, node->innerRewardTuples ,node->innerPage, node->pageIndex, node->innerss->ss_currentRelation, node->innerss->ss_ScanTupleSlot);

						node->innerRewardTuples[node->pageIndex] = node->innerRewardTuples[node->activeInnerRelnPages - 1];
						node->activeInnerRelnPages--;

						node->needOuterPage = true;
						node->needInnerPage = false;
						continue;
					}
				} else {
					// join is done
					elog(INFO, "Join finished normally");
					return NULL;
				}
			}
		}
		if (node->outerTurn && node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				if (node->isExploringOuter
						&& ((node->lastReward > 0 && node->exploreOuterStepCounter < node->innerPageNumber)
								|| (node->lastReward == 0 && (++node->nFailure) < N_FAILURE))) { //stay with current
					node->outerPage->index = 0;
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreOuterStepCounter++;
					node->needInnerPage = true;
					//elog(INFO, " outter reward > 0");
				} else if (node->isExploringOuter && node->exploreOuterStepCounter == node->innerPageNumber) {
					// we have generated all possible joins for the current output page
					// while exploring, no need to store it
					node->needOuterPage = true;
				} else if (node->isExploringOuter && node->lastReward == 0 && (++node->nFailure) >= N_FAILURE) {
					//push the current explored page
					//elog(INFO, "Outer Page: %d, Reward: %d", node->activeOuterRelnPages, node->reward);
					//storeTidsWithReward(node->outerPage, node->outerRewardTuples, node->activeOuterRelnPages,
					//		node->reward);
					storeTIDs(node->outerPage, node->outerRewardTuples, node->activeOuterRelnPages, node->reward);
					node->nFailure = 0;
					node->activeOuterRelnPages++;
					if(GREEDY && node->reward > 0){
						//elog(INFO, "greedy outter");
						node->greedyExploit = true;
						node->needOuterPage = true;
					}else{
						node->outerTurn = false;
						node->outerPage->index = 0;
						node->needInnerPage = true;
					}
					node->reward = 0;
				} else if (!node->isExploringOuter && node->exploitOuterStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->needInnerPage = true;
					node->exploitOuterStepCounter++;
				} else if (!node->isExploringOuter && node->exploitOuterStepCounter == node->innerPageNumber) {
					// Done with this outer page forever
					node->needOuterPage = true;
				} else {
					elog(ERROR, "Khiarlikh...");
				}
				continue;
			}
		}

		if (!node->outerTurn && node->outerPage->index == node->outerPage->tupleCount) {
			if (node->innerPage->index < node->innerPage->tupleCount - 1) {
				node->innerPage->index++;
				node->outerPage->index = 0;
			} else {
				if (node->isExploringInner &&
						((node->lastReward > 0 && node->exploreInnerStepCounter < node->outerPageNumber)
								|| (node->lastReward == 0 && (++node->nFailure) < N_FAILURE))) { //stay with current
					node->innerPage->index = 0;
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreInnerStepCounter++;
					node->needOuterPage = true;
					//elog(INFO, " inner reward > 0");
				} else if (node->isExploringInner && node->exploreInnerStepCounter == node->outerPageNumber) {
					// we have generated all possible joins for the current output page
					// while exploring, no need to store it
					node->needInnerPage = true;
				} else if (node->isExploringInner && node->lastReward == 0 && (++node->nFailure) >= N_FAILURE) {
					//push the current explored page
					//elog(INFO, "Inner Page: %d, Reward: %d", node->activeInnerRelnPages, node->reward);
					//storeTidsWithReward(node->innerPage, node->innerRewardTuples, node->activeInnerRelnPages,
					//		node->reward);
					storeTIDs(node->innerPage, node->innerRewardTuples, node->activeInnerRelnPages, node->reward);
					node->nFailure = 0;
					node->activeInnerRelnPages++;
					if(GREEDY && node->reward > 0){
						//elog(INFO, "greedy innner");
						node->greedyExploit = true;
						node->needInnerPage = true;
					}else{
						node->outerTurn = true;
						node->needOuterPage = true;
						node->innerPage->index = 0;
					}
					node->reward = 0;
				} else if (!node->isExploringInner && node->exploitInnerStepCounter < node->outerPageNumber) {
					node->innerPage->index = 0;
					node->needOuterPage = true;
					node->exploitInnerStepCounter++;
				} else if (!node->isExploringInner && node->exploitInnerStepCounter == node->outerPageNumber) {
					// Done with this outer page forever
					node->needInnerPage = true;
				} else {
					elog(ERROR, "Khiarlikh...");
				}
				continue;
			}
		}
		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		econtext->ecxt_outertuple = outerTupleSlot;
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
		econtext->ecxt_innertuple = innerTupleSlot;
		if (node->outerTurn) {
			//elog(INFO, "Outer Turn: outer: %d, inner: %d", node->outerPage->index, node->innerPage->index);
			node->innerPage->index++;
			if (TupIsNull(innerTupleSlot)) {
				elog(WARNING, "inner tuple is null");
				return NULL;
			}
			if (TupIsNull(outerTupleSlot)) {
				if (node->activeOuterRelnPages > 0) { // still has pages in stack
					// elog(WARNING, "Finishing join while there are active pages");
					elog(INFO, "Null outer detected");
					node->needOuterPage = true;
					continue;
				}
				return NULL;
			}
		} else {
			//elog(INFO, "Inner Turn: inner: %d, outer: %d", node->innerPage->index, node->outerPage->index);
			node->outerPage->index++;
			if (TupIsNull(outerTupleSlot)) {
				elog(WARNING, "outer tuple is null");
				return NULL;
			}
			if (TupIsNull(innerTupleSlot)) {
				if (node->activeInnerRelnPages > 0) { // still has pages in stack
					// elog(WARNING, "Finishing join while there are active pages");
					elog(INFO, "Null inner detected");
					node->needInnerPage = true;
					continue;
				}
				return NULL;
			}
		}

		ENL1_printf("testing qualification");
//		if (otherqual == NULL) {
//			elog(INFO, "otherqual null");
//		}
//		if (joinqual == NULL) {
//			elog(INFO, "joinqual null");
//		}

		if (ExecQual(joinqual, econtext)) {

			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				node->generatedJoins++;
				//elog(INFO, "lastReward %d", node->lastReward);
//				if (node->pageIndex >= node->outerPageNumber) {
//					elog(WARNING, "pageIndex > outerPageNumber!?");
//					return NULL;
//				}
				//TODO do this check earlier in the algorithm
//				if (list_member_int(node->pageIdJoinIdLists[node->pageIndex], node->innerPageCounter)) {
//					continue;
//				}
//				// Add current xid-innerPageCounter to result sets
//				lcons_int(node->innerPageCounter, node->pageIdJoinIdLists[node->pageIndex]);
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecRightBlockNestedLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	// ListCell   *lc;

	CHECK_FOR_INTERRUPTS();ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext);ENL1_printf("entering main loop");

	if (nl->join.inner_unique)
		elog(WARNING, "inner relation is detected as unique");

	if (node->innerTupleCounter == 0)
		ExecReScan(outerPlan);

	for (;;) {
		if (node->needOuterPage) {
			if (node->reachedEndOfOuter) {
				RemoveRelationPage(&(node->outerPage));
				elog(INFO, "Join Done");
				return NULL;
			}
			node->outerPage = CreateRelationPage();
			LoadNextPage(outerPlan, node->outerPage);
			node->outerTupleCounter += node->outerPage->tupleCount;
			node->outerPageCounter++;
			node->needOuterPage = false;
			if (node->outerPage->tupleCount < PAGE_SIZE) {
				node->reachedEndOfOuter = true;
				if (node->outerPage->tupleCount == 0)
					continue;
			}
			ExecReScan(innerPlan);
			node->needInnerPage = true;
			node->rescanCount++;
		}
		if (node->needInnerPage) {
			LoadNextPage(innerPlan, node->innerPage);
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
		}
		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else { // mini join is done
				if (node->innerPage->tupleCount < PAGE_SIZE) { // was last inner page of the iteration
					node->needOuterPage = true;
				} else {
					node->outerPage->index = 0;
					node->needInnerPage = true;
				}
			}
			continue;
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];

		if (TupIsNull(innerTupleSlot)) {
			elog(ERROR, "Inner slot is null");
		}
		if (TupIsNull(outerTupleSlot)) {
			elog(INFO, "outer tuples: %d", node->outerPage->tupleCount);
			elog(INFO, "outer index: %d", node->outerPage->index);
			elog(INFO, "inner tuples: %d", node->innerPage->tupleCount);
			elog(INFO, "inner index: %d", node->innerPage->index);
			elog(INFO, "===============");
			PrintNodeCounters(node);
			elog(ERROR, "Outer slot is null");
		}
		// The next lines are reversed because planner is not aware of flipping inner and outer
		// and is expecting ecxt_outertuple to math the relation on left of the join
		econtext->ecxt_outertuple = innerTupleSlot;
		econtext->ecxt_innertuple = outerTupleSlot;
		node->innerPage->index++;

		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext)) {
			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->generatedJoins++;
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);
		ResetExprContext(econtext);ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecBlockNestedLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext);ENL1_printf("entering main loop");

	if (nl->join.inner_unique)
		elog(WARNING, "inner relation is detected as unique");

	for (;;) {
		if (node->needOuterPage) {
			if (node->reachedEndOfOuter) {
				RemoveRelationPage(&(node->outerPage));
				elog(INFO, "Join Done");
				return NULL;
			}
			//node->outerPage = CreateRelationPage(); //xm
			LoadNextPage(outerPlan, node->outerPage);
			node->outerTupleCounter += node->outerPage->tupleCount;
			node->outerPageCounter++;
			node->needOuterPage = false;
			if (node->outerPage->tupleCount < PAGE_SIZE) {
				node->reachedEndOfOuter = true;
				if (node->outerPage->tupleCount == 0)
					continue;
			}
		}
		if (node->needInnerPage) {
			LoadNextPage(innerPlan, node->innerPage);
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
			if (node->innerPage->tupleCount < PAGE_SIZE) { // done with one outer page, move to next
				foreach(lc, nl->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
					int paramno = nlp->paramno;
					ParamExecData *prm;

					prm = &(econtext->ecxt_param_exec_vals[paramno]);
					/* Param value should be an OUTER_VAR var */
					Assert(IsA(nlp->paramval, Var));
					Assert(nlp->paramval->varno == OUTER_VAR);
					Assert(nlp->paramval->varattno > 0);
					prm->value = slot_getattr(node->outerPage->tuples[node->outerPage->index], nlp->paramval->varattno,
							&(prm->isnull));
					/* Flag parameter value as changed */
					innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
				}ENL1_printf("rescanning inner plan");
				ExecReScan(innerPlan);
				node->rescanCount++;
				node->needOuterPage = true;
				if (node->innerPage->tupleCount == 0) {
					node->needInnerPage = true;
					continue;
				}
				// node->needInnerPage = true;
				// RemoveRelationPage(&(node->outerPage));
				// node->needOuterPage = true;
				// continue;
			}
		}
		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else { // mini join is done
				node->needInnerPage = true;
				node->outerPage->index = 0;
			}
			continue;
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];

		if (TupIsNull(innerTupleSlot)) {
			elog(ERROR, "Inner slot is null");
		}
		if (TupIsNull(outerTupleSlot)) {
			elog(INFO, "outer tuples: %d", node->outerPage->tupleCount);
			elog(INFO, "outer index: %d", node->outerPage->index);
			elog(INFO, "inner tuples: %d", node->innerPage->tupleCount);
			elog(INFO, "inner index: %d", node->innerPage->index);
			elog(INFO, "===============");
			PrintNodeCounters(node);
			elog(ERROR, "Outer slot is null");
		}
		econtext->ecxt_outertuple = outerTupleSlot;
		econtext->ecxt_innertuple = innerTupleSlot;
		node->innerPage->index++;

		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext)) {
			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->generatedJoins++;
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);
		ResetExprContext(econtext);ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecRightRegularNestLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	// ListCell   *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");

	if (node->innerTupleCounter == 0) {
		ExecReScan(innerPlan);
		innerTupleSlot = ExecProcNode(innerPlan);
		econtext->ecxt_innertuple = innerTupleSlot;
		if (TupIsNull(innerTupleSlot)) {
// join is done
			return NULL;
		}
		node->innerTupleCounter++;
	}

	for (;;) {
		/*
		 * get the next outer tuple
		 */

		outerTupleSlot = ExecProcNode(outerPlan);
		if (TupIsNull(outerTupleSlot)) {
			ENL1_printf("no outer tuple, ending join");
			ExecReScan(outerPlan);
			innerTupleSlot = ExecProcNode(innerPlan);
			econtext->ecxt_innertuple = innerTupleSlot;
			if (TupIsNull(innerTupleSlot)) {
				// join is done
				return NULL;
			}
			node->innerTupleCounter++;
			continue;
		}
		node->outerTupleCounter++;
		ENL1_printf("saving new outer tuple information");
		econtext->ecxt_outertuple = outerTupleSlot;

		if (ExecQual(joinqual, econtext)) {
			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENL1_printf("qualification succeeded, projecting tuple");

				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecRegularNestLoop(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*
	 * Reset per-tuple memory context to free any expression evaluation
	 * storage allocated in the previous tuple cycle.
	 */
	ResetExprContext(econtext);

	/*
	 * Ok, everything is setup for the join so now loop until we return a
	 * qualifying join tuple.
	 */
	ENL1_printf("entering main loop");

	for (;;) {
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter) {
			ENL1_printf("getting new outer tuple");

			outerTupleSlot = ExecProcNode(outerPlan);
			node->outerTupleCounter++;

			/*
			 * if there are no more outer tuples, then the join is complete..
			 */
			if (TupIsNull(outerTupleSlot)) {
				ENL1_printf("no outer tuple, ending join");
				return NULL;
			}

			ENL1_printf("saving new outer tuple information");
			econtext->ecxt_outertuple = outerTupleSlot;
			node->nl_NeedNewOuter = false;
			node->nl_MatchedOuter = false;

			/*
			 * fetch the values of any outer Vars that must be passed to the
			 * inner scan, and store them in the appropriate PARAM_EXEC slots.
			 */
			foreach(lc, nl->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
				int paramno = nlp->paramno;
				ParamExecData *prm;

				prm = &(econtext->ecxt_param_exec_vals[paramno]);
				/* Param value should be an OUTER_VAR var */
				Assert(IsA(nlp->paramval, Var));
				Assert(nlp->paramval->varno == OUTER_VAR);
				Assert(nlp->paramval->varattno > 0);
				prm->value = slot_getattr(outerTupleSlot, nlp->paramval->varattno, &(prm->isnull));
				/* Flag parameter value as changed */
				innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
			}

			/*
			 * now rescan the inner plan
			 */
			ENL1_printf("rescanning inner plan");
			ExecReScan(innerPlan);
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		innerTupleSlot = ExecProcNode(innerPlan);
		node->innerTupleCounter++;
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot)) {
			ENL1_printf("no inner tuple, need new outer tuple");

			node->nl_NeedNewOuter = true;

			if (!node->nl_MatchedOuter && (node->js.jointype == JOIN_LEFT || node->js.jointype == JOIN_ANTI)) {
				/*
				 * We are doing an outer join and there were no join matches
				 * for this outer tuple.  Generate a fake join tuple with
				 * nulls for the inner tuple, and return it if it passes the
				 * non-join quals.
				 */
				econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

				ENL1_printf("testing qualification for outer-join tuple");

				if (otherqual == NULL || ExecQual(otherqual, econtext)) {
					/*
					 * qualification was satisfied so we project and return
					 * the slot containing the result tuple using
					 * ExecProject().
					 */
					ENL1_printf("qualification succeeded, projecting tuple");

					return ExecProject(node->js.ps.ps_ProjInfo);
				} else
					InstrCountFiltered2(node, 1);
			}

			/*
			 * Otherwise just return to top of loop for a new outer tuple.
			 */
			continue;
		}

		/*
		 * at this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		ENL1_printf("testing qualification");

		if (ExecQual(joinqual, econtext)) {
			node->nl_MatchedOuter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_ANTI) {
				node->nl_NeedNewOuter = true;
				continue; /* return to top of loop */
			}

			/*
			 * If we only need to join to the first matching inner tuple, then
			 * consider returning this one, but after that continue with next
			 * outer tuple.
			 */
			if (node->js.single_match)
				node->nl_NeedNewOuter = true;

			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENL1_printf("qualification succeeded, projecting tuple");

				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecNestLoop(PlanState *pstate) {
	TupleTableSlot *tts;
	const char *fastjoin = GetConfigOption("enable_fastjoin", false, false);
	const char *blocknestloop = GetConfigOption("enable_block", false, false);
	const char *fliporder = GetConfigOption("enable_fliporder", false, false);
	if (strcmp(fastjoin, "on") == 0) {
		if (strcmp(fliporder, "on") == 0) {
			tts = ExecRightBanditJoin(pstate);
		} else {
			tts = ExecBanditJoin(pstate);
		}
	} else if (strcmp(blocknestloop, "on") == 0) {
		if (strcmp(fliporder, "on") == 0) {
			tts = ExecRightBlockNestedLoop(pstate);
		} else {
			tts = ExecBlockNestedLoop(pstate);
		}
	} else {
		if (strcmp(fliporder, "on") == 0) {
			tts = ExecRightRegularNestLoop(pstate);
		} else {
			tts = ExecRegularNestLoop(pstate);
		}
	}
	return tts;
}

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState* ExecInitNestLoop(NestLoop *node, EState *estate, int eflags) {
	NestLoopState *nlstate;
	const char *fastjoin;
	const char *blocknestloop;
	const char *fliporder;
	int i;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			"initializing node");

	/*
	 * create state structure
	 */
	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan*) node;
	nlstate->js.ps.state = estate;
	nlstate->js.ps.ExecProcNode = ExecNestLoop;

	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &nlstate->js.ps);

	/*
	 * initialize child nodes
	 *
	 * If we have no parameters to pass into the inner rel from the outer,
	 * tell the inner child that cheap rescans would be good.  If we do have
	 * such parameters, then there is no point in REWIND support at all in the
	 * inner child, because it will always be rescanned with fresh parameter
	 * values.
	 */
	outerPlanState(nlstate) = ExecInitNode(outerPlan(node), estate, eflags);
	if (node->nestParams == NIL) {
		eflags |= EXEC_FLAG_REWIND;
		elog(INFO, "nestParams NIL");
	} else
		eflags &= ~EXEC_FLAG_REWIND;
	innerPlanState(nlstate) = ExecInitNode(innerPlan(node), estate, eflags);

	/*
	 * Initialize result slot, type and projection.
	 */
	ExecInitResultTupleSlotTL(estate, &nlstate->js.ps);
	ExecAssignProjectionInfo(&nlstate->js.ps, NULL);

	/*
	 * initialize child expressions
	 */
	if (node->join.plan.qual == NIL) {
		elog(INFO, "join.plan.qual NIL");
	}
	if (node->join.joinqual == NIL) {
		elog(INFO, "join.joinqual NIL");
	}
	nlstate->js.ps.qual = ExecInitQual(node->join.plan.qual, (PlanState*) nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual = ExecInitQual(node->join.joinqual, (PlanState*) nlstate);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	nlstate->js.single_match = (node->join.inner_unique || node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype) {
	case JOIN_INNER:
	case JOIN_SEMI:
		break;
	case JOIN_LEFT:
	case JOIN_ANTI:
		nlstate->nl_NullInnerTupleSlot = ExecInitNullTupleSlot(estate, ExecGetResultType(innerPlanState(nlstate)));
		break;
	default:
		elog(ERROR, "unrecognized join type: %d", (int ) node->join.jointype);
	}

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

	/* Extra inits for bandit join*/
	fliporder = GetConfigOption("enable_fliporder", false, false);
	nlstate->activeOuterRelnPages = 0;
	nlstate->isExploringOuter = true;
	nlstate->lastReward = 0;
	nlstate->needOuterPage = true;
	nlstate->needInnerPage = true;
	nlstate->outerTurn = true;
	nlstate->exploitOuterStepCounter = 0;
	nlstate->innerPageCounter = 0;
	nlstate->innerPageCounterTotal = 0;
	nlstate->outerPageCounter = 0;
	nlstate->reachedEndOfOuter = false;
	nlstate->reachedEndOfInner = false;
	nlstate->innerTupleCounter = 0;
	nlstate->outerTupleCounter = 0;
	nlstate->generatedJoins = 0;
	nlstate->rescanCount = 0;
	nlstate->outerStartKeyValue = 1;
	nlstate->outerEndKeyValue = nlstate->outerStartKeyValue;
	nlstate->greedyExploit = false;
	nlstate->nFailure = 0;

	if (strcmp(fliporder, "on") == 0) {
		nlstate->outerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
	} else {
		nlstate->outerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
	}
	//TODO sometimes the inner plan_rows does not match the exact row numbers
	elog(INFO, "Outer page number: %ld", nlstate->outerPageNumber);
	elog(INFO, "Inner page number: %ld", nlstate->innerPageNumber);

	nlstate->sqrtOfOuterPages = (int) sqrt(nlstate->outerPageNumber);
	nlstate->sqrtOfInnerPages = (int) sqrt(nlstate->innerPageNumber);

//	nlstate->sqrtOfOuterPages = (int) (sqrt(nlstate->outerPageNumber)/2);
//	nlstate->sqrtOfInnerPages = (int) (sqrt(nlstate->innerPageNumber)/2);

//	nlstate->sqrtOfOuterPages = (int) (sqrt(nlstate->outerPageNumber)/5);
//	nlstate->sqrtOfInnerPages = (int) (sqrt(nlstate->innerPageNumber)/5);

//	nlstate->sqrtOfOuterPages = (int) (sqrt(nlstate->outerPageNumber)/20);
//	nlstate->sqrtOfInnerPages = (int) (sqrt(nlstate->innerPageNumber)/20);







//	int tmp;
//	if (nlstate->outerPageNumber < nlstate->innerPageNumber) tmp = nlstate->outerPageNumber;
//	else tmp = nlstate->innerPageNumber;
//	nlstate->sqrtOfOuterPages = (int) (nlstate->outerPageNumber);
//	nlstate->sqrtOfInnerPages = (int) (nlstate->outerPageNumber);
//	elog(INFO, "Outer sqrt page number: %d", nlstate->sqrtOfOuterPages);
//	elog(INFO, "Inner sqrt page number: %d", nlstate->sqrtOfInnerPages);

	//elog(INFO, "sqrtOfOuterPages: %d", nlstate->sqrtOfOuterPages);
	//elog(INFO, "sqrtOfInnerPages: %d", nlstate->sqrtOfInnerPages);

//	nlstate->xids = palloc(nlstate->sqrtOfInnerPages * sizeof(int));
//	nlstate->rewards = palloc(nlstate->sqrtOfInnerPages * sizeof(int));

	nlstate->outerRewardTuples = palloc((nlstate->sqrtOfInnerPages) * sizeof(struct tupleRewards));
	nlstate->innerRewardTuples = palloc((nlstate->sqrtOfOuterPages) * sizeof(struct tupleRewards));

    for(i = 0; i < nlstate->sqrtOfInnerPages; i++) {
        nlstate->outerRewardTuples[i].reward = 0;
    }
	for(i = 0; i < nlstate->sqrtOfOuterPages; i++) {
		nlstate->innerRewardTuples[i].reward = 0;
	}

	nlstate->pageIndex = -1;
	nlstate->lastPageIndex = 0;
//	nlstate->xidScanKey = (ScanKey) palloc(sizeof(ScanKeyData));

	nlstate->outerPage = CreateRelationPage();
	nlstate->innerPage = CreateRelationPage();

	NL1_printf("ExecInitNestLoop: %s\n",
			"node initialized");
	/*
	 elog_node_display(INFO,"Left: ", node->join.plan.lefttree, true);
	 elog_node_display(INFO,"Right: ", node->join.plan.righttree, true);
	 elog(INFO, "Computed inner page count: %ld, and sqrt: %d",
	 nlstate->innerPageNumber, nlstate->sqrtOfInnerPages);
	 */
	fastjoin = GetConfigOption("enable_fastjoin", false, false);
	blocknestloop = GetConfigOption("enable_block", false, false);
	if (strcmp(fastjoin, "on") == 0) {
		elog(INFO, "Running bandit join..");
	} else {
		if (strcmp(blocknestloop, "on") == 0) {
			elog(INFO, "Running block nested loop..");
		} else {
			elog(INFO, "Running nested loop..");
		}
	}
	if (strcmp(fliporder, "on") == 0) {
		elog(INFO, "flipping inner and outer relations");
	}
	return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void ExecEndNestLoop(NestLoopState *node) {
	NL1_printf("ExecEndNestLoop: %s\n",
			"ending node processing");

	PrintNodeCounters(node);
	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->js.ps);

	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);

	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	ExecEndNode(innerPlanState(node));

	NL1_printf("ExecEndNestLoop: %s\n",
			"node processing ended");

	// Releasing memory 
	//list_free
//	i = 0;
//	while (i < node->outerPageNumber){
//		list_free(node->pageIdJoinIdLists[i]);
//		node->pageIdJoinIdLists[i] = NULL;
//		i++;
//	}
	RemoveRelationPage(&(node->outerPage));
	RemoveRelationPage(&(node->innerPage));
//	pfree(node->xids);
//	pfree(node->rewards);
//	pfree(node->xidScanKey);
	pfree(node->outerRewardTuples);
	pfree(node->innerRewardTuples);
	//pfree(node->pageIdJoinIdLists);//TODO remove each entry?
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void ExecReScanNestLoop(NestLoopState *node) {
	PlanState *outerPlan = outerPlanState(node);
	PlanState *innerPlan = innerPlanState(node);
	const char *fliporder;
	fliporder = GetConfigOption("enable_fliporder", false, false);

	/*
	 * If outerPlan->chgParam is not null then plan will be automatically
	 * re-scanned by first ExecProcNode.
	 */
	if (outerPlan->chgParam == NULL)
		ExecReScan(outerPlan);

	/*
	 * innerPlan is re-scanned for each new outer tuple and MUST NOT be
	 * re-scanned from here or you'll get troubles from inner index scans when
	 * outer Vars are used as run-time keys...
	 */

	if (strcmp(fliporder, "on") == 0) {
		RemoveRelationPage(&(node->outerPage));
		RemoveRelationPage(&(node->innerPage));
		node->outerPage = CreateRelationPage();
		node->innerPage = CreateRelationPage();
		ExecReScan(innerPlan);
		node->innerTupleCounter = 0;
	}
}

