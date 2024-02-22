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

/*
Implicit Co-Learning
*/

#include "postgres.h"

#include <math.h>

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
#define MIN(a,b) ((a) <= (b) ? (a) : (b))

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

static int LoadNextOuterPage(PlanState *outerPlan, RelationPage *relationPage, ScanKey xidScanKey, int fromPageIndex) {
	int i;
	TupleTableSlot *tts;
	int fromXid;
	fromXid = fromPageIndex * PAGE_SIZE + 1;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextOuterPage: null page");
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
		ScanKeyEntryInitialize(xidScanKey, //TODO is it fine to init ScanKey once?
				0, // flags
				1, /* attribute number to scan */
				3, /* op's strategy */
				23, /* strategy subtype */
				0, // ((OpExpr *) clause)->inputcollid,	/* collation */
				65, /* reg proc to use */
				fromXid + i); /* constant */
		if (IsA(outerPlan, IndexScanState)) {
			((IndexScanState*) outerPlan)->iss_NumScanKeys = 1;
			((IndexScanState*) outerPlan)->iss_ScanKeys = xidScanKey;
		} else if (IsA(outerPlan, IndexOnlyScanState)) {
			((IndexOnlyScanState*) outerPlan)->ioss_NumScanKeys = 1;
			((IndexOnlyScanState*) outerPlan)->ioss_ScanKeys = xidScanKey;
		} else {
			elog(ERROR, "Outer plan type is not index scan");
		}
		ExecReScan(outerPlan);
		tts = ExecProcNode(outerPlan);
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

static void LoadPageWithTIDs(PlanState *outerPlan, struct tupleRewards *tids, RelationPage *relationPage, int index, Relation heapRelation, TupleTableSlot *tup) {
	Buffer tempBuf = InvalidBuffer;
	int i = 0;
	TupleTableSlot *tts = tup;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextOuterPage: null page");
	}

	relationPage->index = 0;
	relationPage->tupleCount = 0;
	int size = tids[index].size;

	// Remove the old stored tuples
	for (i = 0; i < size; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	
	for (i = 0; i < size; i++) {
		while (true) {
			if (heap_fetch(heapRelation, outerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false,
					NULL)) {
				ExecStoreTuple(&tids[index].tuples[i], tts, tempBuf,
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

static void LoadOuterPageFromStorage(PlanState *outerPlan, struct tuplePages *tids, RelationPage *relationPage, int index, Relation heapRelation, TupleTableSlot *tup){
	Buffer tempBuf = InvalidBuffer;
	int i = 0;
	TupleTableSlot *tts = tup;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextOuterPage: null page");
	}

	relationPage->index = 0;
	relationPage->tupleCount = 0;
	int size = tids[index].size;

	// Remove the old stored tuples
	for (i = 0; i < size; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	
	for (i = 0; i < size; i++) {
		while (true) {
			if (heap_fetch(heapRelation, outerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false,
					NULL)) {
				ExecStoreTuple(&tids[index].tuples[i], tts, tempBuf,
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

static void LoadInnerPageFromStorage(PlanState *innerPlan, struct tuplePages *tids, RelationPage *relationPage, int index, Relation heapRelation, TupleTableSlot *tup) {
	Buffer tempBuf = InvalidBuffer;
	int i = 0;
	TupleTableSlot *tts = tup;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextInnerPage: null page");
	}

	relationPage->index = 0;
	relationPage->tupleCount = 0;
	int size = tids[index].size;

	// Remove the old stored tuples
	for (i = 0; i < size; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	
	for (i = 0; i < size; i++) {
		while (true) {
			
			if (heap_fetch(heapRelation, innerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false, NULL)) {
				
				ExecStoreTuple(&tids[index].tuples[i], tts, tempBuf, false);
				
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

static void LoadPageWithTIDsInner(PlanState *innerPlan, struct pageInfoS *tids, RelationPage *relationPage, int index, Relation heapRelation, TupleTableSlot *tup) {
	Buffer tempBuf = InvalidBuffer;
	int i = 0;
	TupleTableSlot *tts = tup;
	if (relationPage == NULL) {
		elog(ERROR, "LoadNextInnerPage: null page");
	}

	relationPage->index = 0;
	relationPage->tupleCount = 0;
	int size = tids[index].size;

	// Remove the old stored tuples
	for (i = 0; i < size; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	
	for (i = 0; i < size; i++) {
		while (true) {
			if (heap_fetch(heapRelation, innerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false, NULL)) {
				ExecStoreTuple(&tids[index].tuples[i], tts, tempBuf, false);
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

static int popBestPageXid(NestLoopState *node) {
	int i;
	int bestPageIndex;
	int bestXid;

	bestPageIndex = 0;
	for (i = 0; i < node->activeRelationPages; i++) {
		if (node->rewards[i] > node->rewards[bestPageIndex]) {
			bestPageIndex = i;
		}
	}
	elog(INFO, "reward: %d", node->rewards[bestPageIndex]);
	bestXid = node->xids[bestPageIndex];
	node->xids[bestPageIndex] = node->xids[node->activeRelationPages - 1];
	node->rewards[bestPageIndex] = node->rewards[node->activeRelationPages - 1];
	node->activeRelationPages--;
	return bestXid;
}

// This method linearly search all the rewards of explored groups from outer relation in tidRewards table to find the best among them
static int popBestPage(NestLoopState *node) {
	int i;
	int bestPageIndex = 0;

	for (i = 0; i < node->activeRelationPages; i++) {
		if (node->tidRewards[i].reward > node->tidRewards[bestPageIndex].reward) {
			bestPageIndex = i;
		}
	}
	return bestPageIndex;
}

// This method linearly search all the rewards of explored groups from inner relation in exploitBuffer table to find the best among them
static int popBestInnerPage(NestLoopState *node){
	int i; 
	int bestPageIndex = 0;

	for(i=0; i<node->exploitBufferIndex; i++){
		if(node->exploitBuffer[i].reward > node->exploitBuffer[bestPageIndex].reward){
			bestPageIndex = i;
		}
	}
	return bestPageIndex;
}

static void storeTIDs(RelationPage *relationPage, struct tupleRewards *tids, int index, int reward) {
	int i = 0;
	tids[index].reward = reward;
	tids[index].size = relationPage->tupleCount;
	for (i = 0; i < relationPage->tupleCount; i++) {
		tids[index].tuples[i] = *relationPage->tuples[i]->tts_tuple;
	}
}

static void storeTIDsExtended(RelationPage *relationPage, struct tupleRewards *tids, int index, int reward, int pageId, int start, int stop){
	int i = 0;
	tids[index].reward = reward;
	tids[index].page_id = pageId;
	tids[index].fromInner = start;
	tids[index].toInner = stop;
	tids[index].size = relationPage->tupleCount;
	for (i = 0; i < relationPage->tupleCount; i++) {
		tids[index].tuples[i] = *relationPage->tuples[i]->tts_tuple;
	}
}
					
static void PrintNodeCounters(NestLoopState *node) {
	elog(INFO, "Read outer pages: %d", node->outerPageCounter);
	elog(INFO, "Read inner pages: %d", node->innerPageCounterTotal);
	elog(INFO, "Read outer tuples: %ld", node->outerTupleCounter);
	elog(INFO, "Read inner tuples: %ld", node->innerTupleCounter);
	elog(INFO, "Generated joins: %d", node->generatedJoins);
	elog(INFO, "Rescan Count: %d", node->rescanCount);
	elog(INFO, "Current XidPage: %d", node->pageIndex);
	elog(INFO, "Active Relations: %d", node->activeRelationPages);
	elog(INFO, "Total page reads: %d", (node->outerPageCounter + node->innerPageCounterTotal));
}

static TupleTableSlot* ExecRightBanditJoin(PlanState *pstate) {
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
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
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

	// if (nl->join.inner_unique)
	// elog(WARNING, "inner relation is detected as unique");
	//
	if (node->innerTupleCounter == 0)
		ExecReScan(outerPlan);

	for (;;) {
		if (node->needOuterPage) {
			if (!node->reachedEndOfOuter && node->activeRelationPages < node->sqrtOfInnerPages) {
				// explore
				node->isExploring = true;
				node->pageIndex++;
				node->pageIndex = MAX(node->pageIndex, node->lastPageIndex);
				LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
				if (node->outerPage->tupleCount < PAGE_SIZE) {
					elog(INFO, "Reached end of outer");
					node->reachedEndOfOuter = true;
					if (node->outerPage->tupleCount == 0)
						continue;
				}
				node->outerTupleCounter += node->outerPage->tupleCount;
				node->outerPageCounter++;
				node->lastReward = 0;
				node->exploreStepCounter = 1;
			} else if ((!node->reachedEndOfOuter && node->activeRelationPages == node->sqrtOfInnerPages)
					|| (node->reachedEndOfOuter && node->activeRelationPages > 0)) {
				// exploit
				node->outerPage->index = 0;
				node->isExploring = false;
				node->exploitStepCounter = 0;
				node->lastPageIndex = MAX(node->pageIndex, node->lastPageIndex);
				node->pageIndex = popBestPageXid(node);
				LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
			} else {
				// join is done
				elog(INFO, "Join finished normally");
				return NULL;

			}
			node->needOuterPage = false;
			node->needInnerPage = true;
		}


		if (node->needInnerPage) {
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
				node->innerPageCounter = 0;
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
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
		}



		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				node->needInnerPage = true;
				if (node->isExploring && node->lastReward > 0 && node->exploreStepCounter < node->innerPageNumber) { //stay with current
					node->outerPage->index = 0;
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreStepCounter++;
				} else if (node->isExploring && node->exploreStepCounter == node->innerPageNumber) {
					// we have generated all possible joins for the current output page
					// while exploring, no need to store it
					node->needOuterPage = true;
				} else if (node->isExploring && node->lastReward == 0) {
					//push the current explored page
					node->xids[node->activeRelationPages] = node->pageIndex;
					node->rewards[node->activeRelationPages] = node->reward;
					node->activeRelationPages++;
					node->needOuterPage = true;
				} else if (!node->isExploring && node->exploitStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->exploitStepCounter++;
				} else if (!node->isExploring && node->exploitStepCounter == node->innerPageNumber) {
					// Done with this outer page forever
					node->needOuterPage = true;
				} else {
					elog(ERROR, "Khiarlikh...");
				}
				continue;
			}
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
		econtext->ecxt_outertuple = innerTupleSlot;
		econtext->ecxt_innertuple = outerTupleSlot;
		node->innerPage->index++;
		if (TupIsNull(innerTupleSlot)) {
			elog(WARNING, "inner tuple is null");
			return NULL;
		}
		if (TupIsNull(outerTupleSlot)) {
			if (node->activeRelationPages > 0) { // still has pages in stack
				// elog(WARNING, "Finishing join while there are active pages");
				elog(INFO, "Null outer detected");
				node->needOuterPage = true;
				continue;
			}
			return NULL;
		}

		ENL1_printf("testing qualification");
		if (ExecQual(joinqual, econtext)) {

			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				node->generatedJoins++;
				if (node->pageIndex >= node->outerPageNumber) {
					elog(WARNING, "pageIndex > outerPageNumber!?");
					return NULL;
				}
				//TODO do this check earlier in the algorithm
//				if (list_member_int(node->pageIdJoinIdLists[node->pageIndex], node->innerPageCounter)) {	//mx
//					continue;
//				}
//				// Add current xid-innerPageCounter to result sets
//				lcons_int(node->innerPageCounter, node->pageIdJoinIdLists[node->pageIndex]);
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
	}
}

/*
* To check in the explore buffer and exploit buffer to check wheher the inner page in hand is already in the buffer or not
*/
static int checkBuffer(struct pageInfoS *innerPageBuffer, int bufferSize, int current_page_id){
	int index; 
	int page_found = -1;

	for (index=0; index<bufferSize; index++){
		if (innerPageBuffer[index].page_id == current_page_id){	
			page_found = index;
			break;	
		}
	}
	return page_found;
}

/*
* This method works on storing the inner pages in different buffers
* When an inner page is just explored, its' information is stored in one buffer that contains all the inner pages being explored
* When an inner page experiences sufficient exploration it becomes exploitation candidate and stored in the buffer of exploitation candidates
* When an inner page is exploited, it is transferred to the buffer that contains pages that are exploited
*/
static void InnerPageToBuffer(NestLoopState *node){
	int i;
	int page_found_in_buffer = false;
	int page_in_exploit_buffer = false;
	int insertedSlot = -1;
	
	page_found_in_buffer = checkBuffer(node->exploreBuffer, node->exploreBufferIndex, node->innerPageCounter);
	
	if (page_found_in_buffer > -1){
		node->exploreBuffer[page_found_in_buffer].reward += node->lastReward;
		node->exploreBuffer[page_found_in_buffer].explore_counter += 1;
		if (node->lastReward == 0){
			node->exploreBuffer[page_found_in_buffer].failure_counter += 1;
		}
		insertedSlot = page_found_in_buffer;
	}else{	
		page_in_exploit_buffer = checkBuffer(node->exploitBuffer, node->exploitBufferIndex, node->innerPageCounter);

		if (page_in_exploit_buffer > -1){
			//elog(INFO,"Page is in the exploit buffer\n");
		}else{
			int y;
			bool pageAlreadyExploited = false;
			for(y=0; y<node->exploitedInnerIndex; y++){
				if(node->innerPageCounter == node->exploitedInnerPages[y]){
					pageAlreadyExploited = true;
					//elog(INFO, "Inner Page %d is already used for full join\n", node->innerPageCounter);
					break;
				}
			}

			// page is not in the exploit buffer and exploied buffer
			// store the page in the explore buffer for the first time
			if (pageAlreadyExploited == false){
				if(node->exploreBufferIndex < node->exploreBufferSize){
					int insertIndex = node->exploreBufferIndex++;
					
					node->exploreBuffer[insertIndex].page_id = node->innerPageCounter;
					node->exploreBuffer[insertIndex].reward = node->lastReward;
					node->exploreBuffer[insertIndex].explore_counter = 1;
					node->exploreBuffer[insertIndex].size = node->innerPage->tupleCount;

					if (node->lastReward == 0){
						node->exploreBuffer[insertIndex].failure_counter = 1;
					}else{
						node->exploreBuffer[insertIndex].failure_counter = 0;
					}

					int idx;
					for(idx = 0; idx < node->innerPage->tupleCount; idx++){
						node->exploreBuffer[insertIndex].tuples[idx] = *node->innerPage->tuples[idx]->tts_tuple;
					}
					insertedSlot = insertIndex;
				}
			}
		}	
	}

	// if the number of failure of the page just inserted/updated in the explore buffer is N it is transferred to the exploit buffer
	// the slot that carried that page will be opened now and will be available for a new page to be explored
	if(insertedSlot != -1){

		if(node->exploreBuffer[insertedSlot].failure_counter == N_FAILURE_INNER && node->exploitBufferIndex < node->exploitBufferSize){ 

			node->maxExplorePageIdS += 1; //explorable page id increases	

			//transfering all 4 info from explore to exploit
			node->exploitBuffer[node->exploitBufferIndex].page_id = node->exploreBuffer[insertedSlot].page_id;
			node->exploitBuffer[node->exploitBufferIndex].reward = node->exploreBuffer[insertedSlot].reward;
			node->exploitBuffer[node->exploitBufferIndex].explore_counter = node->exploreBuffer[insertedSlot].explore_counter;
			node->exploitBuffer[node->exploitBufferIndex].failure_counter = node->exploreBuffer[insertedSlot].failure_counter;
			node->exploitBuffer[node->exploitBufferIndex].size = node->exploreBuffer[insertedSlot].size;
			
			int idx;
			for(idx = 0; idx < node->exploreBuffer[insertedSlot].size; idx++){
				node->exploitBuffer[node->exploitBufferIndex].tuples[idx] = node->exploreBuffer[insertedSlot].tuples[idx];
				
			}

			node->exploitBufferIndex++;
			
			//replacing all the page information of the current slot by the information of the last page of the buffer
			node->exploreBuffer[insertedSlot].page_id = node->exploreBuffer[node->exploreBufferIndex-1].page_id;
			node->exploreBuffer[insertedSlot].reward = node->exploreBuffer[node->exploreBufferIndex-1].reward;
			node->exploreBuffer[insertedSlot].explore_counter = node->exploreBuffer[node->exploreBufferIndex-1].explore_counter;
			node->exploreBuffer[insertedSlot].failure_counter = node->exploreBuffer[node->exploreBufferIndex-1].failure_counter;
			node->exploreBuffer[insertedSlot].size = node->exploreBuffer[node->exploreBufferIndex-1].size; 

			for(idx = 0; idx < node->exploreBuffer[node->exploreBufferIndex-1].size; idx++){
				node->exploreBuffer[insertedSlot].tuples[idx] = node->exploreBuffer[node->exploreBufferIndex-1].tuples[idx];
			}

			node->exploreBufferIndex--;
		}
	}
}

static void LoadInStorage(RelationPage *relationPage, struct tuplePages *pageStorage, int index, int pageNumber){
	int i;

	for(i = 0; i < relationPage->tupleCount; ++i){
		pageStorage[index].tuples[i] = *relationPage->tuples[i]->tts_tuple;
	}

	pageStorage[index].page_id = pageNumber;
	pageStorage[index].size = relationPage->tupleCount;
}


static TupleTableSlot* ExecBanditJoin(PlanState *pstate) {
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop *nl;
	PlanState *innerPlan;
	PlanState *outerPlan;
	TupleTableSlot *outerTupleSlot; //Tuple info of outer relation
	TupleTableSlot *innerTupleSlot; //Tuple info of inner relation
	ExprState *joinqual;
	ExprState *otherqual;
	ExprContext *econtext;
	ListCell *lc;

	CHECK_FOR_INTERRUPTS();

	
	// get information from the node
	ENL1_printf("getting info from node");

	
	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	node->ss = (ScanState*) outerPlan;
	node->ss_in = (ScanState*) innerPlan;


	
	// * Reset per-tuple memory context to free any expression evaluation
	// * storage allocated in the previous tuple cycle.
	ResetExprContext(econtext);

	// * Ok, everything is setup for the join so now loop until we return a
	// * qualifying join tuple.
	ENL1_printf("entering main loop");
	

	for (;;) {
		/*
		* This block controls the selection process of the candidate for exploitation 
		* When both inner page and outer page have sufficient number of groups as exploitation candidate it picks the best among all for exploitation
		* If outer page has sufficient candidate but the inner page does not, this block will pick exploitation candidate from the outer page only
		*/	 
		if (!node->innerPageFullJoin && (!node->reachedEndOfOuter && node->activeRelationPages == node->sqrtOfInnerPages) || (node->reachedEndOfOuter && node->activeRelationPages > 0)){
			
			node->lastPageIndex = MAX(node->pageIndex, node->lastPageIndex); // track the last explored outer page

			node->bestOuterPageIndex = popBestPage(node); //find the best page to exploit

			int outerReward = node->tidRewards[node->bestOuterPageIndex].reward; // reward of the best page

			int innerReward = -1; 

			// if the buffer of exploitation candidate of inner page is full 
			if(node->exploitBufferIndex == node->exploitBufferSize){
				node->bestIPExploitIndex = popBestInnerPage(node); // find the best page to exploit
				
				innerReward = node->exploitBuffer[node->bestIPExploitIndex].reward; // reward of the best page

				if(node->exploitedInnerIndex < node->exploitedInnerPageSize){
					node->exploitedInnerPages[node->exploitedInnerIndex++] = node->exploitBuffer[node->bestIPExploitIndex].page_id;
				}
			}

			// if the outer reward is better then inner reward, exploit the outer page candidate
			if(outerReward >= innerReward){
				//load the outer page for full join
				LoadPageWithTIDs(outerPlan, node->tidRewards, node->outerPage, node->bestOuterPageIndex,
						node->ss->ss_currentRelation, node->ss->ss_ScanTupleSlot);
				
				node->outerPage->index = 0;  //starting from the first tuple of the page

				// get the range of inner pages that are already joined with the selected outer page to avoid duplicate join
				node->skipInnerPagesFrom = node->tidRewards[node->bestOuterPageIndex].fromInner; 
				node->skipInnerPagesTo = node->tidRewards[node->bestOuterPageIndex].toInner;

				node->outerPageFullJoin = true; 
				node->reachedEndOfInner = true; 
				node->needInnerPage = true;
				
				node->tidRewards[node->bestOuterPageIndex] = node->tidRewards[node->activeRelationPages - 1];
				node->activeRelationPages--;
			}else{
				//load the inner page for full join 
				LoadPageWithTIDsInner(innerPlan, node->exploitBuffer, node->innerPage, node->bestIPExploitIndex,
						node->ss->ss_currentRelation, node->ss->ss_ScanTupleSlot);
				
				node->innerPage->index = 0; //starting from the first tuple of the page

				node->innerPageFullJoin = true; 
				node->reachedEndOfOuter = true; 
				node->needOuterPage = true;

				node->availableSlotExploit = node->bestIPExploitIndex; 
				node->exploitBuffer[node->availableSlotExploit].page_id = -1;
				node->exploitBuffer[node->availableSlotExploit].reward = -1;
			}

			node->isExploring = false;
			node->exploitStepCounter = 1;		
		}
	
		/*
		* This block control the exploitation process
		*/
		if(!node->isExploring){
			if(node->outerPageFullJoin == true){
				//load the inner page 
				if (node->needInnerPage){
					if (node->reachedEndOfInner) {
						// Getting ready for rescan
						foreach(lc, nl->nestParams) {
							NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
							int paramno = nlp->paramno;
							ParamExecData *prm;

							prm = &(econtext->ecxt_param_exec_vals[paramno]);
							// Param value should be an OUTER_VAR var
							Assert(IsA(nlp->paramval, Var));
							Assert(nlp->paramval->varno == OUTER_VAR);
							Assert(nlp->paramval->varattno > 0);
							// prm->value = slot_getattr(outerTupleSlot,
							prm->value = slot_getattr(node->innerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
							// Flag parameter value as changed
							innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
						}
						node->innerPageCounter = 0;
						ExecReScan(innerPlan);
						node->innerRescanCount++; 
						node->reachedEndOfInner = false;
					}

					/*
					* this part is added for skipping those inner pages that are already joined with selected best outer page during
					* its m-Run exploration to avoid duplicate joins
					* it has bug. Need to be checked
					*/
					//bool innerPageSkipTracker = (MAX(node->skipInnerPagesTo, node->skipInnerPagesFrom) - MIN(node->skipInnerPagesTo, node->skipInnerPagesFrom) > 0) ? true : false;

					//if(node->skipInnerPagesFrom <= node->skipInnerPagesTo){
					//	while(node->innerPageCounter+1 >= node->skipInnerPagesFrom && node->innerPageCounter+1 <= node->skipInnerPagesTo){
					//		elog(INFO, "Skipping Inner Pages: %d", node->innerPageCounter+1);
					//		LoadNextPage(innerPlan, node->innerPage);
					//		node->innerPageCounter++;
					//		node->exploitStepCounter++;
					//	}
					//}
					//else{
					//	while(node->innerPageCounter+1 <= node->skipInnerPagesTo){
					//		elog(INFO, "Skipping Inner Pages: %d", node->innerPageCounter+1);
					//		LoadNextPage(innerPlan, node->innerPage);
					//		node->innerPageCounter++;
					//		node->exploitStepCounter++;
					//	}
					//	while(node->innerPageCounter+1 >= node->skipInnerPagesFrom && node->innerPageCounter+1 < node->innerPageNumber){
					//		elog(INFO, "Skipping Inner Pages: %d", node->innerPageCounter+1);
					//		LoadNextPage(innerPlan, node->innerPage);
					//		node->innerPageCounter++;
					//		node->exploitStepCounter++;
					//	}
					//}

					LoadNextPage(innerPlan, node->innerPage);
					node->innerPage->index = 0;

					node->innerPageCounter++;

					node->innerTupleCounter += node->innerPage->tupleCount;
					node->innerPageCounterTotal++;

					if(node->innerPageCounter == node->innerPageNumber){
						if(node->exploitStepCounter < node->innerPageNumber){
							node->reachedEndOfInner = true;
						}
					}
					
					node->needInnerPage = false;	
				}
			}

			if(node->innerPageFullJoin == true){
				//load the outer page 
				if (node->needOuterPage){
					if (node->reachedEndOfOuter) {
						// Getting ready for rescan
						foreach(lc, nl->nestParams) {
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
							outerPlan->chgParam = bms_add_member(outerPlan->chgParam, paramno);
						}
						node->outerPageCounter = 0;
						ExecReScan(outerPlan);
						
						node->outerRescanCount++; 
						node->reachedEndOfOuter = false;
					}
					
					LoadNextPage(outerPlan, node->outerPage);

					node->outerPageCounter++;

					//this outer page is already joined with the current inner page, so skip
					if (node->trackOuterPages[node->outerPageCounter] >= node->innerPageCounter){
						continue;
					}
					node->outerPage->index = 0;

					node->outerPageCounterTotal++;
					node->outerTupleCounter += node->outerPage->tupleCount;

					if(node->outerPageCounter == node->outerPageNumber){

						if(node->exploitStepCounter < node->outerPageNumber){
							node->reachedEndOfOuter = true;
						}
					}
	
					node->needOuterPage = false;
				}
			}
		}

		//this block controls the exploration process
		else if(node->isExploring == true){
			if (node->needOuterPage) {
				if (!node->reachedEndOfOuter && node->activeRelationPages < node->sqrtOfInnerPages) {
					
					node->pageIndex++; 
					node->pageIndex = MAX(node->pageIndex, node->lastPageIndex); 

					if(node->justDoneInnerExploit == true){
						//elog(INFO, "Returned from Inner full join.\n");
						//elog(INFO, "Now should start again from outer page %d\n", node->pageIndex);
						node->justDoneInnerExploit = false;

						foreach(lc, nl->nestParams) {
							NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
							int paramno = nlp->paramno;
							ParamExecData *prm;

							prm = &(econtext->ecxt_param_exec_vals[paramno]);
							// Param value should be an OUTER_VAR var
							//summit: what is the purpose of these 3 asserts?
							Assert(IsA(nlp->paramval, Var));
							Assert(nlp->paramval->varno == OUTER_VAR);
							Assert(nlp->paramval->varattno > 0);
							
							prm->value = slot_getattr(node->outerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
							// Flag parameter value as changed
							outerPlan->chgParam = bms_add_member(outerPlan->chgParam, paramno);
						}
						
						ExecReScan(outerPlan);
						
						node->outerRescanCount++; 

						int z;
						//going to the page from where we should start again
						//because of inner full join, the pointer to the outer is set to the beginning
						for(z=0; z<node->pageIndex; z++){
							LoadNextPage(outerPlan, node->outerPage);
						}
						node->outerPageCounter = node->pageIndex;
					}
					
					LoadNextPage(outerPlan, node->outerPage);

					if (node->outerPage->tupleCount < PAGE_SIZE) {
						elog(INFO, "Reached end of outer");
						node->reachedEndOfOuter = true;
						if (node->outerPage->tupleCount == 0)
							continue;
					}

					node->outerTupleCounter += node->outerPage->tupleCount;
					node->outerPageCounter++;

					node->trackOuterPages[node->outerPageCounter] = 0;

					if(node->innerPageCounter == node->innerPageNumber){
						node->reachedEndOfInner = true; 
					}
					
					node->lastReward = 0;
					node->exploreStepCounter = 1;

				} else {
					// join is done
					elog(INFO, "Join finished normally");
					return NULL;

				}
				node->needOuterPage = false;
				node->needInnerPage = true;
			}

			if (node->needInnerPage) {
				if (node->reachedEndOfInner) {
					// Getting ready for rescan
					foreach(lc, nl->nestParams){
						NestLoopParam *nlp = (NestLoopParam*) lfirst(lc);
						int paramno = nlp->paramno;
						ParamExecData *prm;

						prm = &(econtext->ecxt_param_exec_vals[paramno]);
						// Param value should be an OUTER_VAR var
						Assert(IsA(nlp->paramval, Var));
						Assert(nlp->paramval->varno == OUTER_VAR);
						Assert(nlp->paramval->varattno > 0);
						// prm->value = slot_getattr(outerTupleSlot,
						prm->value = slot_getattr(node->innerPage->tuples[0], nlp->paramval->varattno, &(prm->isnull));
						// Flag parameter value as changed
						innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
					}
					//elog(INFO, "inner relation: starting from the first page");
					node->innerPageCounter = 0;
					ExecReScan(innerPlan);
					node->rescanCount++;
					node->reachedEndOfInner = false;
				}
				
				LoadNextPage(innerPlan, node->innerPage);
				node->innerPageCounter++;
				node->innerPageReadCounter++; 
				
				if (node->innerPage->tupleCount < PAGE_SIZE) {
					node->reachedEndOfInner = true;
					if (node->innerPage->tupleCount == 0)
						continue;
				}
				node->innerTupleCounter += node->innerPage->tupleCount;
				
				node->innerPageCounterTotal++;
				node->needInnerPage = false;
			}		
		}	
		
		//this block controls the entire process of joining two groups and the outcome of the join
		if (node->innerPage->index == node->innerPage->tupleCount) {

			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				
				//this block is working on storing inner relation pages in the explore buffer and exploit buffer
				if (node->isExploring==true && node->exploreStepCounter <= N_FAILURE){
					InnerPageToBuffer(node);
				}

				if(node->isExploring){
					node->needInnerPage = true;
					
					if(node->exploreBufferIndex == node->exploreBufferSize && node->innerPageReadCounter == node->maxExplorePageIdS){
						node->reachedEndOfInner = true;
						node->innerPageReadCounter = 0;
					}
				}else{
					if (node->outerPageFullJoin == true){ // controlling outer page exploitation
						if (node->exploitStepCounter < node->innerPageNumber) {
							node->outerPage->index = 0; 
							node->exploitStepCounter++;
						}else{
							node->outerPageFullJoin = false;
							node->isExploring = true;
							node->needOuterPage = true;
							node->reward = 0;
							node->lastReward = 0;
							node->reachedEndOfInner = true;
							node->needInnerPage = true;
							node->justDoneOuterExploit = true;
							node->trackOuterPages[node->outerPageCounter] = node->innerPageNumber;
							continue;
						}
						node->needInnerPage = true;
					}else if(node->innerPageFullJoin == true){ // controlling inner page exploitation
						if (node->exploitStepCounter < node->outerPageNumber) {
							node->innerPage->index = 0; 
							node->exploitStepCounter++;
						}else{
							node->innerPageFullJoin = false;
							node->needInnerPage = true;
							node->reward = 0;
							node->lastReward = 0;
							node->needOuterPage = true;
							node->justDoneInnerExploit = true;
							continue;
						}
						node->needOuterPage = true;
					}
					
				}
				
				if (node->isExploring && ((node->lastReward > 0 && node->exploreStepCounter < node->innerPageNumber) 
										|| (node->lastReward == 0 && (++node->nFailure) < N_FAILURE))) { //stay with current

					node->outerPage->index = 0; 
					node->reward += node->lastReward;
					node->lastReward = 0;
					node->exploreStepCounter++;

				} else if (node->isExploring && node->exploreStepCounter == node->innerPageNumber) {
					node->needOuterPage = true;
					node->trackOuterPages[node->outerPageCounter] = node->innerPageNumber;
				} else if (node->isExploring && node->lastReward == 0 && (++node->nFailure) >= N_FAILURE) {
					storeTIDsExtended(node->outerPage, node->tidRewards, node->activeRelationPages, node->reward, node->outerPageCounter, node->nextInnerToStartExplore+1, node->innerPageCounter);
					node->nextInnerToStartExplore = node->innerPageCounter;
					node->nFailure = 0; 
					node->reward = 0;	
					node->activeRelationPages++;
					node->needOuterPage = true;
					node->trackOuterPages[node->outerPageCounter] = node->innerPageCounter;
				} 
				continue;
			}
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		econtext->ecxt_outertuple = outerTupleSlot;
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
		econtext->ecxt_innertuple = innerTupleSlot;

		node->innerPage->index++;

		if (TupIsNull(innerTupleSlot)) {
			elog(WARNING, "inner tuple is null");
			return NULL;
		}
		if (TupIsNull(outerTupleSlot)) {
			if (node->activeRelationPages > 0) { // still has pages in stack
				// elog(WARNING, "Finishing join while there are active pages");
				elog(INFO, "Null outer detected");
				node->needOuterPage = true;
				continue;
			}
			return NULL;
		}
		
		ENL1_printf("testing qualification");
		if (ExecQual(joinqual, econtext)) {

			if (otherqual == NULL || ExecQual(otherqual, econtext)) {
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				node->generatedJoins++;
				if (node->pageIndex >= node->outerPageNumber) {
					elog(WARNING, "pageIndex > outerPageNumber!?");
					return NULL;
				}
				return ExecProject(node->js.ps.ps_ProjInfo);
			} else
				InstrCountFiltered2(node, 1);
		} else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
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

	CHECK_FOR_INTERRUPTS(); ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext); ENL1_printf("entering main loop");

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
		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
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

	CHECK_FOR_INTERRUPTS(); ENL1_printf("getting info from node");

	nl = (NestLoop*) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext); ENL1_printf("entering main loop");

	if (nl->join.inner_unique)
		elog(WARNING, "inner relation is detected as unique");

	for (;;) {
		if (node->needOuterPage) {
			if (node->reachedEndOfOuter) {
				RemoveRelationPage(&(node->outerPage));
				elog(INFO, "Join Done");
				return NULL;
			}
//			node->outerPage = CreateRelationPage();	//mx
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
				} ENL1_printf("rescanning inner plan");
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
		ResetExprContext(econtext); ENL1_printf("qualification failed, looping");
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
NestLoopState*
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags) {
	NestLoopState *nlstate;
	int i;
	const char *fastjoin;
	const char *blocknestloop;
	const char *fliporder;

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
	if (node->nestParams == NIL)
		eflags |= EXEC_FLAG_REWIND;
	else
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
		elog(ERROR, "unrecognized join type: %d",
		(int) node->join.jointype);
	}

	/*
	 * finally, wipe the current outer tuple clean.
	 */
	nlstate->nl_NeedNewOuter = true;
	nlstate->nl_MatchedOuter = false;

	/* Extra inits for bandit join*/
	fliporder = GetConfigOption("enable_fliporder", false, false);

	nlstate->activeRelationPages = 0;
	nlstate->isExploring = true;
	nlstate->lastReward = 0;
	nlstate->needOuterPage = true;
	nlstate->needInnerPage = true;
	nlstate->exploitStepCounter = 0;
	nlstate->innerPageCounter = 0;
	nlstate->innerPageCounterTotal = 0;
	nlstate->outerPageCounter = 0;
	nlstate->reachedEndOfOuter = false;
	nlstate->reachedEndOfInner = false;
	nlstate->innerTupleCounter = 0;
	nlstate->outerTupleCounter = 0;
	nlstate->generatedJoins = 0;
	nlstate->rescanCount = 0;
	/* bha - add */
	nlstate->nFailure = 0;
	/* bha - end */
	if (strcmp(fliporder, "on") == 0) {
		nlstate->outerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
	} else {
		nlstate->outerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE + 1;
		nlstate->innerPageNumber = innerPlan(node)->plan_rows / PAGE_SIZE + 1;
	}

	nlstate->sqrtOfInnerPages = (int) sqrt(nlstate->innerPageNumber);
	nlstate->sqrtOfOuterPages = (int) sqrt(nlstate->outerPageNumber);
	nlstate->xids = palloc(nlstate->sqrtOfInnerPages * sizeof(int));
	nlstate->rewards = palloc(nlstate->sqrtOfInnerPages * sizeof(int));
	nlstate->tidRewards = palloc(nlstate->sqrtOfInnerPages * sizeof(struct tupleRewards));
	nlstate->pageIndex = -1;
	nlstate->lastPageIndex = 0;
	nlstate->xidScanKey = (ScanKey) palloc(sizeof(ScanKeyData));

	//below changes are made for ICL
	//inner page explore buffer
	nlstate->exploreBuffer = palloc(nlstate->sqrtOfOuterPages * sizeof(struct pageInfoS));
	nlstate->exploreBufferIndex = 0;
	nlstate->exploreBufferSize = nlstate->sqrtOfOuterPages; 

	// inner page exploit buffer
	nlstate->exploitBuffer = palloc(nlstate->sqrtOfOuterPages * sizeof(struct pageInfoS)); 
	nlstate->exploitBufferSize = nlstate->sqrtOfOuterPages;
	nlstate->exploitBufferIndex = 0;

	// inner page exploited tracker
	nlstate->exploitedInnerPageSize = 100000; 
	nlstate->exploitedInnerPages = palloc(nlstate->exploitedInnerPageSize * sizeof(int)); 
	nlstate->exploitedInnerIndex = 0;

	// track outer page
	nlstate->trackOuterPages = palloc(50000 * sizeof(int)); 

	nlstate->maxExplorePageIdS = nlstate->sqrtOfOuterPages;//at the beginning maximum inner page id that can be stored in the buffer is the square root of outer pages
	
	nlstate->availableSlot = -1;
	nlstate->availableSlotExploit = -1; 
	nlstate->bestIPExploitIndex = -1; 
	nlstate->nextInnerToStartExplore = -1; 

	nlstate->outerPageFullJoin = false; 
	nlstate->innerPageFullJoin = false; 
	nlstate->justDoneOuterExploit = false; 
	nlstate->justDoneInnerExploit = false; 

	nlstate->innerRescanCount = 0; 
	nlstate->outerRescanCount = 0; 
	nlstate->outerPageCounterTotal = 0; 
	nlstate->innerPageReadCounter = 0;

	nlstate->skipInnerPagesTo = 0; 
	nlstate->skipInnerPagesFrom = 0; 
	

	for (i = 0; i < nlstate->sqrtOfInnerPages; i++) {
		nlstate->tidRewards[i].reward = 0;
	}

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
		elog(INFO, "Running bandit join with TID scan..");
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
	int i;
	NL1_printf("ExecEndNestLoop: %s\n",
			"ending node processing");

	PrintNodeCounters(node);
	//elog(INFO, "Passed PrintNodeCounter");
	/*
	 * Free the exprcontext
	 */
	
	ExecFreeExprContext(&node->js.ps);
	//elog(INFO, "Passed ExecFreeExprContext");
	/*
	 * clean out the tuple table
	 */
	ExecClearTuple(node->js.ps.ps_ResultTupleSlot);
	//elog(INFO, "Passed ExecClearTuple");
	/*
	 * close down subplans
	 */
	ExecEndNode(outerPlanState(node));
	//elog(INFO, "ExecEndNode(outerPlanState)");
	ExecEndNode(innerPlanState(node));
	//elog(INFO, "ExecEndNode(innerPlanState)");
	NL1_printf("ExecEndNestLoop: %s\n",
			"node processing ended");

	// Releasing memory
	//list_free
	i = 0;
//	while (i < node->outerPageNumber){	//mx
//		list_free(node->pageIdJoinIdLists[i]);
//		node->pageIdJoinIdLists[i] = NULL;
//		i++;
//	}
	RemoveRelationPage(&(node->outerPage));
	//elog(INFO, "Passed RemoveRelationPage(&(node->outerPage))");
	RemoveRelationPage(&(node->innerPage));
	//elog(INFO, "Passed RemoveRelationPage(&(node->innerPage))");
	pfree(node->xids);
	pfree(node->rewards);
	pfree(node->xidScanKey);
	pfree(node->tidRewards);
	pfree(node->exploreBuffer);
	//elog(INFO, "Passed pfree(node->exploreBuffer)");
	pfree(node->exploitBuffer);
	//elog(INFO, "Passed (node->exploitBuffer)");
	pfree(node->innerRelStorage);

	//pfree(node->outerRelStorage);
	//elog(INFO, "Passed pfree(node->outerRelStorage)");
	pfree(node->exploitedInnerPages);
	pfree(node->trackOuterPages);
	//elog(INFO, "Passed pfree(node->exploitedInnerPages)");
//	pfree(node->pageIdJoinIdLists);//TODO remove each entry?	//mx
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

