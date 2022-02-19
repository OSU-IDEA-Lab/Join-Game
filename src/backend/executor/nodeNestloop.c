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

#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/guc.h"


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

static RelationPage* CreateRelationPageOne(NestLoopState* node,int size) {
	int i,j;
	//NestLoopState *node = castNode(NestLoopState, planState);
//	int size = PAGE_SIZE * times;
	RelationPage* relationPage = palloc(sizeof(RelationPage));

//	for(j = 0;j < node->level-1;j++){
//		size = (int)sqrt(size);
//	}

	relationPage->total = size;
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	for (i = 0; i < size; i++){
		relationPage->tuples[i] = NULL;
	}
	return relationPage;
}

static void RemoveRelationPageOne(NestLoopState* node,RelationPage** relationPageAdr) {
	int i,j;
	//NestLoopState *node = castNode(NestLoopState, planState);
//	int size = PAGE_SIZE;
	RelationPage* relationPage;
	relationPage  = *relationPageAdr;
	if (relationPage == NULL) {
		return;
	}
//	for(j = 0;j < node->level-1;j++){
//		size = (int)sqrt(size);
//	}
	for (i = 0; i < relationPage->total; i++){
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	pfree(relationPage);
	(*relationPageAdr) = NULL;
}


static int LoadNextPageOne(PlanState* planState, RelationPage* relationPage) {
	int i,j;
	//NestLoopState *node = castNode(NestLoopState, planState);
//	int size = PAGE_SIZE;
	if (relationPage == NULL){
		elog(ERROR, "LoadNextPageOne: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
//	for(j = 0;j < node->level-1;j++){
//		size = (int)sqrt(size);
//	}
	// Remove the old stored tuples
	for (i = 0; i < relationPage->total; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	for (i = 0; i < relationPage->total; i++) {
	 	TupleTableSlot* tts = ExecProcNode(planState);
		if (TupIsNull(tts)){
			relationPage->tuples[i] = NULL;
			break;
		} else {
			relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
			ExecCopySlot(relationPage->tuples[i], tts);
			relationPage->tupleCount++;
			if (IsA(planState, NestLoopState)) {
				relationPage->startKeyValue[i] = castNode(NestLoopState, planState)->startKeyValue;
//				relationPage->exploreOrNot[i] = castNode(NestLoopState, planState)->isExploring;
			}
		}
	}
	return relationPage->tupleCount;
}

static RelationPage* CreateRelationPage() {
	int i;
	RelationPage* relationPage = palloc(sizeof(RelationPage));
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	relationPage->total = PAGE_SIZE;
	for (i = 0; i < PAGE_SIZE; i++){
		relationPage->tuples[i] = NULL;
	}
	return relationPage;
}

static void RemoveRelationPage(RelationPage** relationPageAdr) {
	int i;
	RelationPage* relationPage;
	relationPage  = *relationPageAdr;
	if (relationPage == NULL) {
		return;
	}
	for (i = 0; i < PAGE_SIZE; i++){
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	pfree(relationPage);
	(*relationPageAdr) = NULL;
}


static int LoadNextPage(PlanState* planState, RelationPage* relationPage) {
	int i;
	//NestLoopState *node = castNode(NestLoopState, planState);
	if (relationPage == NULL){
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
		TupleTableSlot* tts = ExecProcNode(planState);
		if (TupIsNull(tts)){
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

//static int LoadNextOuterPageOne(PlanState* outerPlan, RelationPage* relationPage, ScanKey xidScanKey, int fromIndex,int size) {
//	int i,j=0;
//	TupleTableSlot* tts;
////	NestLoopState *node = castNode(NestLoopState, outerPlan);
////	int fromXid;
////	fromXid = fromIndex;
//	if (relationPage == NULL){
//		elog(ERROR, "LoadNextOuterPage: null page");
//	}
//	relationPage->index = 0;
//	relationPage->tupleCount = 0;
//	size = relationPage->total;
//	// Remove the old stored tuples
//	for (i = 0; i < size; i++) {
//		if (!TupIsNull(relationPage->tuples[i])) {
//			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
//			relationPage->tuples[i] = NULL;
//		}
//	}
//	for (i = 0; i < size; i++) {
//		while(true){
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
//				((IndexScanState*)outerPlan)->iss_NumScanKeys = 1;
//				((IndexScanState*)outerPlan)->iss_ScanKeys = xidScanKey;
//			} else if (IsA(outerPlan, IndexOnlyScanState)) {
//				((IndexOnlyScanState*)outerPlan)->ioss_NumScanKeys = 1;
//				((IndexOnlyScanState*)outerPlan)->ioss_ScanKeys = xidScanKey;
//			} else {
//				elog(ERROR, "Outer plan type is not index scan");
//			}
//			ExecReScan(outerPlan);
//		 	tts = ExecProcNode(outerPlan);
//			if (TupIsNull(tts)){
//				continue;
//	//			relationPage->tuples[i] = NULL;
//	//			break;
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

static int LoadNextOuterPage(PlanState* outerPlan, RelationPage* relationPage, ScanKey xidScanKey, int fromIndex) {
	int i,j=0;
	TupleTableSlot* tts;
//	NestLoopState *node = castNode(NestLoopState, outerPlan);
//	int fromXid;
//	fromXid = fromIndex;
	if (relationPage == NULL){
		elog(ERROR, "LoadNextOuterPage: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	int size = relationPage->total;
	// Remove the old stored tuples
	for (i = 0; i < size; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	for (i = 0; i < size; i++) {
		while(true){
			ScanKeyEntryInitialize(xidScanKey, //TODO is it fine to init ScanKey once?
					0, // flags
					1, /* attribute number to scan */
					3, /* op's strategy */
					23, /* strategy subtype */
					0, // ((OpExpr *) clause)->inputcollid,	/* collation */
					65, /* reg proc to use */
					fromIndex + j); /* constant */
			j++;
			if (IsA(outerPlan, IndexScanState)) {
				((IndexScanState*)outerPlan)->iss_NumScanKeys = 1;
				((IndexScanState*)outerPlan)->iss_ScanKeys = xidScanKey;
			} else if (IsA(outerPlan, IndexOnlyScanState)) {
				((IndexOnlyScanState*)outerPlan)->ioss_NumScanKeys = 1;
				((IndexOnlyScanState*)outerPlan)->ioss_ScanKeys = xidScanKey;
			} else {
				elog(ERROR, "Outer plan type is not index scan");
			}
			ExecReScan(outerPlan);
		 	tts = ExecProcNode(outerPlan);
			if (TupIsNull(tts)){
				continue;
	//			relationPage->tuples[i] = NULL;
	//			break;
			} else {
				relationPage->tuples[i] = MakeSingleTupleTableSlot(tts->tts_tupleDescriptor);
				ExecCopySlot(relationPage->tuples[i], tts);
				relationPage->tupleCount++;
			}
			break;
		}

	}
//	return relationPage->tupleCount;
	return j;
}

static int popBestPageXid(NestLoopState *node) {
	int i;
	int bestPageIndex;
	int bestXid;
	
	bestPageIndex = 0;
	for (i = 0; i < node->activeRelationPages; i++) {
		if (node->rewards[i] > node->rewards[bestPageIndex]){
			bestPageIndex = i;
		}
	}
//	if(node->level == 2)
	elog(INFO,"node.level is %d ---best reward is %d, outter page count is %d,join tuples is %d----",
			node->level,node->rewards[bestPageIndex],node->startKeyValue,node->generatedJoins);
	bestXid = node->xids[bestPageIndex];
	node->xids[bestPageIndex] = node->xids[node->activeRelationPages - 1];
	node->rewards[bestPageIndex] = node->rewards[node->activeRelationPages - 1];
	node->activeRelationPages--;
	return bestXid;
}

void globalRewardPass(NestLoopState *node,int key){
	int i;
	for (i = 0; i < node->activeRelationPages; i++) {
		if (node->xids[i] == key){
			node->rewards[i] += 5;	//global reward 5 passing
			elog(INFO,"find substate respongding block %d, pass the global reward",key);
			return;
		}
	}
}

static void PrintNodeInitInfo(NestLoopState *node){
	elog(INFO, "---------------------------------------------");
	elog(INFO, "Node Level: %d", node->level);
	elog(INFO, "Node is Top: %d", node->isTop);
	elog(INFO, "Node M runs: %d", node->sqrtOfInnerPages);
	elog(INFO, "Node outer plan page size: %d", node->outerPage->total);
	elog(INFO, "Node inner plan page size: %d", node->innerPage->total);
	elog(INFO, "---------------------------------------------");

}

static void PrintNodeCounters(NestLoopState *node){
	elog(INFO, "Read outer pages: %d", node->outerPageCounter);
	elog(INFO, "Read inner pages: %d", node->innerPageCounterTotal);
	elog(INFO, "Read outer tuples: %ld", node->outerTupleCounter);
	elog(INFO, "Read inner tuples: %ld", node->innerTupleCounter);
	elog(INFO, "Generated joins: %d", node->generatedJoins);
	elog(INFO, "Rescan Count: %d", node->rescanCount);
	//elog(INFO, "Current XidPage: %d", node->pageIndex);
	elog(INFO, "Active Relations: %d", node->activeRelationPages);
	elog(INFO, "Total page reads: %d", (node->outerPageCounter + node->innerPageCounterTotal));
	elog(INFO, "**************************************************");
}

static TupleTableSlot* ExecRightBanditJoin(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);

	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	ListCell   *lc;
	int tmp;
	int tmpReward;
	NestLoopState *substate;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;

	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;

	/*xiemian*/
//	elog(INFO, "level: %ld", node->level);
//	elog(INFO, "isTop: %ld", node->isTop);
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
//	if (node->outerTupleCounter == 0)
//		ExecReScan(outerPlan);


	for (;;)
	{
		if (node->needOuterPage) {
			if(node->isTop && node->reachedEndOfOuter && node->activeRelationPages == 0){	//join done
				elog(INFO, "Join finished normally %d",node->reachedEndOfOuter);
				PrintNodeCounters(node);
				return NULL;
			}

			if ((node->activeRelationPages >= node->sqrtOfInnerPages) ||
					(node->isTop && node->reachedEndOfOuter)){
				// exploit
				if(node->level == 2)
					elog(INFO, "level 2 exploit");
				node->outerPage->index = 0;
				node->isExploring = false;
				node->exploitStepCounter = 0;
//				node->lastPageIndex = MAX(node->pageIndex, node->lastPageIndex);
				tmp = popBestPageXid(node);
//				elog(INFO, "level %d exploit, best page start at %d",node->level,tmp);
//				if(node->level == 2)
//					LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey,tmp);
//				else
				LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, tmp);
				//elog(INFO, "Level %d Read best outer pages start from %d",node->level, tmp);
			} else {	// explore
				if(!node->reachedEndOfOuter){
					node->outerPageCounter++;
				}else{
					elog(INFO, "+++++++++++++++++++Reach outplan end, then loop++++++++++++++++++++++++++++++" );
					node->endKeyValue = 1;
					node->outerPageCounter = 0;
					node->reachedEndOfOuter = false;
				}
				node->isExploring = true;
				node->startKeyValue = node->endKeyValue;
				//node->pageIndex++;
				//node->pageIndex = MAX(node->pageIndex, node->lastPageIndex);

//				if(node->level == 2)
//					node->endKeyValue += LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->endKeyValue);
//				else
				node->endKeyValue += LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->endKeyValue);
				//node->endKeyValue += LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->endKeyValue);
				//elog(INFO, "Level %d Read outer pages: start from %d to %d",node->level, node->startKeyValue,node->endKeyValue);
//				if (node->outerPage->tupleCount < PAGE_SIZE) {
//					elog(INFO, "Reached end of outer, and the level is %d",node->level);
//					node->reachedEndOfOuter = true;
//					if (node->outerPage->tupleCount == 0) continue;
//				}
				if (node->outerPageCounter >= node->outerPageNumber) {
					elog(INFO, "Reached end of outer, and the level is %d",node->level);
					node->reachedEndOfOuter = true;
					if (node->outerPage->tupleCount == 0) continue;
				}
				node->outerTupleCounter += node->outerPage->tupleCount;
				node->lastReward = 0;
				if(node->level == 2)
					node->exploreStepCounter = 1;
				else
					node->exploreStepCounter = 1;
			}
			node->needOuterPage = false;
			node->needInnerPage = true;
		}

		if (node->needInnerPage){
			if (node->reachedEndOfInner) {
				// Getting ready for rescan
//				foreach(lc, nl->nestParams)
//				{
//					NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
//					int paramno = nlp->paramno;
//					ParamExecData *prm;
//
//					prm = &(econtext->ecxt_param_exec_vals[paramno]);
//					// Param value should be an OUTER_VAR var
//					Assert(IsA(nlp->paramval, Var));
//					Assert(nlp->paramval->varno == OUTER_VAR);
//					Assert(nlp->paramval->varattno > 0);
//					// prm->value = slot_getattr(outerTupleSlot,
//					prm->value = slot_getattr(node->outerPage->tuples[0],
//							nlp->paramval->varattno,
//							&(prm->isnull));
//					// Flag parameter value as changed
//					innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
//				}
				if(node->level == 1)
					ExecReScan(innerPlan);
				node->innerPageCounter = 0;
				node->rescanCount++;
				node->reachedEndOfInner = false;
			}
			LoadNextPageOne(innerPlan, node->innerPage);
			//****************level 2, page size is 1;

//			if(LoadNextPageOne(innerPlan, node->innerPage) != PAGE_SIZE)
//				elog(INFO,"cannot get 32 tuples per page");
			node->innerPageCounter++;
//			if (node->innerPage->tupleCount < node->innerPage->total) {
			if (node->innerPageCounter >= node->innerPageNumber){
				node->reachedEndOfInner = true;
				if (node->innerPage->tupleCount == 0) continue;
			}
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
		}

		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				//elog(INFO,"node level is %d, last reward is %d,node reward is %d,Explore is %d, innerpageIndex is %d",
						//node->level,node->lastReward,node->reward,node->isExploring,node->innerPageCounter);
				node->needInnerPage = true;
				if (node->isExploring && node->lastReward > 0
						&& node->exploreStepCounter < node->innerPageNumber) { //stay with current
					//elog(INFO,"node level is %d, last reward is %d",node->level,node->lastReward);
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
					node->xids[node->activeRelationPages] = node->startKeyValue;
					node->rewards[node->activeRelationPages] = node->reward;
					//elog(INFO,"node level is %d, node reward is %d",node->level,node->reward);
					node->reward = 0;
					node->activeRelationPages++;
					node->needOuterPage = true;
				} else if (!node->isExploring && node->exploitStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->exploitStepCounter++;
				} else if (!node->isExploring && node->exploitStepCounter >= node->innerPageNumber) {
					// Done with this outer page forever
					node->needOuterPage = true;
				} else {
					elog(ERROR, "Khiarlikh...");
				}
				continue;
			}
		}

		innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
		node->innerPage->index++;
		econtext->ecxt_innertuple = innerTupleSlot;

		if (TupIsNull(innerTupleSlot)){
			elog(WARNING, "inner tuple is null");
			return NULL;
		}

		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		econtext->ecxt_outertuple = outerTupleSlot;

		if (TupIsNull(outerTupleSlot)){
			//elog(INFO, "Null outer detected,explore is %d, exploit step counter %d, inner page num %d, level is %d, page counter %d, startkey %d, endkey %d, index is %d, total is %d",
			//		node->isExploring,node->exploitStepCounter,node->innerPageNumber,node->level,node->outerPageCounter,node->startKeyValue,node->endKeyValue,node->outerPage->index,node->outerPage->tupleCount);
			if (node->activeRelationPages > 0) { // still has pages in stack
				// elog(WARNING, "Finishing join while there are active pages");
				node->needOuterPage = true;
				continue;
			}
			return NULL;
		}



		ENL1_printf("testing qualification");
		if (ExecQual(joinqual, econtext))
		{

			if (otherqual == NULL || ExecQual(otherqual, econtext))
			{
				ENL1_printf("qualification succeeded, projecting tuple");
				//elog(INFO,"node level is %d, last reward is %d,node reward is %d",node->level,node->lastReward,node->reward);
				if(node->isExploring){
					node->lastReward++;
					//elog(INFO,"node level is %d, last reward is %d,node reward is %d",node->level,node->lastReward,node->reward);
				}
//				if(node->level == 2){
//					substate = castNode(NestLoopState, innerPlanState(node));
//					//elog(INFO,"global reward in upper layer");
//					globalRewardPass(substate,node->innerPage->startKeyValue[node->innerPage->index]);
//				}

//				tmpReward = 0;
//				while(IsA(innerPlanState(substate), NestLoopState)){	/*xiemian*/
//					tmpReward = tmpReward+5;
//					substate = castNode(NestLoopState,innerPlanState(node));
//					substate->lastReward += tmpReward;
//			//		elog(INFO, "inner plan level: %ld", castNode(NestLoopState,innerPlanState(nlstate))->level);
//			//		elog(INFO, "inner plan isTop: %ld", castNode(NestLoopState,innerPlanState(nlstate))->isTop);
//				}
				node->generatedJoins++;
//				if (node->pageIndex >= node->outerPageNumber){
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
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);
		ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecBanditJoin(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	ListCell   *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
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


	// if (nl->join.inner_unique)
		// elog(WARNING, "inner relation is detected as unique");


	for (;;)
	{
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
					if (node->outerPage->tupleCount == 0) continue;
				}
				node->outerTupleCounter += node->outerPage->tupleCount;
				node->outerPageCounter++;
				node->lastReward = 0;
				node->exploreStepCounter = 1;
			} else if ((!node->reachedEndOfOuter && node->activeRelationPages == node->sqrtOfInnerPages) || 
					(node->reachedEndOfOuter && node->activeRelationPages > 0)){
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
					NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
					int paramno = nlp->paramno;
					ParamExecData *prm;

					prm = &(econtext->ecxt_param_exec_vals[paramno]);
					// Param value should be an OUTER_VAR var 
					Assert(IsA(nlp->paramval, Var));
					Assert(nlp->paramval->varno == OUTER_VAR);
					Assert(nlp->paramval->varattno > 0);
					// prm->value = slot_getattr(outerTupleSlot,
					prm->value = slot_getattr(node->outerPage->tuples[0],
							nlp->paramval->varattno,
							&(prm->isnull));
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
				if (node->innerPage->tupleCount == 0) continue;
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
				if (node->isExploring && node->lastReward > 0 
						&& node->exploreStepCounter < node->innerPageNumber) { //stay with current
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
		econtext->ecxt_outertuple = outerTupleSlot;
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index]; 
		econtext->ecxt_innertuple = innerTupleSlot;
		node->innerPage->index++;
		if (TupIsNull(innerTupleSlot)){
			elog(WARNING, "inner tuple is null");
			return NULL;
		}
		if (TupIsNull(outerTupleSlot)){
			if (node->activeRelationPages > 0) { // still has pages in stack
				// elog(WARNING, "Finishing join while there are active pages");
				elog(INFO, "Null outer detected");
				node->needOuterPage = true;
				continue;
			}
			return NULL;
		}

		ENL1_printf("testing qualification");
		if (ExecQual(joinqual, econtext))
		{

			if (otherqual == NULL || ExecQual(otherqual, econtext))
			{
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				node->generatedJoins++;
				if (node->pageIndex >= node->outerPageNumber){
					elog(WARNING, "pageIndex > outerPageNumber!?");
					return NULL;
				}
				//TODO do this check earlier in the algorithm
				if (list_member_int(node->pageIdJoinIdLists[node->pageIndex], node->innerPageCounter)) {
					continue;
				}
				// Add current xid-innerPageCounter to result sets
				lcons_int(node->innerPageCounter, node->pageIdJoinIdLists[node->pageIndex]);  
				return ExecProject(node->js.ps.ps_ProjInfo);
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		ResetExprContext(econtext);
		ENL1_printf("qualification failed, looping");
	}
}


static TupleTableSlot* ExecRightBlockNestedLoop(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	// ListCell   *lc;

	CHECK_FOR_INTERRUPTS();
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = innerPlanState(node);
	innerPlan = outerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext);
	ENL1_printf("entering main loop");

	if (nl->join.inner_unique)
		elog(WARNING, "inner relation is detected as unique");

	if (node->innerTupleCounter == 0)
		ExecReScan(outerPlan);

	for (;;) {
		if (node->needOuterPage) {
			if (node->reachedEndOfOuter){
				RemoveRelationPage(&(node->outerPage));
				elog(INFO, "Join Done");
				return NULL; 
			}
			node->outerPage = CreateRelationPage();
			LoadNextPage(outerPlan, node->outerPage);
			node->outerTupleCounter += node->outerPage->tupleCount;
			node->outerPageCounter++;
			node->needOuterPage = false;
			if (node->outerPage->tupleCount < PAGE_SIZE){ 
				node->reachedEndOfOuter = true;
				if (node->outerPage->tupleCount == 0) continue;
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
			if (node->outerPage->index < node->outerPage->tupleCount - 1){
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else { // mini join is done 
				if (node->innerPage->tupleCount < PAGE_SIZE){ // was last inner page of the iteration
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
		if (TupIsNull(outerTupleSlot)){
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
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);
		ResetExprContext(econtext);
		ENL1_printf("qualification failed, looping");
	}
}


static TupleTableSlot* ExecBlockNestedLoop(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	ListCell   *lc;

	CHECK_FOR_INTERRUPTS();
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
	joinqual = node->js.joinqual;
	otherqual = node->js.ps.qual;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	econtext = node->js.ps.ps_ExprContext;
	ResetExprContext(econtext);
	ENL1_printf("entering main loop");

	if (nl->join.inner_unique)
		elog(WARNING, "inner relation is detected as unique");

//	if(node->level != 1)
//		node->innerPageNumber = 1;

	if (node->innerTupleCounter == 0){
		ExecReScan(outerPlan);
		if(node->level == 1)
			ExecReScan(innerPlan);
	}


	for (;;) {
		if (node->needOuterPage) {
			if(node->isTop){
				elog(INFO,"top layer need new outer page, %d",node->outerPageCounter);
			}else{
				elog(INFO,"first layer need new outer page, %d",node->outerPageCounter);
			}
			if (node->reachedEndOfOuter){
				if(node->isTop){
					RemoveRelationPage(&(node->outerPage));
					elog(INFO, "Join Done");
					return NULL;
				}else{
					ENL1_printf("rescanning outer plan");
					ExecReScan(outerPlan);
					node->outerPageCounter = 0;
					node->reachedEndOfOuter = false;
				}
			}
//			node->outerPage = CreateRelationPage();
			LoadNextPage(outerPlan, node->outerPage);
			node->outerTupleCounter += node->outerPage->tupleCount;
			node->outerPageCounter++;
			node->needOuterPage = false;
			node->needInnerPage = true;
//			if (node->outerPage->tupleCount < PAGE_SIZE){
//				//elog(INFO, "Reached end of outer, and the level is %d",node->level);
//				node->reachedEndOfOuter = true;
//				if (node->outerPage->tupleCount == 0) continue;
//			}
			if (node->outerPageCounter == node->outerPageNumber) {
				elog(INFO, "Reached end of outer, and the level is %d",node->level);
				node->reachedEndOfOuter = true;
				if (node->outerPage->tupleCount == 0) continue;
			}
		}
		if (node->needInnerPage) {
			if (node->innerPageCounter >= node->innerPageNumber){
//				node->reachedEndOfInner = true;
//				if (node->innerPage->tupleCount == 0) continue;
//			}
//
//			if (node->innerPage->tupleCount < PAGE_SIZE){ // done with one outer page, move to next
//				foreach(lc, nl->nestParams)
//				{
//					NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
//					int			paramno = nlp->paramno;
//					ParamExecData *prm;
//
//					prm = &(econtext->ecxt_param_exec_vals[paramno]);
//					/* Param value should be an OUTER_VAR var */
//					Assert(IsA(nlp->paramval, Var));
//					Assert(nlp->paramval->varno == OUTER_VAR);
//					Assert(nlp->paramval->varattno > 0);
//					prm->value = slot_getattr(node->outerPage->tuples[node->outerPage->index],
//							nlp->paramval->varattno,
//							&(prm->isnull));
//					/* Flag parameter value as changed */
//					innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
//							paramno);
//				}
				ENL1_printf("rescanning inner plan");
				if(node->level == 1){
					ExecReScan(innerPlan);
				}
				node->innerPageCounter = 0;
				node->rescanCount++;
				node->needOuterPage = true;
				if (node->innerPage->tupleCount == 0){
					node->needInnerPage = true;
					continue;
				}
				// node->needInnerPage = true;
				// RemoveRelationPage(&(node->outerPage));
				// node->needOuterPage = true;
				// continue;
			}
			LoadNextPage(innerPlan, node->innerPage);
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
		} 

		if (node->innerPage->index == node->innerPage->tupleCount) {
			if (node->outerPage->index < node->outerPage->tupleCount - 1){
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
		if (TupIsNull(outerTupleSlot)){
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
//				elog(INFO,"level %d generate 1 matching tuple",node->level);
				node->generatedJoins++;
				return ExecProject(node->js.ps.ps_ProjInfo);
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);
		ResetExprContext(econtext);
		ENL1_printf("qualification failed, looping");
	}
}


static TupleTableSlot* ExecRightRegularNestLoop(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	// ListCell   *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
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

	for (;;)
	{
		/*
		 * get the next outer tuple 
		 */

		outerTupleSlot = ExecProcNode(outerPlan);
		if (TupIsNull(outerTupleSlot))
		{
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

		if (ExecQual(joinqual, econtext))
		{
			if (otherqual == NULL || ExecQual(otherqual, econtext))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENL1_printf("qualification succeeded, projecting tuple");

				return ExecProject(node->js.ps.ps_ProjInfo);
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}

static TupleTableSlot* ExecRegularNestLoop(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;
	ListCell   *lc;

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

	nl = (NestLoop *) node->js.ps.plan;
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

	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{
			ENL1_printf("getting new outer tuple");

			outerTupleSlot = ExecProcNode(outerPlan);
			node->outerTupleCounter++;

			/*
			 * if there are no more outer tuples, then the join is complete..
			 */
			if (TupIsNull(outerTupleSlot))
			{
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
				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
				int			paramno = nlp->paramno;
				ParamExecData *prm;

				prm = &(econtext->ecxt_param_exec_vals[paramno]);
				/* Param value should be an OUTER_VAR var */
				Assert(IsA(nlp->paramval, Var));
				Assert(nlp->paramval->varno == OUTER_VAR);
				Assert(nlp->paramval->varattno > 0);
				prm->value = slot_getattr(outerTupleSlot,
										  nlp->paramval->varattno,
										  &(prm->isnull));
				/* Flag parameter value as changed */
				innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
													 paramno);
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

		if (TupIsNull(innerTupleSlot))
		{
			ENL1_printf("no inner tuple, need new outer tuple");

			node->nl_NeedNewOuter = true;

			if (!node->nl_MatchedOuter &&
				(node->js.jointype == JOIN_LEFT ||
				 node->js.jointype == JOIN_ANTI))
			{
				/*
				 * We are doing an outer join and there were no join matches
				 * for this outer tuple.  Generate a fake join tuple with
				 * nulls for the inner tuple, and return it if it passes the
				 * non-join quals.
				 */
				econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

				ENL1_printf("testing qualification for outer-join tuple");

				if (otherqual == NULL || ExecQual(otherqual, econtext))
				{
					/*
					 * qualification was satisfied so we project and return
					 * the slot containing the result tuple using
					 * ExecProject().
					 */
					ENL1_printf("qualification succeeded, projecting tuple");

					return ExecProject(node->js.ps.ps_ProjInfo);
				}
				else
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

		if (ExecQual(joinqual, econtext))
		{
			node->nl_MatchedOuter = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_ANTI)
			{
				node->nl_NeedNewOuter = true;
				continue;		/* return to top of loop */
			}

			/*
			 * If we only need to join to the first matching inner tuple, then
			 * consider returning this one, but after that continue with next
			 * outer tuple.
			 */
			if (node->js.single_match)
				node->nl_NeedNewOuter = true;

			if (otherqual == NULL || ExecQual(otherqual, econtext))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENL1_printf("qualification succeeded, projecting tuple");

				return ExecProject(node->js.ps.ps_ProjInfo);
			}
			else
				InstrCountFiltered2(node, 1);
		}
		else
			InstrCountFiltered1(node, 1);

		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);

		ENL1_printf("qualification failed, looping");
	}
}


static TupleTableSlot* ExecNestLoop(PlanState *pstate)
{
	TupleTableSlot *tts;
	const char* fastjoin = GetConfigOption("enable_fastjoin", false, false);
	const char* blocknestloop = GetConfigOption("enable_block", false, false);
	const char* fliporder = GetConfigOption("enable_fliporder", false, false);
	if (strcmp(fastjoin, "on") == 0){
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
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
	NestLoopState *nlstate;
	NestLoopState *substate;	/*xiemian*/
	int i;
	const char* fastjoin;
	const char* blocknestloop; 
	const char* fliporder;

	/* check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)));

	NL1_printf("ExecInitNestLoop: %s\n",
			   "initializing node");

	/*
	 * create state structure
	 */
	nlstate = makeNode(NestLoopState);
	nlstate->js.ps.plan = (Plan *) node;
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
	nlstate->js.ps.qual =
		ExecInitQual(node->join.plan.qual, (PlanState *) nlstate);
	nlstate->js.jointype = node->join.jointype;
	nlstate->js.joinqual =
		ExecInitQual(node->join.joinqual, (PlanState *) nlstate);

	/*
	 * detect whether we need only consider the first matching inner tuple
	 */
	nlstate->js.single_match = (node->join.inner_unique ||
								node->join.jointype == JOIN_SEMI);

	/* set up null tuples for outer joins, if needed */
	switch (node->join.jointype)
	{
		case JOIN_INNER:
		case JOIN_SEMI:
			break;
		case JOIN_LEFT:
		case JOIN_ANTI:
			nlstate->nl_NullInnerTupleSlot =
				ExecInitNullTupleSlot(estate,
									  ExecGetResultType(innerPlanState(nlstate)));
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

	substate = NULL;
	nlstate->isTop = true;	/*xiemian*/
	nlstate->fromTop = true;	/*xiemian*/
	if(IsA(innerPlanState(nlstate), NestLoopState)){	/*xiemian*/
		substate = castNode(NestLoopState,innerPlanState(nlstate));
		nlstate->level = substate->level + 1;
		substate->isTop = false;
		substate->fromTop = false;
//		elog(INFO, "inner plan level: %ld", castNode(NestLoopState,innerPlanState(nlstate))->level);
//		elog(INFO, "inner plan isTop: %ld", castNode(NestLoopState,innerPlanState(nlstate))->isTop);
	}else{
		nlstate->level = 1;
	}
//	elog(INFO, "level: %ld", nlstate->level);
//	elog(INFO, "isTop: %ld", nlstate->isTop);

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
	nlstate->startKeyValue = 1;
	nlstate->endKeyValue = nlstate->startKeyValue;
	//if (strcmp(fliporder, "on") == 0) {
	nlstate->outerPageNumber = outerPlan(node)->plan_rows / (TIMES*PAGE_SIZE);
	nlstate->innerPageNumber = innerPlan(node)->plan_rows / (TIMES*PAGE_SIZE);
//	if (strcmp(fliporder, "on") == 0) {
//		for(i = 0;i < nlstate->level-1;i++){
//			nlstate->innerPageNumber = (int)(substate->innerPageNumber*0.9);
//		}
//	}
	//TODO sometimes the inner plan_rows does not match the exact row numbers 

//	 elog(INFO, "Outer row number: %lf", outerPlan(node)->plan_rows);
//	 elog(INFO, "Inner row number: %lf", innerPlan(node)->plan_rows);

	nlstate->sqrtOfInnerPages = (int)sqrt(nlstate->innerPageNumber);
//	for(i = 0;i < nlstate->level-1;i++){
//		nlstate->sqrtOfInnerPages = (int)sqrt(substate->sqrtOfInnerPages);
//	}

	nlstate->xids = palloc(nlstate->sqrtOfInnerPages * sizeof(int));	/*xiemian*/
	nlstate->rewards = palloc(nlstate->sqrtOfInnerPages * sizeof(int));	/*xiemian*/
	nlstate->pageIndex = -1;
	nlstate->lastPageIndex = 0;
	nlstate->xidScanKey = (ScanKey) palloc(sizeof(ScanKeyData));
//	nlstate->pageIdJoinIdLists = palloc(nlstate->outerPageNumber * sizeof(List*));
//	i = 0;
//	while (i < nlstate->outerPageNumber){
//		nlstate->pageIdJoinIdLists[i] = NIL;
//		i++;
//	}


	if (strcmp(fliporder, "on") == 0){
		if(nlstate->level == 2){
			nlstate->innerPage = CreateRelationPageOne(nlstate,(TIMES*PAGE_SIZE));
			nlstate->outerPage = CreateRelationPageOne(nlstate,(TIMES*PAGE_SIZE));
//			nlstate->innerPage = CreateRelationPageOne(nlstate,PAGE_SIZE);
//			nlstate->outerPage = CreateRelationPageOne(nlstate,PAGE_SIZE);
		}else {
			nlstate->innerPage = CreateRelationPageOne(nlstate,(TIMES*PAGE_SIZE));
			nlstate->outerPage = CreateRelationPageOne(nlstate,(TIMES*PAGE_SIZE));
		}
	}else{
		nlstate->outerPage = CreateRelationPage();
		nlstate->innerPage = CreateRelationPage();
	}


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
	if (strcmp(fastjoin, "on") == 0){
		elog(INFO, "Running bandit join..");
	} else {
		if (strcmp(blocknestloop, "on") == 0) {
			elog(INFO, "Running block nested loop..");
		} else {
			elog(INFO, "Running nested loop..");
		}
	}
	if (strcmp(fliporder, "on") == 0) {
			elog(INFO, "Multi-Relation Bandit Join");
	}
	PrintNodeInitInfo(nlstate);
	return nlstate;
}

/* ----------------------------------------------------------------
 *		ExecEndNestLoop
 *
 *		closes down scans and frees allocated storage
 * ----------------------------------------------------------------
 */
void
ExecEndNestLoop(NestLoopState *node)
{
	int i;
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
	//RemoveRelationPage(&(node->outerPage));
	RemoveRelationPageOne(node,&(node->outerPage));
	RemoveRelationPageOne(node,&(node->innerPage));
	pfree(node->xids);
	pfree(node->rewards);
	pfree(node->xidScanKey);
//	pfree(node->pageIdJoinIdLists);//TODO remove each entry?
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoop(NestLoopState *node)
{
	PlanState  *outerPlan = outerPlanState(node);
	PlanState  *innerPlan = innerPlanState(node);
	const char* fliporder;
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
		RemoveRelationPageOne(node,&(node->outerPage));
		RemoveRelationPageOne(node,&(node->innerPage));

		if(node->level == 2){
//			node->innerPage = CreateRelationPageOne(node,1);
//			node->outerPage = CreateRelationPageOne(node,2*PAGE_SIZE);
			node->innerPage = CreateRelationPageOne(node,(TIMES*PAGE_SIZE));
			node->outerPage = CreateRelationPageOne(node,(TIMES*PAGE_SIZE));
		}else {
			node->innerPage = CreateRelationPageOne(node,(TIMES*PAGE_SIZE));
			node->outerPage = CreateRelationPageOne(node,(TIMES*PAGE_SIZE));
		}

//		node->outerPage = CreateRelationPage();

//		RemoveRelationPageOne(node,&(node->innerPage));
//		node->innerPage = CreateRelationPageOne(node);

		ExecReScan(innerPlan);
		node->innerTupleCounter = 0;
	}
}

