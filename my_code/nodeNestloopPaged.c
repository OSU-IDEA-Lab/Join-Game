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

#include "executor/execdebug.h"
#include "executor/nodeNestloop.h"
#include "miscadmin.h"
#include "utils/memutils.h"


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
 *				This is achieved by maintaining a scan position on the outer
 *				relation.
 *
 *		Initial States:
 *		  -- the outer child and the inner child
 *			   are prepared to return the first tuple.
 * ----------------------------------------------------------------
 */

int loadNextPage(PlanState* planState, RelationPage* relationPage) {
	int i;
	bool hitNull;
	relationPage->tupleCount = 0;
	relationPage->reward = 0;
	relationPage->index = 0;
	for (i = 0; i < PAGE_SIZE; i++) {
	 	TupleTableSlot* tts = ExecProcNode(planState);
		if (TupIsNull(tts)){
			elog(INFO, "found null tup while loading");
			relationPage->tuples[i] = NULL;
			break;
		}
		relationPage->tuples[i] = MakeTupleTableSlot(CreateTupleDescCopy(tts->tts_tupleDescriptor));
		ExecCopySlot(relationPage->tuples[i], tts);
		relationPage->tupleCount++;
	}
	return relationPage->tupleCount;
}

RelationPage* popBestPage(NestLoopState *node) {
	int bestPageIndex = 0;
	int i;
	int size;
	RelationPage* tmp;
	size = node->activeRelationPages;
	for (i = 1; i < size; i++) {
		if (node->relationPages[i]->reward > node->relationPages[bestPageIndex]->reward) {
			bestPageIndex = i;
		}
	}
	tmp = node->relationPages[size - 1];
	node->relationPages[size - 1] = node->relationPages[bestPageIndex];
	node->relationPages[bestPageIndex] = tmp;
	node->activeRelationPages--;

	// elog(INFO, "finding best active page..");
	// elog(INFO, "  pop best reward: %d", node->relationPages[size-1]->reward);
	// elog(INFO, "  heap size: %d", node->activeRelationPages--);
	return node->relationPages[size - 1];
}

static TupleTableSlot *
ExecNestLoop(PlanState *pstate)
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

	if (nl->join.inner_unique) {
		elog(INFO, "inner relation is detected as unique");
	}

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
		node->forLoopCounter++;
		if (node->needOuterPage) {
			if (node->outerPage != NULL){
				pfree(node->outerPage);
			}
			node->outerPage = palloc(sizeof(RelationPage));
			int count = loadNextPage(outerPlan, node->outerPage);
			elog(INFO, "  read %d new outer tuples", count);
			node->outerTupleCounter += count;
			node->needOuterPage = false;
			node->outerPageCounter++;
			continue;
		}
		if (node->needInnerPage) {
			if (node->innerPage != NULL){
				pfree(node->innerPage);
			}
			node->innerPage = palloc(sizeof(RelationPage));
			int count = loadNextPage(innerPlan, node->innerPage);
			elog(INFO, "  read %d inner tuples", count);
			node->innerTupleCounter += count;
			node->needInnerPage = false;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
		} 

		if (node->innerPage->index < PAGE_SIZE && node->outerPage->index < PAGE_SIZE) {
			outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
			innerTupleSlot = node->innerPage->tuples[node->innerPage->index];
		} else if (node->innerPage->index == PAGE_SIZE) {
			if (node->outerPage->index < PAGE_SIZE - 1){
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else { // outer page reached its end
				node->needInnerPage = true;
				node->outerPage->index = 0;
				// return NULL;
			}
			continue;
		} else {
			elog(ERROR, "shouldn't endup here");
		}
		
		if (TupIsNull(innerTupleSlot))
		{
			elog(INFO, " current inner is null");
			elog(INFO, "  inner tups: %d outer tups: %d ", 
					node->innerTupleCounter, node->outerTupleCounter);
			elog(INFO, "  inner index %d, counts: %d", node->innerPage->index, node->innerPage->tupleCount);
			//TODO rescan may not work if you don't set the param
			ExecReScan(innerPlan);
			node->innerTupleCounter = 0;
			node->needInnerPage = true;
			node->needOuterPage = true;
			continue;
		}
		if (TupIsNull(outerTupleSlot)){
			elog(INFO, " current outer is null");
			elog(INFO, "inner tups: %d outer tups: %d", 
					node->innerTupleCounter, node->outerTupleCounter);
			elog(INFO, "join done!");
			return NULL;
		}
		econtext->ecxt_outertuple = outerTupleSlot;
		econtext->ecxt_innertuple = innerTupleSlot;
		node->innerPage->index++;
		/*
		if (node->outerPage->index >= PAGE_SIZE) { 
			if (node->activeRelationPages < SQRT_OF_N && !node->outerDone) { 
				node->needOuterPage = true;
				node->isExploring = true;
			} else {
				if (node->activeRelationPages == 0) {
					elog(INFO, "  total inner pages: %d, current inner pages: %d outer pages: %d ", 
							node->innerPageCounterTotal, node->innerPageCounter, node->outerPageCounter);
					elog(INFO, "join done!");
					return NULL;
				}
				node->outerPage = popBestPage(node);
				node->isExploring = false;
			} 
			node->lastReward = 0;
			node->needInnerPage = true;
			node->outerPage->index = 0;
			continue;
		} 
		if (node->innerPage->index >= PAGE_SIZE) {
			if (node->outerPage->index < PAGE_SIZE - 1) {
				node->outerPage->index++;
				node->innerPage->index = 0;
			} else {
				if (node->isExploring) {
					if (node->lastReward > 0) { // stay with this outer page
						node->outerPage->index = 0;
						node->lastReward = 0;
					} else {  // need to explore or expoit new outer page
						node->outerPage->reward = node->lastReward;
						node->relationPages[node->activeRelationPages++] = node->outerPage;
						node->outerPage->index = PAGE_SIZE; 
					}
				} else {
					if (node->exploitStepCounter < INNER_PAGE_COUNT) { // stay with this outer page
						node->outerPage->index = 0;
						node->exploitStepCounter++;
					} else {  // need to explore or expoit new outer page
						// elog(WARNING, "one exploit accomiplished");
						node->exploitStepCounter = 0;
						node->outerPage->index = PAGE_SIZE; 
					}
				}
				node->needInnerPage = true;
			}
			continue;
		}
		outerTupleSlot = node->outerPage->tuples[node->outerPage->index];
		econtext->ecxt_outertuple = outerTupleSlot;
		innerTupleSlot = node->innerPage->tuples[node->innerPage->index]; 
		econtext->ecxt_innertuple = innerTupleSlot;
		*/


		/*
		if (TupIsNull(outerTupleSlot)) {
			if (!node->outerDone) {
				node->outerDone = true;
				node->outerPage->index = PAGE_SIZE; 
				continue;
			} else {
				elog(INFO, "  total inner pages: %d, current inner pages: %d outer pages: %d ", 
						node->innerPageCounterTotal, node->innerPageCounter, node->outerPageCounter);
				elog(ERROR, "khiarlikh");
			}
		}

		if (TupIsNull(innerTupleSlot))
		{
			elog(INFO, "getde khiarligha");
			elog(INFO, "  total inner pages: %d, current inner pages: %d outer pages: %d ", 
				node->innerPageCounterTotal, node->innerPageCounter, node->outerPageCounter);
			elog(INFO, "  heap size: %d exploit size: %d, for loop: %d", 
				node->activeRelationPages, node->exploitStepCounter, node->forLoopCounter);

			// fetch the values of any outer Vars that must be passed to the
			// inner scan, and store them in the appropriate PARAM_EXEC slots.
			foreach(lc, nl->nestParams)
			{
				NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
				int			paramno = nlp->paramno;
				ParamExecData *prm;

				prm = &(econtext->ecxt_param_exec_vals[paramno]);
				// Param value should be an OUTER_VAR var 
				Assert(IsA(nlp->paramval, Var));
				Assert(nlp->paramval->varno == OUTER_VAR);
				Assert(nlp->paramval->varattno > 0);
				prm->value = slot_getattr(outerTupleSlot,
						nlp->paramval->varattno,
						&(prm->isnull));
				// Flag parameter value as changed 
				innerPlan->chgParam = bms_add_member(innerPlan->chgParam, paramno);
			}
			ExecReScan(innerPlan);
			node->needInnerPage = true;
			node->innerPageCounter = 0;
			continue;
		}
		node->innerPage->index++;
		*/


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

			if (otherqual == NULL || ExecQual(otherqual, econtext))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				ENL1_printf("qualification succeeded, projecting tuple");
				node->lastReward++;
				/*
				elog(INFO, "  generating join");
				elog(INFO, "   inner tups: %d inner index: %d, inner count: %d",
				 	node->innerTupleCounter, node->innerPage->index, node->innerPage->tupleCount);
				elog(INFO, "   outer tups: %d outer index: %d, outer count: %d",
				 	node->outerTupleCounter, node->outerPage->index, node->outerPage->tupleCount);
				if (node->innerPage == NULL || node->outerPage == NULL) {
					elog(INFO, "  one page is null");
				} else {
					elog(INFO, " pages are ready");
				}
				if (node->innerPage->tuples == NULL || node->outerPage->tuples == NULL) {
					elog(INFO, "  tuples array is null");
				} else {
					elog(INFO, " tuples arrays are ready");
				}
				if (node->innerPage->tuples[node->innerPage->index] == NULL){
					elog(INFO, "  pre inner is null");
				} else {
					elog(INFO, "  pre inner is not null");
				}
				if (TupIsNull(node->innerPage->tuples[node->innerPage->index])){
					elog(INFO, "  inner is null");
				} else {
					elog(INFO, "  inner is not null");
				}
				if (TupIsNull(node->outerPage->tuples[node->outerPage->index])){
					elog(INFO, "  outer is null");
				} else {
					elog(INFO, "  outer is not null");
				}
				*/
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

/* ----------------------------------------------------------------
 *		ExecInitNestLoop
 * ----------------------------------------------------------------
 */
NestLoopState *
ExecInitNestLoop(NestLoop *node, EState *estate, int eflags)
{
	NestLoopState *nlstate;

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

	/* Extra inits for our method */
	// TODO set m based on relation size
	nlstate->activeRelationPages = 0;
	nlstate->isExploring = true;
	nlstate->lastReward = 0;
	nlstate->needOuterPage = true;
	nlstate->needInnerPage = true;
	nlstate->exploitStepCounter = 0;
	nlstate->innerPageCounter = 0;
	nlstate->innerPageCounterTotal = 0;
	nlstate->outerPageCounter = 0;
	nlstate->forLoopCounter = 0;
	nlstate->outerDone = false;
	nlstate->innerTupleCounter = 0;
	nlstate->outerTupleCounter = 0;

	NL1_printf("ExecInitNestLoop: %s\n",
			   "node initialized");
	elog(INFO, "ExecInitNestLoop: %s\n",
			   "node initialized");
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
	elog(INFO, "ExecEndNestLoop");
	NL1_printf("ExecEndNestLoop: %s\n",
			   "ending node processing");

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
}

/* ----------------------------------------------------------------
 *		ExecReScanNestLoop
 * ----------------------------------------------------------------
 */
void
ExecReScanNestLoop(NestLoopState *node)
{
	PlanState  *outerPlan = outerPlanState(node);

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

}
