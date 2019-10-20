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

// Stats 
// const int PAGE_SIZE = 8;
// const int SQRT_OF_N = 46; // this is sqrt of inner relation page count
// const int INNER_PAGE_COUNT = 2147; // inner relation page count 

// const int PAGE_SIZE = 128;
// const int SQRT_OF_N = 391; // this is sqrt of inner relation page count
// const int INNER_PAGE_COUNT = 152904; // inner relation page count 


// outer has 26664 tuples, 3333 pages
// inner has 1225105 tuples, 153138.1 pages
// inner has alsmot 1.2M and 150K pages

static RelationPage* CreateRelationPage() {
	int i;
	RelationPage* relationPage = palloc(sizeof(RelationPage));
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	relationPage->reward = 0;
	for (i = 0; i < PAGE_SIZE; i++){
		relationPage->tuples[i] = NULL;
	}
	relationPage->joinedPageIds = NULL;
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

static void RemoveAllRelationPages(NestLoopState* node) {
	while (node->activeRelationPages > 0){
		RemoveRelationPage(&(node->relationPages[node->activeRelationPages - 1]));
		node->activeRelationPages--;
	}
}

static void AddPageIdToJoinedPages(RelationPage* relationPage, int pageId) {
	lcons_int(pageId, relationPage->joinedPageIds);
}

static bool HasJoinedPage(RelationPage* relationPage, int pageId) {
	if (relationPage->joinedPageIds == NULL) return false;
	return list_member_int(relationPage->joinedPageIds, pageId);
}

static int LoadNextPage(PlanState* planState, RelationPage* relationPage) {
	int i;
	if (relationPage == NULL){
		elog(ERROR, "LoadNextPage: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	relationPage->reward = 0;
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

static RelationPage* PopBestPage(NestLoopState *node) {
	int bestPageIndex;
	int i;
	RelationPage* tmp;
	bestPageIndex = 0;
	for (i = 1; i < node->activeRelationPages; i++) {
		if (node->relationPages[i]->reward > node->relationPages[bestPageIndex]->reward) {
			bestPageIndex = i;
		}
	}
	tmp = node->relationPages[node->activeRelationPages - 1];
	node->relationPages[node->activeRelationPages - 1] = node->relationPages[bestPageIndex];
	node->relationPages[bestPageIndex] = tmp;
	node->activeRelationPages--;
	return node->relationPages[node->activeRelationPages];
}

static void PrintNodeCounters(NestLoopState *node){
	elog(INFO, "Read outer pages: %d", node->outerPageCounter);
	elog(INFO, "Read outer tuples: %ld", node->outerTupleCounter);
	elog(INFO, "Read inner pages: %d", node->innerPageCounterTotal);
	elog(INFO, "Read inner tuples: %ld", node->innerTupleCounter);
	elog(INFO, "Generated joins: %d", node->generatedJoins);
	elog(INFO, "Rescan Count: %d", node->rescanCount);
}

static TupleTableSlot* ExecFastNestLoop(PlanState *pstate)
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
			if (!node->reachedEndOfOuter){
				if (node->activeRelationPages < node->sqrtOfInnerPages) { 
					if (node->outerPageCounter % 1000 == 0)
						elog(INFO, "Read pages: %d", node->outerPageCounter);
					node->isExploring = true;
					node->outerPage = CreateRelationPage(); 
					LoadNextPage(outerPlan, node->outerPage);
					if (node->outerPage->tupleCount < PAGE_SIZE) {
						node->reachedEndOfOuter = true;
						if (node->outerPage->tupleCount == 0) continue;
					}
					node->outerTupleCounter += node->outerPage->tupleCount;
					node->outerPageCounter++;
					node->lastReward = 0;
				} else { // if (node->activeRelationPages == node->sqrtOfInnerPagesSQRT_OF_N
					node->outerPage = PopBestPage(node);
					node->outerPage->index = 0;
					node->isExploring = false;
					node->exploitStepCounter = 0;
				}  
			} else {
				if (node->activeRelationPages > 0) { // still has pages in stack
					node->outerPage = PopBestPage(node);
					node->outerPage->index = 0;
					node->isExploring = false;
					node->exploitStepCounter = 0;
				} else { // join is done
					PrintNodeCounters(node);
					return NULL;
				}
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
				if (node->isExploring && node->lastReward > 0) { //stay with current
					node->outerPage->index = 0;
					node->outerPage->reward += node->lastReward;
					node->lastReward = 0;
				} else if (node->isExploring && node->lastReward == 0) {
					//push the current explored page
					node->relationPages[node->activeRelationPages++] = node->outerPage;
					node->needOuterPage = true;
				} else if (!node->isExploring && node->exploitStepCounter < node->innerPageNumber) { 
					node->outerPage->index = 0;
					node->exploitStepCounter++;
				} else if (!node->isExploring && node->exploitStepCounter == node->innerPageNumber) {
					// Done with this outer page forever
					RemoveRelationPage(&(node->outerPage));
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
			elog(INFO, "Read outer pages: %d", node->outerPageCounter);
			return NULL;
		}
		if (TupIsNull(outerTupleSlot)){
			elog(WARNING, "outer tuple is null");
			elog(INFO, "Read outer pages: %d", node->outerPageCounter);
			if (node->activeRelationPages > 0) { // still has pages in stack
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
				if (HasJoinedPage(node->outerPage, node->innerPageCounter)) {
					continue;
				}
				AddPageIdToJoinedPages(node->outerPage, node->innerPageCounter);
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

static TupleTableSlot *
ExecRegularNestLoop(PlanState *pstate)
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
	if (strcmp(fastjoin, "on") == 0){
		tts = ExecFastNestLoop(pstate);
	} else {
		// tts = ExecPagedNestLoop(pstate);
		tts = ExecRegularNestLoop(pstate);
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

	/* Extra inits for our method */
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
	nlstate->innerPageNumber = (long)node->join.plan.righttree->plan_rows / PAGE_SIZE + 1;
	nlstate->sqrtOfInnerPages = (int)sqrt(nlstate->innerPageNumber);
	nlstate->relationPages = palloc(nlstate->sqrtOfInnerPages * sizeof(RelationPage*));

	// nlstate->outerPage = CreateRelationPage();  
	nlstate->innerPage = CreateRelationPage();

	NL1_printf("ExecInitNestLoop: %s\n",
			   "node initialized");
	/*
	elog(INFO, "ExecInitNestloop");
	elog_node_display(INFO,"Left: ", node->join.plan.lefttree, true);
	elog_node_display(INFO,"Right: ", node->join.plan.righttree, true);
	elog(INFO, "Computed inner page count: %ld, and sqrt: %d", 
		nlstate->innerPageNumber, nlstate->sqrtOfInnerPages);
	*/
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
	// Releasing memory 
	RemoveRelationPage(&(node->outerPage));
	RemoveRelationPage(&(node->innerPage));
	RemoveAllRelationPages(node);
	pfree(node->relationPages);
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
