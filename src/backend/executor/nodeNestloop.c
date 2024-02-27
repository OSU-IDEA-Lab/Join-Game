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

#include "utils/builtins.h"
#include "fmgr.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "utils/rel.h"
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

#define PGNST8_LEFT_PAGE_MAX_SIZE 320
#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE 4096

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
		* Initalize Right Table Cacheif it has not be initalized
		*/
		if(!outerPlan->oslBnd8RightTableCacheInitialized){
			elog(INFO, "Right init Start ----------------------------------------");
			while (outerPlan->oslBnd8RightTableCacheHead<OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE){
				// Fill Table
				innerTupleSlot = ExecProcNode(innerPlan);
				if (TupIsNull(innerTupleSlot)) {
					break;
				}
				// Add the tuple to the list
				if (TupIsNull(outerPlan->oslBnd8RightTableCache[outerPlan->oslBnd8RightTableCacheHead])) {
					outerPlan->oslBnd8RightTableCache[outerPlan->oslBnd8RightTableCacheHead] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
				}
				ExecCopySlot(outerPlan->oslBnd8RightTableCache[outerPlan->oslBnd8RightTableCacheHead], innerTupleSlot);
				outerPlan->oslBnd8RightTableCacheHead++;
				outerPlan->oslBnd8RightTableCacheSize++;
			}
			elog(INFO, "Initialization Complete for inner table cache, current head: %u", outerPlan->oslBnd8RightTableCacheHead);
			outerPlan->oslBnd8RightTableCacheInitialized = true;
			elog(INFO, "rescanning inner plan");
			ExecReScan(innerPlan);
			elog(INFO, "Right Init End ----------------------------------------\n");
		}
		
		/*
		* Read new left Page, if new outer page is needed
		*/
		if (outerPlan->nl_needNewOuterPage){
			outerPlan->pgNst8LeftPageHead = 0;
			outerPlan->pgNst8LeftPageSize = 0;
			
			// Fill Table
			while (!outerPlan->pgNst8LeftParsedFully & outerPlan->pgNst8LeftPageHead < PGNST8_LEFT_PAGE_MAX_SIZE) {
				outerTupleSlot = ExecProcNode(outerPlan);
				if (TupIsNull(outerTupleSlot)) { 
					// elog(INFO, "Finished Parsing left table: %u", outerPlan->pgNst8LeftPageHead);
					outerPlan->pgNst8LeftParsedFully = true;
					break;
				}

				// Allocate Memory if it has not been allocated
				if (TupIsNull(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead])) {
					outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
				}

				// Insert Tuple to Left Page
				// outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
				ExecCopySlot(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead], outerTupleSlot);
				outerPlan->pgNst8LeftPageHead++;
				outerPlan->pgNst8LeftPageSize++;
				// elog(INFO, "Tuple inserted in left page, Current pgNst8LeftPageHead: %u", outerPlan->pgNst8LeftPageHead);
			}
			outerPlan->nl_needNewOuterPage = false;

			ENL1_printf("rescanning inner plan");
			// elog(INFO, "rescanning inner plan");
			ExecReScan(innerPlan);
		}
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{	
			ENL1_printf("getting new outer tuple");
			// outerTupleSlot = ExecProcNode(outerPlan);

			/* Scan From Page*/
			if (outerPlan->pgNst8LeftPageSize ==0){
				outerTupleSlot = NULL;
			}
			else{
				outerPlan->pgNst8LeftPageHead--;
				outerTupleSlot = outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead];
			}

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
			// ENL1_printf("rescanning inner plan");
			// ExecReScan(innerPlan);
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		node->nl_NeedNewOuter = true;
		// If this is a fresh left page scan, and get new inner tuple
		if (outerPlan->pgNst8LeftPageHead == outerPlan->pgNst8LeftPageSize-1){
			// elog(INFO, "Getting New Inner Tuple for left page head @: %u", outerPlan->pgNst8LeftPageHead);
			innerTupleSlot = ExecProcNode(innerPlan);
			// Allocate Memory if it has not been allocated
			if (TupIsNull(outerPlan->pgNst8_innertuple[0])) {
				outerPlan->pgNst8_innertuple[0] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
			}
			if (!TupIsNull(innerTupleSlot)){
			ExecCopySlot(outerPlan->pgNst8_innertuple[0], innerTupleSlot);
			}
		}
		else{
			// elog(INFO, "Using the same Inner Tuple for left page head @: %u", outerPlan->pgNst8LeftPageHead);
			innerTupleSlot = outerPlan->pgNst8_innertuple[0];
			// if page end, Loop back Page Head 
			if (outerPlan->pgNst8LeftPageHead == 0){
				outerPlan->pgNst8LeftPageHead = outerPlan->pgNst8LeftPageSize;
			}
		}
		econtext->ecxt_innertuple = innerTupleSlot;
		outerPlan->pgNst8InnerTableParseCount++;
		// elog(INFO, "pgNst8InnerTableParseCount @: %u", outerPlan->pgNst8InnerTableParseCount);

		if (TupIsNull(innerTupleSlot))
		{
			ENL1_printf("no inner tuple, need new outer tuple");
			// elog(INFO, "no inner tuple, need new outer tuple");

			outerPlan->nl_needNewOuterPage = true;

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
	
	elog(INFO, "Nested loop is called:");
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

	// Initialize variables
	PlanState  *outerPlan;
	outerPlan = outerPlanState(nlstate);
	
	outerPlan->nl_needNewOuterPage = true;
	outerPlan->pgNst8LeftPageHead = 0;
	outerPlan->pgNst8LeftPageSize = 0;
	outerPlan->pgNst8LeftParsedFully = false;
	outerPlan->pgNst8InnerTableParseCount = 0;

	outerPlan->oslBnd8RightTableCacheInitialized = false;
	outerPlan->oslBnd8RightTableCacheHead = 0;

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
	
	nlstate->outerAttrNum = 1;
	nlstate->innerAttrNum = 2;

	NL1_printf("ExecInitNestLoop: %s\n",
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
	PlanState  *outerPlan;
	outerPlan = outerPlanState(node);
	
	// Free up the Memory
	int i;
	for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++) {
		if (!TupIsNull(outerPlan->pgNst8LeftPage[i])) {
			ExecDropSingleTupleTableSlot(outerPlan->pgNst8LeftPage[i]);
			outerPlan->pgNst8LeftPage[i] = NULL;
		}
	}
	// Free up the Memory
	if (!TupIsNull(outerPlan->pgNst8_innertuple[0])) {
		ExecDropSingleTupleTableSlot(outerPlan->pgNst8_innertuple[0]);
		outerPlan->pgNst8_innertuple[0] = NULL;
	}
	// Free up the Memory
	for (i = 0; i < OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE; i++) {
		if (!TupIsNull(outerPlan->oslBnd8RightTableCache[i])) {
			ExecDropSingleTupleTableSlot(outerPlan->oslBnd8RightTableCache[i]);
			outerPlan->oslBnd8RightTableCache[i] = NULL;
		}
	}

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

	node->nl_NeedNewOuter = true;
	node->nl_MatchedOuter = false;
}
