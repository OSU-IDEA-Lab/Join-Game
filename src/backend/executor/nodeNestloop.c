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

#define BND8_RIGHT_TABLE_SIZE 110
#define BND8_LEFT_TABLE_SIZE 2
#define BND8_FAILURE_CONSTANT_N 20

static TupleTableSlot *
ExecNestLoop(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *tmpTupleSlot;
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

	
	/*
	 * Initalize left and right tables for Bandit Run
	 */
	if (!outerPlan->state->oslBnd8RightTableInitialized) {
		elog(INFO, "\n----------------------------------------");
		elog(INFO, "Initalization False for inner tuple table, current oslBnd8RightTupTableHead: %u", outerPlan->state->oslBnd8RightTupTableHead);
		elog(INFO, "outerPlan->state->oslBnd8RightTableInitialized: %s", outerPlan->state->oslBnd8RightTableInitialized ? "true" : "false");

		for (;;) {
			innerTupleSlot = ExecProcNode(innerPlan);
			if (TupIsNull(innerTupleSlot)) {
				break;
			}


			// Add the tuple to the list
			outerPlan->state->oslBnd8RightTableTuples[outerPlan->state->oslBnd8RightTupTableHead] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
			ExecCopySlot(outerPlan->state->oslBnd8RightTableTuples[outerPlan->state->oslBnd8RightTupTableHead], innerTupleSlot);
			outerPlan->state->oslBnd8RightTupTableHead++;
			elog(INFO, "Initalization Ongoing for inner tuple table, Current oslBnd8RightTupTableHead: %u", outerPlan->state->oslBnd8RightTupTableHead);

			if (outerPlan->state->oslBnd8RightTupTableHead >= BND8_RIGHT_TABLE_SIZE) {
				break;
			}
		}
		ENL1_printf("rescanning inner plan");
		ExecReScan(innerPlan);
		outerPlan->state->oslBnd8RightTableInitialized = true;
		elog(INFO, "\nInitalization Complete for inner tuple table, current oslBnd8RightTupTableHead: %u", outerPlan->state->oslBnd8RightTupTableHead);
		elog(INFO, "outerPlan->state->oslBnd8RightTableInitialized: %s", outerPlan->state->oslBnd8RightTableInitialized ? "true" : "false");
		elog(INFO, "----------------------------------------\n");
	}

	/*
	 * Initalize left and left tables for Bandit Run
	 */
	if (!outerPlan->state->oslBnd8LeftTableInitialized) {
		elog(INFO, "\n----------------------------------------");
		elog(INFO, "Initialization False for outer tuple table, curr_count: %u", outerPlan->state->oslBnd8LeftTupTableHead);
		elog(INFO, "outerPlan->state->oslBnd8LeftTableInitialized: %s", outerPlan->state->oslBnd8LeftTableInitialized ? "true" : "false");

		for (;;) {
			outerTupleSlot = ExecProcNode(outerPlan);
			if (TupIsNull(outerTupleSlot)) {
				break;
			}

			// Add the tuple to the list
			outerPlan->state->oslBnd8LeftTableTuples[outerPlan->state->oslBnd8LeftTupTableHead] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
			ExecCopySlot(outerPlan->state->oslBnd8LeftTableTuples[outerPlan->state->oslBnd8LeftTupTableHead], outerTupleSlot);
			outerPlan->state->oslBnd8LeftTableRewards[outerPlan->state->oslBnd8LeftTupTableHead] = 0;

			outerPlan->state->oslBnd8ToExploreTupleIdxs[outerPlan->state->oslBnd8ToExploreTupleIdxsHead] = outerPlan->state->oslBnd8LeftTupTableHead;
			outerPlan->state->oslBnd8ToExploitTupleIdxs[outerPlan->state->oslBnd8ToExploitTupleIdxsHead] = outerPlan->state->oslBnd8LeftTupTableHead;

			outerPlan->state->oslBnd8LeftTupTableHead++;
			outerPlan->state->oslBnd8ToExploreTupleIdxsHead++;
			outerPlan->state->oslBnd8ToExploitTupleIdxsHead++;
			elog(INFO, "Initalization Ongoing for outer tuple table, curr_count: %u", outerPlan->state->oslBnd8LeftTupTableHead);

			if (outerPlan->state->oslBnd8LeftTupTableHead >= BND8_LEFT_TABLE_SIZE) {
				break;
			}
		}
		outerPlan->state->oslBnd8LeftTableInitialized = true;

		elog(INFO, "\nInitialization Complete for outer tuple table, curr_count: %u", outerPlan->state->oslBnd8LeftTupTableHead);
		elog(INFO, "outerPlan->state->oslBnd8LeftTableInitialized: %s", outerPlan->state->oslBnd8LeftTableInitialized ? "true" : "false");
		elog(INFO, "----------------------------------------\n");
	}


	for (;;)
	{
		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{	
			if (outerPlan->state->oslBnd8ToExploitTupleIdxsHead == 0 & outerPlan->state->oslBnd8LeftTableParsedFully ){
				ENL1_printf("no outer tuple, ending join");
				elog(INFO, "no outer tuple left to exploit, ending join");
				return NULL;
			}

			if (outerPlan->state->oslBnd8ToExploreTupleIdxsHead > 0){
				outerPlan->state->oslBnd8ToExploreTupleIdxsHead--;
				int toExploreLeftTableIndx = outerPlan->state->oslBnd8ToExploreTupleIdxs[outerPlan->state->oslBnd8ToExploreTupleIdxsHead];
				outerTupleSlot = outerPlan->state->oslBnd8LeftTableTuples[toExploreLeftTableIndx];
				outerPlan->state->oslBnd8TmpTupleTable[0] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
				ExecCopySlot(outerPlan->state->oslBnd8TmpTupleTable[0], outerTupleSlot);
				elog(INFO, "\n Peeked top outer tuple for Exploration, at index : %d", toExploreLeftTableIndx);
				outerTupleSlot = outerPlan->state->oslBnd8TmpTupleTable[0];

				// setup for exploration
				outerPlan->state->oslBnd8InExplorationPhase = true;
				outerPlan->state->oslBnd8InExploitationPhase = false;
				outerPlan->state->oslBnd8RightTupTableHead = BND8_RIGHT_TABLE_SIZE;
				outerPlan->state->oslBnd8CurrNumSuccess = 0;
				outerPlan->state->oslBnd8CurrNumFailure = 0;
				outerPlan->state->oslBnd8CurrLeftTableTupleIdxForInnerLoop = toExploreLeftTableIndx;
			}
			else{
				if (outerPlan->state->oslBnd8ToExploitTupleIdxsHead > 0){
					// setup for exploitaiton 
					// Find Max reward tuple and pop it out of the tuple table. 
					// Sort the outerPlan->state->oslBnd8ToExploitTupleIdxs such that the head has max reward tuple idx; [Ascending order]
					// pop the most rewarding tuple
					outerPlan->state->oslBnd8ToExploitTupleIdxsHead--;
					int toExploitLeftTableIndx = outerPlan->state->oslBnd8ToExploitTupleIdxs[outerPlan->state->oslBnd8ToExploitTupleIdxsHead];
					outerTupleSlot = outerPlan->state->oslBnd8LeftTableTuples[toExploitLeftTableIndx];
					outerPlan->state->oslBnd8TmpTupleTable[0] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
					ExecCopySlot(outerPlan->state->oslBnd8TmpTupleTable[0], outerTupleSlot);
					elog(INFO, "\n Popped top outer tuple for exploitation, at index : %d", toExploitLeftTableIndx);
					outerTupleSlot = outerPlan->state->oslBnd8TmpTupleTable[0];
					// outerTupleSlot now contains the most rewarding tuple

					// setup for exploitation
					outerPlan->state->oslBnd8InExplorationPhase = false;
					outerPlan->state->oslBnd8InExploitationPhase = true;
					outerPlan->state->oslBnd8CurrNumSuccess = 0;
					outerPlan->state->oslBnd8CurrNumFailure = 0;
					outerPlan->state->oslBnd8CurrLeftTableTupleIdxForInnerLoop = toExploitLeftTableIndx;

					// Also get new one for exploration Tuple if exists.
					tmpTupleSlot = ExecProcNode(outerPlan);
					if (TupIsNull(tmpTupleSlot)){
						elog(INFO, "\n Not pushing anything new to exploit, curr oslBnd8ToExploitTupleIdxsHead %d", outerPlan->state->oslBnd8ToExploitTupleIdxsHead);
						outerPlan->state->oslBnd8LeftTableParsedFully = true;
					}
					else {
						if (!outerPlan->state->oslBnd8LeftTableParsedFully){
							// Add the tuple to the list
							outerPlan->state->oslBnd8LeftTableTuples[toExploitLeftTableIndx] = MakeSingleTupleTableSlot(tmpTupleSlot->tts_tupleDescriptor);
							ExecCopySlot(outerPlan->state->oslBnd8LeftTableTuples[toExploitLeftTableIndx], tmpTupleSlot);
							// outerPlan->state->oslBnd8LeftTupTableHead now contains the new exploration tuple 

							// update rewards for to explore tuple
							outerPlan->state->oslBnd8LeftTableRewards[toExploitLeftTableIndx] = 0;

							// update the to explore tuple idxs
							outerPlan->state->oslBnd8ToExploreTupleIdxs[outerPlan->state->oslBnd8ToExploreTupleIdxsHead] = toExploitLeftTableIndx;
							outerPlan->state->oslBnd8ToExploreTupleIdxsHead++;
							
							outerPlan->state->oslBnd8ToExploitTupleIdxs[outerPlan->state->oslBnd8ToExploitTupleIdxsHead] = toExploitLeftTableIndx;
							outerPlan->state->oslBnd8ToExploitTupleIdxsHead++;
							elog(INFO, "\n Pushed outer tuple for exploitation at index : %d", toExploitLeftTableIndx);
							elog(INFO, "\n Pushed outer tuple for Exploration at index : %d", toExploitLeftTableIndx);
						}
					}
				}
			}
			


			/*
			 * if there are no more outer tuples, then the join is complete..
			 */
			if (TupIsNull(outerTupleSlot))
			{
				ENL1_printf("no outer tuple, ending join");
				elog(INFO, "no outer tuple, ending join");
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

		Assert((outerPlan->state->oslBnd8InExplorationPhase || outerPlan->state->oslBnd8InExploitationPhase));
		Assert(!(outerPlan->state->oslBnd8InExplorationPhase & outerPlan->state->oslBnd8InExploitationPhase));

		// If exploration , just read from memory
		if (outerPlan->state->oslBnd8InExplorationPhase & !outerPlan->state->oslBnd8InExploitationPhase){
			if(outerPlan->state->oslBnd8RightTupTableHead > 0){
				outerPlan->state->oslBnd8RightTupTableHead--;
				innerTupleSlot = outerPlan->state->oslBnd8RightTableTuples[outerPlan->state->oslBnd8RightTupTableHead];
				elog(INFO, "\n Popped inner tuple, curr oslBnd8RightTupTableHead: %d", outerPlan->state->oslBnd8RightTupTableHead);
			}
			else {
				innerTupleSlot = NULL;
			}
		}
		// If exploitation , just read from disk
		if(!outerPlan->state->oslBnd8InExplorationPhase & outerPlan->state->oslBnd8InExploitationPhase){
				ENL1_printf("Reading new inner tuple from Disk");
				innerTupleSlot = ExecProcNode(innerPlan);
				elog(INFO, "Read new inner tuple");
				}


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
		if (!outerPlan->state->oslBnd8InExploitationPhase & outerPlan->state->oslBnd8InExplorationPhase){
			elog(INFO, "\n In exploration phase, oslBnd8CurrNumFailure: %d", outerPlan->state->oslBnd8CurrNumFailure);
			if(outerPlan->state->oslBnd8CurrNumFailure>=BND8_FAILURE_CONSTANT_N){
				node->nl_NeedNewOuter = true;
				continue;		/* return to top of loop */
			}
		}
		outerPlan->state->oslBnd8CurrNumFailure++;
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
				outerPlan->state->oslBnd8CurrNumFailure--;
				outerPlan->state->oslBnd8CurrNumSuccess++;
				if (!outerPlan->state->oslBnd8InExploitationPhase & outerPlan->state->oslBnd8InExplorationPhase){
					outerPlan->state->oslBnd8LeftTableRewards[outerPlan->state->oslBnd8CurrLeftTableTupleIdxForInnerLoop] = outerPlan->state->oslBnd8CurrNumSuccess;
				}
				outerPlan->state->oslBnd8CurrLeftTableTupleIdxForInnerLoop++;
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
#define BANDIT_TMP_TABLE_SIZE 4096

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
	* Clear out Tuple Tables
	*/
	// Function to free the array of TupleTableSlot pointers
	PlanState  *outerPlan;
	outerPlan = outerPlanState(node);
	for (int i = 0; i < BANDIT_TMP_TABLE_SIZE; ++i) {
		if (outerPlan->state->oslBnd8LeftTableTuples[i] != NULL) {
			ExecClearTuple(outerPlan->state->oslBnd8LeftTableTuples[i]);
			outerPlan->state->oslBnd8LeftTableTuples[i] = NULL; // Set the pointer to NULL after freeing
		}
	}
	for (int i = 0; i < BANDIT_TMP_TABLE_SIZE; ++i) {
		if (outerPlan->state->oslBnd8RightTableTuples[i] != NULL) {
			ExecClearTuple(outerPlan->state->oslBnd8RightTableTuples[i]);
			outerPlan->state->oslBnd8RightTableTuples[i] = NULL; // Set the pointer to NULL after freeing
		}
	}
	outerPlan = innerPlanState(node);
	for (int i = 0; i < BANDIT_TMP_TABLE_SIZE; ++i) {
		if (outerPlan->state->oslBnd8LeftTableTuples[i] != NULL) {
			ExecClearTuple(outerPlan->state->oslBnd8LeftTableTuples[i]);
			outerPlan->state->oslBnd8LeftTableTuples[i] = NULL; // Set the pointer to NULL after freeing
		}
	}
	for (int i = 0; i < BANDIT_TMP_TABLE_SIZE; ++i) {
		if (outerPlan->state->oslBnd8RightTableTuples[i] != NULL) {
			ExecClearTuple(outerPlan->state->oslBnd8RightTableTuples[i]);
			outerPlan->state->oslBnd8RightTableTuples[i] = NULL; // Set the pointer to NULL after freeing
		}
	}

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

	node->nl_NeedNewOuter = true;
	node->nl_MatchedOuter = false;
}
// 