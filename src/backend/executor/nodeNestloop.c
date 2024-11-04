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

/*
 * Hyper-parameters of ICL
 */
#define PGNST8_LEFT_PAGE_MAX_SIZE 100 // Memory Size for ToExploitBatch after every exploration.
#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE 1000 // In Memory Size Right Table Cache Size, used for exploration. 
#define OSL_BND8_RIGHT_EXP_CACHE_MAX_SIZE 100 // In Memory Size Right Exploitation Cache Size, used for exploitation. 
#define MUST_EXPLORE_TUPLE_COUNT_N 5000 // Number of tuples that must be explored before Exploitation can happen. 
#define FAILURE_COUNT_N 500 // Number of failures allowed during exploration, before jumping into next outer tuple, for exploration
#define DEBUG_FLAG 0 // print statements will be activate if set to 1

/*
 * States of ICL
 */
#define LEFT_TO_RIGHT		0
#define RIGHT_TO_LEFT		1

static TupleTableSlot *
seedToExploitLeftPage(PlanState *pstate){
	NestLoopState *node = castNode(NestLoopState, pstate);
	NestLoop   *nl;
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	TupleTableSlot *returnTupleSlot;
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
	if(DEBUG_FLAG){elog(INFO, "New Left Page Read Called, oslBnd8_ExplorationStarted: %u", outerPlan->oslBnd8_ExplorationStarted);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->pgNst8LeftPageHead: %u", outerPlan->pgNst8LeftPageHead);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8_numTuplesExplored: %u", outerPlan->oslBnd8_numTuplesExplored);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8_currExploreTupleFailureCount: %u", outerPlan->oslBnd8_currExploreTupleFailureCount);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8_currExploreTupleReward: %u", outerPlan->oslBnd8_currExploreTupleReward);}
	if(DEBUG_FLAG){elog(INFO, "innerPlan->oslBnd8RightTableCacheHead: %u", innerPlan->oslBnd8RightTableCacheHead);}
	if(DEBUG_FLAG){elog(INFO, "node->nl_NeedNewOuter: %u", node->nl_NeedNewOuter);}

	if(outerPlan->oslBnd8_ExplorationStarted){
		// Do not initialize variables again
		int dummy;
	}
	else{
		switch (node->direction)
		{
			case LEFT_TO_RIGHT:
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
				
				// Only rescan if innerPlan->chgParam is not NULL
				if (innerPlan->chgParam != NULL) {
					ExecReScan(innerPlan);
				}
				
				int j;
				
				for (j = 1; j <= node->innerPointer; j++) {
					innerTupleSlot = ExecProcNode(innerPlan);
				}
				
				for (j = 1; j < innerPlan->oslBnd8RightTableCacheSize; j++) {
					if (innerPlan->rightNFailure[j] >= FAILURE_COUNT_N){
						innerTupleSlot = ExecProcNode(innerPlan);
						node->innerPointer++;
						if (TupIsNull(innerTupleSlot)) {
							outerPlan->pgNst8LeftParsedFully = true;
							break;
						}
						// Replace the tuple to the list
						if (TupIsNull(innerPlan->oslBnd8RightTableCache[j])) {
							innerPlan->oslBnd8RightTableCache[j] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
						}
						ExecCopySlot(innerPlan->oslBnd8RightTableCache[j], innerTupleSlot);
						innerPlan->pgReward[j] = 0;
						innerPlan->rightNFailure[j] = 0;
					}
				}
				
				for (j = 0; j < PGNST8_LEFT_PAGE_MAX_SIZE; j++) {
					outerPlan->pgReward[j] = 0;
				}
				
				outerPlan->pgNst8LeftPageHead = 0;
				outerPlan->pgNst8LeftPageSize = 0;
				innerPlan->oslBnd8RightTableCacheHead = 0;
				innerPlan->oslBnd8RightExpCacheHead = 0;
				node->maxOuterReward = 0;
				outerPlan->zeroRewardExists = true;
				
				break;

			case RIGHT_TO_LEFT:
				/*
				 * fetch the values of any inner Vars that must be passed to the
				 * outer scan, and store them in the appropriate PARAM_EXEC slots.
				 */				
				foreach(lc, nl->nestParams)
				{
					NestLoopParam *nlp = (NestLoopParam *) lfirst(lc);
					int            paramno = nlp->paramno;
					ParamExecData *prm;

					prm = &(econtext->ecxt_param_exec_vals[paramno]);
					/* Param value should be an INNER_VAR var */
					Assert(IsA(nlp->paramval, Var));
					Assert(nlp->paramval->varno == INNER_VAR);
					Assert(nlp->paramval->varattno > 0);
					prm->value = slot_getattr(innerTupleSlot,
											nlp->paramval->varattno,
											&(prm->isnull));
					/* Flag parameter value as changed */
					outerPlan->chgParam = bms_add_member(outerPlan->chgParam,
															paramno);
				}						
				
				// Only rescan if outerPlan->chgParam is not NULL
				if (outerPlan->chgParam != NULL) {
					ExecReScan(outerPlan);
				}
				
				int x;
				
				for (x = 1; x <= node->outerPointer; x++) {
					outerTupleSlot = ExecProcNode(outerPlan);
				}
				
				for (x = 0; x < OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE; x++) {
					innerPlan->pgReward[x] = 0;
					innerPlan->rightNFailure[x] = 0;
				}
				
				for (x = 0; x < OSL_BND8_RIGHT_EXP_CACHE_MAX_SIZE; x++) {
					innerPlan->rightExpReward[x] = 0;
				}

				outerPlan->pgNst8LeftPageHead = 0;
				innerPlan->oslBnd8RightTableCacheSize = 0;
				innerPlan->oslBnd8RightTableCacheHead = 0;				
				innerPlan->oslBnd8RightExpCacheSize = 0;
				innerPlan->oslBnd8RightExpCacheHead = 0;
				node->maxInnerReward = 0;
				innerPlan->zeroRewardExists = true;
				
				break;

			default:
        		// Do nothing
        		break;
		}

		outerPlan->cursorReward = 0;
		innerPlan->cursorExpReward = 0;
		outerPlan->oslBnd8_currExploreTupleFailureCount =0;
		outerPlan->oslBnd8_numTuplesExplored=0;
		outerPlan->oslBnd8_ExplorationStarted = true;

		if (node->direction == 2) {
			outerPlan->pgNst8LeftPageHead = 0;
			outerPlan->pgNst8LeftPageSize = 0;
			innerPlan->oslBnd8RightTableCacheSize = 0;
			innerPlan->oslBnd8RightTableCacheHead = 0;				
			innerPlan->oslBnd8RightExpCacheSize = 0;
			innerPlan->oslBnd8RightExpCacheHead = 0;
			node->maxInnerReward = 0;
			node->maxOuterReward = 0;
			outerPlan->zeroRewardExists = true;
			innerPlan->zeroRewardExists = true;
		}
	
		node->nl_NeedNewOuter = true;
	}

	/* Read a page such that They can be exploited*/
	while (!outerPlan->pgNst8LeftParsedFully & outerPlan->cursorReward < PGNST8_LEFT_PAGE_MAX_SIZE){
		/*
		 * If we don't have an outer tuple, get the next one and reset the inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{
			if (outerPlan->oslBnd8_numTuplesExplored > MUST_EXPLORE_TUPLE_COUNT_N){
				if(DEBUG_FLAG){elog(INFO, "num_tuples_explored greater than N, num_tuples_explored: %u", outerPlan->oslBnd8_numTuplesExplored);}
				break;
			}

			ENL1_printf("getting new outer tuple");
			if(DEBUG_FLAG){elog(INFO, "outerPlan-> Disk Read Tuple");}
			outerTupleSlot = ExecProcNode(outerPlan);
			node->outerPointer++;
			if (TupIsNull(outerTupleSlot)) { 
				elog(INFO, "Finished Parsing left table: %u", outerPlan->pgNst8LeftPageHead);
				outerPlan->pgNst8LeftParsedFully = true;
				break;
			}
			if (TupIsNull(outerPlan->oslBnd8_currExploreTuple)) { outerPlan->oslBnd8_currExploreTuple = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);}
			if (!TupIsNull(outerTupleSlot)){ ExecCopySlot(outerPlan->oslBnd8_currExploreTuple, outerTupleSlot);}
			outerPlan->oslBnd8_currExploreTupleReward = 0;
			innerPlan->oslBnd8RightTableCacheHead = 0;

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
		}

		/*
		* we have an outerTuple, try to get the next inner tuple.
		*/
		ENL1_printf("getting new inner tuple");
		if(DEBUG_FLAG){elog(INFO, "getting new inner tuple");}

		if (innerPlan->oslBnd8RightTableCacheHead >= innerPlan->oslBnd8RightTableCacheSize){
			if (innerPlan->oslBnd8RightTableCacheHead < (OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE - 1)){
				// Fill Table
				innerTupleSlot = ExecProcNode(innerPlan);
				node->innerPointer++;
				if (TupIsNull(innerTupleSlot)) {
					outerPlan->pgNst8LeftParsedFully = true;
					break;
				}
				// Add the tuple to the list
				if (TupIsNull(innerPlan->oslBnd8RightTableCache[innerPlan->oslBnd8RightTableCacheHead])) {
					innerPlan->oslBnd8RightTableCache[innerPlan->oslBnd8RightTableCacheHead] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
				}
				ExecCopySlot(innerPlan->oslBnd8RightTableCache[innerPlan->oslBnd8RightTableCacheHead], innerTupleSlot);
				innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead] = 0;
				innerPlan->rightNFailure[innerPlan->oslBnd8RightTableCacheHead] = 0;
				innerPlan->oslBnd8RightTableCacheSize++;
			}
		}

		innerTupleSlot = innerPlan->oslBnd8RightTableCache[innerPlan->oslBnd8RightTableCacheHead];
		econtext->ecxt_innertuple = innerTupleSlot;
		innerPlan->oslBnd8RightTableCacheHead++;
		
		ENL1_printf("testing qualification");
		if(DEBUG_FLAG){elog(INFO, "testing qualification");}
		node->nl_MatchedOuter = ExecQual(joinqual, econtext);
		if(node->nl_MatchedOuter){
			outerPlan->oslBnd8_currExploreTupleReward++;
			innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1]++;
		}
		else{
			outerPlan->oslBnd8_currExploreTupleFailureCount++;
			innerPlan->rightNFailure[innerPlan->oslBnd8RightTableCacheHead - 1]++;
			//elog(INFO, "1. inner reward is %u", innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1]);
			if ( (innerPlan->rightNFailure[innerPlan->oslBnd8RightTableCacheHead - 1] == FAILURE_COUNT_N ) ) {
				if (innerPlan->oslBnd8RightExpCacheSize < OSL_BND8_RIGHT_EXP_CACHE_MAX_SIZE){
					node->nl_NeedNewInner = true;
					// Add the tuple to the right (inner) exploitation cache
					if (TupIsNull(innerPlan->oslBnd8RightExpCache[innerPlan->oslBnd8RightExpCacheHead])) {
						innerPlan->oslBnd8RightExpCache[innerPlan->oslBnd8RightExpCacheHead] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
					}
					ExecCopySlot(innerPlan->oslBnd8RightExpCache[innerPlan->oslBnd8RightExpCacheHead], innerTupleSlot);
					innerPlan->rightExpReward[innerPlan->oslBnd8RightExpCacheHead] = innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];

					innerPlan->oslBnd8RightExpCacheSize++;
					innerPlan->oslBnd8RightExpCacheHead++;
					node->maxInnerReward = node->maxInnerReward + innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];
				}
				else {
					if (innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1] > 0) {
						//int p = 0;
						//for (p = 0; p < OSL_BND8_RIGHT_EXP_CACHE_MAX_SIZE; p++) {
						//	if (innerPlan->rightExpReward[p] >= innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1]) {
						//		continue;
						//	} 
						//	else {
						//		ExecCopySlot(innerPlan->oslBnd8RightExpCache[p], innerTupleSlot);
						//		node->maxInnerReward = node->maxInnerReward - innerPlan->rightExpReward[p];
						//		node->maxInnerReward = node->maxInnerReward + innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];
						//		innerPlan->rightExpReward[p] = innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];
						//		break;
						//	}
						//}
						//new
						bool replaced = false;
						
						if (innerPlan->zeroRewardExists) {
							while (innerPlan->cursorExpReward < OSL_BND8_RIGHT_EXP_CACHE_MAX_SIZE) {
								if (innerPlan->rightExpReward[innerPlan->cursorExpReward] == 0) {
									ExecCopySlot(innerPlan->oslBnd8RightExpCache[innerPlan->cursorExpReward], innerTupleSlot);
									node->maxInnerReward = node->maxInnerReward + innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];
									innerPlan->rightExpReward[innerPlan->cursorExpReward] = innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];
									innerPlan->cursorExpReward++;
									replaced = true;
									break;
								}
								innerPlan->cursorExpReward++;
							}
						
							if (!replaced) {
								innerPlan->zeroRewardExists = false;
							}
						}
						
						if (!replaced) {
							int lowestIndex = -1;
							int lowestReward = INT_MAX;
							int i;
							for (i = 0; i < OSL_BND8_RIGHT_EXP_CACHE_MAX_SIZE; i++) {
								if (innerPlan->rightExpReward[i] < lowestReward) {
									lowestReward = innerPlan->rightExpReward[i];
									lowestIndex = i;
								}
							}
						
							if (lowestReward < innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1]) {
								ExecCopySlot(innerPlan->oslBnd8RightExpCache[lowestIndex], innerTupleSlot);
								node->maxInnerReward = node->maxInnerReward - lowestReward;
								node->maxInnerReward = node->maxInnerReward + innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];
								innerPlan->rightExpReward[lowestIndex] = innerPlan->pgReward[innerPlan->oslBnd8RightTableCacheHead - 1];
							}
						}
						//new
					}
				}
			}
		}
		
		if(DEBUG_FLAG){elog(INFO, "node->nl_MatchedOuter: %u", node->nl_MatchedOuter);}
		if(DEBUG_FLAG){elog(INFO, "innerPlan->oslBnd8RightTableCacheHead: %u", innerPlan->oslBnd8RightTableCacheHead);}
		
		// Finish exploring for the current outer tuple, determine if it has to be stored into the memory
		if ((innerPlan->oslBnd8RightTableCacheHead == (OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE - 1)) || 
			(outerPlan->oslBnd8_currExploreTupleFailureCount > FAILURE_COUNT_N)){
			outerPlan->oslBnd8_numTuplesExplored++;		
			node->nl_NeedNewOuter = true;
			outerPlan->oslBnd8_currExploreTupleFailureCount = 0;
			innerPlan->oslBnd8RightTableCacheHead = 0;
			
			// Allocate Memory if it has not been allocated
			if (outerPlan->pgNst8LeftPageSize < PGNST8_LEFT_PAGE_MAX_SIZE){
				if(DEBUG_FLAG){elog(INFO, "outerPlan->pgNst8LeftPageHead: %u", outerPlan->pgNst8LeftPageHead);}
				if(DEBUG_FLAG){elog(INFO, "Adding it to the Left Page, num_tuples_explored: %u", outerPlan->oslBnd8_numTuplesExplored);}
				if (TupIsNull(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead])) {outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead] = MakeSingleTupleTableSlot(outerPlan->oslBnd8_currExploreTuple->tts_tupleDescriptor);}
				ExecCopySlot(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead], outerPlan->oslBnd8_currExploreTuple);
				outerPlan->pgReward[outerPlan->pgNst8LeftPageSize] = outerPlan->oslBnd8_currExploreTupleReward;
				node->maxOuterReward = node->maxOuterReward + outerPlan->pgReward[outerPlan->pgNst8LeftPageSize];
				
				outerPlan->pgNst8LeftPageHead++;
				outerPlan->pgNst8LeftPageSize++;
				if(DEBUG_FLAG){elog(INFO, "Adding complete to the Left Page, num_tuples_explored: %u", outerPlan->oslBnd8_numTuplesExplored);}
			}
			// Otherwise, they will be parsed.
			else{
				if (outerPlan->oslBnd8_currExploreTupleReward > 0){
					//int p = 0;
					//for (p = 0; p < PGNST8_LEFT_PAGE_MAX_SIZE; p++) {
					//	if (outerPlan->pgReward[p] >= outerPlan->oslBnd8_currExploreTupleReward) {
					//		continue;
					//	} 
					//	else {
					//		ExecCopySlot(outerPlan->pgNst8LeftPage[p], outerPlan->oslBnd8_currExploreTuple);
					//		node->maxOuterReward = node->maxOuterReward - outerPlan->pgReward[p];
					//		node->maxOuterReward = node->maxOuterReward + outerPlan->oslBnd8_currExploreTupleReward;
					//		outerPlan->pgReward[p] = outerPlan->oslBnd8_currExploreTupleReward;
					//		break;
					//	}
					//}
					//new
					bool replaced = false;
					
					if (outerPlan->zeroRewardExists) {
						while (outerPlan->cursorReward < PGNST8_LEFT_PAGE_MAX_SIZE) {
							if (outerPlan->pgReward[outerPlan->cursorReward] == 0) {
								ExecCopySlot(outerPlan->pgNst8LeftPage[outerPlan->cursorReward], outerPlan->oslBnd8_currExploreTuple);
								node->maxOuterReward = node->maxOuterReward + outerPlan->oslBnd8_currExploreTupleReward;
								outerPlan->pgReward[outerPlan->cursorReward] = outerPlan->oslBnd8_currExploreTupleReward;
								outerPlan->cursorReward++;
								replaced = true;
								break;
							}
							outerPlan->cursorReward++;
						}
					
						if (!replaced) {
							outerPlan->zeroRewardExists = false;
						}
					}
					
					if (!replaced) {
						int lowestIndex = -1;
						int lowestReward = INT_MAX;
						int i;
						for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++) {
							if (outerPlan->pgReward[i] < lowestReward) {
								lowestReward = outerPlan->pgReward[i];
								lowestIndex = i;
							}
						}
					
						if (lowestReward < outerPlan->oslBnd8_currExploreTupleReward) {
							ExecCopySlot(outerPlan->pgNst8LeftPage[lowestIndex], outerPlan->oslBnd8_currExploreTuple);
							node->maxOuterReward = node->maxOuterReward - lowestReward;
							node->maxOuterReward = node->maxOuterReward + outerPlan->oslBnd8_currExploreTupleReward;
							outerPlan->pgReward[lowestIndex] = outerPlan->oslBnd8_currExploreTupleReward;
						}
					}
					//new
				}
			}
		}

		if(node->nl_MatchedOuter){
			ENL1_printf("qualification succeeded, projecting tuple");
			return ExecProject(node->js.ps.ps_ProjInfo);
		}
		/*
		 * Tuple fails qual, so free per-tuple memory and try again.
		 */
		ResetExprContext(econtext);
		ENL1_printf("qualification failed, looping");
	}

	if(DEBUG_FLAG){
		elog(INFO, "Exploration Complete with pgNst8LeftPageSize: %u", outerPlan->pgNst8LeftPageSize);
	}

	if(DEBUG_FLAG){elog(INFO, "Seed Complete with: %u", outerPlan->pgNst8LeftPageSize);}
	outerPlan->oslBnd8_ExplorationStarted = false;
	if(DEBUG_FLAG){elog(INFO, "Exploration Completed, Seeding Left Page Complete");}

	return returnTupleSlot;
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
	TupleTableSlot *returnTupleSlot;
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
	
	if (node->innerRelCount == 0){
		node->innerRelCount = innerPlan->plan->plan_rows;
	}
	
	if (node->outerRelCount == 0){
		node->outerRelCount = outerPlan->plan->plan_rows;
	}

	ENL1_printf("entering main loop");
	
	int z=0;
	for (;;)
	{
		/*
		 * Read new left Page, if new outer page is needed
		 */
        if (node->nl_needNewBest){
            returnTupleSlot = seedToExploitLeftPage(pstate);
            if (!TupIsNull(returnTupleSlot)){
                if(DEBUG_FLAG){elog(INFO, "Returning tuple. outerPlan->pgNst8LeftPageHead: %u", outerPlan->pgNst8LeftPageHead);}
                return returnTupleSlot;
            }
            if(DEBUG_FLAG){elog(INFO, "Exploration Completed, Seeding Left Page Complete");}
            
            /*
             * Decide whether to exploit left cache (outer) or right cache (inner)
             */
			
			//avg reward of left cache per one right cache tuple is as below.
			double avgLeftReward = (double)node->maxOuterReward / (double)outerPlan->pgNst8LeftPageSize;
			//avg reward of right cache per one left cache tuple is as below.
			double avgRightReward = (double)node->maxInnerReward / (double)innerPlan->oslBnd8RightExpCacheSize;
			
			if ( (avgRightReward * (node->outerRelCount - node->outerPointer) * innerPlan->oslBnd8RightExpCacheSize ) >
				(avgLeftReward * (node->innerRelCount - node->innerPointer) * outerPlan->pgNst8LeftPageSize) ){
                node->direction = RIGHT_TO_LEFT;
				elog(INFO, "Inner Cache Picked for Exploitation");
            } else {
                node->direction = LEFT_TO_RIGHT;
				elog(INFO, "Outer Cache Picked for Exploitation");
            }
            node->nl_needNewBest = false;
        }
		
        /*
         * Exploit the chosen cache depending on the direction
         */
        switch (node->direction)
        {
            case LEFT_TO_RIGHT:
				/*
				* If we don't have an outer tuple, get the next one and reset the
				* inner scan.
				*/
				if (node->nl_NeedNewOuter)
				{
					if (outerPlan->pgNst8LeftPageHead == 0){
						outerPlan->pgNst8LeftPageHead = outerPlan->pgNst8LeftPageSize;
					}
					ENL1_printf("getting new outer tuple");
		
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
				}
		
				/*
				* we have an outerTuple, try to get the next inner tuple.
				*/
				ENL1_printf("getting new inner tuple");
		
				node->nl_NeedNewOuter = true;
				// If this is a fresh left page scan, and get new inner tuple
				if (outerPlan->pgNst8LeftPageHead != outerPlan->pgNst8LeftPageSize-1){
					innerTupleSlot = outerPlan->pgNst8_innertuple[0];
					// if page end, Loop back Page Head 
				}
				else{
					innerTupleSlot = ExecProcNode(innerPlan);
					// Allocate Memory if it has not been allocated
					if (TupIsNull(outerPlan->pgNst8_innertuple[0])) {
						outerPlan->pgNst8_innertuple[0] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
					}
					if (!TupIsNull(innerTupleSlot)){
					ExecCopySlot(outerPlan->pgNst8_innertuple[0], innerTupleSlot);
					}
				}
				if (outerPlan->pgNst8LeftPageHead == 0){
					outerPlan->pgNst8LeftPageHead = outerPlan->pgNst8LeftPageSize;
				}
		
				econtext->ecxt_innertuple = innerTupleSlot;
		
				if (TupIsNull(innerTupleSlot))
				{
					ENL1_printf("no inner tuple, need new outer tuple");
		
					node->nl_needNewBest = true;
		
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
				break;

            case RIGHT_TO_LEFT:
				/*
				* If we don't have an inner tuple, get the next one and reset the
				* outer scan.
				*/
				if (node->nl_NeedNewInner)
				{
					if (innerPlan->oslBnd8RightExpCacheHead == 0){
						innerPlan->oslBnd8RightExpCacheHead = innerPlan->oslBnd8RightExpCacheSize;
					}
					ENL1_printf("getting new inner tuple");
		
					/* Scan From Page*/
					if (innerPlan->oslBnd8RightExpCacheSize ==0){
						innerTupleSlot = NULL;
					}
					else{
						innerPlan->oslBnd8RightExpCacheHead--;
						innerTupleSlot = innerPlan->oslBnd8RightExpCache[innerPlan->oslBnd8RightExpCacheHead];
					}

					/*
					* if there are no more inner tuples, then the join is complete..
					*/
					if (TupIsNull(innerTupleSlot))
					{
						ENL1_printf("no inner tuple, ending join");
						return NULL;
					} 
		
					ENL1_printf("saving new inner tuple information");
					econtext->ecxt_innertuple = innerTupleSlot;
					
					node->nl_NeedNewInner = false;
					node->nl_MatchedInner = false;
				}
		
				/*
				* we have an innerTuple, try to get the next outer tuple.
				*/
				ENL1_printf("getting new outer tuple");
		
				node->nl_NeedNewInner = true;
				// If this is a fresh right page scan, and get new outer tuple
				if (innerPlan->oslBnd8RightExpCacheHead != innerPlan->oslBnd8RightExpCacheSize-1){
					outerTupleSlot = innerPlan->pgNst8_outertuple[0];
					// if page end, Loop back Page Head 
				}
				else{
					outerTupleSlot = ExecProcNode(outerPlan);
					// Allocate Memory if it has not been allocated
					if (TupIsNull(innerPlan->pgNst8_outertuple[0])) {
						innerPlan->pgNst8_outertuple[0] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
					}
					if (!TupIsNull(outerTupleSlot)){
					ExecCopySlot(innerPlan->pgNst8_outertuple[0], outerTupleSlot);
					}
				}
				if (innerPlan->oslBnd8RightExpCacheHead == 0){
					innerPlan->oslBnd8RightExpCacheHead = innerPlan->oslBnd8RightExpCacheSize;
				}
		
				econtext->ecxt_outertuple = outerTupleSlot;
		
				if (TupIsNull(outerTupleSlot))
				{
					ENL1_printf("no outer tuple, need new inner tuple");
		
					node->nl_needNewBest = true;
		
					if (!node->nl_MatchedInner &&
						(node->js.jointype == JOIN_LEFT ||
						node->js.jointype == JOIN_ANTI))
					{
						/*
						* We are doing an inner join and there were no join matches
						* for this inner tuple.  Generate a fake join tuple with
						* nulls for the outer tuple, and return it if it passes the
						* non-join quals.
						*/
						econtext->ecxt_outertuple = node->nl_NullOuterTupleSlot;
		
						ENL1_printf("testing qualification for inner-join tuple");
		
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
					* Otherwise just return to top of loop for a new inner tuple.
					*/
					continue;
				}
		
				/*
				* at this point we have a new pair of outer and inner tuples so we
				* test the outer and inner tuples to see if they satisfy the node's
				* qualification.
				*
				* Only the joinquals determine MatchedInner status, but all quals
				* must pass to actually return the tuple.
				*/
				ENL1_printf("testing qualification");
		
				if (ExecQual(joinqual, econtext))
				{
					node->nl_MatchedInner = true;
		
					/* In an antijoin, we never return a matched tuple */
					if (node->js.jointype == JOIN_ANTI)
					{
						node->nl_NeedNewInner = true;
						continue;		/* return to top of loop */
					}
		
					/*
					* If we only need to join to the first matching outer tuple, then
					* consider returning this one, but after that continue with next
					* inner tuple.
					*/
					if (node->js.single_match)
						node->nl_NeedNewInner = true;
		
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
				break;

            default:
                elog(ERROR, "Invalid direction in nested loop join");

		}
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
	
	elog(INFO, "ICL is called:");
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

	NL1_printf("ExecInitNestLoop: %s\n",
			   "node initialized");

	/* Initialize variables */
	PlanState  *outerPlan;
	PlanState  *innerPlan;
	outerPlan = outerPlanState(nlstate);
	innerPlan = innerPlanState(nlstate);
	
	nlstate->nl_needNewBest = true;
	nlstate->nl_MatchedInner = false;
	
	outerPlan->pgNst8LeftPageHead = 0;
	outerPlan->pgNst8LeftPageSize = 0;
	outerPlan->pgNst8LeftParsedFully = false;
	outerPlan->zeroRewardExists = true;

	innerPlan->oslBnd8RightTableCacheHead = 0;
	innerPlan->zeroRewardExists = true;

	outerPlan->oslBnd8_ExplorationStarted = false;

	nlstate->direction = 2;	
	
	nlstate->maxOuterReward = 0;
	nlstate->maxInnerReward = 0;
	
	innerPlan->oslBnd8RightExpCacheHead = 0;
	
	nlstate->outerPointer = 0;
	nlstate->innerPointer = 0;
	
	nlstate->outerRelCount = 0;
	nlstate->innerRelCount = 0;

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
	PlanState  *innerPlan;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	
	// Free up the Memory
	int i;
	for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++) {
		if (!TupIsNull(outerPlan->pgNst8LeftPage[i])) {
			ExecDropSingleTupleTableSlot(outerPlan->pgNst8LeftPage[i]);
			outerPlan->pgNst8LeftPage[i] = NULL;
			outerPlan->pgReward[i] = 0;
		}
	}
	// Free up the Memory
	if (!TupIsNull(outerPlan->pgNst8_innertuple[0])) {
		ExecDropSingleTupleTableSlot(outerPlan->pgNst8_innertuple[0]);
		outerPlan->pgNst8_innertuple[0] = NULL;
	}
	// Free up the Memory
	for (i = 0; i < OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE; i++) {
		if (!TupIsNull(innerPlan->oslBnd8RightTableCache[i])) {
			ExecDropSingleTupleTableSlot(innerPlan->oslBnd8RightTableCache[i]);
			innerPlan->oslBnd8RightTableCache[i] = NULL;
			innerPlan->pgReward[i] = 0;
			innerPlan->rightNFailure[i] = 0;
		}
	}
	// Free up the Memory
	if (!TupIsNull(outerPlan->oslBnd8_currExploreTuple)) {
		ExecDropSingleTupleTableSlot(outerPlan->oslBnd8_currExploreTuple);
		outerPlan->oslBnd8_currExploreTuple = NULL;
	}

	// Free up the Memory
	for (i = 0; i < OSL_BND8_RIGHT_EXP_CACHE_MAX_SIZE; i++) {
		if (!TupIsNull(innerPlan->oslBnd8RightExpCache[i])) {
			ExecDropSingleTupleTableSlot(innerPlan->oslBnd8RightExpCache[i]);
			innerPlan->oslBnd8RightExpCache[i] = NULL;
			innerPlan->rightExpReward[i] = 0;
		}
	}
	
	// Free up the Memory
	if (!TupIsNull(innerPlan->pgNst8_outertuple[0])) {
		ExecDropSingleTupleTableSlot(innerPlan->pgNst8_outertuple[0]);
		innerPlan->pgNst8_outertuple[0] = NULL;
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
