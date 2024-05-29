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

#include "executor/hashjoin.h"
#include "executor/nodeHash.h"


/*
 * States of ripple join state machine
 */
#define BI_DIRECTION		0
#define LEFT_TO_RIGHT		1
#define RIGHT_TO_LEFT		2


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
static TupleTableSlot *
ExecNestLoop(PlanState *pstate)
{
	NestLoopState *node = castNode(NestLoopState, pstate);
	PlanState  *innerPlan;
	PlanState  *outerPlan;
	TupleTableSlot *outerTupleSlot;
	TupleTableSlot *innerTupleSlot;
	ExprState  *joinqual;
	ExprState  *otherqual;
	ExprContext *econtext;

	unsigned int direction;
	

	CHECK_FOR_INTERRUPTS();

	/*
	 * get information from the node
	 */
	ENL1_printf("getting info from node");

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
		direction = node->direction;

		switch (direction)
		{
			/*
		 	 * Read a new outer tuple from the disk and an inner tuple from the memory.
		 	 */
			case LEFT_TO_RIGHT:
				// elog(INFO, "Left to Right");
				if (node->nl_NeedNewOuter && !node->ripple_outerEnd)
				{
					// elog(INFO, "getting new outer tuple");
					outerTupleSlot = ExecProcNode(outerPlan);
					node->nl_MatchedOuter = false;
					/*
		 			 * If no tuple from outer table,
		 			 */					
					if (TupIsNull(outerTupleSlot))
					{
						if (node->ripple_innerEnd)
						{
							// elog(INFO, "no outer and inner tuple, ending join");
							return NULL;
						}
						node->ripple_outerEnd = true;
						node->nl_NeedNewInner = true;
						node->direction = RIGHT_TO_LEFT;

						if (!node->nl_MatchedInner &&
							(node->js.jointype == JOIN_RIGHT ||
				 			node->js.jointype == JOIN_ANTI))
						{
							econtext->ecxt_outertuple = node->nl_NullOuterTupleSlot;

							// elog(INFO, "testing qualification for inner-join tuple");
							if (otherqual == NULL || ExecQual(otherqual, econtext))
							{
								// elog(INFO, "qualification succeeded, projecting tuple");
								return ExecProject(node->js.ps.ps_ProjInfo);
							}
							else
								InstrCountFiltered2(node, 1);
						}
						continue;
					}
					else
					{
						if (!node->ripple_innerEnd)
						{
							/*
							 * If the outer page has available memory,
							 */							
							if (TupIsNull(outerPlan->rippleLeftPage[node->rippleLeftSize]))
							{
								// elog(INFO, "store new outer tuple into left page");
								outerPlan->rippleLeftPage[node->rippleLeftSize] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
							}
							/*
							 * In case if the outer page does not have available memory, stop.
							 */
							else
							{
								elog(ERROR, "The left cache exceeds, stopping the join process.");
								return NULL;
							}
							ExecCopySlot(outerPlan->rippleLeftPage[node->rippleLeftSize], outerTupleSlot);
							node->rippleLeftSize++;
						}
						node->rippleRightHead = 0;
						node->nl_NeedNewOuter = false;
						node->nl_MatchedOuter = false;
					}
					// elog(INFO, "saving new outer tuple information");
					econtext->ecxt_outertuple = outerTupleSlot;
				}
				/*
				 * No need to take an outer tuple from the disk, take the latest outer tuple from the memory.
				 */
				else
				{
					node->rippleLeftHead = node->rippleLeftSize - 1;
					//outerTupleSlot = outerPlan->rippleLeftPage[node->rippleLeftHead];
					//econtext->ecxt_outertuple = outerTupleSlot;
					
				}
				
				/*
				 * Take an inner tuple from the memory.
				 */
				// elog(INFO, "getting old inner tuple");
				innerTupleSlot = innerPlan->rippleRightPage[node->rippleRightHead];
            	node->rippleRightHead++;

				if (node->rippleRightHead == node->rippleRightSize)
				{
					if (node->ripple_innerEnd)
					{
						// elog(INFO, "no inner tuple, getting a new outer tuple again");
						node->nl_NeedNewOuter = true;
						node->rippleRightHead = 0;
					}
					else
					{
						// elog(INFO, "right page join finished, getting new inner tuple");
						node->nl_NeedNewInner = true;
						node->direction = RIGHT_TO_LEFT;
					}
				}
				// elog(INFO, "saving old inner tuple information");
				econtext->ecxt_innertuple = innerTupleSlot;

				break;


			/*
			 * Read a new inner tuple from the disk and an outer tuple from the memory.
			 */
			case RIGHT_TO_LEFT:
				// elog(INFO, "Right to Left");
				if (node->nl_NeedNewInner && !node->ripple_innerEnd)
				{
					// elog(INFO, "getting new inner tuple");
					innerTupleSlot = ExecProcNode(innerPlan);
					node->nl_MatchedInner = false;
					/*
					 * If no tuple from inner table,
		 			 */
					if (TupIsNull(innerTupleSlot))
					{
						if (node->ripple_outerEnd)
						{
							// elog(INFO, "no outer and inner tuple, ending join");
							return NULL;
						}
						node->ripple_innerEnd = true;
						node->nl_NeedNewOuter = true;
						node->direction = LEFT_TO_RIGHT;

						if (!node->nl_MatchedOuter &&
							(node->js.jointype == JOIN_LEFT ||
							 node->js.jointype == JOIN_ANTI))
						{
							econtext->ecxt_innertuple = node->nl_NullInnerTupleSlot;

							// elog(INFO, "testing qualification for outer-join tuple");
							if (otherqual == NULL || ExecQual(otherqual, econtext))
							{
								// elog(INFO, "qualification succeeded, projecting tuple");
								return ExecProject(node->js.ps.ps_ProjInfo);
							}
							else
								InstrCountFiltered2(node, 1);
						}
						continue;
					}
					else
					{
						if (!node->ripple_outerEnd)
						{
							/*
							 * If the inner page has available memory,
							 */											
							if (TupIsNull(innerPlan->rippleRightPage[node->rippleRightSize]))
							{
								// elog(INFO, "store new inner tuple into right page");
								innerPlan->rippleRightPage[node->rippleRightSize] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
							}
							/*
							 * In case if the inner page does not have available memory, stop.
							 */							
							else
							{
								elog(ERROR, "The right cache exceeds, stopping the join process.");
								return NULL;
							}
							ExecCopySlot(innerPlan->rippleRightPage[node->rippleRightSize], innerTupleSlot);
							node->rippleRightSize++;
						}
						node->rippleLeftHead = 0;
						node->nl_NeedNewInner = false;
						node->nl_MatchedInner = false;
					}
					// elog(INFO, "saving new inner tuple information");
					econtext->ecxt_innertuple = innerTupleSlot;
				}
				/*
				 * No need to take an inner tuple from the disk, take the latest inner tuple from the memory.
				 */
				else
				{
					node->rippleRightHead = node->rippleRightSize - 1;
					//innerTupleSlot = innerPlan->rippleRightPage[node->rippleRightHead];
					//econtext->ecxt_innertuple = innerTupleSlot;
				}

				/*
				 * Take an outer tuple from the memory.
				 */
				// elog(INFO, "getting old outer tuple");
				outerTupleSlot = outerPlan->rippleLeftPage[node->rippleLeftHead];
				node->rippleLeftHead++;

				if (node->rippleLeftHead == node->rippleLeftSize)
				{
					if (node->ripple_outerEnd)
					{
						// elog(INFO, "no outer tuple, getting new inner tuple again");
						node->nl_NeedNewInner = true;
						node->rippleLeftHead = 0;
					}
					else
					{
						// elog(INFO, "left page join finished, getting a new outer tuple");
						node->nl_NeedNewOuter = true;
						node->direction = LEFT_TO_RIGHT;
					}
				}
				// elog(INFO, "saving old outer tuple information");
				econtext->ecxt_outertuple = outerTupleSlot;

				break;


			/*
			 * Read a new inner tuple from the disk and a new outer tuple from the disk.
			 */
			case BI_DIRECTION:
				// elog(INFO, "Start");
				// elog(INFO, "getting new outer tuple");
				outerTupleSlot = ExecProcNode(outerPlan);
				if (TupIsNull(outerTupleSlot))
				{
					// elog(INFO, "no outer tuple, ending join");
					return NULL;
				}
				/*
				 * If memory is available for an outer table
				 */
				if (TupIsNull(outerPlan->rippleLeftPage[node->rippleLeftSize]))
				{
					outerPlan->rippleLeftPage[node->rippleLeftSize] = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);
				}
				ExecCopySlot(outerPlan->rippleLeftPage[node->rippleLeftSize], outerTupleSlot);
				node->rippleLeftSize++;

				// elog(INFO, "saving new outer tuple information");
				econtext->ecxt_outertuple = outerTupleSlot;
				node->nl_NeedNewOuter = false;
				node->nl_MatchedOuter = false;
				node->rippleRightHead = 0;

				// elog(INFO, "getting new inner tuple");
				innerTupleSlot = ExecProcNode(innerPlan);
				if (TupIsNull(innerTupleSlot))
				{
					// elog(INFO, "no inner tuple, ending join");
					return NULL;
				}
				/*
				 * If memory is available for an inner table
				 */
				if (TupIsNull(innerPlan->rippleRightPage[node->rippleRightSize]))
				{
					innerPlan->rippleRightPage[node->rippleRightSize] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
				}
				ExecCopySlot(innerPlan->rippleRightPage[node->rippleRightSize], innerTupleSlot);
				node->rippleRightSize++;

				// elog(INFO, "saving new inner tuple information");
				econtext->ecxt_innertuple = innerTupleSlot;
				node->nl_NeedNewInner = true;
				node->nl_MatchedInner = false;
				node->rippleLeftHead = 0;
				
				node->direction = RIGHT_TO_LEFT;

				break;

			default:
				elog(ERROR, "unrecognized ripple join direction: %d",
					 (int) direction);
		}


		/*
		 * at this point we have a new pair of inner and outer tuples so we
		 * test the inner and outer tuples to see if they satisfy the node's
		 * qualification.
		 *
		 * Only the joinquals determine MatchedOuter status, but all quals
		 * must pass to actually return the tuple.
		 */
		if (ExecQual(joinqual, econtext))
		{
			node->nl_MatchedOuter = true;
			node->nl_MatchedInner = true;

			/* In an antijoin, we never return a matched tuple */
			if (node->js.jointype == JOIN_ANTI)
			{
				switch (direction)
				{
					case LEFT_TO_RIGHT:
						node->nl_NeedNewOuter = true;
						node->direction = RIGHT_TO_LEFT;
						break;

					case RIGHT_TO_LEFT:
						node->nl_NeedNewInner = true;
						node->direction = LEFT_TO_RIGHT;
						break;
				}
				continue;		/* return to top of loop */
			}

			/*
			 * If we only need to join to the first matching inner tuple, then
			 * consider returning this one, but after that continue with next
			 * outer tuple.
			 */
			if (node->js.single_match)
			{
				switch (direction)
				{
					case LEFT_TO_RIGHT:
						node->nl_NeedNewOuter = true;
						node->direction = RIGHT_TO_LEFT;
						break;
						
					case RIGHT_TO_LEFT:
						node->nl_NeedNewInner = true;
						node->direction = LEFT_TO_RIGHT;
						break;
				}				
			}

			if (otherqual == NULL || ExecQual(otherqual, econtext))
			{
				/*
				 * qualification was satisfied so we project and return the
				 * slot containing the result tuple using ExecProject().
				 */
				// elog(INFO, "qualification succeeded, projecting tuple");
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
	
	elog(INFO, "Ripple join is called:");
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
			nlstate->nl_NullOuterTupleSlot =
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
	
	/*
	 * Hash Ripple join initialization
	 */
	nlstate->direction = 0;
	nlstate->nl_NeedNewInner = false;
	nlstate->nl_MatchedInner = false;
	nlstate->ripple_outerEnd = false;
	nlstate->ripple_innerEnd = false;

    nlstate->rippleLeftHead = 0;
    nlstate->rippleRightHead = 0;
    nlstate->rippleLeftSize = 0;
    nlstate->rippleRightSize = 0;
	
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
	elog(INFO, "Left Size = %d", node->rippleLeftSize);
	elog(INFO, "Right Size = %d", node->rippleRightSize);

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
	 * clean out the ripple pages
	 */
	PlanState  *outerPlan;
	PlanState  *innerPlan;
	outerPlan = outerPlanState(node);
	innerPlan = innerPlanState(node);
	
	int i;
	for (i = 0; i < node->rippleLeftSize; i++)
	{
		if (!TupIsNull(outerPlan->rippleLeftPage[i]))
		{
			ExecDropSingleTupleTableSlot(outerPlan->rippleLeftPage[i]);
			outerPlan->rippleLeftPage[i] = NULL;
		}
	}

	for (i = 0; i < node->rippleRightSize; i++)
	{
		if (!TupIsNull(innerPlan->rippleRightPage[i]))
		{
			ExecDropSingleTupleTableSlot(innerPlan->rippleRightPage[i]);
			innerPlan->rippleRightPage[i] = NULL;
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
