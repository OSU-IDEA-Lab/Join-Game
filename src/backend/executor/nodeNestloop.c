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

#include "executor/tuptable.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
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
#define MEMORY_MAX        33784
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

#define PGNST8_LEFT_PAGE_MAX_SIZE 100  //(1588/3)*1
#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE 3 * PGNST8_LEFT_PAGE_MAX_SIZE// 3226 // In Memory Size Right Table Cache Size, used for exploration. 
#define MUST_EXPLORE_TUPLE_COUNT_N 1499 //3226 // Number of tuples that must be explored before Exploitation can happen. 
#define FAILURE_COUNT_N 100 // Number of failures allowed during exploration, before jumping into next outer tuple, for exploration

static void calculateConfidenceInterval(NestLoopState *node) {
	double totalmean = 0.0;
	double temp_mean = 0.0;
    double half_width = 0.0;
	double lower_bound = 0.0;
	double upper_bound = 0.0;
	double est_lower_bound = 0.0;
	double est_upper_bound = 0.0;
	double z_score = 0.0;
	int i = 0;
	int j = 0;
	double totalvariance = 0.0;
	double temp_variance = 0.0;
	double tuple_variance_numr = 0.0;
	double tuple_variance_denr = 0.0;
	double temp_value = 0.0;
	double temp_std = 0.0;
	double mean_std;
	double x = 0.0;
	double y = 0.0;
	unsigned int tuple_trails = 0;
	
	for (i = 0; i < (node->numExplored); i++){
		node->outertupleinfo[i].mean_numr = 0.0;
		node->outertupleinfo[i].mean_denr = 0.0;

		for (j = 0; j < (node->outertupleinfo[i].explore_num_trails); j++){
			temp_value = (double) node->outertupleinfo[i].outerestinfo[j].e_value / (double) (node->T_steps);
			
			//initialize
			node->outertupleinfo[i].outerestinfo[j].h_value = 0.0;
			if (temp_value > 0) {
				node->outertupleinfo[i].outerestinfo[j].h_value = (double) sqrt(temp_value);
			}
		}
	}
	
	//Mean calculation using 4.2.1(without the commented part)
	for (i = 0; i < (node->numExplored); i++){
		node->outertupleinfo[i].mean_numr = 0.0;
		node->outertupleinfo[i].mean_denr = 0.0;
		for (j = 0; j < (node->outertupleinfo[i].explore_num_trails); j++){
			if (node->outertupleinfo[i].outerestinfo[j].e_value != 0) {
				node->outertupleinfo[i].mean_numr += (double) (node->outertupleinfo[i].outerestinfo[j].h_value * node->outertupleinfo[i].outerestinfo[j].Y / node->outertupleinfo[i].outerestinfo[j].e_value);
			} else {
				node->outertupleinfo[i].mean_numr += 0.0;
			}
			node->outertupleinfo[i].mean_denr += (double) node->outertupleinfo[i].outerestinfo[j].h_value;
		}
		
		if (node->outertupleinfo[i].exploit_num_trails > 1) {
			tuple_trails = node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails;
			if (tuple_trails > 0) {
				if (node->outertupleinfo[i].exploit_success_count > 1){
					node->outertupleinfo[i].mean_numr += ( (double) (node->outertupleinfo[i].exploit_success_count) * (sqrt (node->outertupleinfo[i].exploit_reward_ratio / (double) (node->T_steps))) );
				}
				node->outertupleinfo[i].mean_denr += ( (double) (node->outertupleinfo[i].exploit_num_trails) * ( sqrt(node->outertupleinfo[i].exploit_reward_ratio / (double) (node->T_steps) )) );
			}
			else {
				node->outertupleinfo[i].mean_numr += 0.0;
				node->outertupleinfo[i].mean_denr += 0.0;
			}
		}
		
		if (node->outertupleinfo[i].mean_denr != 0) {
			node->outertupleinfo[i].tuple_mean = ((double) node->outertupleinfo[i].mean_numr / (double) node->outertupleinfo[i].mean_denr);
		}
		else {
			node->outertupleinfo[i].tuple_mean = 0.0;  // Or another appropriate default value
		}
	}
	
	for (i = 0; i < (node->numExplored); i++){
		temp_mean += (double) ( node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails ) * (double) node->outertupleinfo[i].tuple_mean;
	}
	
	totalmean = (double) temp_mean / (double) node->T_steps;
	temp_mean = 0.0;
	double est_count = 0.0;
	est_count = (double) (totalmean * node->numInnerTuples * node->numOuterTuples);
	
	//Variance calculation
	for (i = 0; i < (node->numExplored); i++){
		double diff = (node->outertupleinfo[i].explore_success_count + node->outertupleinfo[i].exploit_success_count) - node->outertupleinfo[i].tuple_mean;
		tuple_variance_numr += diff * diff;  // Sum of squared differences
		tuple_variance_denr = (double) (node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails);
		
		if (tuple_variance_denr != 0) {
			node->outertupleinfo[i].tuple_variance = (double) (tuple_variance_numr / tuple_variance_denr);
		}
    long long total = (long long)node->numOuterTuples * node->numInnerTuples;
		tuple_variance_denr = 0.0;
    }
	
	
	double overall_variance_numr = 0.0;
	double total_observations = 0.0;
	double overall_variance = 0.0;
	
	for (i = 0; i < node->numExplored; i++) {
		double weight = (node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails);
		overall_variance_numr += node->outertupleinfo[i].tuple_variance * weight;
		total_observations += weight;
	}
	
	overall_variance = (double) (overall_variance_numr / total_observations);
	mean_std = (double) sqrt(overall_variance / node->T_steps);
	
    z_score = 1.96;
	half_width = z_score * mean_std;

    // Calculate the lower and upper bounds of the confidence interval
    lower_bound = totalmean - half_width;
    upper_bound = totalmean + half_width;
	
	est_lower_bound = (double) lower_bound * node->numOuterTuples * node->numInnerTuples;
	est_upper_bound = (double) upper_bound * node->numOuterTuples * node->numInnerTuples;
	
	
	// double actual_count = 240000000.0; //q9 - 10gig - 240000000
	// double actual_count = 15000.0; //q10 - 10gig - 15000000
	//double actual_count = 60000000.0; //q15 - 10gig - 60000000
	// double actual_count = 364504949107.0; //q11 - 10gig - 364504949107
	// double actual_count = 79718102.0; //cars - lv=1 - 79718102
	// double actual_count = 196252372.0; //cars - lv=2 - 196252372
	// double actual_count = 731365682.0; //cars - lv=3 - 731365682
	// double actual_count = 643362131662.0; //WDC - lv=3
	// double actual_count = 200104045466.0; //WDC - lv=2
	// double actual_count = 132825725525.0; //WDC - lv=1
	// double actual_count = 441028105215.0; //movies - lv=8
	// double actual_count = 94726938069.0; //movies - lv=7
	// double actual_count = 12346832945.0; //movies - lv=6
	// double actual_count = 8209203516968.0; //CARS 3 relation LV1
	// double actual_count = 14732202322694.0; //CARS 3 relation LV2
	// double actual_count = 68024571557730.0; //CARS 3 relation LV3
	// double actual_count = 1110213527784328.0; //WDC 3 relation LV1
	// double actual_count = 828823572494860.0; //WDC 3 relation LV2
	// double actual_count = 1096082288421912.0; //WDC 3 relation LV3
	// double actual_count = 684922675194.0; //Movies 3 relation LV6
	// double actual_count = 4116968483912.0; //Movies 3 relation LV7
double actual_count = 441028105215.0;

	x = (double) (node->overallCount / actual_count) * 100;
	y = (double) (fabs(actual_count - est_count) / actual_count) * 100;
	
	elog(INFO, "Mean calculation: (%f %f)", x, y);
	elog(INFO, "Variance: (%u %f %f %f)", node->overallCount, est_lower_bound, est_count, est_upper_bound);
	
}

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

	if(outerPlan->oslBnd8_ExplorationStarted){
		// Do not initialize variables again
		int dummy;
	}
	else{
		ExecReScan(innerPlan);

		outerPlan->cursorReward = 0;
		outerPlan->oslBnd8RightTableCacheSize = 0;
		outerPlan->oslBnd8_currExploreTupleFailureCount =0;
		outerPlan->oslBnd8_numTuplesExplored=0;
		outerPlan->pgNst8LeftPageHead = 0;
		outerPlan->pgNst8LeftPageSize = 0;
		outerPlan->exploitCacheHead = 0;
		outerPlan->exploitCacheSize = 0;
		outerPlan->oslBnd8_ExplorationStarted=true;
		outerPlan->zeroRewardExists = true;
	}

	/* Read a page such that They can be exploited*/
	while (!outerPlan->pgNst8LeftParsedFully & outerPlan->cursorReward < MUST_EXPLORE_TUPLE_COUNT_N){
		/*
		* If we don't have an outer tuple, get the next one and reset the
		* inner scan.
		*/
		
		if (node->nl_NeedNewOuter)
		{
			if (outerPlan->oslBnd8_numTuplesExplored > MUST_EXPLORE_TUPLE_COUNT_N){
				break;
			}

			ENL1_printf("getting new outer tuple");
			
			outerTupleSlot = ExecProcNode(outerPlan);
			if (TupIsNull(outerTupleSlot)) { 
				outerPlan->pgNst8LeftParsedFully = true;
				break;
			}
			if (TupIsNull(outerPlan->oslBnd8_currExploreTuple)) { outerPlan->oslBnd8_currExploreTuple = MakeSingleTupleTableSlot(outerTupleSlot->tts_tupleDescriptor);}
			if (!TupIsNull(outerTupleSlot)){ ExecCopySlot(outerPlan->oslBnd8_currExploreTuple, outerTupleSlot);}
			outerPlan->oslBnd8_currExploreTupleReward = 0;
			outerPlan->oslBnd8RightTableCacheHead = 0;
			outerPlan->oslBnd8_numTuplesExplored++;
			node->curNumJoins = 0;

			//initialize
			node->outertupleinfo[outerPlan->outerTupIdx].tupidx = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].h_explore = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].p_r = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].exploit_num_trails = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].exploit_success_count = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].prob_e_exploit = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].h_exploit = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].tuple_mean = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].tuple_variance = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].mean_numr = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].mean_denr = 0.0;
			node->outertupleinfo[outerPlan->outerTupIdx].total_trails = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag = 0;

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
		}

		/*
		* we have an outerTuple, try to get the next inner tuple.
		*/
		ENL1_printf("getting new inner tuple");

		if ( outerPlan->oslBnd8RightTableCacheHead >= outerPlan->oslBnd8RightTableCacheSize ){
			if ( outerPlan->oslBnd8RightTableCacheHead < (OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE - 1 ) ){
				// Fill Table
				innerTupleSlot = ExecProcNode(innerPlan);
				node->innerIdx++;
				if (TupIsNull(innerTupleSlot)) {
					break;
				}
				// Add the tuple to the list
				if (TupIsNull(outerPlan->oslBnd8RightTableCache[outerPlan->oslBnd8RightTableCacheHead])) {
					outerPlan->oslBnd8RightTableCache[outerPlan->oslBnd8RightTableCacheHead] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
				}
				ExecCopySlot(outerPlan->oslBnd8RightTableCache[outerPlan->oslBnd8RightTableCacheHead], innerTupleSlot);
				outerPlan->oslBnd8RightTableCacheSize++;
			}
		}

		innerTupleSlot = outerPlan->oslBnd8RightTableCache[outerPlan->oslBnd8RightTableCacheHead];
		econtext->ecxt_innertuple = innerTupleSlot;
		outerPlan->oslBnd8RightTableCacheHead++;
		
		ENL1_printf("testing qualification");
		node->nl_MatchedOuter = ExecQual(joinqual, econtext);
		node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails++;
		node->T_steps++;
		node->curNumJoins++;
		//initialize
		node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].e_value = 0.0;
		
		node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].e_value
		= node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio;
		if(node->nl_MatchedOuter){
			outerPlan->oslBnd8_currExploreTupleReward++;
			node->genExplore++;
			node->overallCount++;
			node->exploreCount++;
			node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count++;
			//initialize
			node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].Y = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].Y = 1;
			if ( (node->overallCount%100 == 0) && (node->overallCount != node->currentCount) ) {
				node->currentCount = node->overallCount;

				calculateConfidenceInterval(node);
			}
		}
		else{
			outerPlan->oslBnd8_currExploreTupleFailureCount++;
			//initialize
			node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].Y = 0;
		}

		if (node->curNumJoins == (FAILURE_COUNT_N + 1)){
			node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue = outerPlan->oslBnd8_currExploreTupleFailureCount;
			node->outertupleinfo[outerPlan->outerTupIdx].p_r = (double) (outerPlan->oslBnd8_currExploreTupleReward / node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails);
		}
		
		if (node->curNumJoins > (FAILURE_COUNT_N + 2)){
			node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio = (double) (1 - ( pow( (1 - (node->outertupleinfo[outerPlan->outerTupIdx].p_r)), (node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue)) ));
		} else {
			node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio = (double) 1;
		}
		node->outertupleinfo[outerPlan->outerTupIdx].total_trails++;
		
		// Finish exploring for the current outer tuple, determine if it has to be stored into the memory
		if ((outerPlan->oslBnd8RightTableCacheHead == (OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE - 1)) || 
			(outerPlan->oslBnd8_currExploreTupleFailureCount > FAILURE_COUNT_N) ){
			node->nl_NeedNewOuter = true;
			outerPlan->oslBnd8_currExploreTupleFailureCount = 0;
			outerPlan->oslBnd8RightTableCacheHead = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].tupidx = outerPlan->outerTupIdx;
			node->numExplored++;
			if (outerPlan->outerTupIdx != node->numOuterTuples - 1) {
				node->outertupleinfo[outerPlan->outerTupIdx + 1].explore_reward_ratio
					= (double) pow( (1 - node->outertupleinfo[outerPlan->outerTupIdx].p_r), node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue ) / (double) (node->numOuterTuples - node->numExplored) ;
			}
			
			// Allocate Memory if it has not been allocated
			// If 
			if (outerPlan->pgNst8LeftPageSize < MUST_EXPLORE_TUPLE_COUNT_N){
				
				outerPlan->pgReward[outerPlan->pgNst8LeftPageSize] = outerPlan->oslBnd8_currExploreTupleReward;
				if (TupIsNull(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead])) {outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead] = MakeSingleTupleTableSlot(outerPlan->oslBnd8_currExploreTuple->tts_tupleDescriptor);}
				ExecCopySlot(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead], outerPlan->oslBnd8_currExploreTuple);
				outerPlan->outerIndex[outerPlan->pgNst8LeftPageSize] = outerPlan->outerTupIdx;
				node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag = 1;
				outerPlan->pgNst8LeftPageHead++;
				outerPlan->pgNst8LeftPageSize++;
			}
			outerPlan->outerTupIdx++;
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

	outerPlan->oslBnd8_ExplorationStarted = false;
	outerPlan->outerTupIdx--;
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
	outerTupleSlot = NULL;
	
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

	ENL1_printf("entering main loop");
	for (;;)
	{
		/*
		* Read new left Page, if new outer page is needed
		*/
		if(node->numExplored >= node->numOuterTuples){
			ENL1_printf("Finished left relation");
			return NULL;
		}
		if (outerPlan->nl_needNewOuterPage){
			srand(time(NULL));
			returnTupleSlot = seedToExploitLeftPage(pstate);
			if (!TupIsNull(returnTupleSlot)){
				return returnTupleSlot;
			}

			int i;
			double totalReward = 1.0;
			double total_prob_e_exploit = 0.0;
			int selectedCount = 0;
			double randomValue = 0.0;
			int total_zeros = 0;
			
			
			// Assign a small baseline reward to zero-reward tuples and calculate total reward
			for (i = 0; i < node->numExplored; i++) {
				if (node->outertupleinfo[i].explore_success_count == 0){
					total_zeros += 1;  // Assign small reward for zero-reward tuples
				}
				totalReward += node->outertupleinfo[i].explore_success_count;
			}

			
			for (i = 0; i < node->numExplored; i++) {
				if (node->outertupleinfo[i].estimate_flag != 2){
					if (node->outertupleinfo[i].explore_success_count == 0){
						node->outertupleinfo[i].prob_e_exploit = 0.1 / total_zeros;
					}else{
						node->outertupleinfo[i].prob_e_exploit =  ( (double)node->outertupleinfo[i].explore_success_count / totalReward ) * 0.9;
					}
					
					node->outertupleinfo[i].exploit_reward_ratio = node->outertupleinfo[i].prob_e_exploit;
					total_prob_e_exploit += node->outertupleinfo[i].prob_e_exploit;
				}
			}

			// elog(INFO, "The total prob is %f", total_prob_e_exploit);
			double cum_prob = 0.0;
			int    lo, hi, mid, chosen;
			
			for (i = 0; i < node->numExplored; i++) {
				if (node->outertupleinfo[i].estimate_flag != 2){
					node->outertupleinfo[i].prob_e_exploit /= total_prob_e_exploit;
					node->outertupleinfo[i].exploit_reward_ratio = node->outertupleinfo[i].prob_e_exploit;
					cum_prob += node->outertupleinfo[i].prob_e_exploit;
    				node->outertupleinfo[i].cum_prob = cum_prob;
				}
			}
			
			int j = 0;
			int count = -1;
			double total = 0.0;
			

			for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++)
			{
				randomValue = (double) rand() / ((double) RAND_MAX + 1.0);

				lo = 0;
				hi = MUST_EXPLORE_TUPLE_COUNT_N - 1;
				chosen = -1;

				while (lo <= hi)
				{
					mid = lo + (hi - lo) / 2;
					if (node->outertupleinfo[mid].cum_prob >= randomValue)
					{
						chosen = mid;
						hi = mid - 1;
					}
					else
						lo = mid + 1;
				}

				if (chosen >= 0)
				{
					if (TupIsNull(outerPlan->Exploit_cache[i]))
						outerPlan->Exploit_cache[i] =
							MakeSingleTupleTableSlot(outerPlan->oslBnd8_currExploreTuple->tts_tupleDescriptor);

					ExecCopySlot(outerPlan->Exploit_cache[i], outerPlan->pgNst8LeftPage[chosen]);
					outerPlan->exploitCacheIndex[i] = i;
					outerPlan->exploitCacheSize++;
					outerPlan->exploitCacheHead = outerPlan->exploitCacheSize;
				}
			}
			elog(INFO, "The exploit cache size is %d", outerPlan->exploitCacheSize);
			outerPlan->nl_needNewOuterPage = false;	
		}
		

		

		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{	
			ENL1_printf("getting new outer tuple");

			if (outerPlan->exploitCacheSize ==0 ){
				// elog(INFO, "Error?");
				outerTupleSlot = NULL;
			}
			else{
				
				outerPlan->exploitCacheHead--;
				outerTupleSlot = outerPlan->Exploit_cache[outerPlan->exploitCacheHead];
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
		}

		/*
		 * we have an outerTuple, try to get the next inner tuple.
		 */
		ENL1_printf("getting new inner tuple");

		node->nl_NeedNewOuter = true;
		// If this is a fresh left page scan, and get new inner tuple
		if (outerPlan->exploitCacheHead != outerPlan->exploitCacheSize-1){
			innerTupleSlot = outerPlan->pgNst8_innertuple[0];
		}
		else{
			innerTupleSlot = ExecProcNode(innerPlan);
			node->innerIdx++;
			// Allocate Memory if it has not been allocated
			if (TupIsNull(outerPlan->pgNst8_innertuple[0])) {
				outerPlan->pgNst8_innertuple[0] = MakeSingleTupleTableSlot(innerTupleSlot->tts_tupleDescriptor);
			}
			if (!TupIsNull(innerTupleSlot)){
			ExecCopySlot(outerPlan->pgNst8_innertuple[0], innerTupleSlot);
			}
		}

		if (outerPlan->exploitCacheHead == 0){
			outerPlan->exploitCacheHead = outerPlan->exploitCacheSize;
		}

		econtext->ecxt_innertuple = innerTupleSlot;
		outerPlan->pgNst8InnerTableParseCount++;
		if (TupIsNull(innerTupleSlot))
		{
			ENL1_printf("no inner tuple, need new outer tuple");

			outerPlan->nl_needNewOuterPage = true;

			int i;
			for (i = 0; i < node->numExplored; i++) {
				if (node->outertupleinfo[i].estimate_flag != 2) {
					node->outertupleinfo[i].estimate_flag = 2;
				}
			}

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
		node->outertupleinfo[outerPlan->exploitCacheIndex[outerPlan->exploitCacheHead]].exploit_num_trails++;
		node->T_steps++;
		node->outertupleinfo[outerPlan->exploitCacheIndex[outerPlan->exploitCacheHead]].total_trails++;
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
				node->genExploit++;
				node->outertupleinfo[outerPlan->exploitCacheIndex[outerPlan->exploitCacheHead]].exploit_success_count++;
				node->overallCount++;
				if ( (node->overallCount%100 == 0) && (node->overallCount != node->currentCount) ) {
					node->currentCount = node->overallCount;
					
					calculateConfidenceInterval(node);
				}
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

	// Initialize variables
	PlanState  *outerPlan;
	outerPlan = outerPlanState(nlstate);
	
	outerPlan->nl_needNewOuterPage = true;
	outerPlan->pgNst8LeftPageHead = 0;
	outerPlan->pgNst8LeftPageSize = 0;
	outerPlan->exploitCacheHead = 0;
	outerPlan->exploitCacheSize = 0;
	outerPlan->outerTupCount = 0;
	outerPlan->pgNst8LeftParsedFully = false;
	outerPlan->pgNst8InnerTableParseCount = 0;

	outerPlan->oslBnd8RightTableCacheInitialized = false;
	outerPlan->oslBnd8RightTableCacheHead = 0;

	outerPlan->oslBnd8_ExplorationStarted = false;
	outerPlan->zeroRewardExists = true;

	// outerPlan->oslBnd8InExplorationPhase = true;

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

	nlstate->numInnerTuples = innerPlan(node)->plan_rows;
	nlstate->numOuterTuples = outerPlan(node)->plan_rows;
		
	nlstate->totalSteps = nlstate->numOuterTuples * nlstate->numInnerTuples;
	
	nlstate->outertupleinfo = palloc(sizeof(struct tupleInfo) * 100000);
	int i;
	int j;
	for (i = 0; i < 100000; i++) {
		nlstate->outertupleinfo[i].outerestinfo = palloc(sizeof(struct estInfo) * 1000);
	}
	
	nlstate->T_steps = 0;
	nlstate->genExplore = 0;
	nlstate->genExploit = 0;
	nlstate->overallCount = 0;
	nlstate->currentCount = 0;
	nlstate->exploreCount = 0;
	nlstate->numExplored = 0;
	nlstate->curNumJoins = 0;
	//estimation
	outerPlan->outerTupIdx = 0;
	nlstate->innerIdx = 0;

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
	int j;
	for (i = 0; i < MUST_EXPLORE_TUPLE_COUNT_N; i++) {
		if (!TupIsNull(outerPlan->pgNst8LeftPage[i])) {
			ExecDropSingleTupleTableSlot(outerPlan->pgNst8LeftPage[i]);
			outerPlan->pgNst8LeftPage[i] = NULL;
			outerPlan->pgReward[i] = 0;
			outerPlan->outerIndex[i] = 0;
		}
	}
	for(i=0; i<PGNST8_LEFT_PAGE_MAX_SIZE; i++){
		if(!TupIsNull(outerPlan->Exploit_cache[i])){
			ExecDropSingleTupleTableSlot(outerPlan->Exploit_cache[i]);
			outerPlan->Exploit_cache[i] = NULL;
			outerPlan->exploitCacheIndex[i] = 0;

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
	// Free up the Memory
	if (!TupIsNull(outerPlan->oslBnd8_currExploreTuple)) {
		ExecDropSingleTupleTableSlot(outerPlan->oslBnd8_currExploreTuple);
		outerPlan->oslBnd8_currExploreTuple = NULL;
	}

	// Free up the Memory
	for (i = 0; i < (100000); i++) {
		pfree(node->outertupleinfo[i].outerestinfo);
	}
	pfree(node->outertupleinfo);
	
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