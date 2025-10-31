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

#define PGNST8_LEFT_PAGE_MAX_SIZE 30 // Memory Size for ToExploitBatch after every exploration.
#define OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE 50 // In Memory Size Right Table Cache Size, used for exploration. 
#define MUST_EXPLORE_TUPLE_COUNT_N 50 // For TPC-H:10gig - Number of tuples that must be explored before Exploitation can happen. 
//#define MUST_EXPLORE_TUPLE_COUNT_N 1500 // For Cars Dataset - Number of tuples that must be explored before Exploitation can happen.
#define FAILURE_COUNT_N 5 // Number of failures allowed during exploration, before jumping into next outer tuple, for exploration
#define DEBUG_FLAG 0 // print statements will be activate if set to 1

static void calculateSimpleConfidenceInterval(NestLoopState *node) {
	double totalmean = 0.0;
	double temp_mean = 0.0;
    double half_width = 0.0;
	double lower_bound = 0.0;
	double upper_bound = 0.0;
	double est_lower_bound = 0.0;
	double est_upper_bound = 0.0;
	double est_mean = 0.0;
	double z_score = 0.0;
	int i = 0;
	int j = 0;
	double totalvariance = 0.0;
	double temp_variance = 0.0;
	double tuple_variance_numr = 0.0;
	double tuple_variance_denr = 0.0;
	double temp_value = 0.0;
	double mean_std;
	double x = 0.0;
	double y = 0.0;
	
    // Mean calculation: loop through each outer tuple
	for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++){
        // Calculate the mean success rate for this outer tuple
        node->outertupleinfo[node->outerIndex[i]].mean_numr = 0.0;
        node->outertupleinfo[node->outerIndex[i]].mean_denr = 0.0;
        
		node->outertupleinfo[node->outerIndex[i]].mean_numr = node->outertupleinfo[node->outerIndex[i]].explore_success_count + node->outertupleinfo[node->outerIndex[i]].exploit_success_count;
		node->outertupleinfo[node->outerIndex[i]].mean_denr = node->outertupleinfo[node->outerIndex[i]].explore_num_trails + node->outertupleinfo[node->outerIndex[i]].exploit_num_trails;
        // Calculate the mean success rate for the current outer tuple
        if (node->outertupleinfo[node->outerIndex[i]].mean_denr != 0) {
            node->outertupleinfo[node->outerIndex[i]].tuple_mean = node->outertupleinfo[node->outerIndex[i]].mean_numr / node->outertupleinfo[node->outerIndex[i]].mean_denr;
        } else {
            node->outertupleinfo[node->outerIndex[i]].tuple_mean = 0.0;
        }
    }

	for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++){
		temp_mean += (double) ( node->outertupleinfo[node->outerIndex[i]].explore_num_trails + node->outertupleinfo[node->outerIndex[i]].exploit_num_trails ) * node->outertupleinfo[node->outerIndex[i]].tuple_mean;
	}
	
	totalmean =  temp_mean / (double)node->T_steps;
	temp_mean = 0.0;
	
	est_mean = (double) (totalmean * 200 * 1000);

    // Variance calculation: calculate the variance of the success rates
	for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++){
		double diff = (node->outertupleinfo[node->outerIndex[i]].explore_success_count + node->outertupleinfo[node->outerIndex[i]].exploit_success_count) - node->outertupleinfo[node->outerIndex[i]].tuple_mean;
		tuple_variance_numr += diff * diff;  // Sum of squared differences
		tuple_variance_denr = (double) (node->outertupleinfo[node->outerIndex[i]].explore_num_trails + node->outertupleinfo[node->outerIndex[i]].exploit_num_trails);
		
		if (tuple_variance_denr != 0) {
			node->outertupleinfo[i].tuple_variance = (double) (tuple_variance_numr / tuple_variance_denr);
		}
		tuple_variance_numr = 0.0;
		tuple_variance_denr = 0.0;
    }
	
	
	double overall_variance_numr = 0.0;
	double total_observations = 0.0;
	double overall_variance = 0.0;
	
	for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++) {
		double weight = (node->outertupleinfo[node->outerIndex[i]].explore_num_trails + node->outertupleinfo[node->outerIndex[i]].exploit_num_trails);
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
	
	est_lower_bound = (double) lower_bound * 200 * 1000;
	est_upper_bound = (double) upper_bound * 200 * 1000;
	
	
	// double actual_count = 240000000.0; //q9 - 10gig - 240000000
	//double actual_count = 15000000.0; //q10 - 10gig - 15000000
	//double actual_count = 60000000.0; //q15 - 10gig - 60000000
	// double actual_count = 364504949107.0; //q11 - 10gig - 364504949107
	//double actual_count = 240020.0; //q9 - 0.001gig - 240020
	// double actual_count = 79718102.0; //cars - lv=1 - 79718102
	// double actual_count = 196252372.0; //cars - lv=2 - 196252372
	// double actual_count = 731365682.0; //cars - lv=3 - 731365682
	// double actual_count = 596708110.0; //movies - lv=10 - 596708110 approximate
	// double actual_count = 137766.0; //shuffle 1 of movies 0.5% dataset;
    // double actual_count = 149225.0; // shuffle2 of movies 0.5% dataset
    // double actual_count = 160540.0; // shuffle3 of movies 0.5% dataset
	double actual_count = 1562.0;
	x = (double) (node->overallCount / actual_count) * 100;
	y = (double) (fabs(actual_count - est_mean) / actual_count) * 100;
	
	elog(INFO, "Exploitation(%f %f)", x, y);
	elog(INFO, "Exploitation(%u %f %f %f)", node->overallCount, est_lower_bound, est_mean, est_upper_bound);

}

static void calculateConfidenceInterval(NestLoopState *node) {
	double totalmean = 0.0;
	double temp_mean = 0.0;
    double half_width = 0.0;
	double lower_bound = 0.0;
	double upper_bound = 0.0;
	double est_lower_bound = 0.0;
	double est_upper_bound = 0.0;
	double z_score = 0.0;
	long long i = 0;
	long long j = 0;
	double totalvariance = 0.0;
	double temp_variance = 0.0;
	double tuple_variance_numr = 0.0;
	double tuple_variance_denr = 0.0;
	double temp_value = 0.0;
	double temp_std = 0.0;
	double mean_std;
	double x = 0.0;
	double y = 0.0;
	unsigned long long tuple_trails = 0;

	if(DEBUG_FLAG)elog(INFO, "number of tuples is %d", node->cacheSize);

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
	
	//Mean calculation
	for (i = 0; i < (node->numExplored); i++){
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
	est_count = (double) (totalmean * 200 * 1000);
	
	//Variance calculation
	for (i = 0; i < (node->numExplored); i++){
		for (j = 0; j < (node->outertupleinfo[i].explore_num_trails); j++){
			if (node->outertupleinfo[i].outerestinfo[j].e_value != 0) {
				tuple_variance_numr += (double) ( (node->outertupleinfo[i].outerestinfo[j].h_value * node->outertupleinfo[i].outerestinfo[j].h_value) * 
									pow( ( (node->outertupleinfo[i].outerestinfo[j].Y / node->outertupleinfo[i].outerestinfo[j].e_value) -  node->outertupleinfo[i].tuple_mean), 2) );
			} else {
				tuple_variance_numr += 0.0;
			}
			tuple_variance_denr += (double) node->outertupleinfo[i].outerestinfo[j].h_value;
		}
		
		if (node->outertupleinfo[i].exploit_num_trails > 1) {
			tuple_trails = node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails;
			if (tuple_trails > 0) {
				tuple_variance_numr += (double) (node->outertupleinfo[i].exploit_success_count) * ( (sqrt(node->outertupleinfo[i].exploit_reward_ratio / (double) node->T_steps) * sqrt(node->outertupleinfo[i].exploit_reward_ratio / (double) node->T_steps))
								* pow( ( 1 -  node->outertupleinfo[i].tuple_mean), 2) );
				tuple_variance_numr += (double) (node->outertupleinfo[i].exploit_num_trails - node->outertupleinfo[i].exploit_success_count) * ( (sqrt(node->outertupleinfo[i].exploit_reward_ratio / (double) node->T_steps) * sqrt(node->outertupleinfo[i].exploit_reward_ratio / (double) tuple_trails))
								* pow( ( 0 -  node->outertupleinfo[i].tuple_mean), 2) );
				tuple_variance_denr += (double) (node->outertupleinfo[i].exploit_num_trails) * ( sqrt(node->outertupleinfo[i].exploit_reward_ratio / (double) (node->T_steps) ));
			}
			
			else {
				tuple_variance_numr += 0.0;
				tuple_variance_denr += 0.0;
			}
		}
		
		tuple_variance_denr = (double) (tuple_variance_denr * tuple_variance_denr);
		
		if (tuple_variance_denr != 0) {
			node->outertupleinfo[i].tuple_variance = ((double) tuple_variance_numr / (double) tuple_variance_denr);
		}
		tuple_variance_numr = 0.0;
		tuple_variance_denr = 0.0;
	}
	
	for (i = 0; i < (node->numExplored); i++){
		temp_std += (double) sqrt(node->outertupleinfo[i].tuple_variance) * ( node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails );
	}
		
	mean_std = (double) temp_std / node->T_steps;
	
    z_score = 1.96;
	half_width = z_score * mean_std;
	
    lower_bound = totalmean - half_width;
    upper_bound = totalmean + half_width;
	
	est_lower_bound = (double) (lower_bound * 200 * 1000);
	est_upper_bound = (double) (upper_bound * 200 * 1000);
	
	
	// double actual_count = 240000000.0; //q9 - 10gig - 240000000
	//double actual_count = 15000000.0; //q10 - 10gig - 15000000
	//double actual_count = 60000000.0; //q15 - 10gig - 60000000
	// double actual_count = 364504949107.0; //q11 - 10gig - 364504949107
	//double actual_count = 240020.0; //q9 - 0.001gig - 240020
	// double actual_count = 79718102.0; //cars - lv=1 - 79718102
	// double actual_count = 196252372.0; //cars - lv=2 - 196252372
	// double actual_count = 731365682.0; //cars - lv=3 - 731365682
	double actual_count = 1562;
	// double actual_count = 596708110.0; //movies - lv=10 - 596708110 approximate
	// double actual_count = 60293145.0; //shuffle 1 of movies 10% dataset;
    // double actual_count = 61068130.0; // shuffle2 of movies 10% dataset
    // double actual_count = 60789596.0; // shuffle3 of movies 10% dataset
	// double actual_count = 137766.0; //shuffle 1 of movies 0.5% dataset;
    // double actual_count = 149225.0; // shuffle2 of movies 0.5% dataset
    // double actual_count = 160540.0; // shuffle3 of movies 0.5% dataset
	x = (double) (node->overallCount / actual_count) * 100;
	y = (double) (fabs(actual_count - est_count) / actual_count) * 100;
	
	elog(INFO, "Mean calculation: (%f %f)", x, y);
	elog(INFO, "Variance: (%u %f %f %f)", node->overallCount, est_lower_bound, est_count, est_upper_bound);
	
}

static void calculateMiddleConfidenceInterval(NestLoopState *node) {
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
	
	//Mean calculation using 4.2.1(without the commented part)
	for (i = 0; i < (node->numExplored); i++){
		node->outertupleinfo[i].mean_numr = 0.0;
		node->outertupleinfo[i].mean_denr = 0.0;
		if(DEBUG_FLAG) elog(INFO, "For tuple %d", i);		
		for (j = 0; j < (node->outertupleinfo[i].explore_num_trails); j++){
			if (node->outertupleinfo[i].outerestinfo[j].e_value != 0) {
				node->outertupleinfo[i].mean_numr += (double) (node->outertupleinfo[i].outerestinfo[j].Y );
				if(DEBUG_FLAG) elog(INFO, "exploration Numerator currently is %f", node->outertupleinfo[i].mean_numr);
				// node->outertupleinfo[i].mean_numr += (double) (node->outertupleinfo[i].outerestinfo[j].Y );
			} else {
				node->outertupleinfo[i].mean_numr += 0.0;
			}
			node->outertupleinfo[i].mean_denr += (double) node->outertupleinfo[i].outerestinfo[j].e_value;
			if(DEBUG_FLAG) elog(INFO, "exploration Denominator currently is %f", node->outertupleinfo[i].mean_denr);
			// node->outertupleinfo[i].mean_denr = (double) node->outertupleinfo[i].outerestinfo[j].e_value;
		}
		
		if (node->outertupleinfo[i].exploit_num_trails > 1) {
			tuple_trails = node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails;
			if (tuple_trails > 0) {
				if (node->outertupleinfo[i].exploit_success_count > 1){
					node->outertupleinfo[i].mean_numr += ( (double) (node->outertupleinfo[i].exploit_success_count));
					if(DEBUG_FLAG) elog(INFO, "exploitaiton Numerator currently is %f", node->outertupleinfo[i].mean_numr);
				}
				node->outertupleinfo[i].mean_denr += ( (double) (node->outertupleinfo[i].exploit_num_trails));
				if(DEBUG_FLAG) elog(INFO, "exploitaiton Denominator currently is %f", node->outertupleinfo[i].mean_denr);
			}
			else {
				node->outertupleinfo[i].mean_numr += 0.0;
				node->outertupleinfo[i].mean_denr += 0.0;
			}
			
		}

		// elog(INFO, "exploit_success_count %d, exploit_reward_ratio %f, T_steps %d", node->outertupleinfo[i].exploit_success_count, node->outertupleinfo[i].exploit_reward_ratio, node->T_steps);
		// elog(INFO, "Numerator = %f, denominator = %f", node->outertupleinfo[i].mean_numr, node->outertupleinfo[i].mean_denr );
		
		if (node->outertupleinfo[i].mean_denr != 0) {
			node->outertupleinfo[i].tuple_mean = ((double) node->outertupleinfo[i].mean_numr / (double) node->outertupleinfo[i].mean_denr);
			if(DEBUG_FLAG) elog(INFO, "Mean of the tuple %d is %f", i, node->outertupleinfo[i].tuple_mean);
		}
		else {
			node->outertupleinfo[i].tuple_mean = 0.0;  // Or another appropriate default value
		}
	}

	for (i = 0; i < (node->numExplored); i++){
		temp_mean += (double) ( node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails ) * node->outertupleinfo[i].tuple_mean;
	}
	
	totalmean = (double) temp_mean / node->T_steps;
	temp_mean = 0.0;
	
	double est_count = 0.0;
	est_count = (double) (totalmean * 200 * 1000);

    // Variance calculation: calculate the variance of the success rates
	for (i = 0; i < (node->numExplored); i++){
		double diff = (node->outertupleinfo[i].explore_success_count + node->outertupleinfo[i].exploit_success_count) - node->outertupleinfo[i].tuple_mean;
		tuple_variance_numr += diff * diff;  // Sum of squared differences
		tuple_variance_denr = (double) (node->outertupleinfo[i].explore_num_trails + node->outertupleinfo[i].exploit_num_trails);
		
		if (tuple_variance_denr != 0) {
			node->outertupleinfo[i].tuple_variance = (double) (tuple_variance_numr / tuple_variance_denr);
		}
		tuple_variance_numr = 0.0;
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
	
	est_lower_bound = (double) lower_bound * 200 * 1000;
	est_upper_bound = (double) upper_bound * 200 * 1000;
	
	
	// double actual_count = 240000000.0; //q9 - 10gig - 240000000
	//double actual_count = 15000000.0; //q10 - 10gig - 15000000
	//double actual_count = 60000000.0; //q15 - 10gig - 60000000
	// double actual_count = 364504949107.0; //q11 - 10gig - 364504949107
	//double actual_count = 240020.0; //q9 - 0.001gig - 240020
	// double actual_count = 79718102.0; //cars - lv=1 - 79718102
	// double actual_count = 196252372.0; //cars - lv=2 - 196252372
	// double actual_count = 731365682.0; //cars - lv=3 - 731365682
	// double actual_count = 596708110.0; //movies - lv=10 - 596708110 approximate
	// double actual_count = 137766.0; //shuffle 1 of movies 0.5% dataset;
    // double actual_count = 149225.0; // shuffle2 of movies 0.5% dataset
    double actual_count = 1562.0; // shuffle3 of movies 0.5% dataset
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
	if(DEBUG_FLAG){elog(INFO, "New Left Page Read Called, oslBnd8_ExplorationStarted: %u", outerPlan->oslBnd8_ExplorationStarted);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->pgNst8LeftPageHead: %u", outerPlan->pgNst8LeftPageHead);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8_numTuplesExplored: %u", outerPlan->oslBnd8_numTuplesExplored);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8_currExploreTupleFailureCount: %u", outerPlan->oslBnd8_currExploreTupleFailureCount);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8_currExploreTupleReward: %u", outerPlan->oslBnd8_currExploreTupleReward);}
	if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8RightTableCacheHead: %u", outerPlan->oslBnd8RightTableCacheHead);}
	if(DEBUG_FLAG){elog(INFO, "node->nl_NeedNewOuter: %u", node->nl_NeedNewOuter);}

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
		outerPlan->oslBnd8_ExplorationStarted=true;
		outerPlan->zeroRewardExists = true;
	}

	/* Read a page such that They can be exploited*/
	while (!outerPlan->pgNst8LeftParsedFully & outerPlan->cursorReward < PGNST8_LEFT_PAGE_MAX_SIZE){
		/*
		* If we don't have an outer tuple, get the next one and reset the
		* inner scan.
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
			if (TupIsNull(outerTupleSlot)) { 
				if(DEBUG_FLAG)elog(INFO, "Finished Parsing left table: %u", outerPlan->pgNst8LeftPageHead);
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
		if(DEBUG_FLAG){elog(INFO, "getting new inner tuple");}

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
		
		ENL1_printf("testing qualification");
		if(DEBUG_FLAG){elog(INFO, "testing qualification");}
		node->nl_MatchedOuter = ExecQual(joinqual, econtext);
		node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails++;
		node->T_steps++;
		node->curNumJoins++;
		//initialize
		node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].e_value = 0.0;
		
		node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].e_value
		= node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio;

		if(node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails > FAILURE_COUNT_N && node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count <= 0){
			outerPlan->totalReward++;
			node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count++;
			node->genExplore++;
			node->exploreCount++;
			outerPlan->oslBnd8_currExploreTupleReward++;
			outerPlan->cur_explore_rewards++;
		}

		if(node->nl_MatchedOuter){
			outerPlan->oslBnd8_currExploreTupleReward++;
			node->genExplore++;
			node->overallCount++;
			node->exploreCount++;
			node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count++;
			//initialize
			node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].Y = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].Y = 1;
			if ( (node->overallCount%1 == 0) && (node->overallCount != node->currentCount) ) {
				//if(DEBUG_FLAG)elog(INFO, "node->overallCount is %u", node->overallCount);
				node->currentCount = node->overallCount;
				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].tupidx = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].tupidx);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].h_explore = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].h_explore);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].p_r = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].p_r);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].exploit_num_trails = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].exploit_num_trails);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].exploit_success_count = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].exploit_success_count);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].prob_e_exploit = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].prob_e_exploit);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].h_exploit = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].h_exploit);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].tuple_mean = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].tuple_mean);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].tuple_variance = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].tuple_variance);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].mean_numr = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].mean_numr);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].mean_denr = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].mean_denr);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].total_trails = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].total_trails);

				// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag = %u",
				// 	node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag);

				// elog(INFO, "This is Exploration");
				calculateConfidenceInterval(node);
				// calculateSimpleConfidenceInterval(node);
				// calculateMiddleConfidenceInterval(node);
			}
		}
		else{
			outerPlan->oslBnd8_currExploreTupleFailureCount++;
			if(DEBUG_FLAG) elog(INFO, "This is incrementation of failure count");
			//initialize
			node->outertupleinfo[outerPlan->outerTupIdx].outerestinfo[node->outertupleinfo[outerPlan->outerTupIdx].total_trails].Y = 0;
		}

		if (node->curNumJoins == (FAILURE_COUNT_N + 1)){
			node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue = outerPlan->oslBnd8_currExploreTupleFailureCount;
			node->outertupleinfo[outerPlan->outerTupIdx].p_r = (double) (outerPlan->oslBnd8_currExploreTupleReward / node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails);
			if(DEBUG_FLAG) elog(INFO, "P_R is %f and explore_Nvalue is %d", node->outertupleinfo[outerPlan->outerTupIdx].p_r, node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue);
		}
		
		if (node->curNumJoins > (FAILURE_COUNT_N + 2)){
			node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio = (double) (1 - ( pow( (1 - (node->outertupleinfo[outerPlan->outerTupIdx].p_r)), (node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue)) ));
			if(DEBUG_FLAG) elog(INFO, "Explore reward ratio is %f", node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio);
		} else {
			node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio = (double) 1;
			if(DEBUG_FLAG) elog(INFO, "Explore reward ratio is %f", node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio);
		}
		node->outertupleinfo[outerPlan->outerTupIdx].total_trails++;
		
		if(DEBUG_FLAG){elog(INFO, "node->nl_MatchedOuter: %u", node->nl_MatchedOuter);}
		if(DEBUG_FLAG){elog(INFO, "outerPlan->oslBnd8RightTableCacheHead: %u", outerPlan->oslBnd8RightTableCacheHead);}
		if(DEBUG_FLAG) elog(INFO, "outerPlan->oslBnd8RightTableCacheHead - %d == (OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE - 1) = %d", outerPlan->oslBnd8RightTableCacheHead, outerPlan->oslBnd8RightTableCacheHead == (OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE - 1));
		if(DEBUG_FLAG) elog(INFO, "outerPlan->oslBnd8_currExploreTupleFailureCount - %d > FAILURE_COUNT_N = %d", outerPlan->oslBnd8_currExploreTupleFailureCount, outerPlan->oslBnd8_currExploreTupleFailureCount > FAILURE_COUNT_N);
		
		// Finish exploring for the current outer tuple, determine if it has to be stored into the memory
		if ((outerPlan->oslBnd8RightTableCacheHead == (OSL_BND8_RIGHT_TABLE_CACHE_MAX_SIZE - 1)) || 
			(outerPlan->oslBnd8_currExploreTupleFailureCount > FAILURE_COUNT_N) ){
			node->nl_NeedNewOuter = true;
			outerPlan->oslBnd8_currExploreTupleFailureCount = 0;
			outerPlan->oslBnd8RightTableCacheHead = 0;
			node->outertupleinfo[outerPlan->outerTupIdx].tupidx = outerPlan->outerTupIdx;
			// if(DEBUG_FLAG)elog(INFO, "Number of explored tuples = %u", node->numExplored);
			node->numExplored++;
			if(DEBUG_FLAG) elog(INFO, "Number of explored tuples = %u", node->numExplored);
			if (outerPlan->outerTupIdx != node->numOuterTuples - 1) {
				node->outertupleinfo[outerPlan->outerTupIdx + 1].explore_reward_ratio
					= (double) pow( (1 - node->outertupleinfo[outerPlan->outerTupIdx].p_r), node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue ) / (double) (node->numOuterTuples - node->numExplored) ;
			}
			
			float probability = 0.0;
			float randomValue = 0.0;
			if(outerPlan->totalReward > 0){
				probability = node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count / (float)outerPlan->totalReward;
				node->outertupleinfo[outerPlan->outerTupIdx].prob_e_exploit =  ( (double)node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count / (double)outerPlan->totalReward );
				node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio = node->outertupleinfo[outerPlan->outerTupIdx].prob_e_exploit;
				if(DEBUG_FLAG)elog(INFO, "exploit_reward_ratio of %d is %f", outerPlan->outerTupIdx, node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio);
			}else{
				if(DEBUG_FLAG)elog(INFO, "Setting probability to 0.1");
				probability = 1/(double)200;
				node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio = 0;
			}

			outerPlan->pgNst8LeftPageHead = 0;
			int i;
			for(i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++){
				bool eligible = false;
				randomValue = (float) rand() / RAND_MAX;

				if(DEBUG_FLAG)elog(INFO, "The probability is %f and the random value is %f", probability, randomValue);
				
				if (randomValue <= probability){
					eligible = true;
				}

				if (outerPlan->pgNst8LeftPageHead < PGNST8_LEFT_PAGE_MAX_SIZE && eligible > 0){
				
					outerPlan->pgReward[outerPlan->pgNst8LeftPageSize] = outerPlan->oslBnd8_currExploreTupleReward;
					if(DEBUG_FLAG)elog(INFO, "tuple %d is being added to the cache: %u", outerPlan->outerTupIdx, outerPlan->pgNst8LeftPageHead);
					if(DEBUG_FLAG)elog(INFO, "Adding it to the LEft Page, num_tuples_explored: %u", outerPlan->oslBnd8_numTuplesExplored);
					if (TupIsNull(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead])) {outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead] = MakeSingleTupleTableSlot(outerPlan->oslBnd8_currExploreTuple->tts_tupleDescriptor);}
					ExecCopySlot(outerPlan->pgNst8LeftPage[outerPlan->pgNst8LeftPageHead], outerPlan->oslBnd8_currExploreTuple);
					outerPlan->outerIndex[outerPlan->pgNst8LeftPageSize] = outerPlan->outerTupIdx;
					node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag = 1;
					// elog(INFO, "The left cache size is: %d", outerPlan->pgNst8LeftPageSize);
					if(DEBUG_FLAG)elog(INFO, "The tuple %d is being placed in position %d", outerPlan->outerTupIdx, outerPlan->pgNst8LeftPageHead);
					if(DEBUG_FLAG){elog(INFO, "Adding complete to the LEft Page, num_tuples_explored: %u", outerPlan->oslBnd8_numTuplesExplored);}
				}
				outerPlan->pgNst8LeftPageHead++;
				outerPlan->pgNst8LeftPageSize = outerPlan->pgNst8LeftPageHead;
			}
			outerPlan->outerTupIdx++;
		}
		outerPlan->oslBnd8RightTableCacheHead++;

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

	if(outerPlan->outerTupIdx >= MUST_EXPLORE_TUPLE_COUNT_N){
		int j = 0;
		int i = 0;

		for(i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++){
			outerPlan->duplicate[i] = false;
		}

		for(i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++){
			if(outerPlan->duplicate[i]){
				continue;
			}
			if(TupIsNull(outerPlan->pgNst8LeftPage[i])){
				outerPlan->pgNst8LeftPage[i] = MakeSingleTupleTableSlot(outerPlan->oslBnd8_currExploreTuple->tts_tupleDescriptor);
				ExecCopySlot(outerPlan->pgNst8LeftPage[i], outerPlan->oslBnd8_currExploreTuple);
				outerPlan->outerIndex[i] = outerPlan->outerTupIdx;
				node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag = 1;
			}
			for(j=i+1; j<PGNST8_LEFT_PAGE_MAX_SIZE; j++){
				if(outerPlan->duplicate[j]){
					continue;
				}
				if(outerPlan->outerIndex[i] == outerPlan->outerIndex[j]){
					// elog(INFO, "The tuple %d is a duplicate of: %d", outerPlan->outerIndex[i], outerPlan->outerIndex[j]);
					outerPlan->duplicate[j] = true;
					// elog(INFO, "The tuple %d has been marked duplicate %d", j, outerPlan->duplicate[j]);
				}
			}
		}
	}

	if(DEBUG_FLAG){
		if(DEBUG_FLAG)elog(INFO, "Exploration Complete with pgNst8LeftPageSize: %u", outerPlan->pgNst8LeftPageSize);
	}

	if(DEBUG_FLAG){elog(INFO, "Seed Complete with: %u", outerPlan->pgNst8LeftPageSize);}
	outerPlan->oslBnd8_ExplorationStarted = false;
	outerPlan->outerTupIdx--;
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

	ENL1_printf("entering main loop");
	for (;;)
	{
		/*
		* Read new left Page, if new outer page is needed
		*/
		if (outerPlan->nl_needNewOuterPage){
			returnTupleSlot = seedToExploitLeftPage(pstate);
			if (!TupIsNull(returnTupleSlot)){
				if(DEBUG_FLAG){elog(INFO, "Returning tuple. outerPlan->pgNst8LeftPageHead: %u", outerPlan->pgNst8LeftPageHead);}
				return returnTupleSlot;
			}
			elog(INFO, "Exploration Completed");

			outerPlan->nl_needNewOuterPage = false;	
			
		}

		/*
		 * If we don't have an outer tuple, get the next one and reset the
		 * inner scan.
		 */
		if (node->nl_NeedNewOuter)
		{	
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
		if (outerPlan->pgNst8LeftPageHead != outerPlan->pgNst8LeftPageSize-1){
			innerTupleSlot = outerPlan->pgNst8_innertuple[0];
			// if page end, Loop back Page Head 
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
		if (outerPlan->pgNst8LeftPageHead == 0){
			outerPlan->pgNst8LeftPageHead = outerPlan->pgNst8LeftPageSize;
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
				// if(DEBUG_FLAG)elog(INFO, "Tuple %u has estimate flat: %u", i, node->outertupleinfo[i].estimate_flag);
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
		if(!outerPlan->duplicate[outerPlan->pgNst8LeftPageHead]){

			ENL1_printf("testing qualification");
			node->outertupleinfo[outerPlan->outerIndex[outerPlan->pgNst8LeftPageHead]].exploit_num_trails++;
			node->T_steps++;
			node->outertupleinfo[outerPlan->outerIndex[outerPlan->pgNst8LeftPageHead]].total_trails++;

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
					node->outertupleinfo[outerPlan->outerIndex[outerPlan->pgNst8LeftPageHead]].exploit_success_count++;
					node->overallCount++;
					if ( (node->overallCount%1 == 0) && (node->overallCount != node->currentCount) ) {
						//if(DEBUG_FLAG)elog(INFO, "node->overallCount is %u", node->overallCount);
						node->currentCount = node->overallCount;
						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].tupidx = %u",
						// node->outertupleinfo[outerPlan->outerTupIdx].tupidx);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_num_trails);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_success_count);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_Nvalue);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].explore_reward_ratio);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].h_explore = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].h_explore);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].p_r = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].p_r);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].exploit_num_trails = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].exploit_num_trails);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].exploit_success_count = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].exploit_success_count);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].exploit_reward_ratio);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].prob_e_exploit = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].prob_e_exploit);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].h_exploit = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].h_exploit);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].tuple_mean = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].tuple_mean);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].tuple_variance = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].tuple_variance);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].mean_numr = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].mean_numr);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].mean_denr = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].mean_denr);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].total_trails = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].total_trails);

						// if(DEBUG_FLAG)elog(INFO, "node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag = %u",
						// 	node->outertupleinfo[outerPlan->outerTupIdx].estimate_flag);

						calculateConfidenceInterval(node);
						// calculateSimpleConfidenceInterval(node);
						// calculateMiddleConfidenceInterval(node);
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
	
	if(DEBUG_FLAG)elog(INFO, "Nested loop is called:");
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
	
	if(DEBUG_FLAG)elog(INFO, "num of outer tuples is: %u", nlstate->numOuterTuples);
	if(DEBUG_FLAG)elog(INFO, "num of inner tuples is: %u", nlstate->numInnerTuples);
	
	nlstate->totalSteps = nlstate->numOuterTuples * nlstate->numInnerTuples;
	
	//nlstate->outertupleinfo = palloc(sizeof(struct tupleInfo) * nlstate->numOuterTuples);
	//int i;
	//int j;
	//for (i = 0; i < nlstate->numOuterTuples; i++) {
	//	nlstate->outertupleinfo[i].outerestinfo = palloc(sizeof(struct estInfo) * nlstate->numInnerTuples);
	//}
	
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
	for (i = 0; i < PGNST8_LEFT_PAGE_MAX_SIZE; i++) {
		if (!TupIsNull(outerPlan->pgNst8LeftPage[i])) {
			ExecDropSingleTupleTableSlot(outerPlan->pgNst8LeftPage[i]);
			outerPlan->pgNst8LeftPage[i] = NULL;
			outerPlan->pgReward[i] = 0;
			outerPlan->outerIndex[i] = 0;
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
	
	for (i = 0; i < PGNST8_LEFT_REM_EXPLORED; i++) {
		if (!TupIsNull(outerPlan->pgNst8LeftRem[i])) {
			ExecDropSingleTupleTableSlot(outerPlan->pgNst8LeftRem[i]);
			outerPlan->pgNst8LeftRem[i] = NULL;
			outerPlan->Rem_pgReward[i] = 0;
			outerPlan->Rem_outerIndex[i] = 0;
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