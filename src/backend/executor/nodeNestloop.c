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

static RelationPage* CreateRelationPage() {
	int i;
	RelationPage* relationPage = palloc(sizeof(RelationPage));
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	for (i = 0; i < PAGE_SIZE; i++){
		relationPage->tuples[i] = NULL;
	}
	return relationPage;
}

static void RemoveRelationPage(RelationPage** relationPageAdr, int pageSize) {
	int i;
	RelationPage* relationPage;
	relationPage  = *relationPageAdr;
	if (relationPage == NULL) {
		return;
	}
	for (i = 0; i < pageSize; i++){
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	pfree(relationPage);
	(*relationPageAdr) = NULL;
}


static int LoadNextPage(PlanState* planState, RelationPage* relationPage, int pageSize) {
	int i;
	if (relationPage == NULL){
		elog(ERROR, "LoadNextPage: null page");
	}
	relationPage->index = 0;
	relationPage->tupleCount = 0;
	// Remove the old stored tuples
	for (i = 0; i < pageSize; i++) {
		if (!TupIsNull(relationPage->tuples[i])) {
			ExecDropSingleTupleTableSlot(relationPage->tuples[i]);
			relationPage->tuples[i] = NULL;
		}
	}
	for (i = 0; i < pageSize; i++) {
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

static int LoadNextOuterPage(PlanState* outerPlan, RelationPage* relationPage, ScanKey xidScanKey, int fromPageIndex) {
	int i;
	TupleTableSlot* tts;
	int fromXid;
	fromXid = fromPageIndex * PAGE_SIZE + 1;
	if (relationPage == NULL){
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

void LoadPageWithTIDs(PlanState* outerPlan, struct tupleRewards* tids, RelationPage* relationPage, int index, Relation heapRelation, TupleTableSlot* tup) {
    Buffer tempBuf = InvalidBuffer;
    int i,j = 0;
    TupleTableSlot* tts = tup;
    if(relationPage == NULL) {
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

    for ( i = 0; i < size; i++) {
        while(true) {
            if(heap_fetch(heapRelation, outerPlan->state->es_snapshot, &tids[index].tuples[i], &tempBuf, false, NULL)) {
                ExecStoreTuple(&tids[index].tuples[i],
                        tts,
                        tempBuf,
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
    elog(INFO, "reward: %d", node->rewards[bestPageIndex]);
	bestXid = node->xids[bestPageIndex];
	node->xids[bestPageIndex] = node->xids[node->activeRelationPages - 1];
	node->rewards[bestPageIndex] = node->rewards[node->activeRelationPages - 1];
	node->activeRelationPages--;
	return bestXid;
}

static int popBestPage(NestLoopState *node) {
    int i;
    int bestPageIndex = 0;
    int bestXid;

    for(i = 0; i < node->activeRelationPages; i++) {
        if (node->tidRewards[i].reward > node->tidRewards[bestPageIndex].reward) {
            bestPageIndex = i;
        }
    }
//    elog(INFO, "reward of best block: %d", node->tidRewards[bestPageIndex].reward);
    node->tidRewards[bestPageIndex].reward = -1;
    return bestPageIndex;
}

void storeTIDs(RelationPage* relationPage, struct tupleRewards* tids, int index, int reward, int nFail, double pChoose) {
    int i = 0;
    tids[index].reward = reward;
    tids[index].nFail = nFail;
    tids[index].size = relationPage->tupleCount;
    for(i = 0; i < relationPage->tupleCount; i++) {
        tids[index].tuples[i] = *relationPage->tuples[i]->tts_tuple;
    }
    tids[index].pSucc = reward * 1.0 / (reward + nFail) + BASE_PROB;
    tids[index].pFail = 1 - tids[index].pSucc;
    tids[index].pChoose = pChoose;
    tids[index].updated = false;
//    tids[index].mcmcSuc = reward;
//    tids[index].mcmcFail = nFail;
	tids[index].mcmcSuc = 1;
	tids[index].mcmcFail = 0;
    elog(INFO, "store tids with rewards %d,fail %d, psuc %f, pchoose %f", reward, nFail, tids[index].pSucc, pChoose);
}

//double gaussrand_NORMAL() {
//	static double V1, V2, S;
//	static int phase = 0;
//	double X;
//
//	if (phase == 0){
//		do{
//			srand((unsigned)time(NULL));
//			double U1 = (double) rand() / RAND_MAX;
//			srand((unsigned)time(NULL));
//			double U2 = (double) rand() / RAND_MAX;
//
//			V1 = 2 * U1 - 1;
//			V2 = 2 * U2 - 1;
//
//			S = V1 * V1 + V2 * V2;
//		}while (S >= 1 || S == 0);
//
//		X = V1 * sqrt(-2.0 * log(S) / S);
//	}
//	else
//	{
//		X = V2 * sqrt(-2.0 * log(S) / S);
//	}
//
//	phase = 1 - phase;
//
//	return X;
//}



double calPC(double psuc, double explored, double scala){
	return exp((psuc + N_FAILURE/explored)/scala) / (exp((psuc + N_FAILURE/explored)/scala) + exp(1/scala));
}

bool MCMCSample(int prevSuc, int prevFail, double pSuc, double explored){
	//elog(INFO, "mcmc sample aug is %d, %d, %f, %f", prevSuc, prevFail, pSuc, log(pSuc));
	double acceptRatio;
	bool loop = true;
	int suc, fail;
	bool res;
	struct timeval seed;


	while(loop){
		nanosleep((const struct timespec[]){{0, 1017L}}, NULL);
		gettimeofday( &seed, NULL);
		srand((unsigned int)(seed.tv_usec));

		if (rand() % 100 / 100.0 > 0.5){
			res = true;
			acceptRatio = pSuc /(pow(pSuc, prevSuc) * pow((1-pSuc), prevFail));
			//elog(INFO, "true sample with acceptRatio %f with suc %f",  acceptRatio, pSuc);
		}else{
			res = false;
			acceptRatio = (1 - pSuc) / (pow(pSuc, prevSuc) * pow((1-pSuc), prevFail) );
			//elog(INFO, "false sample with acceptRatio %f with suc %f", acceptRatio, pSuc);
		}
		nanosleep((const struct timespec[]){{0, 1601L}}, NULL);
		gettimeofday( &seed, NULL);
		srand((unsigned int)(seed.tv_usec));
		double ram = rand() % 100000 / 100000.0;

		if(acceptRatio < 0) acceptRatio = -acceptRatio;
		if(ram < acceptRatio){
			//elog(INFO, "res is %d with acceptRaio %f and ram %f", res, acceptRatio, ram);
			loop = false;
		}
	}

	return res;
}

void updateProb(NestLoopState *node){
	bool loop = true;
	int i, j, k = 0;
	double  suc, nfail, psuc, nextDiff, dev, tmp;

	while (loop){
		k++;
		elog(INFO, "%d times:", k);
		loop = false;
	    for(i = 0; i < node->activeRelationPages; i++) {
	    	if(node->tidRewards[i].updated) continue;

	    	suc = node->tidRewards[i].reward * 1.0;
	    	nfail = node->tidRewards[i].nFail * 1.0;
	    	tmp = N_FAILURE/(suc + nfail);
	    	psuc = node->tidRewards[i].pSucc;
	    	dev = 0.0;
	    	for(j = 0; j < R_VALUE; j++){
	    		bool samp = MCMCSample(node->tidRewards[i].mcmcSuc, node->tidRewards[i].mcmcFail, node->tidRewards[i].pSucc, suc + nfail);

	    		if(samp){
	    			dev += 1 / psuc + 1.0 + tmp - exp(1+tmp)/(exp(1+tmp) + exp(1));
	    			node->tidRewards[i].mcmcSuc = 1;
	    			node->tidRewards[i].mcmcFail = 0;
	    		}else{
	    			dev -= 1 / (1 - psuc) + tmp - exp(tmp)/(exp(tmp) + exp(1));
	    			node->tidRewards[i].mcmcSuc = 0;
	    			node->tidRewards[i].mcmcFail = 1;
	    		}
	    	}
	    	dev = dev / R_VALUE;
	    	nextDiff = STEP_VAL * (suc/psuc - nfail/(1-psuc) + psuc + tmp - exp(psuc + tmp)/(exp(psuc + tmp) + exp(1)) - dev);
	    	node->tidRewards[i].pSucc += nextDiff;
	    	if(node->tidRewards[i].pSucc <= 0 || node->tidRewards[i].pSucc >= 1) node->tidRewards[i].pSucc = BASE_PROB;
	    	node->tidRewards[i].pFail = 1 - node->tidRewards[i].pSucc;
			elog(INFO, "the %d tuple has pSucc as %f with update is %f, dev is %f, suc is %f, fail is %f",
						i, node->tidRewards[i].pSucc, nextDiff, dev, suc, nfail);
			if(nextDiff < 0) nextDiff = -nextDiff;
			if(nextDiff > CONVERGE_LIMIT) loop = true;
	    }
	}
    for(i = 0; i < node->activeRelationPages; i++)
    	node->tidRewards[i].updated = true;

	elog(INFO, "successfully converge");
	return;
}



double gumbelNoise(){
	struct timeval seed;

	nanosleep((const struct timespec[]){{0, 1073L}}, NULL);

	gettimeofday( &seed, NULL);
	srand((unsigned int)(seed.tv_usec));

	double rdm = rand() % 100000 / 100000.0;


	return -log(-log(rdm / P_SCALA));
}

double calculateUk(NestLoopState *node){
	return (node->reward + N_FAILURE) * 1.0 / node->exploreStepCounter;
}

bool isKeepExplore(NestLoopState *node){
	bool res = true;
	double gumN1 = gumbelNoise();
	double gumN2 = gumbelNoise();
	if (calculateUk(node) + gumN1 < 1 + gumN2) res = false;
	//elog(INFO, "gumN1 %f, gumN2 %f", gumN1, gumN2);
	return res;
}

double calculateProb(NestLoopState *node, int scala){
	return exp(calculateUk(node) / scala) / (exp(calculateUk(node) / scala) + exp(1 / scala));
}

void calProbChoose(NestLoopState *node){
	node->pChoose *= calculateProb(node, P_SCALA);
	//elog(INFO, "pchoose is %f, prob is %f with reward is %d, explore is %d", node->pChoose, calculateProb(node, 1), node->reward, node->exploreStepCounter);
	return;
}

static void PrintNodeCounters(NestLoopState *node){
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

//void addNewProb(NestLoopState *node){
//	struct tupleRewards* tids = node->tidRewards;
//	tids->prob.tolRnds++;
//	rndProb rnd;
//	rnd.prob = calculateProb(node, 1);
//	rnd.next = tids->prob.rnd;
//	tids->prob.rnd = &rnd;
//	return;
//}

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
    node->ss = (ScanState*)outerPlan;

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
			if (!node->greedyExploit && !node->reachedEndOfOuter && node->activeRelationPages < node->sqrtOfInnerPages) {
				// explore
				node->isExploring = true;
				node->pageIndex++;
				node->pageIndex = MAX(node->pageIndex, node->lastPageIndex);
				//LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
				LoadNextPage(outerPlan, node->outerPage, PAGE_SIZE);
                if (node->outerPage->tupleCount < PAGE_SIZE) {
					elog(INFO, "Reached end of outer");
					node->reachedEndOfOuter = true;
					if (node->outerPage->tupleCount == 0) continue;
				}
				node->outerTupleCounter += node->outerPage->tupleCount;
				node->outerPageCounter++;
				node->lastReward = 0;
				node->exploreStepCounter = 1;
			} else if (node->greedyExploit || (!node->reachedEndOfOuter && node->activeRelationPages == node->sqrtOfInnerPages) ||
					(node->reachedEndOfOuter && node->activeRelationPages > 0)){
				// exploit
				/***************************************************/
				node->reachedEndOfOuter = true; // only for test to get full join results of stored arms;
				// while checking the results, please take a look at popBestPage() and then switch the current exploited arm with the last one to map the full join results with the corresponding arm.
				/***************************************************/
				updateProb(node);
				node->greedyExploit = false;
				node->prevGeneratedJoins = node->generatedJoins;
				node->outerPage->index = 0;
				node->isExploring = false;
				node->exploitStepCounter = 0;
				node->lastPageIndex = MAX(node->pageIndex, node->lastPageIndex);
				//node->pageIndex = popBestPageXid(node);
				node->pageIndex = popBestPage(node);
				//LoadNextOuterPage(outerPlan, node->outerPage, node->xidScanKey, node->pageIndex);
				LoadPageWithTIDs(outerPlan, node->tidRewards ,node->outerPage, node->pageIndex, node->ss->ss_currentRelation, node->ss->ss_ScanTupleSlot);
//                elog(INFO,"0 before, %d,%d",node->tidRewards[node->pageIndex].reward,node->tidRewards[node->pageIndex].tuples[0].t_self.ip_posid);
				node->tidRewards[node->pageIndex] = node->tidRewards[node->activeRelationPages - 1];
//				elog(INFO,"1 after, %d,%d",node->tidRewards[node->pageIndex].reward,node->tidRewards[node->pageIndex].tuples[0].t_self.ip_posid);
//				node->tidRewards[node->activeRelationPages - 1].reward = 10;
//				node->tidRewards[node->activeRelationPages - 1].tuples[0].t_self.ip_posid = 10000;
//				elog(INFO,"2 after, %d,%d",node->tidRewards[node->pageIndex].reward,node->tidRewards[node->pageIndex].tuples[0].t_self.ip_posid);
//				elog(INFO,"3 after, %d,%d",node->tidRewards[node->activeRelationPages - 1].reward,node->tidRewards[node->activeRelationPages - 1].tuples[0].t_self.ip_posid);
				node->activeRelationPages--;
//				elog(INFO,"Entry into exploit and active page is %d right now!!!",node->activeRelationPages);
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
			LoadNextPage(innerPlan, node->innerPage, PAGE2_SIZE);
			if (node->innerPage->tupleCount < PAGE2_SIZE) {
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
				if (node->isExploring){		//exploration phase
					if (node->exploreStepCounter == node->innerPageNumber) {
						node->needOuterPage = true;
					} else{
						if(node->lastReward == 0){	//deal with the reward of current blocks join
							node->nFailure++;
						}else{
							node->reward += node->lastReward;
							node->lastReward = 0;
						}

						calProbChoose(node);
						if (isKeepExplore(node)){	//keep exploring
							node->outerPage->index = 0;
							node->exploreStepCounter++;
						}else{ //end of exploring
							storeTIDs(node->outerPage, node->tidRewards, node->activeRelationPages, node->reward, node->nFailure, node->pChoose);
							node->reward = 0;	//mx
							node->nFailure = 0;
							node->pChoose = 1.0;
							node->activeRelationPages++;
							node->needOuterPage = true;
						}

					}
//				if (node->isExploring &&
//						((node->lastReward > 0 && node->exploreStepCounter < node->innerPageNumber)
//						|| (node->lastReward == 0 && (++node->nFailure) < N_FAILURE))) { //stay with current
//					node->outerPage->index = 0;
//					node->reward += node->lastReward;
//					node->lastReward = 0;
//					node->exploreStepCounter++;
//				} else if (node->isExploring && node->exploreStepCounter == node->innerPageNumber) {
//					// we have generated all possible joins for the current output page
//					// while exploring, no need to store it
//					node->needOuterPage = true;
////			} else if (node->isExploring && node->lastReward == 0 && ++node->nFailure) >= N_FAILURE) {
//				} else if (node->isExploring && node->lastReward == 0 && (++node->nFailure) >= N_FAILURE && (node->reward + gumbelNoise(true) > gumbelNoise(false))) {
//					//push the current explored page
//					//node->xids[node->activeRelationPages] = node->pageIndex;
//					//node->rewards[node->activeRelationPages] = node->reward;
//					storeTIDs(node->outerPage, node->tidRewards, node->activeRelationPages, node->reward);
//					if(GREEDY && node->reward > 0){
//						node->greedyExploit = true;
//					}
//                    node->reward = 0;	//mx
//                    node->nFailure = 0;
//					node->activeRelationPages++;
//					node->needOuterPage = true;
				} else if (!node->isExploring && node->exploitStepCounter < node->innerPageNumber) {
					node->outerPage->index = 0;
					node->exploitStepCounter++;
				} else if (!node->isExploring && node->exploitStepCounter == node->innerPageNumber) {
					// Done with this outer page forever
					elog(INFO, "total matching tuples of best block: %d by %d - %d ", node->generatedJoins - node->prevGeneratedJoins, node->generatedJoins,node->prevGeneratedJoins);
					node->needOuterPage = true;
				} else {
					elog(INFO,"nFailure is %d, explore is %d, explorestep is %d",node->nFailure,node->isExploring,node->exploreStepCounter);
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
//				if (list_member_int(node->pageIdJoinIdLists[node->pageIndex], node->innerPageCounter)) {	//mx
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

	for (;;) {
		if (node->needOuterPage) {
			if (node->reachedEndOfOuter){
				RemoveRelationPage(&(node->outerPage), PAGE_SIZE);
				elog(INFO, "Join Done");
				return NULL;
			}
//			node->outerPage = CreateRelationPage();	//mx
			LoadNextPage(outerPlan, node->outerPage, PAGE_SIZE);
			node->outerTupleCounter += node->outerPage->tupleCount;
			node->outerPageCounter++;
			if (node->outerPage->tupleCount < PAGE_SIZE){
				node->reachedEndOfOuter = true;
				if (node->outerPage->tupleCount == 0) continue;
			}
			node->needOuterPage = false;
		}
		if (node->needInnerPage) {
			LoadNextPage(innerPlan, node->innerPage, PAGE2_SIZE);
			node->innerTupleCounter += node->innerPage->tupleCount;
			node->innerPageCounter++;
			node->innerPageCounterTotal++;
			node->needInnerPage = false;
			if (node->innerPage->tupleCount < PAGE2_SIZE){ // done with one outer page, move to next
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
					prm->value = slot_getattr(node->outerPage->tuples[node->outerPage->index],
							nlp->paramval->varattno,
							&(prm->isnull));
					/* Flag parameter value as changed */
					innerPlan->chgParam = bms_add_member(innerPlan->chgParam,
							paramno);
				}
				ENL1_printf("rescanning inner plan");
				ExecReScan(innerPlan);
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
	if (strcmp(fastjoin, "on") == 0){
		tts = ExecBanditJoin(pstate);
	} else if (strcmp(blocknestloop, "on") == 0) {
		tts = ExecBlockNestedLoop(pstate);
	} else {
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
	int i;
	const char* fastjoin;
	const char* blocknestloop;

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
	nlstate->reward = 0;
	nlstate->nFailure = 0;
	nlstate->pChoose = 1.0;
	nlstate->greedyExploit = false;
	nlstate->prevGeneratedJoins = 0;

	nlstate->outerPageNumber = outerPlan(node)->plan_rows / PAGE_SIZE;
	nlstate->innerPageNumber = innerPlan(node)->plan_rows / PAGE2_SIZE;

	//TODO sometimes the inner plan_rows does not match the exact row numbers
	// elog(INFO, "Outer page number: %ld", nlstate->outerPageNumber);
	// elog(INFO, "Inner page number: %ld", nlstate->innerPageNumber);


//	nlstate->sqrtOfInnerPages = 2*(int)sqrt(nlstate->innerPageNumber);
//	nlstate->sqrtOfInnerPages = 4*(int)sqrt(nlstate->innerPageNumber);
	//nlstate->sqrtOfInnerPages = 20*(int)sqrt(nlstate->innerPageNumber);
//	nlstate->sqrtOfInnerPages = 50*(int)sqrt(nlstate->innerPageNumber);
//	nlstate->sqrtOfInnerPages = (int)(nlstate->outerPageNumber - 1);

//	nlstate->sqrtOfInnerPages = (int)sqrt(nlstate->innerPageNumber);
//	nlstate->sqrtOfInnerPages = (int) (sqrt(nlstate->innerPageNumber)/10);
	//nlstate->sqrtOfInnerPages = (int) (sqrt(nlstate->innerPageNumber)/20);
	nlstate->sqrtOfInnerPages = 10;
	elog(INFO, "sqrtOfInnerPages: %ld", nlstate->sqrtOfInnerPages);
//	nlstate->sqrtOfInnerPages = (int) nlstate->innerPageNumber + 1;

	nlstate->xids = palloc(nlstate->sqrtOfInnerPages * sizeof(int));
	nlstate->rewards = palloc(nlstate->sqrtOfInnerPages * sizeof(int));
	nlstate->tidRewards = palloc(nlstate->sqrtOfInnerPages * sizeof(struct tupleRewards));
//	nlstate->tidRewards->prob.tolRnds = 0;
    nlstate->pageIndex = -1;
	nlstate->lastPageIndex = 0;
	nlstate->xidScanKey = (ScanKey) palloc(sizeof(ScanKeyData));
//    for(i = 0; i < nlstate->sqrtOfInnerPages; i++) {
//        nlstate->tidRewards[i].reward = 0;
//        nlstate->tidRewards[i].updated = false;
//        nlstate->tidRewards[i].nFail = 0;
//    }
//	nlstate->pageIdJoinIdLists = palloc(nlstate->outerPageNumber * sizeof(List*)); //mx
	i = 0;
//	while (i < nlstate->outerPageNumber){	//mx
//		nlstate->pageIdJoinIdLists[i] = NIL;
//		i++;
//	}

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
	if (strcmp(fastjoin, "on") == 0){
		elog(INFO, "Running bandit join..");
	} else {
		if (strcmp(blocknestloop, "on") == 0) {
			elog(INFO, "Running block nested loop..");
		} else {
			elog(INFO, "Running nested loop..");
		}
	}

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
	i = 0;
//	while (i < node->outerPageNumber){	//mx
//		list_free(node->pageIdJoinIdLists[i]);
//		node->pageIdJoinIdLists[i] = NULL;
//		i++;
//	}
	RemoveRelationPage(&(node->outerPage), PAGE_SIZE);
	RemoveRelationPage(&(node->innerPage), PAGE2_SIZE);
	pfree(node->xids);
	pfree(node->rewards);
	pfree(node->xidScanKey);
    pfree(node->tidRewards);
//	pfree(node->pageIdJoinIdLists);//TODO remove each entry?	//mx
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

