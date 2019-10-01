#include <postgres.h>
#include <fmgr.h>

//#include "median_state.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(median_transfn);

typedef struct DatumChainNode
{
    Datum datum;
    struct DatumChainNode *next;
} DatumChainNode;

typedef struct MedianState {
  int num_elemens;
  struct DatumChainNode* root;
} MedianState;

/*
 * Median state transfer function.
 *
 * This function is called for every value in the set that we are calculating
 * the median for. On first call, the aggregate state, if any, needs to be
 * initialized.
 */
Datum
median_transfn(PG_FUNCTION_ARGS)
{
    MemoryContext agg_context;

    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_transfn called in non-aggregate context");

    bool first_argument = PG_ARGISNULL(0);

    if (first_argument) {
      MedianState *median_state = (MedianState *) palloc(sizeof(MedianState));
      if (!PG_ARGISNULL(1)) {
        median_state->num_elemens = 1;
        median_state->root = (DatumChainNode *) palloc(sizeof(DatumChainNode));
        median_state->root->datum = PG_GETARG_DATUM(1);
        median_state->root->next = NULL;
      } else {
        median_state->num_elemens = 0;
        median_state->root = NULL;
      }
      PG_RETURN_POINTER(median_state);
    } else {
      MedianState *median_state = (MedianState *) PG_GETARG_POINTER(0);
      if (!PG_ARGISNULL(1)) {
          median_state->num_elemens += 1;
          if (median_state->root == NULL) {
              median_state->root = (DatumChainNode *) palloc(sizeof(DatumChainNode));
              median_state->root->datum = PG_GETARG_DATUM(1);
              median_state->root->next = NULL;
          } else {
          }
      }
      PG_RETURN_POINTER(median_state);
    }

    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(median_finalfn);

/*
 * Median final function.
 *
 * This function is called after all values in the median set has been
 * processed by the state transfer function. It should perform any necessary
 * post processing and clean up any temporary state.
 */
Datum
median_finalfn(PG_FUNCTION_ARGS)
{
    MemoryContext agg_context;

    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_finalfn called in non-aggregate context");

    PG_RETURN_NULL();
}

