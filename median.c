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
  int num_elements;
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
    
    MedianState *median_state;
    DatumChainNode* new_node;

    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_transfn called in non-aggregate context");
    
    median_state = (MedianState *) PG_GETARG_POINTER(0);

    if (median_state == NULL) { // First time the function is invoked
      // Prepare state
      median_state = (MedianState *) palloc(sizeof(MedianState));
      median_state->num_elements = 0;
      median_state->root = NULL;
    } 
      
    if (!PG_ARGISNULL(1)) {
      median_state->num_elements += 1;
      
      // Append new node in front of chain
      new_node = (DatumChainNode *) palloc(sizeof(DatumChainNode));
      new_node->datum = PG_GETARG_DATUM(1); // here median_state is corrupted!
      new_node->next = median_state->root;
      
      median_state->root = new_node;
    } 

    PG_RETURN_POINTER(median_state);
}

int compare_datum(const void* left, const void* right);

int compare_datum(const void* left, const void* right) {
  Datum l = *(const Datum*)left;
  Datum r = *(const Datum*)right;
  
  if (l < r) {
    return -1;
  } 
  else if (l > r) {
    return 1;
  }
  
  return 0;
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
    MedianState *median_state;
    Datum* datum_array;
    Datum* array;
    DatumChainNode* node;
    DatumChainNode* node_to_free;
    int num_elements;
    int center_element;

    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "median_finalfn called in non-aggregate context");
    
    elog(INFO, "final call");
    
    median_state = (MedianState *) PG_GETARG_POINTER(0);
    
    if (median_state == NULL) {
      PG_RETURN_NULL();
    }
    
    num_elements = median_state->num_elements;
    center_element = num_elements/2;
    
    array = datum_array = palloc(sizeof(Datum) * num_elements);
    
    node = median_state->root;
    
    while (node != NULL) {
      *datum_array = node->datum;
      ++datum_array;
      node_to_free = node;
      node = node->next;
      pfree(node_to_free);
    }
    
    pfree(median_state);
    
    qsort(array, num_elements, sizeof(Datum), compare_datum);

    if (num_elements % 2 == 0) {
      PG_RETURN_DATUM((array[center_element]+array[center_element-1])/2);
    } 
    
    PG_RETURN_DATUM(array[center_element]);
}

