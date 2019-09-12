#include "kernel.h"
#include "parameters.h"

using namespace std;

/***********************************************
 *
 * Prototype Declarations
 *
 ***********************************************/
extern int getAtrAdr(list<ATR> atr, string relNam, string atrNam);
extern void put2Sync(TUPLE h);
extern void delTuple(TUPLE *t);

/***********************************************
 * 
 * Functions
 *
 ***********************************************/
extern DATA_TYPE
getDataType4AS(list<ATR> atr, string relNam, string atrNam) 
{
  list<ATR>::iterator ita;

  for (ita = atr.begin();
       ita != atr.end(); ita++) {
    if ((relNam == ita->relNam) && 
				(atrNam == ita->atrNam)) {
			break;
		}
  } 

  return ita->dt;
}

static void 
doRunSelection(NODE *n, TUPLE *t)
{
  int c;
  DATA_TYPE dt;
	SCHEMA *schema = n->child->schema;
  
  c = getAtrAdr(schema->atr, n->s.c.relNam, n->s.c.atrNam);
  dt = getDataType4AS(schema->atr, n->s.c.relNam, n->s.c.atrNam);

  switch (dt) {
		case INT: {
			switch (n->s.c.cmp) {
				case EQ: {
					if ((int)n->s.c.val != *(int *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case LEQ: {
					if ((int)n->s.c.val <  *(int *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case GEQ: {
					if ((int)n->s.c.val >  *(int *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case NEQ: {
					if ((int)n->s.c.val == *(int *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case G: {
					if ((int)n->s.c.val >= *(int *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case L: {
					if ((int)n->s.c.val <= *(int *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				default: {
					ERR; 
				} break;
			}
		} break;
		case UINT: {
			switch (n->s.c.cmp) {
				case EQ: {
					if ((uint)n->s.c.val != *(uint *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case LEQ: {
					if ((uint)n->s.c.val <  *(uint *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case GEQ: {
					if ((uint)n->s.c.val >  *(uint *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case NEQ: {
					if ((uint)n->s.c.val == *(uint *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case G: {
					if ((uint)n->s.c.val >= *(uint *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case L: {
					if ((uint)n->s.c.val <= *(uint *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				default: {
					ERR; 
				} break;
			}
		} break;
		case DBL: {
			switch (n->s.c.cmp) {
				case EQ: {
					if ((double)n->s.c.val != *(double *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case LEQ: {
					if ((double)n->s.c.val <  *(double *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				}	break;
				case GEQ: {
					if ((double)n->s.c.val >  *(double *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case NEQ: {
					if ((double)n->s.c.val == *(double *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case G: {
					if ((double)n->s.c.val >= *(double *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				case L: {
					if ((double)n->s.c.val <= *(double *)&t->v[c]) {
						delTuple(t); 
						return;
					} 
				} break;
				default: {
					ERR; 
				} break;
			}
		} break;
		default: {
			DDD(dt);
			ERR;
		} break;
  }
  n->enqueue(t);
}

extern void
doSearch() 
{
  while (true) {
    t = n->dequeue();
    if (!t) break;
    doRunSelection(n, t);
  }
}
