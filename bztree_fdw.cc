extern "C" {
#include "postgres.h"
#include "access/attnum.h"
#include "utils/relcache.h"
#include "access/reloptions.h"
#include "access/nbtree.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/relation.h"
#include "access/relscan.h"
#include "access/xact.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "utils/rel.h"
#include "nodes/makefuncs.h"
#include "nodes/pathnodes.h"
#include "catalog/dependency.h"
#include "catalog/pg_operator.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/storage.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/builtins.h"
#include "utils/index_selfuncs.h"
#include "utils/selfuncs.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "tcop/utility.h"
#include "postmaster/bgworker.h"
#include "pgstat.h"
#include "executor/executor.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lock.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include <math.h>
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif
};

#include "bztree.h"


#define BZTREE_EQ_PROC			  1
#define BZTREE_OPTIONS_PROC		  2
#define BZTREE_NPROCS			  2

/* Scan strategies */
#define BZTREE_EQUAL_STRATEGY	  1
#define BZTREE_NSTRATEGIES		  1

#define BZTREE_SCAN_SIZE          (64*1024)
#define BZTREE_HEIGHT_ESTIMATION  4

static pmwcas::DescriptorPool* bztree_pool;

int bztree_mem_size;
int bztree_descriptor_pool_size;
int bztree_max_indexes;

static shmem_startup_hook_type  PreviousShmemStartupHook = NULL;
static HTAB*   bztree_hash;
static LWLock* bztree_hash_lock;

/* Kind of relation options for bztree index */
static relopt_kind bztree_relopt_kind;

static bool bztree_initialized;

extern "C" void	_PG_init(void);
extern "C" void	_PG_fini(void);

struct BzTreeOptions
{
	int32	vl_len_;		/* varlena header (do not touch directly!) */
	bztree::BzTree::ParameterSet param;

	BzTreeOptions() {
		SET_VARSIZE(this, sizeof(BzTreeOptions));
	}
};

static BzTreeOptions *
makeDefaultBzTreeOptions(void)
{
	return new (palloc(sizeof(BzTreeOptions))) BzTreeOptions();
}

struct BzTreeHashEntry
{
	Oid relid;
	bztree::BzTree* tree;
};

static void
bztree_shmem_startup(void)
{
	HASHCTL info;

	if (PreviousShmemStartupHook)
	{
		PreviousShmemStartupHook();
    }
	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(BzTreeHashEntry);
	bztree_hash = ShmemInitHash("bztree hash",
								bztree_max_indexes, bztree_max_indexes,
								&info,
							 HASH_ELEM | HASH_BLOBS);
	bztree_hash_lock = &(GetNamedLWLockTranche("bztree"))->lock;
}

void _PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "bztree extension should be loaded via shared_preload_libraries");

	DefineCustomIntVariable("bztree.memory_size",
                            "Size of memory for bztree indexes.",
							NULL,
							&bztree_mem_size,
							4*1024*1024,
							1024*1024,
							INT_MAX,
							PGC_POSTMASTER,
							GUC_UNIT_KB,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("bztree.descriptor_pool_size",
                            "Size of descriptor pool for pmwcas.",
							NULL,
							&bztree_descriptor_pool_size,
							64*1024,
							1024,
							INT_MAX,
							PGC_BACKEND,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("bztree.max_indexes",
                            "Maximal numbe of bztree indexes.",
							NULL,
							&bztree_max_indexes,
							1024,
							1,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	bztree_relopt_kind = add_reloption_kind();

	add_int_reloption(bztree_relopt_kind, "split_threshold",
					  "BzTree split threshold",
					  3072, 128, 64*1024,
					  AccessExclusiveLock);

	add_int_reloption(bztree_relopt_kind, "merge_threshold",
					  "BzTree merge threshold",
					  1024, 128, 64*1024,
					  AccessExclusiveLock);

	add_int_reloption(bztree_relopt_kind, "leaf_node_size",
					  "BzTree leaf node size",
					  4096, 128, 64*1024,
					  AccessExclusiveLock);

	RequestAddinShmemSpace(hash_estimate_size(bztree_max_indexes, sizeof(BzTreeHashEntry)));
	RequestNamedLWLockTranche("bztree", 1);

	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = bztree_shmem_startup;

	if (!bztree_initialized)
	{
		bztree_initialized = true;
#ifdef PMDK
		pmwcas::InitLibrary(pmwcas::PMDKAllocator::Create("pool_bztree", "layout_bztree", bztree_mem_size),
                        pmwcas::PMDKAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
		auto allocator = reinterpret_cast<pmwcas::PMDKAllocator *>(pmwcas::Allocator::Get());
		bztree::Allocator::Init(allocator);
#else
		pmwcas::InitLibrary(pmwcas::NumaAllocator::Create,
							pmwcas::NumaAllocator::Destroy,
							pmwcas::LinuxEnvironment::Create,
							pmwcas::LinuxEnvironment::Destroy);
		auto allocator = pmwcas::Allocator::Get();
	#endif
		allocator->Allocate((void **) &bztree_pool, sizeof(pmwcas::DescriptorPool));
		new(bztree_pool) pmwcas::DescriptorPool(bztree_descriptor_pool_size, MaxConnections, false);
	}
}

void _PG_fini(void)
{
	shmem_startup_hook = PreviousShmemStartupHook;
	pmwcas::UninitLibrary();
}

//
// Store pointer to BzTree in shared memory in reloptions column of pg_class
//
static void
StoreIndexPointer(Relation index, bztree::BzTree* tree)
{
	LWLockAcquire(bztree_hash_lock, LW_EXCLUSIVE);
	BzTreeHashEntry* entry = (BzTreeHashEntry*)hash_search(bztree_hash, &RelationGetRelid(index), HASH_ENTER, NULL);
	entry->tree = tree;
	LWLockRelease(bztree_hash_lock);
}

//
// Extract pointer to BzTree from reloptions column of pg_class
//
static bztree::BzTree*
GetIndexPointer(Relation index)
{
	bztree::BzTree* tree;
	LWLockAcquire(bztree_hash_lock, LW_SHARED);
	BzTreeHashEntry* entry = (BzTreeHashEntry*)hash_search(bztree_hash, &RelationGetRelid(index), HASH_FIND, NULL);
	Assert(entry != NULL);
	tree = entry->tree;
	LWLockRelease(bztree_hash_lock);
	return tree;
}

static Datum
ShortVarlena(Datum datum, int typeLength, char storage)
{
    /* Make sure item to be inserted is not toasted */
    if (typeLength == -1) {
        datum = PointerGetDatum(PG_DETOAST_DATUM_PACKED(datum));
    }

    if (typeLength == -1 && storage != 'p' && VARATT_CAN_MAKE_SHORT(datum)) {
        /* convert to short varlena -- no alignment */
        Pointer val = DatumGetPointer(datum);
        uint32 shortSize = VARATT_CONVERTED_SHORT_SIZE(val);
        Pointer temp = (Pointer)palloc0(shortSize);
        SET_VARSIZE_SHORT(temp, shortSize);
        memcpy(temp + 1, VARDATA(val), shortSize - 1);
        datum = PointerGetDatum(temp);
    }

    return datum;
}

//
// Serialize attribute value in string.
// Right now only equality comparison is supported
// so providing proper sort order is not needed
//
static void
SerializeAttribute(TupleDesc tupleDescriptor,
				   Index index,
				   Datum datum,
				   StringInfo buffer)
{
    Form_pg_attribute attributeForm = TupleDescAttr(tupleDescriptor, index);
    bool byValue = attributeForm->attbyval;
    int typeLength = attributeForm->attlen;
    char storage = attributeForm->attstorage;

    /* copy utility gets varlena with 4B header, same with constant */
    datum = ShortVarlena(datum, typeLength, storage);

    int offset = buffer->len;
    int datumLength = att_addlength_datum(offset, typeLength, datum);

    enlargeStringInfo(buffer, datumLength);
    char *current = buffer->data + buffer->len;

    if (typeLength > 0) {
        if (byValue) {
            store_att_byval(current, datum, typeLength);
        } else {
            memcpy(current, DatumGetPointer(datum), typeLength);
        }
    } else {
        memcpy(current, DatumGetPointer(datum), datumLength - offset);
    }
    buffer->len = datumLength;
}

//
// Serialize key attributes
//
static void
SerializeKey(StringInfo key, TupleDesc tupDesc, Datum* values, bool* isnull)
{
	for (int i = 0; i < tupDesc->natts; i++)
	{
		if (isnull[i])
			elog(ERROR, "NULL keys are not supported by bztree");
		SerializeAttribute(tupDesc,
						   i,
						   values[i],
						   key);
	}
}

struct BzTreeBuildContext
{
	StringInfoData  key;
	bztree::BzTree* tree;

	BzTreeBuildContext() { 
		initStringInfo(&key);
	}
	~BzTreeBuildContext() {
		pfree(key.data);
	}
};

static void
bztreeBuildCallback(Relation index,
					ItemPointer tid,
					Datum *values,
					bool *isnull,
					bool tupleIsAlive,
					void *state)
{
	BzTreeBuildContext* ctx = (BzTreeBuildContext*)state;
	ctx->key.len = 0;
	SerializeKey(&ctx->key, RelationGetDescr(index), values, isnull);
}

static bztree::BzTree*
BuildBzTree(Relation index)
{
	BzTreeOptions* opts = (BzTreeOptions*)index->rd_options;
	if (opts == NULL)
		opts = makeDefaultBzTreeOptions();
	bztree::BzTree* tree = bztree::BzTree::New(opts->param, bztree_pool);
	StoreIndexPointer(index, tree);
	return tree;
}

//
// BzTree operations implementation
//
extern "C"
{
	static void bztree_buildempty(Relation index);
	static IndexBuildResult * bztree_build(Relation heap, Relation index, IndexInfo *indexInfo);
	static bool bztree_insert(Relation rel, Datum *values, bool *isnull,
							  ItemPointer ht_ctid, Relation heapRel,
							  IndexUniqueCheck checkUnique,
							  IndexInfo *indexInfo);
	static IndexScanDesc bztree_beginscan(Relation rel, int nkeys, int norderbys);
	static void bztree_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
							  ScanKey orderbys, int norderbys);
	static void bztree_endscan(IndexScanDesc scan);
	static bool bztree_gettuple(IndexScanDesc scan, ScanDirection dir);
	static IndexBulkDeleteResult *	bztree_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
													  IndexBulkDeleteCallback callback, void *callback_state);
	static IndexBulkDeleteResult *	bztree_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

	static bytea * bztree_options(Datum reloptions, bool validate);
	static void    bztree_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
									   Cost *indexStartupCost, Cost *indexTotalCost,
									   Selectivity *indexSelectivity, double *indexCorrelation,
									   double *indexPages);

	PG_FUNCTION_INFO_V1(bztree_handler);
};


static void
bztree_buildempty(Relation index)
{
	(void)BuildBzTree(index);
}

static IndexBuildResult *
bztree_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
	IndexBuildResult *result;
	BzTreeBuildContext ctx;
	ctx.tree = BuildBzTree(index);
	RelationOpenSmgr(index);
	double reltuples = table_index_build_scan(heap, index, indexInfo, true, true,
											  bztreeBuildCallback,
											  (void *)&ctx, NULL);
	/*
	 * Return statistics
	 */
	result = (IndexBuildResult *) palloc(sizeof(IndexBuildResult));

	result->heap_tuples = reltuples;
	result->index_tuples = reltuples;

	return result;
}

struct BzTreeCursor {
	std::unique_ptr<bztree::Iterator> iterator;
	StringInfoData key;

	BzTreeCursor() {
		initStringInfo(&key);
	}
	~BzTreeCursor() {
		pfree(key.data);
	}
};


static bool
bztree_insert(Relation rel, Datum *values, bool *isnull,
			  ItemPointer ht_ctid, Relation heapRel,
			  IndexUniqueCheck checkUnique,
			  IndexInfo *indexInfo)
{
	bztree::BzTree* tree = GetIndexPointer(rel);
	BzTreeCursor cursor;
	SerializeKey(&cursor.key, RelationGetDescr(rel), values, isnull);
#if 0
	if (checkUnique == UNIQUE_CHECK_YES) {
		auto iterator = tree->RangeScanBySize(cursor.key.data, cursor.key.len, BZTREE_SCAN_SIZE);
		while (true) {
			auto r = iterator->GetNext();
			if (r && memcmp(r->GetKey(), cursor.key.data, cursor.key.len) == 0) {
				// check visibility
				return false;
			} else {
				break;
			}
		}
	}
#endif
	appendBinaryStringInfo(&cursor.key, (char*)ht_ctid, sizeof(*ht_ctid));
	auto payload = ((uint64_t)ItemPointerGetBlockNumber(ht_ctid) << 16) | ItemPointerGetOffsetNumber(ht_ctid);
	auto rc = tree->Insert(cursor.key.data, cursor.key.len, payload);
	if (!rc.IsOk()) {
		elog(ERROR, "Failed to insert record in bztree index");
	}
	return true;
}

static IndexScanDesc
bztree_beginscan(Relation rel, int nkeys, int norderbys)
{
	IndexScanDesc scan = RelationGetIndexScan(rel, nkeys, norderbys);
	scan->opaque = new (palloc(sizeof(BzTreeCursor))) BzTreeCursor();
	return scan;
}

static void
bztree_rescan(IndexScanDesc scan, ScanKey scankey, int nscankeys,
			ScanKey orderbys, int norderbys)
{
	BzTreeCursor* cursor = (BzTreeCursor*)scan->opaque;
	/* Update scan key, if a new one is given */
	if (scankey && scan->numberOfKeys > 0)
	{
		memmove(scan->keyData,
				scankey,
				scan->numberOfKeys * sizeof(ScanKeyData));
	}
	cursor->iterator = NULL;
	resetStringInfo(&cursor->key);
}

static bool
bztree_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	BzTreeCursor* cursor = (BzTreeCursor*)scan->opaque;
	if (!cursor->iterator)
	{
		Relation index = scan->indexRelation;
		TupleDesc tupDesc = RelationGetDescr(index);
		bztree::BzTree* tree = GetIndexPointer(index);

		for (int i = 0; i < scan->numberOfKeys; i++)
		{
			ScanKey	sk = &scan->keyData[i];
			Assert(sk->sk_attno == i+1);
			SerializeAttribute(tupDesc,
							   i,
							   sk->sk_argument,
							   &cursor->key);
		}
		cursor->iterator = tree->RangeScanBySize(cursor->key.data, cursor->key.len, BZTREE_SCAN_SIZE);
	}
	auto r = cursor->iterator->GetNext();
	if (!r) {
		return false;
	}
	if (memcmp(r->GetKey(), cursor->key.data, cursor->key.len) == 0) {
		auto payload = r->GetPayload();
		ItemPointerSet(&scan->xs_heaptid, payload >> 16, payload & 0xFFFF);
		return true;
	}
	return false;
}

static void
bztree_endscan(IndexScanDesc scan)
{
	BzTreeCursor* cursor = (BzTreeCursor*)scan->opaque;
	cursor->~BzTreeCursor();
	pfree(cursor);
}

static IndexBulkDeleteResult *
bztree_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback, void *callback_state)
{
	bztree::BzTree* tree = GetIndexPointer(info->index);

	if (stats == NULL)
		stats = (IndexBulkDeleteResult *) palloc0(sizeof(IndexBulkDeleteResult));

	// key with zero length should be smaller than all other keys
	auto iter = tree->RangeScanBySize("", 0, BZTREE_SCAN_SIZE);
	while (true) {
		auto r = iter->GetNext();
		if (!r) {
			break;
		}
		auto payload = r->GetPayload();
		ItemPointerData t_tid;
		ItemPointerSet(&t_tid, payload >> 16, payload & 0xFFFF);

		if (callback(&t_tid, callback_state))
		{
			tree->Delete(r->GetKey(), r->meta.GetKeyLength());
			stats->tuples_removed += 1; /* # removed during vacuum operation */
		}
		else
			stats->num_index_tuples += 1;	/* tuples remaining */
	}
	return stats;
}

static IndexBulkDeleteResult *
bztree_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
	return stats;
}

static bytea *
bztree_options(Datum reloptions, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"split_threshold", RELOPT_TYPE_INT, offsetof(BzTreeOptions, param.split_threshold)},
		{"merge_threshold", RELOPT_TYPE_INT, offsetof(BzTreeOptions, param.merge_threshold)},
		{"leaf_node_size",  RELOPT_TYPE_INT, offsetof(BzTreeOptions, param.leaf_node_size)}
	};

	return (bytea *) build_reloptions(reloptions, validate,
									  bztree_relopt_kind,
									  sizeof(BzTreeOptions),
									  tab, lengthof(tab));
}

static void
bztree_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
	IndexOptInfo *index = path->indexinfo;
	GenericCosts costs;
	Cost		 descentCost;

	MemSet(&costs, 0, sizeof(costs));

	genericcostestimate(root, path, loop_count, &costs);

	if (index->tree_height < 0) /* unknown? */
	{
		index->tree_height = BZTREE_HEIGHT_ESTIMATION; // TODO: calculate or store tree height 
	}

	/*
	 * Add a CPU-cost component to represent the costs of initial descent. We
	 * just use log(N) here not log2(N) since the branching factor isn't
	 * necessarily two anyway.  As for btree, charge once per SA scan.
	 */
	if (index->tuples > 1)		/* avoid computing log(0) */
	{
		descentCost = ceil(log(index->tuples)) * cpu_operator_cost;
		costs.indexStartupCost += descentCost;
		costs.indexTotalCost += costs.num_sa_scans * descentCost;
	}
	/*
	 * Likewise add a per-page charge, calculated the same as for btrees.
	 */
	descentCost = (index->tree_height + 1) * 50.0 * cpu_operator_cost;
	costs.indexStartupCost += descentCost;
	costs.indexTotalCost += costs.num_sa_scans * descentCost;

	*indexStartupCost = costs.indexStartupCost;
	*indexTotalCost = costs.indexTotalCost;
	*indexSelectivity = costs.indexSelectivity;
	*indexCorrelation = costs.indexCorrelation;
	*indexPages = costs.numIndexPages;
}

Datum
bztree_handler(PG_FUNCTION_ARGS)
{
	IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

	amroutine->amstrategies =  BZTREE_NSTRATEGIES;
	amroutine->amsupport = BZTREE_NPROCS;
	amroutine->amoptsprocnum = BZTREE_OPTIONS_PROC;
	amroutine->amcanorder = false;
	amroutine->amcanorderbyop = false;
	amroutine->amcanbackward = false;
	amroutine->amcanunique = false;
	amroutine->amcanmulticol = true;
	amroutine->amoptionalkey = false;
	amroutine->amsearcharray = false;
	amroutine->amsearchnulls = false;
	amroutine->amstorage = false;
	amroutine->amclusterable = false;
	amroutine->ampredlocks = false;
	amroutine->amcanparallel = false;
	amroutine->amcaninclude = false;
	amroutine->amusemaintenanceworkmem = false;
	amroutine->amparallelvacuumoptions = 0;
	amroutine->amkeytype = InvalidOid;

	amroutine->ambuild = bztree_build;
	amroutine->ambuildempty = bztree_buildempty;
	amroutine->aminsert = bztree_insert;
    amroutine->ambulkdelete = bztree_bulkdelete;
	amroutine->amvacuumcleanup = bztree_vacuumcleanup;
	amroutine->amcanreturn = NULL;
	amroutine->amcostestimate = bztree_costestimate;
	amroutine->amoptions = bztree_options;
	amroutine->amproperty = NULL;
	amroutine->ambuildphasename = NULL;
	amroutine->amvalidate = NULL;

	amroutine->ambeginscan = bztree_beginscan;
	amroutine->amrescan = bztree_rescan;
	amroutine->amgettuple = bztree_gettuple;
	amroutine->amgetbitmap = NULL;
	amroutine->amendscan = bztree_endscan;
	amroutine->ammarkpos = NULL;
	amroutine->amrestrpos = NULL;
	amroutine->amestimateparallelscan = NULL;
	amroutine->aminitparallelscan = NULL;
	amroutine->amparallelrescan = NULL;

	PG_RETURN_POINTER(amroutine);
}
