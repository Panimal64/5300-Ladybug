#include "btree.h"

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?!
}

BTreeIndex::~BTreeIndex() {
	// FIXME - free up stuff
}

// Create the index.
void BTreeIndex::create() {
	// FIXME
}

// Drop the index.
void BTreeIndex::drop() {
	// FIXME
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
	// FIXME
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
	// FIXME
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	// FIXME
	return nullptr;
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
	// FIXME
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
	return nullptr;
	// FIXME
}



