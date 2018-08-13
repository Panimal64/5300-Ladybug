#include "btree.h"
#include<algorithm>

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
        this->build_key_profile();
}

BTreeIndex::~BTreeIndex() {
    if (this->stat != nullptr)
        delete this->stat;
    if (this->root != nullptr)
        delete this->root;
}

// Create the index.
void BTreeIndex::create() {

	this->file.create();
    this->stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
    this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
    this->closed = false;
    // build the index
    this->file.open();
    Handles* handles = this->relation.select();
    for (auto const& handle: *handles)
        this->insert(handle);
    this->file.close();
    delete handles;

}

// Drop the index.
void BTreeIndex::drop() {
    this->file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
	if (this->closed) {
        this->file.open();
        this->stat = new BTreeStat(this->file, this->STAT, this->key_profile);
        if (this->stat->get_height() == 1) 
            this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
        else
            this->root =  new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, true);
        this->closed = false;
    }
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
	this->file.close();
    delete stat;
    delete root;
    this->closed = true;
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	return this->_lookup(this->root, this->stat->get_height(), this->tkey(key_dict));
}

//recursive function for lookup
Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
     if (height == 1){
        BTreeLeaf* leafNode = (BTreeLeaf*)node;
        Handle handle = leafNode->find_eq(key);
        Handles* handles = nullptr;
        handles->push_back(handle);
        return handles;
    } else {
        BTreeInterior* intNode = (BTreeInterior*)node;
        return this->_lookup(intNode->find(key, height), height-1, key);
    }
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
    KeyValue *tkey = this->tkey(this->relation.project(handle, &this->key_columns));
    Insertion split_root = this->_insert(this->root, this->stat->get_height(), tkey, handle);

//if we split the root, grow the tree up one level
    if (&split_root.first != nullptr) {
        BTreeInterior* newRoot = new BTreeInterior(this->file, 0, this->key_profile, true);
        newRoot->set_first(this->root->get_id());
        newRoot->insert(&split_root.second, split_root.first);
        newRoot->save();
        this->stat->set_root_id(newRoot->get_id());
        this->stat->set_height(this->stat->get_height() + 1);
        this->stat->save();
        delete this->root;
        this->root = newRoot;
    } 
}

// recursive function for insert (private)
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
    Insertion retVal;
    if (height == 1) {
        BTreeLeaf* leafNode = (BTreeLeaf*)node;
        retVal = leafNode->insert(key, handle);
        leafNode->save();
        return retVal;
    } else {
        BTreeInterior* intNode = (BTreeInterior*)node;
        Insertion new_kid = this->_insert(intNode->find(key, height), height-1, key, handle);
        if (&new_kid.first != nullptr) {
            retVal = intNode->insert(&new_kid.second, new_kid.first);
            intNode->save();
            return retVal;
        } 
    }
    return retVal;
}


void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME
}

KeyValue *BTreeIndex::tkey(ValueDict const *key) const {
    KeyValue* toReturn = nullptr;
    for (auto const& column_name: key_columns)
        toReturn->push_back(key->find(column_name)->second);
	return toReturn;
}

void BTreeIndex::build_key_profile() {
    ColumnAttributes column_attributes = this->relation.get_column_attributes();
    ColumnNames column_names = this->relation.get_column_names();
    for (auto const& column_name: this->key_columns)    
        this->key_profile.push_back(column_attributes.at(find(column_names.begin(), column_names.end(), column_name)-column_names.begin()).get_data_type());
}

