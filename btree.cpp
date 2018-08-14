#include "btree.h"
#include<algorithm>
#include<iostream>
#include<map>

using namespace std;   

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
    this->stat = nullptr;
    this->root = nullptr;
}

// Create the index.
void BTreeIndex::create() {

	this->file.create();
    this->stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
    this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
    this->closed = false;
    // build the index
    Handles* handles = this->relation.select();
    try {
        for (auto const& handle: *handles)
            this->insert(handle);
    }catch (DbRelationError& e) {
        this->file.drop();
        throw;
    }
    close();
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
            this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, false);
        else
            this->root =  new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, false);
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
	Handles * handles = this->_lookup(this->root, this->stat->get_height(), this->tkey(key_dict));
    return handles;
}

//recursive function for lookup
Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
    Handles* handles = new Handles; 
    if (height == 1){
        try{
            BTreeLeaf* leafNode = (BTreeLeaf*)node;
            Handle handle = leafNode->find_eq(key);
            handles->push_back(handle);
        } catch (...){}
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
    open();
    ValueDict * row = this->relation.project(handle, &this->key_columns);
    KeyValue *key = this->tkey(row);
    Insertion split_root = this->_insert(this->root, this->stat->get_height(), key, handle);
//if we split the root, grow the tree up one level
    if (!BTreeNode::insertion_is_none(split_root)) {
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
    close();
    delete row;
    delete key;
}

// recursive function for insert (private)
Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
    if (height == 1) {
        BTreeLeaf* leafNode = (BTreeLeaf*)node;
        Insertion retVal = leafNode->insert(key, handle);
        leafNode->save();
        return retVal;
    } else {
        BTreeInterior* intNode = (BTreeInterior*)node;
        Insertion new_kid = this->_insert(intNode->find(key, height), height-1, key, handle);
        if (!BTreeNode::insertion_is_none(new_kid)) {
            Insertion retVal = intNode->insert(&new_kid.second, new_kid.first);
            intNode->save();
            return retVal;
        } 
        return BTreeNode::insertion_none();
    }
}


void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME
}

KeyValue *BTreeIndex::tkey(ValueDict const *key) const {
    if (key == nullptr)
        return nullptr;
    KeyValue* toReturn = new KeyValue;
    for (Identifier column_name: this->key_columns) {
        toReturn->push_back(key->at(column_name));
    }
	return toReturn;
}

void BTreeIndex::build_key_profile() {
    ColumnAttributes* column_attr = new ColumnAttributes;
    column_attr = this->relation.get_column_attributes(key_columns);
    for (auto  attr: *column_attr)
        this->key_profile.push_back(attr.get_data_type());
}

