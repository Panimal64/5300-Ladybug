/* 
 * @file btree.cpp
 * Contains definitions for BTreeIndex
 * A physical implmentation of dbIndex
 *
 * @author Jacob Mouser, Greg Deresinski, Kevin Lundeen
 */

#include "btree.h"
#include<algorithm>
#include<map>

using namespace std;   
/* 
 * BTreeIndex constructor
 *
 * @param Relation relation - on which the index is built
 * @param Identifier name - name of index
 * @param ColumnNames key columns - name of columns for index
 * @param bool unique if it is a unique key
 */
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
/*
 * destructor for BTreeIndex
 */
BTreeIndex::~BTreeIndex() {
    if (this->stat != nullptr)
        delete this->stat;
        
    if (this->root != nullptr)
        delete this->root;
    this->stat = nullptr;
    this->root = nullptr;
}

/*
 * create the index
 */
void BTreeIndex::create() {
	this->file.create();
    this->stat = new BTreeStat(this->file, this->STAT, this->STAT + 1, this->key_profile);
    this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
    this->closed = false;
    // build the index/ insert already existing rows
    try {
        for (auto const& handle: *this->relation.select()) {
            this->insert(handle);
        }
    //if create fails, drop what you have done
    }catch (DbRelationError& e) {
        this->file.drop();
        throw;
    }
}

/* 
 * Drop the index.
 */
void BTreeIndex::drop() {
    this->file.drop();
}

/*
 *  Open existing index. Enables: lookup, range, insert, delete, update.
 */
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

/*
 *  Closes the index. Disables: lookup, range, insert, delete, update.
 */
void BTreeIndex::close() {
	this->file.close();
    delete stat;
    delete root;
    stat = nullptr;
    root = nullptr;
    this->closed = true;
}

/*
 * Find all the rows whose columns are equal to key. 
 * Assumes key is a dictionary whose keys are the column
 * names in the index. Returns a list of row handles.
 * @param ValueDict* key_dict - which you are atempting to lookup
 * @return Handles  to lookup result
 */
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
	Handles* handles =  this->_lookup(this->root, this->stat->get_height(), this->tkey(key_dict));
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

/*
 * for selecting/inserting/deleting over a range
 * NOT IMPLMENTED
 */
Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

/* 
 * Insert a row with the given handle. Row must exist in relation already.
 * @param Handle handle of row to insert
 */
void BTreeIndex::insert(Handle handle) {
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
}

/*
 *  recursive function for insert (private)
 */
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

/*
 * delete a value from the index NOT IMPLMENTED
 */
void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
	// FIXME
}

// private method for extracting key values from a ValueDict
KeyValue *BTreeIndex::tkey(ValueDict const *key) const {
    KeyValue* toReturn = new KeyValue;
    for (Identifier column_name: this->key_columns) {
        toReturn->push_back(key->at(column_name));
    }
	return toReturn;
}

// private method for initializing the key_profile in constructor
void BTreeIndex::build_key_profile() {
    ColumnAttributes* column_attr = new ColumnAttributes;
    column_attr = this->relation.get_column_attributes(key_columns);
    for (auto  attr: *column_attr)
        this->key_profile.push_back(attr.get_data_type());
}

/**
 * Helper function for test.
 */
bool test_compare(BTreeIndex &index, HeapTable &table, ValueDict *test, ValueDict *compare)
{
    ValueDicts* result = new ValueDicts;
    Handles* index_handle = index.lookup(test);
    if (!index_handle->empty()) {
        for (Handle& i : *index_handle) {
            result->push_back(table.project(i));
        }
    }

    if (result->empty() && compare->empty()) {
        delete result;
        return true;
    }

    if (result->empty() || compare->empty()) {
        delete result;
        return true;
    }


    for (ValueDict * result_index : *result)
    {
        for (auto const& entry: *compare) {
            if (entry.second != (*result_index)[entry.first]) {
                delete result;
                return false;
            }
        }
    }

    delete result;
    return true;
}

/**
 * tests all functionality of BTreeIndex
 * @return bool true if passes
*/
bool test_btree() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    
    HeapTable table("_test_btree_cpp", column_names, column_attributes);
    table.create();

    // add values
    ValueDict *row1 = new ValueDict;
    ValueDict *row2 = new ValueDict;
    (*row1)["a"] = 12;
    (*row1)["b"] = 99;
    (*row2)["a"] = 88;
    (*row2)["b"] = 101;


    for (unsigned int i = 0; i < 100; i++)
    {
        ValueDict brow;
        brow["a"] = i + 100;
        brow["b"] = -i;
        table.insert(&brow);
    }


    ColumnNames index_column;
    index_column.push_back(column_names.at(0));
    BTreeIndex index(table, "test_index", index_column, true);
    index.create();


    ValueDict *test_row = new ValueDict;
    // Test 1
    (*test_row)["a"] = 12;
    if(!test_compare(index, table, test_row, row1))
        return false;


    // Test 2
    (*test_row)["a"] = 88;
    if(!test_compare(index, table, test_row, row2))
        return false;


    // Test 3
    (*test_row)["a"] = 6;
    if (!test_compare(index, table, test_row, row2))
       return false;


    for (unsigned int j = 0; j < 10; j++) {
        for (unsigned int i = 0; i < 1000; i++) {
            (*test_row)["a"] = i + 100;
            (*test_row)["b"] = -i;
            if(!test_compare(index, table, test_row, test_row))
                return false;
        }
    }

    return true;
}



