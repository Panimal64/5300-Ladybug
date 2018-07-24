#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <vector>
#include <iostream>
#include "db_cxx.h"
#include "heap_storage.h"
#include "storage_engine.h"
using namespace std;

typedef uint16_t u16;

/* * * * * * * * * * * 
 * SlotPage Functions
 * * * * * * * * * * */
//Constructor for slotted page
//Provided by Professor Lundeen
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
// Provided by Professor Lundeen
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError) {
    if (!has_room((u16)data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16) data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1U;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get record from a block. If no block exists or was deleted, return None
Dbt* SlottedPage::get(RecordID record_id) const{

	u16 size, loc;
	get_header(size, loc, record_id);

	if(loc == 0){
		return nullptr;
	}
	
	return new Dbt(this->address(loc),size);
}

// Replace a record with specified data.
// Return DbBlockNoRoomError if there is not enough space.
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError){
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();

    if (new_size > size) {
        u16 extra = new_size - size;

        if (!has_room(extra))
            throw DbBlockNoRoomError("not enough room for enlarged record (SlottedPage::put)");
        
        slide(loc, loc - extra);
        memcpy(this->address(loc-extra), data.get_data(), new_size);       
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    //if (!has_room(new_size))
    //    throw DbBlockNoRoomError("Not enough space");
    //if (new_size > size){
    //    slide(loc,loc-new_size-size);
    //    memcpy(address(loc-new_size-size), data.get_data(),new_size);
    //}
    //else{
    //    memcpy(address(loc),data.get_data(),new_size);
    //    slide(loc+new_size,loc+size);
    //}
    get_header(size,loc,record_id);
    put_header(record_id, new_size, loc);
}

// Mark record as deleted by setting size and location to 0.
void SlottedPage::del(RecordID record_id){
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc+size);
}

// Gets all non-deleted record IDs
RecordIDs* SlottedPage::ids(void) const {
    u16 size, loc;
    RecordIDs* records = new RecordIDs;
    for (u16 i = 1; i <= num_records; i++){
    	get_header(size, loc, i);
    	if(loc != 0)
    		records->push_back(i);
    }
    return records;
}

// Gets the size and offset for a record.
// If record id is 0, it is the block header.
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) const {
    size = get_n((u16)4*id);
    loc = get_n((u16)(4*id + 2));
}


// Store the size and offset for given id. For id of zero, store the block header.
// Provided by professor Lundeen
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    if (id == 0) { 
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16)(4*id), size);
    put_n((u16)(4*id + 2), loc);
}

// Determines whether there is room for record.
bool SlottedPage::has_room(u16 size) const {
    u16 available = this->end_free - (u16)((this->num_records + 1) * 4);
    return size <= available; 
}

// Moves records around within a block to adhere to slotted page format.
void SlottedPage::slide(u16 start, u16 end){
    u16 shift = end - start;
    u16 size, loc;

    // If there is no shift, do nothing
    if (shift == 0)
        return;

    // Move data
    //memcpy((u16)(this->address(end_free+1)), (u16)(this->address(end_free+1+shift)),shift);
    void *to = this->address((u16)(this->end_free + 1 + shift));
    void *from = this->address((u16)(this->end_free + 1));
    int bytes = start - (this->end_free + 1U);
    char temp[bytes];
    memcpy(temp, from, bytes);
    memcpy(to, temp, bytes);

    // Adjust headers.
    RecordIDs* records = ids();
    for(RecordIDs::iterator i = records->begin();i!=records->end();i++) {
        get_header(size,loc, *i);
        if(loc <= start)
            put_header(*i,size,loc+shift);
    }
    delete records;
    end_free+=shift;
    put_header();
}


// Get 2-byte integer at given offset in block.
//Provided by Professor Lundeen
u16 SlottedPage::get_n(u16 offset) const {
    return *(u16*)this->address(offset);
}


// Put a 2-byte integer at given offset in block.
// Provided by professor Lundeen
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}


// Make a void* pointer for a given offset into the data block.
// Provided by professor Lundeen
void* SlottedPage::address(u16 offset) const {
    return (void*)((char*)this->block.get_data() + offset);
}


/* * * * * * * * * * *
 * HeapFile Functions
 * * * * * * * * * * */

// Constructor
HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0) {
	this->dbfilename = this->name + ".db";
}
// Create file
void HeapFile::create(void) {
	db_open(DB_CREATE | DB_EXCL);
	get_new();
}

// Delete file
void HeapFile::drop(void) {
    close();
	Db db(_DB_ENV,0);
	db.remove(dbfilename.c_str(),nullptr,0);
}

// Open file
void HeapFile::open(void) {
	db_open();
}

// Close file
void HeapFile::close(void) {
	db.close(0);
    closed = true;
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
// Provided by professor Lundeen
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    delete page;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, this->last);
}


// Returns a specific slotted page
SlottedPage* HeapFile::get(BlockID block_id) {
    Dbt key(&block_id, sizeof(block_id));
    Dbt retData;
    // Get page that matches Block ID.
    this->db.get(nullptr, &key, &retData, 0);
    return new SlottedPage(retData, block_id, false);
}


// Adds block to specific location
void HeapFile::put(DbBlock* block){
    BlockID id = block->get_block_id();
    // Create new Dbt structure using the new ID, and the size.
    Dbt key(&id, sizeof(id));
    this->db.put(nullptr,&key,block->get_block(),0);
}


// Returns list of Block IDs
BlockIDs* HeapFile::block_ids() const {
    BlockIDs* bid_list = new BlockIDs();
    for(u16 i = 1; i <= (this->last); i++){
        bid_list -> push_back(i);
    }
    return bid_list;
}

// Creates and opens DB
void HeapFile::db_open(uint flags){
    DB_BTREE_STAT *stat;
    if(!closed){
    	return;
    }
    db.set_re_len(DbBlock::BLOCK_SZ);
    dbfilename = name + ".db";
    db.open(nullptr, dbfilename.c_str(), nullptr, DB_RECNO, flags, 0644);
    
    if(flags == 0){
    	db.stat(nullptr, &stat, 0);
    	last = stat->bt_ndata;
    } else {
        this->last = 0;
    }

    closed = false;
}


/* * * * * * * * * * * *
 * HeapTable Functions
 * * * * * * * * * * * */
//Provided by Professor Lundeen
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes )
        : DbRelation(table_name,column_names,column_attributes), file(table_name) { }


// Executes CREATE TABLE <table_name> (<columns>)
void HeapTable::create(){
	file.create();
}

// Executes CREATE TABLE IF NOT EXISTS <table_name> (<columns>)
void HeapTable::create_if_not_exists(){

    try{
        open();
    } catch (DbException& e) {
        create();
    }
}

// Executes DROP TABLE <table_name>
void HeapTable::drop(){
	file.drop();
}

// Open a table to allow INSERT, UPDATE, DELETE, SELECT, and PROJECT functions
void HeapTable::open(){
	file.open();
}

// Close table
void HeapTable::close(){
	file.close();
}

// Executes INSERT INTO <table_name> (<row_keys>) VALUES (<row_values>)
// Returns a handle for the inserted row.
Handle HeapTable::insert(const ValueDict* row){
    open();    
    Handle hand = append(validate(row));
    return hand;
}

// Will be used to execute UPDATE functions
void HeapTable::update(const Handle handle, const ValueDict* new_values){
	throw DbRelationError("UPDATE NOT YET IMPLEMENTED");
}

// Will be used to execute DELETE functions
// Provided by professor Lundeen
void HeapTable::del(const Handle handle){
	open();
	BlockID block_id = handle.first;
	RecordID record_id = handle.second;
	SlottedPage* block = this->file.get(block_id);
	block->del(record_id);
	this->file.put(block);
	delete block;
}

// Returns list of handles for all rows
Handles* HeapTable::select(){
    //return all rows in table
    return select(nullptr);
}


// Returns list of handles for specified rows.
// Provided by professor Lundeen
Handles* HeapTable::select(const ValueDict* where) {
    open();
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();

    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();

        for (auto const& record_id: *record_ids) {
            Handle handle(block_id, record_id);
            if (selected(handle, where))
                handles->push_back(Handle(block_id, record_id));
        }           
        delete record_ids;
        delete block;
    }

    delete block_ids;
    return handles;
}


// Returns all fields of the given handle
ValueDict* HeapTable::project(Handle handle){
    return project(handle, &this->column_names);
}


// Returns specific fields of the given handle
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names){
    BlockID bid = handle.first;
    RecordID rid = handle.second;
    SlottedPage* block = file.get(bid);
    Dbt* data = block->get(rid);
    ValueDict* row = unmarshal(data);
    delete data;
    delete block;
    if(column_names->empty()){
        return row;
    }
    ValueDict* res = new ValueDict();
    for(auto const& column_name: *column_names) {
        if(row->find(column_name) == row->end()){
            throw(DbRelationError("'" + column_name + "'' does not exisit in table.\n"));
        }
        (*res)[column_name] = (*row)[column_name];
    }
    delete row;

    return res;     
}

// Validates whether a row is ready for insertion.
ValueDict* HeapTable::validate(const ValueDict* row) const {
	ValueDict* result = new ValueDict();
    Value value;
    ValueDict::const_iterator rowInfo;
    for (ColumnNames::const_iterator i = column_names.begin(); i != column_names.end(); i++) {
        rowInfo = row->find(*i);
        if(rowInfo==row->end()){
            throw DbRelationError("don't know how to handle NULLs, defaults, etc. yet");
        } else {
            value = rowInfo->second;
        }
        (*result) [*i] = value;
    }

    return result;
}

// Appends a record to file.
// Assumes that row is valid.
Handle HeapTable::append(const ValueDict* row){
    Dbt* newData = marshal(row); 
    SlottedPage* block = this->file.get(this->file.get_last_block_id());
    RecordID recordID;
    Handle result;
    try {
        recordID = block->add(newData);
    } catch(DbException& e) {
        block = this->file.get_new();
        recordID = block->add(newData);
    }
    this->file.put(block);
    delete[] (char*)newData->get_data();
    delete block;
    result.first = file.get_last_block_id();
    result.second = recordID;
    return result;
}


// Return the bits to go into the file
// Caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
// Provided by professor Lundeen
Dbt* HeapTable::marshal(const ValueDict* row) const {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // More than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
			if (offset + 4 > DbBlock::BLOCK_SZ - 4)
				throw DbRelationError("row too big to marshal");            
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u_long size = value.s.length();
            if (size > UINT16_MAX)
				throw DbRelationError("text field too long to marshal");
			if (offset + 2 + size > DbBlock::BLOCK_SZ)
				throw DbRelationError("row too big to marshal");
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); // Assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            if (offset + 1 > DbBlock::BLOCK_SZ - 1)
                throw DbRelationError("row too big to marshal");
            *(uint8_t*) (bytes + offset) = (uint8_t)value.n;
            offset += sizeof(uint8_t);    
        } else {
            throw DbRelationError("Only know how to marshal INT, TEXT, and BOOLEAN");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

// Converts marshaled object back to original object type
// Update from Milestone3_prep
ValueDict* HeapTable::unmarshal(Dbt* data) const {
    ValueDict *row = new ValueDict();
    Value value;
    char *bytes = (char*)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
    	ColumnAttribute ca = this->column_attributes[col_num++];
		value.data_type = ca.get_data_type();
    	if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
    		value.n = *(int32_t*)(bytes + offset);
    		offset += sizeof(int32_t);
    	} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
    		u16 size = *(u16*)(bytes + offset);
    		offset += sizeof(u16);
    		char buffer[DbBlock::BLOCK_SZ];
    		memcpy(buffer, bytes+offset, size);
    		buffer[size] = '\0';
    		value.s = string(buffer);  // assume ascii for now
            offset += size;
        } else if (ca.get_data_type() == ColumnAttribute::DataType::BOOLEAN) {
            value.n = *(uint8_t*)(bytes + offset);
            offset += sizeof(uint8_t);    
    	} else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
    	}
		(*row)[column_name] = value;
    }
    return row;
    /*
    u16 textSize, count = 0, offset = 0, size = data->get_size();
    char block[size];
    memcpy(block, data->get_data(), size);
    ValueDict* result = new ValueDict();
    Value value;
    for(ColumnNames::iterator i = column_names.begin(); i != column_names.end(); i++, count++) {
        if(column_attributes[count].get_data_type() == ColumnAttribute::INT) {
            value.data_type = ColumnAttribute::INT;           
            value.n = *(u16*) (block + offset);
            offset += sizeof(value.n);
        } 
        else if(column_attributes[count].get_data_type() == ColumnAttribute::TEXT) {
            textSize = *(u16*)(block + offset);       
            offset += sizeof(textSize);
            value.data_type = ColumnAttribute::TEXT;
            char temp [textSize];
            memcpy(temp, (block + offset), textSize);
            value.s = temp;
            offset += textSize;
        } 
        else {
            throw DbRelationError("Only unmarshals INT and TEXT");
        }
        (*result)[*i] = value;
    }
    return result;
    */
}

// See if the row at the given handle satisfies the given where clause
//Provided by Professor Lundeen
bool HeapTable::selected(Handle handle, const ValueDict* where) {
	if (where == nullptr)
		return true;
	ValueDict* row = this->project(handle, where);
	return *row == *where;
}

// TEST FOR HEAP STORAGE
bool test_heap_storage(){

	ColumnNames column_names;

	column_names.push_back("a");
	column_names.push_back("b");

	ColumnAttributes column_attributes;
	ColumnAttribute ca(ColumnAttribute::INT);

	column_attributes.push_back(ca);
	ca.set_data_type(ColumnAttribute::TEXT);
	column_attributes.push_back(ca);
	
	HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
	
	table1.create();
	cout << "create ok" << endl;
	
	table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
	cout << "drop ok" << endl;
	
	HeapTable table("_test_data_cpp", column_names, column_attributes);
	
	table.create_if_not_exists();
	cout << "create_if_not_exists ok" << endl;
	
	ValueDict row;
	row["a"] = Value(12);
	row["b"] = Value("Hello!");

	table.insert(&row);
	cout << "insert ok" << endl;
	
	Handles* handles = table.select();
	cout << "select ok " << handles->size() << endl;
	
	ValueDict *result = table.project((*handles)[0]);
	cout << "project ok" << endl;
	
	Value value = (*result)["a"];
	
	if (value.n != 12){
		return false;
    }

	value = (*result)["b"];

	if (value.s != "Hello!"){
		return false;
    }

	table.drop();

	return true;
}
