/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
#include "ParseTreeToString.h"
#include "schema_tables.h"
#include <algorithm>
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr; // add this for index implementation

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
						out << (value.n == 0 ? "false" : "true");
						break;    
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}
//DESTRUCTOR
//destroy  a current query result if null
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }

}
//EXECUTE
//Executes Query statement
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    //initialize table and indices
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
    }
    if (SQLExec::indices == nullptr)
        SQLExec::indices = new Indices(); // init SQLExec::indices
    //Check for statement type and pass
    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

QueryResult *SQLExec::insert(const InsertStatement *statement) {
    Identifier tableID = statement->tableName;
    DbRelation& table = SQLExec::tables->get_table(tableID);
    ColumnNames trueNames;
    ColumnAttributes column_attributes;
    SQLExec::tables->get_columns(tableID, trueNames, column_attributes);
    ColumnNames stmtColumns;
    ValueDict row;
    if (statement->columns !=NULL) {
        for (uint i = 0; i < statement->columns->size(); i++) {
            Identifier column = statement->columns->at(i);
            if (find(trueNames.begin(), trueNames.end(), column) != trueNames.end()) {                 
                Expr* expr = statement->values->at(i);
                switch  (expr->type) {
                    case kExprLiteralString:
                        row[column] = Value(string(expr->name));
                        break;
                    case kExprLiteralFloat:
                        row[column] = Value(float(expr->fval));
                        break;
                    case kExprLiteralInt:
                        row[column] = Value(int(expr->ival));
                        break;
                    default:
                        throw SQLExecError("Unrecognized column type");
                        break;
                }
            } else throw SQLExecError("Unrecognized column name");
    
        }
    }
    
    //create index on new values
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = SQLExec::indices->select(&where);
    u_long nIndex = handles->size();

    //insert into table
    Handle handle_t = table.insert(&row);
    table.close();
    string returnStatment = string("successfully inserted 1 row into ") + tableID;
    if (nIndex == 1)
        returnStatment += string(" and ") + to_string(nIndex) + string(" index");
    if (nIndex > 1)
        returnStatment += string(" and ") + to_string(nIndex) + string(" indices");
    return new QueryResult(returnStatment);
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {
    return new QueryResult("DELETE statement not yet implemented");  // FIXME
}

QueryResult *SQLExec::select(const SelectStatement *statement) {
    return new QueryResult("SELECT statement not yet implemented");  // FIXME
}

//COLUMN DEFINITION
//get the type of column being described, and store data identifier and attribute back to passed address
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    //set identifier as statement col's name
    column_name = col->name;
    //get col's type. set column attribute to match statement col
    switch(col->type){
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
	default:
            throw SQLExecError("unrecognized data type (column_definition)");
    }
}

//CREATE
//Creates a table or index based on input statement
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

//CREATE TABLE
//Will create a table with defined columns of names and attributes
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    //Identifiers and column variables
    Identifier tableID;
    Identifier column_name;
    ColumnAttribute column_attribute;
    ColumnNames column_names;
    ColumnAttributes column_attributes;   
    ValueDict row;

    //get statement info
    tableID= statement->tableName;

    //get the attribute of variable for each columns
    for(ColumnDefinition *col: *statement->columns){
        column_definition(col,column_name, column_attribute); //append statment to var
        column_names.push_back(column_name); //append to list of column names
        column_attributes.push_back(column_attribute);
    }
    //create a row, get handles
    row["table_name"]= tableID;
    Handle table_handle = SQLExec::tables->insert(&row);

    //create column handle
    try {
        Handles column_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            //apply values to match row definitions
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                column_handles.push_back(columns.insert(&row));
            }

            // Create Relation
            DbRelation& table = SQLExec::tables->get_table(tableID);
            //check if table exists already
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception& e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: column_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception& e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(table_handle);
        } catch (...) {}
        throw;
    }
    //return query
    return new QueryResult("created " + tableID);
}

//CREATE INDEX
//Creates an index for a table for specific column variables
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    //Get statement identifiers
    Identifier table_name = statement->tableName;
    Identifier index_name = statement->indexName;

    //get all column names of index statement
    ColumnNames column_names;
    for (auto const& col : *statement->indexColumns)
        column_names.push_back(col); // store all the index columns

    //define all index columns and types
    ValueDict row, where; // row for create index, where for check if the column(s) or table actually exist
    Handles c_handles;
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);      // get _columns from _tables
    try {
        for (uint i = 0; i < column_names.size(); i++) {
            //apply identifier
            row["table_name"] = table_name;
            row["index_name"] = index_name;
            row["column_name"] = column_names[i];
            row["seq_in_index"] = Value(i+1); // start from 1 not 0
            row["index_type"] = Value(string(statement->indexType));
            row["is_unique"] = Value(string(statement->indexType) == "BTREE");
            c_handles.push_back(SQLExec::indices->insert(&row));    // insert into _indices
            // check if the column(s) specified to the index actually exist in the underlying table.
            where["table_name"] = table_name;                       // predicate2 (table name)
            where["column_name"] = column_names[i];                 // predicate1 (column name)
            if (columns.select(&where)->empty())                    // if there is no match, this should be a wrong command
                throw SQLExecError("Error: there is no " + column_names[i] + " column in " + table_name + " table");
        }

        //create index
        DbIndex& indice = SQLExec::indices->get_index(table_name,index_name); // currently has problem here for indice.create
        indice.create();
        //actually create the index
    // if something wrong, delete all the handles
    } catch (exception& e) {
        for (auto const &handle: c_handles) {
            SQLExec::indices->del(handle);
        }
        throw;
    }

    //return query
    return new QueryResult("create index " + index_name);
}

//DROP SWITCH
//will DROP a table or index depending on statement specification
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}
//DROP TABLE
//Will drop a table and all indices on that table
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    //check if correct statement
    if (statement->type != DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");
    Identifier tableID = statement->name;
    if (tableID == Tables::TABLE_NAME || tableID == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(tableID);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(tableID);
    //get indice table and handle
    DbRelation& indicesTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    Handles* handles_indices = indicesTable.select(&where);
    //get names for all indices
    IndexNames indexIDs = SQLExec::indices->get_index_names(tableID); //check professor!!!!!!

    //delete from relation indices
    for (auto const& handle: *handles_indices)
        indicesTable.del(handle);

    //get every index in index name, and drop them
    for (auto const& index_id: indexIDs) {
        DbIndex& index = SQLExec::indices->get_index(tableID, index_id);
        index.drop();
    }

    // remove from _columns schema,
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* handles = columns.select(&where);
    //delete from relation
    for (auto const& handle: *handles)
        columns.del(handle);

    //delete handles
    delete handles_indices;
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    //return query
    return new QueryResult(string("dropped ") + tableID);

}
//DROP INDEX
//will drop a specific index on given table
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    //check if valid statement
    if (statement->type != DropStatement::kIndex)
        throw SQLExecError("unrecognized DROP type");

    //Index identifiers
    Identifier tableID = statement->name;
    Identifier indexID = statement->indexName;
    ValueDict where;
    where["table_name"] = Value(tableID);
    where["index_name"] = Value(indexID);

    //get indice table and handles
    DbRelation& indicesTable = SQLExec::tables->get_table(Indices::TABLE_NAME);
    Handles* handles_indices = indicesTable.select(&where);

    //get specific index
    DbIndex& index = SQLExec::indices->get_index(tableID, indexID);

    //delete from relation
    for (auto const& handle: *handles_indices)
        indicesTable.del(handle);

    //drop index
    index.drop();

    //delete handles
    delete handles_indices;

    //return query
    return new QueryResult("drop index " + indexID);

}

//Query Results based on statement specifications
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}
//SHOW INDEX
//will return indices from a specified table
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    // get _indices from _tables
    DbRelation& indicesTable = SQLExec::tables->get_table(Indices::TABLE_NAME);

    //appends all the columns
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    //appends all the attributes
    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::INT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::BOOLEAN));

    // use select to get all the handles within the specific table
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = indicesTable.select(&where);
    u_long n = handles->size();

    // use project to get the specific columns
    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = indicesTable.project(handle, column_names);
        rows->push_back(row);
    }
    //delete handles
    delete handles;

    //return query
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

//SHOW TABLES
//Statement Query for tables will show tables of a schema
//shows all tables in _tables
QueryResult *SQLExec::show_tables() {

    //Gets columns for a specific table
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    //create handle
    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
    }

    //delete handles
    delete handles;

    //return query
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

//SHOW COLUMNS
//returns columns of a specified table
//will display all the columns containing name and type stored in _columns for a table
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    //gets tables
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    //appends the name, column name, and unit type
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    //get attributes
    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));


    //handle will get the specific table being reference in the statement
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = columns.select(&where);
    u_long n = handles->size();

    //project columns to row
    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    //free memory

    delete handles;

    //return query
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}
