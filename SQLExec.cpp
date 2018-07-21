/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
#include "ParseTreeToString.h"
#include "schema_tables.h"

using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

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

//destructor
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


//Executes Query
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    if(tables != nullptr){
        SQLExec::tables = new Tables();
    }

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

//get the type of column being described, and store data identifier and attribute back to passed address
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    //set identifier as statement col's name
    column_name = col->name;
    //get col's type. set column attribute to match statement col
    switch(col->type){
        case ColumnAttribute::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnAttribute::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            out << "???";
    }
    throw SQLExecError("not implemented");  // FIXME
}

//Creates a table based on input statement
// REF(https://github.com/hyrise/sql-parser/blob/master/src/sql/CreateStatement.h)
QueryResult *SQLExec::create(const CreateStatement *statement) {

    //Variables
    Identifier tableID;
    Identifier column_name;
    ColumnAttribute column_attribute;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    ValueDict row;
    std::string message;
    //get statement info
    tableID= statement->tableName;
    for(ColumnDefinition *col: *statement->columns){
        column_definition(col,column_name, column_attribute); //append statment to var
        column_names.push_back(column_name); //append to list of column names
        column_attributes.push_back(column_attribute);
    }
    //create a row, get handles
    row["table_name"]= tableID;
    Handle table_handle = SQLExec::tables->insert(&row);

    //create column handle an
    try {
        Handles column_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {

            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                column_handles.push_back(columns.insert(&row));
            }

            // Create Relation
            DbRelation& table = SQLExec::tables->get_table(tableID);
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
    return new QueryResult("Created " + tableID);
}



//drops table on schema
//Will delete all columns for table then delete table
QueryResult *SQLExec::drop(const DropStatement *statement) {
    if (statement->type != DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");

    Identifier tableID = statement->name;
    if (tableID == Tables::TABLE_NAME || tableID == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(tableID);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(tableID);

    // remove from _columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* handles = columns.select(&where);
    for (auto const& handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    return new QueryResult(string("dropped ") + tableID);

}

//Query Results based on statement specifications
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

//Statement Query for tables will show tables of a schema
QueryResult *SQLExec::show_tables() {

    //Gets columns for a specific table
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 2;

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME)
            rows->push_back(row);
    }
    //free memory
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

//returns columns of a specified table
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    //gets tables
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    //appends the name, column name, and unit type
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");


    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    //handle will get the specific table being reference in the statement
    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    //free memory
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}