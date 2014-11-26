/* Copyright 2011 ZAO "Begun".
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 * This library is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __SLAVE_TABLE_H_
#define __SLAVE_TABLE_H_


#include <string>
#include <vector>
#include <map>

#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

#include "field.h"
#include "recordset.h"
#include "SlaveStats.h"


namespace slave
{

typedef boost::shared_ptr<Field> PtrField;
typedef boost::function<void (RecordSet&)> callback;

enum EventKind
{
    eNone   = 0,
    eInsert = (1 << 0),
    eUpdate = (1 << 1),
    eDelete = (1 << 2),
    eAll    = (1 << 0) | (1 << 1) | (1 << 2)
};
typedef EventKind filter;
typedef EventKind EventKindList[3];

inline const EventKindList& eventKindList()
{
    static EventKindList result = {eInsert, eUpdate, eDelete};
    return result;
}

inline bool should_process(EventKind filter, EventKind kind) { return (filter & kind) == kind; }

class Table {

public:

    std::vector<PtrField> fields;

    callback m_callback;
    EventKind m_filter;

    void call_callback(slave::RecordSet& _rs, ExtStateIface &ext_state) {

        // Some stats
        ext_state.incTableCount(full_name);
        ext_state.setLastFilteredUpdateTime();

        m_callback(_rs);
    }


    const std::string table_name;
    const std::string database_name;

    std::string full_name;

    Table(const std::string& db_name, const std::string& tbl_name) :
        table_name(tbl_name), database_name(db_name),
        full_name(database_name + "." + table_name)
        {}

    Table() {}

};

}

#endif
