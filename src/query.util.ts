import { DBQuery, DBQueryFilterOperator } from '@naturalcycles/db-lib'
import { FilterQuery, FindOneOptions } from 'mongodb'

// Map DBQueryFilterOp to Mongo "Comparison query operator"
const OP_MAP: Record<DBQueryFilterOperator, string> = {
  '=': '$eq',
  '<': '$lt',
  '<=': '$lte',
  '>': '$gt',
  '>=': '$gte',
  in: '$in',
}

const FNAME_MAP: Record<string, string> = {
  id: '_id',
}

export function dbQueryToMongoQuery (
  dbQuery: DBQuery,
): {
  query: FilterQuery<any>
  options: FindOneOptions
} {
  const options = {} as FindOneOptions

  // filter
  const query = dbQuery._filters.reduce(
    (q, f) => {
      const fname = FNAME_MAP[f.name] || f.name
      q[fname] = {
        ...q[fname], // in case there is a "between" query
        [OP_MAP[f.op] || f.op]: f.val,
      }
      return q
    },
    {} as FilterQuery<any>,
  )

  // order
  options.sort = dbQuery._orders.reduce((map, ord) => {
    map[FNAME_MAP[ord.name] || ord.name] = ord.descending ? -1 : 1
    return map
  }, {})

  // limit
  options.limit = dbQuery._limitValue || undefined

  // selectedFields
  if (dbQuery._selectedFieldNames) {
    options.projection = dbQuery._selectedFieldNames.reduce(
      (map, field) => {
        map[FNAME_MAP[field] || field] = 1
        return map
      },
      { _id: 1 },
    )
  }

  return {
    query,
    options,
  }
}
