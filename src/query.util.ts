import { DBQuery, DBQueryFilterOperator, ObjectWithId } from '@naturalcycles/db-lib'
import { Filter, FilterOperators, FindOptions } from 'mongodb'

// Map DBQueryFilterOp to Mongo "Comparison query operator"
const OP_MAP: Partial<Record<DBQueryFilterOperator, keyof FilterOperators<any>>> = {
  '==': '$eq',
  '<': '$lt',
  '<=': '$lte',
  '>': '$gt',
  '>=': '$gte',
  in: '$in',
  'not-in': '$nin',
  // todo: array-contains, array-contains-any are not supported currently
}

const FNAME_MAP: Record<string, string> = {
  id: '_id',
}

export function dbQueryToMongoQuery<ROW extends ObjectWithId>(
  dbQuery: DBQuery<ROW>,
): {
  query: Filter<ROW>
  options: FindOptions<ROW>
} {
  const options = {} as FindOptions<ROW>

  // filter
  // eslint-disable-next-line unicorn/no-array-reduce
  const query = dbQuery._filters.reduce((q, f) => {
    const fname = FNAME_MAP[f.name as string] || (f.name as string)
    q[fname] = {
      ...q[fname], // in case there is a "between" query
      [OP_MAP[f.op] || f.op]: f.val,
    }
    return q
  }, {} as Filter<any>)

  // order
  // eslint-disable-next-line unicorn/no-array-reduce, unicorn/prefer-object-from-entries
  options.sort = dbQuery._orders.reduce((map, ord) => {
    map[FNAME_MAP[ord.name as string] || ord.name] = ord.descending ? -1 : 1
    return map
  }, {})

  // limit
  options.limit = dbQuery._limitValue || undefined

  // selectedFields
  if (dbQuery._selectedFieldNames) {
    // eslint-disable-next-line unicorn/no-array-reduce
    options.projection = dbQuery._selectedFieldNames.reduce(
      (map, field) => {
        map[FNAME_MAP[field as string] || field] = 1
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
