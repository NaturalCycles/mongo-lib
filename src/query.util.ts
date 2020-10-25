import { DBQuery, DBQueryFilterOperator, ObjectWithId } from '@naturalcycles/db-lib'
import { FilterQuery, FindOneOptions, QuerySelector } from 'mongodb'

// Map DBQueryFilterOp to Mongo "Comparison query operator"
const OP_MAP: Partial<Record<DBQueryFilterOperator, keyof QuerySelector<any>>> = {
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
  query: FilterQuery<any>
  options: FindOneOptions<any>
} {
  const options = {} as FindOneOptions<any>

  // filter
  const query = dbQuery._filters.reduce((q, f) => {
    const fname = FNAME_MAP[f.name] || f.name
    q[fname] = {
      ...q[fname], // in case there is a "between" query
      [OP_MAP[f.op] || f.op]: f.val,
    }
    return q
  }, {} as FilterQuery<any>)

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
