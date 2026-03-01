## About

The Aviato DSL is a custom query language inspired by Elasticsearch based on JSON that allows the usage of complex filters easily.

## DSL Structure

All the following attributes are placed at the top level of the DSL. All fields are required unless otherwise stated
|Attribute| Type | Description |
|--|--|--|
| `offset` | `integer` | A required integer with a max value of 10,000 defining how many items the database should skip before it returns results. |
| `limit` | `integer` | A required integer with a max value of 250 defining how many items the database should return per “page.”|
| `sort` | `Array<object>` | An array of objects defining the order of the returned results. The database will attempt to use the first defined sort, and break ties with the following sorts. Each object in the sort array should have a singular key, which defines the column that the sort should be performed on. This can be empty. In text searches, use the `score` sort to return most relevant results |
| `nameQuery`| Optional `string`| An optional string that will search the given table using the value inside of `nameQuery` using full text search on the `name` or `fullName` columns. If the `score` sort is used alongside this field, results will be sorted by textual relevancy, and ties will be broken by ai relevancy.|
| `filters` | DSL Filters | An optional array of objects that can be used to perform complex cross-table filters.

## Filter Object

Only one of the following fields can be used per filter object
| Attribute Name | Type | Description |
| -- | -- | -- |
| AND | `Array<FilterObject>` | [Optional] Used to perform “AND” queries. All filter objects inside this must evaluate to true in order for the parent filter object to evaluate to true. |
| OR | `Array<FilterObject>` | [Optional] Used to perform “OR” queries. One or more filter objects inside this must evaluate to true in order for the parent filter object to evaluate to true |
| `{columnPath}` | `EvaluationObject` | [Optional] Defines the evaluation object used for comparisons. The attribute name is a “path” to the column that can cross tables using dot notation.|

### Column Paths

This can reference three things: **scalar fields**, **relational fields**, and **function calls**

**Scalar Fields:** These are simple scalar fields that exist on the table being queried specified in the “collections” attribute. They must exist as a scalar field in the Prisma schema

**Relational Fields**: These are relational fields, with no cap on the amount of relations. Each relation is separated by dot notation. The last specified column must be a scalar column. You do NOT navigate by specifying the model name, but instead by the “virtual column name.” Ex: If you’re querying the Person table, and want to filter on the WorkExperienceLink table, the constructed column path would be `workExperienceList.startDate` and NOT `WorkExperienceList.startDate`. For example, to filter for companies with a specific founder, use `companyFoundingLink.person.id`

**Function Calls**: You can call custom pre-approved functions with inputs by simply calling the function like normal. Ex you would pass `vesting_to_total_vested()` . To pass custom arguments in, simply pass them in as how you would speciffy scalar/relational fields: `vesting_to_total_vested(experienceList.startDate, experienceList.company.vestingScheduleList[1]['schedule'])`

Note that you can only use non-aggregate functions in filters. However, you can use aggregate functions in sort.
Currently approved functions include:

---

```ts
get_titles(positionList: jsonb[], filter: 'current' | 'past' | 'all'): string[]
```

Accept an experience positionList array, and returns all titles of all positions as an array of strings.
This function is deprecated and should not be used for new queries.
For current-role matching, use both:
- `experienceList.positionList['title']` for title matching
- `experienceList.positionList['endDate']` with `{"operation":"eq","value":null}`
as separate filters. The DSL parser groups them into the same nested query when they share the `experienceList` path prefix.

Behavior notes:
- `experienceList.positionList['title']` alone matches the title in any position (past or present), because there is no current-role constraint.
- `get_titles(experienceList.positionList, 'current')` applies an `endDate = null` check on the same position entry, so it only matches current roles. This function is being deprecated; avoid using it for new queries.
- You can use `experienceList.positionList['title']` and `experienceList.positionList['endDate'] = null` as separate filters. They are grouped into the same nested query because they share the `experienceList` path prefix.

Result example:
- `experienceList.positionList['title']` alone -> 3900 results
- `get_titles(experienceList.positionList, 'current')` -> 1400 results
- The extra 2500 people had that title in a past job.
- For current titles, use `experienceList.positionList['title']` + `experienceList.positionList['endDate'] = null`, or use the `currentTitles` query param on simple search.

Update:
- A fix has been pushed: all filters in the same nested object/group are now applied to the same experience object.

---

```ts
count(field: any): number
```

Similar to sql 
(). Useful for counting relations or arrays.
This is an aggregate function

---

```ts
vesting_to_total_vested(startdate: timestamp, vesting_schedule: jsonb): number
```

Accepts a vesting schedule and start date, and returns the total shares vested.

---

```ts
max_timestamp_from_json(obj: jsonb): Timestamp
```

Accepts a historical data object, and returns the latest timestamp available

---

```ts
get_lexicographically_greatest_element(arr: string[]): string
```

Accepts an array of strings, and returns the lexographically greatest element

---

**JSON Queries**: You might find yourself wanting to query a JSON column’s subattributes. You can’t use dots (otherwise the DSL parser will think you’re trying to make a relational query). Instead, use square brackets. This can be used in tandem with the dots for scalar fields. Ex: `experienceList.company.vestingScheduleList[1]['schedule']`. Single quotes are used for JSON sub attribute names, numbers without quotes are used for specifying a specific index in a JSON array.

### Evaluation Object

| Attribute Name | Type                                                       | Description                                                                                                                                                                                                                                                                                                             |
| -------------- | ---------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `operation`    | `enum`                                                     | The equality operator. Supported values: `eq` `lt` `lte` `gt` `gte` `noteq` `fts` `in` `notin` `geowithin` `textcontains`                                                                                                                                                                                               |
| `value`        | `string` or `string[]` or `number` or `number[]` or `Date` | The value to compare the column against using the equality operator                                                                                                                                                                                                                                                     |
| `quantifier`   | Optional: `some` or `none`                                 | If not supplied, defaults to `some` for maximum performance. Be careful when using `none`, as this will hurt performance. This attribute describes the number of rows that need to match the operation when we encounter a one-to-many relationship. If specifying a function, only the `some` quantifier is supported. |

- The `eq` `noteq` `lt` `lte` `gt` and `gte` fields support `strings` ,`numbers`, and `Dates`
- The `fts` field supports `strings` and does full text filtering (not ranking)
- The `in` `notin` fields support `arrays` of `numbers` or `strings`
- The `geowithin` field supports `arrays` of `string` and does a filter on the specified bounding box. This only supports certain coordinate type fields: `["(latitude, longitude)", "(latitude, longitude)"]` Where the first set of coordinates is the upper left coordinate of the bounding box, and the second set of coordinates is the lower right coordinate.
  - Ex `["(38.532667, -123.237130)", "(37.019316, -120.397669)"]`
- The `textcontains` field supports only strings.

## Search Clarification

- Free person search is intended for quick identification/verification before returning results.
- To retrieve `headline` and other enriched fields, use simple search with `enrich=true` (costs 1 credit per result) or call `/person/enrich` separately.

## Additional API Clarifications

- Company name filters in simple search are broader text matches. Company ID filters are exact identifier matches and are more precise.
- It is expected that ID-based company searches can return fewer results than name-based searches, because not every record has every external identifier populated.
- Person ID formats may vary; treat IDs as opaque and resolve entities first before use.

### Query Payload Format

- For DSL endpoints, send `dsl` as a JSON object, not as a stringified JSON blob.
- Example (valid):

```json
{
  "dsl": {
    "limit": 1,
    "offset": 0,
    "filters": [
      {
        "id": {
          "operation": "eq",
          "value": "YUoTk8RTS_Mm2qt2I-FjzdKyu5PgRFw"
        }
      }
    ]
  }
}
```

### Lookup and Enrichment Notes

- Email-based person lookup is not supported via person search. Use `/person/enrich` with email for that workflow.
- Company lookup by website should use company enrichment as the preferred path.
- Person enrichment supports a preview-style flag.

### Operator Semantics

- `fts` is best for keyword search.
- `textcontains` performs substring matching and is better for full phrase matching.
- `fts` can match any of the keywords provided, so broad queries may return many results.
- `eq` and `in` perform strict equality matching.

### Seniority Score

- `1`: Entry Level / Junior
- `2`: Mid Level / Professional
- `3`: Senior Level / Team Lead
- `4`: Management / Principal
- `5`: Director
- `6`: C-Suite
- `7`: Board Member

### Error Responses

- Search error responses were updated to be more descriptive and to better pinpoint query issues.

## Sort Object Example

```typescript
{
	"myColumn": {
		"order": "desc"
	}
}
```

The sort attribute also supports all column paths that the filters attribute supports. This includes sorting by relations, sorting by functions, including aggregate functions like COUNT()

```typescript
{
	"COUNT(experienceList.id)": {
		order: "desc"
	},
},
```
