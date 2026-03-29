Make a new evidence report
===================

* Construct the SQL as per the instructions and put them as a new file in evidence/sources/postgres
* Then make a new evidence page in evidence/pages
* Then add the new page to the report index page evidence/pages/index.md

## How to write the SQL files

Got it — just static source SQL files, no DuckDB, no display layer, no query files.

Here is the tight, one-page advanced cheat sheet for sources/<source_name>/*.sql only.

⸻

Evidence.dev — Source SQL Files (Advanced Cheat Sheet)

Core model (what these files actually are)
	•	Each .sql file is a single SELECT statement
	•	Runs directly on your source database (Postgres, Databricks, Snowflake, etc.)
	•	Result is materialized as a table
	•	Table name = <source_name>.<file_name>

sources/claims/top_providers.sql
→ claims.top_providers


⸻

1. You can use full native SQL (this is the main “power”)

These files support everything your database supports, including:
	•	CTEs (WITH)
	•	window functions
	•	temp subqueries
	•	joins across schemas
	•	vendor-specific syntax (Spark SQL, Postgres JSON ops, etc.)
	•	analytic functions
	•	lateral joins (if supported)
	•	recursive queries

Example:

with ranked as (
    select
        npi,
        taxonomy_code,
        count(*) as claim_count,
        dense_rank() over (
            partition by taxonomy_code
            order by count(*) desc
        ) as rnk
    from claims
    group by 1, 2
)
select *
from ranked
where rnk <= 10

👉 There is no Evidence-specific limitation here — this is just your DB.

⸻

2. Deterministic output matters (implicit constraint)

Because results are materialized:
	•	Always define stable column names
	•	Avoid nondeterministic constructs unless intentional

Bad:

select now() as run_time

Good:

select *, current_date as snapshot_date

Reason: these outputs become persistent datasets.

⸻

3. Ordering affects storage efficiency (subtle but real)

Docs note that ordering improves compression/performance.

order by state, city, provider_name

This matters because output is stored (typically as Parquet under the hood).

⸻

4. Pre-aggregation is a first-class pattern

Unlike query-layer SQL, this is where you should collapse data early.

Example:

select
    service_year,
    taxonomy_code,
    count(*) as claim_count,
    sum(allowed_amount) as total_allowed
from claims
group by 1, 2

Why:
	•	reduces storage
	•	speeds downstream queries
	•	avoids repeated heavy scans

⸻

5. Column shaping is permanent (treat like schema design)

This is effectively your data modeling layer.

Do it once here:

select
    npi,
    upper(trim(provider_name)) as provider_name,
    taxonomy_code,
    coalesce(primary_specialty, 'UNKNOWN') as specialty,
    city,
    state
from raw.providers

Key idea:
	•	rename ugly fields here
	•	normalize formats here
	•	downstream SQL should not fix this again

⸻

6. File-level isolation (no cross-file references)

Critical limitation:
	•	Source SQL files cannot reference each other

❌ Not allowed:

select * from ${other_source_query}

Each file is:
	•	independent
	•	standalone
	•	executed directly against DB

If you need reuse → use views in the database, not Evidence.

⸻

7. Multi-stage logic must be inside one file

Since files don’t chain, you must embed complexity using:
	•	CTE pipelines
	•	nested subqueries

Pattern:

with base as (
    select * from claims where allowed_amount > 0
),
grouped as (
    select
        npi,
        sum(allowed_amount) as total
    from base
    group by 1
)
select *
from grouped
where total > 100000


⸻

8. You can join anything your DB can access

Including:
	•	multiple schemas
	•	cross-database links (if DB supports)
	•	external tables (S3, etc.)
	•	views and materialized views

Example:

select
    p.npi,
    p.provider_name,
    c.claim_count
from providers p
join claims_summary c
    on p.npi = c.npi


⸻

9. No parameterization inside source SQL

Unlike query files:

❌ Not supported:

where state = '${params.state}'

These files are:
	•	static
	•	deterministic
	•	environment-independent

If you need dynamic filtering → not done here.

⸻

10. One query = one output table (no multiple statements)

You cannot:
	•	define multiple outputs
	•	run DDL
	•	create temp tables outside the query

❌ Not allowed:

create temp table x as ...
select * from x

Instead:

✔ use CTEs:

with x as (...)
select * from x


⸻

11. Avoid non-portable side effects

Because these are extraction queries:
	•	no INSERT, UPDATE, DELETE
	•	no transaction control
	•	no session state assumptions

Think:

👉 “pure SELECT → dataset”

⸻

12. Performance strategy (what actually matters)

Best-performing source SQL files:
	•	filter early
	•	project minimal columns
	•	aggregate early
	•	avoid wide joins unless necessary
	•	sort output (for storage efficiency)

⸻

13. Practical mental model

Treat each file as:

A materialized, versionable, production-grade table definition written in raw SQL

Not:
	•	an ad hoc query
	•	not a view layer
	•	not a dashboard query

⸻

Minimal “gold standard” template

with base as (
    select
        npi,
        taxonomy_code,
        allowed_amount,
        service_date
    from raw.claims
    where allowed_amount > 0
),
aggregated as (
    select
        date_trunc('year', service_date) as service_year,
        taxonomy_code,
        count(*) as claim_count,
        sum(allowed_amount) as total_allowed
    from base
    group by 1, 2
)
select *
from aggregated
order by service_year, taxonomy_code

## How to make reports

The user will tell you which charts to make, but you should almost always have a Big Value chart for results will less than 3 rows.
Data Table for all tables, and then a specific additional chart if the user asks for it.

Perfect — here’s the barebones pattern library you actually want.

No fluff. Just copy/paste patterns.

⸻

1. Data Table (from a table)

```sql my_data
select *
from claims.top_providers
limit 100

<DataTable data={my_data}/>
```


	•	Shows full table by default  ￼

Optional (clean columns):

<DataTable data={my_data}>
  <Column id=npi/>
  <Column id=provider_name/>
  <Column id=claim_count fmt=num0/>
</DataTable>


⸻

2. Big Value (summary number)

```sql summary
select
    sum(claim_count) as total_claims
from claims.top_providers

<BigValue data={summary} column=total_claims/>
```


or inline:

Total claims: <Value data={summary} column=total_claims/>

	•	<Value> pulls a single column value from query  ￼

⸻

3. Line Chart (single series)

```sql monthly
select
    service_month,
    claim_count
from claims.monthly_claims
order by service_month

---

# 4. Line Chart (multiple series)

```md
```sql monthly_by_type
select
    service_month,
    taxonomy_code,
    claim_count
from claims.monthly_taxonomy

* `series=` splits into multiple lines  [oai_citation:2‡Evidence Docs](https://docs.evidence.dev/components/charts/line-chart?utm_source=chatgpt.com)

---

# 5. Vertical Bar Chart

```md
```sql top_states
select
    state,
    provider_count
from claims.provider_counts_by_state
order by provider_count desc
limit 10

* Same pattern as line chart — just swap component  [oai_citation:3‡Evidence Docs](https://docs.evidence.dev/core-concepts/components?utm_source=chatgpt.com)

---

# 6. Scatter Plot

```md
```sql scatter_data
select
    provider_count,
    total_allowed
from claims.state_summary

---

# 7. Core pattern (this is the only thing to remember)

Every component follows this shape:

```md
<Component
    data={query_name}
    x=column
    y=column
    series=optional
/>

Rules:
	•	data = query name
	•	x = first axis
	•	y = metric(s)
	•	series = grouping (optional)

Evidence auto-fills defaults if you omit some fields  ￼

⸻

8. Where to find everything else

Use this page (your link):

👉 All Evidence Components￼

It includes:
	•	area charts
	•	stacked bars
	•	bubble charts
	•	sankey
	•	heatmaps
	•	funnel charts

⸻

Mental model (keep this)
	•	SQL builds the dataset
	•	Component = just a thin visualization layer
	•	Every chart = data + x + y (+ series)

⸻

If you want next step, I can give you a “which chart to use when” cheat sheet tuned for healthcare / claims data.
