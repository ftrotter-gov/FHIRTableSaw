Port Inlaw to the FHIRTableSaw Project
=========================

I have temporarily added another project folder to this one that contains the InLaw classes.
InLaw is a thin layer on top of GreatExpectations python data expectation libraries.
It should be entirely seperable from the parent codebase.

I would like to be able to write InLaw tests, (which are in turn based on Great Expectations, which are in turn based on Pandas)..
against both the Postgresql version of the FHIRTableSaw project as well as the DuckDB database files.

I think it also makes sense to also port the DBTable classes.

For context read in the npd_etl folder:

* AI_Instruction/DBTable.md
* AI_Instruction/InLaw_asset.md (InLaw will not be inside a Dagster asset for this project)
* workspace/npd-etl/src/utils/dbtable.py
* workspace/npd-etl/src/utils/inlaw.py

A plan will include:

* Copying these files as appopriate into the FHIRTableSaw folder (and git repo).
* Ensuring that they function against both DuckDB and Postgresql data contexts

What else should be part of this development plan?
