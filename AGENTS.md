# AI Agent Instructions for FHIRTableSaw

This document contains rules and guidelines for AI agents working on the FHIRTableSaw project.


## Rules

* Always use the SQL-on-FHIR ViewDefinition mechanism in order to map data from the FHIR spec to the tabular representations. This will not work in the other direction, but from FHIR->Postgresql this is the only implementation path that should be used.
* Never write passwords into sourcecode that is not excluded by .gitignore
* When you create initial password configuration files they should ALWAYS be added to gitignore.
* In fact.. you should NEVER NEVER NEVER be writing passwords at all. Please let me do that part.
* Always use snake_case without spaces, for column names on the relational model tables.
