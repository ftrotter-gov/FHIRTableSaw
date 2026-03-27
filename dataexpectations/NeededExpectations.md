NDH / Directory Data Expectations

## 1. Total Practitioner Count
This test verifies the total number of practitioner resources present in the dataset. Its purpose is to establish a stable, high-level volume benchmark so that major upstream ingestion failures, deduplication errors, or accidental filtering become immediately visible.

The expectation should compare the current practitioner count to an accepted baseline or allowable range. Large deviations should be treated as a serious warning because they usually indicate a broad data loss, duplication event, or structural break in the practitioner ingestion pipeline.

## 2. Total Organization Count
This test verifies the total number of organization resources in the dataset. Like the practitioner count test, it serves as a basic census-level expectation to ensure that the organization layer of the directory is being loaded consistently over time.

The expectation should compare the observed count against a prior known-good count or an expected range. Significant increases or decreases may suggest source changes, ingestion failures, organization-type misclassification, or problems in deduplication logic.

## 3. Provider Organization Type Coverage
This test verifies the number of organizations carrying the specific organization type of `provider`. The purpose is to ensure that the subset of organizations expected to function as provider organizations is present in a plausible quantity and that organization typing logic is working correctly.

This expectation should specifically filter organizations by the relevant organization type coding and compare the result to an expected range. A sharp drop may indicate broken code mapping or source data omissions, while a sharp rise may indicate overclassification or improper default assignment.

## 4. PractitionerRole Taxonomy Volume
This test verifies the total number of taxonomy codings expressed through practitioner roles. The goal is to measure whether specialty and role coding is being carried through the dataset in a complete and structurally consistent way.

Because taxonomy is often repeated across roles and is central to specialty-based querying, this expectation should monitor both total taxonomy volume and its relationship to the number of practitioner roles. Large shifts may indicate lost codings, parsing failures, or changes in how specialties are represented.

## 5. Overall Gender Distribution
This test measures the gender split across the full practitioner population and checks whether it remains approximately balanced, with an expected rough distribution near 50/50. This is not intended as a strict demographic truth claim, but as a broad anomaly-detection expectation.

The test should allow a tolerance band rather than enforcing an exact split. A dramatic movement away from the historical pattern may indicate missing gender values, source bias, partial ingestion, or a broken mapping of administrative sex or gender fields.

## 6. Surgeon Gender Distribution
This test measures gender distribution specifically among practitioners classified as surgeons. The expectation is that the dataset should show a higher proportion of men than women within this specialty grouping.

This is a directional reasonableness test, not a rigid demographic assertion. It should confirm that the observed distribution continues to follow the known broad pattern for surgeons, while allowing normal variation across sources and time periods.

## 7. Pediatrician Gender Distribution
This test measures gender distribution specifically among practitioners classified as pediatricians. The expectation is that the dataset should show a higher proportion of women than men within this specialty grouping.

As with the surgeon gender test, this is a directional expectation meant to catch obvious distortions. If the pattern suddenly reverses or flattens dramatically, that may signal taxonomy assignment issues, specialty grouping errors, or loss of gender values.

## 8. Psychiatrist Count
This test verifies the number of psychiatrists represented in the dataset. It is intended to provide a specialty-specific benchmark for a clinically important provider group and to catch classification or ingestion problems that may not be visible in the overall physician counts.

The expectation should be based on the relevant NUCC taxonomy or specialty grouping logic and compared against a stable range. Large shifts should prompt review of taxonomy mappings, specialty normalization, and source coverage.

## 9. Physician Type Ranking by NUCC Code
This test counts physicians by NUCC taxonomy code, sorts the results in descending order, and verifies that the top 10 categories approximately match the top 10 hard-coded in the source code. The purpose is to ensure that the specialty distribution produced by the live data remains aligned with the assumptions built into the codebase.

This expectation should compare both membership and approximate ordering, with reasonable tolerance for minor rank movement. It is especially valuable as a guardrail against silent taxonomy drift, broken grouping logic, or stale hard-coded assumptions in downstream code.

## 10. Total Location Count
This test verifies the total number of location resources in the dataset. Because locations often represent the physical address layer of the directory, this count is a useful structural signal for whether organizational and service-site geography is being loaded properly.

The expectation should compare the current total against a known-good baseline or acceptable band. Major changes may indicate missing address records, location deduplication issues, or a break in location extraction from source data.

## 11. Practitioner Language Coverage
This test measures the percentage of doctors in the dataset who have one or more spoken languages recorded. The purpose is to assess the completeness of patient-facing accessibility metadata and to detect whether language information is being consistently carried through the pipeline.

This expectation should focus on the share of practitioners with at least one language entry rather than the raw language count alone. A sharp drop may indicate loss of communication metadata, while an unexpected spike may indicate overpopulation or bad defaulting behavior.

## 12. Organizations Without Organizational Affiliations
This test measures the percentage of organizations that have no organizational affiliations. Its purpose is to quantify how much of the organization graph is disconnected from the affiliation layer and to detect changes in relationship completeness.

The expectation should treat this as a quality and completeness signal rather than assuming that zero affiliations is always wrong. Some organizations may legitimately stand alone, but large changes in this percentage often indicate failures in affiliation ingestion or linkage logic.

## 13. Practitioners Without PractitionerRoles
This test measures the percentage of practitioners that have no practitioner roles. Because practitioner roles are often the main way to connect practitioners to organizations, locations, specialties, and endpoints, this is a critical connectivity expectation.

A high percentage of role-less practitioners may indicate incomplete loading or broken relationship generation. This test should therefore be monitored carefully, especially after changes to role ingestion, identity resolution, or cross-resource linking.

## 14. Maximum Addresses per Organization
This test identifies the maximum number of addresses associated with any single organization. The purpose is to understand the upper bound of address multiplicity in the dataset and to catch pathological cases such as duplication explosions or malformed source expansion.

This expectation is most useful as an outlier detector. Very high maxima should trigger inspection of the specific organization records involved, since they may reflect legitimate complex systems, but they may also reveal runaway duplication or bad address normalization.

## 15. Maximum Phone and Fax Count per Organization
This test identifies the maximum number of phone numbers and fax numbers associated with any single organization. It is intended to measure the upper bound of telecom multiplicity and to flag suspicious organizations with unusually bloated contact metadata.

As with the maximum-address test, this is primarily an outlier expectation. A sudden jump in the maximum may indicate a parsing defect, telecom duplication bug, or a source that changed how repeated contact values are expressed.

## 16. Total Endpoint Count
This test verifies the total number of endpoints in the dataset. Endpoints are strategically important because they represent the electronic connectivity layer of the directory, including FHIR and Direct-style destinations.

The expectation should compare the current count to an accepted baseline or range. Significant changes may indicate endpoint source instability, broken endpoint extraction, or unexpected shifts in linkage between endpoints and the organizations or roles that reference them.

## 17. Average Endpoints by Endpoint Type
This test calculates the average number of endpoints by endpoint type, specifically focusing on FHIR and DirectTrust. The goal is to understand how electronic connectivity is distributed across those two major endpoint classes and whether each type remains within a plausible range over time.

This expectation should report separate averages for FHIR and DirectTrust and compare them to known-good historical values. It is especially useful for detecting one-sided ingestion failures where one endpoint class continues to load correctly while the other silently degrades.
