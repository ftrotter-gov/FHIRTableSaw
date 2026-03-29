# Additional Data Expectation Tests for Healthcare Directory Interoperability

This document proposes additional data validation tests focused on interoperability and trust indicators. These tests help healthcare system participants assess whether directory data is reliable, complete, and trustworthy enough for safe clinical and operational integration.

## Address Completeness Validation

This test validates that organizations and locations have complete, usable physical addresses with all critical components present (street address, city, state, and postal code). The purpose is to ensure that directory participants can physically locate healthcare entities when needed for patient referrals, emergency situations, or business verification. Incomplete addresses represent a significant trust barrier because they suggest either poor data maintenance or entities that may not want to be easily found.

Beyond simple presence checks, this test should also validate address component coherence - for example, that ZIP codes match the stated city and state, and that state abbreviations are valid. Organizations or locations with incomplete or incoherent addresses should be flagged as potentially unreliable network participants. A sudden increase in incomplete addresses may indicate upstream data quality degradation or problems with address parsing and normalization logic in the ingestion pipeline.

## Contact Information Completeness

This test measures the percentage of organizations that have at least one viable contact method (phone number, fax, or email) populated in their telecom fields. Being able to contact a healthcare organization is fundamental to interoperability - without contact information, referrals cannot be coordinated, questions cannot be answered, and trust cannot be established through direct communication. Organizations lacking any contact information should be treated as red flags within a healthcare directory.

The test should distinguish between different contact types and flag organizations that have only fax numbers (increasingly obsolete) or that rely solely on a single contact method. Multiple contact channels indicate organizational maturity and reliability. This test also serves as a proxy for data freshness - if organizations have no contact information or only outdated contact types, it suggests the directory data may be stale and the organization may no longer be actively participating in the network.

## Active Status Distribution

This test monitors the ratio of active to inactive resources across all major resource types (Organization, Practitioner, Location, PractitionerRole). The expectation is that the vast majority of resources in an operational directory should have an active status, with inactive resources representing a small, stable percentage. Dramatic shifts in this ratio - such as a sudden spike in inactive resources or a complete absence of status tracking - indicate either data quality problems or significant real-world changes that require investigation.

This test is critical for trust because inactive resources should not be presented to directory consumers as viable options for patient care. If the directory fails to properly mark inactive entities, it can lead to failed referrals, patient safety issues, and loss of confidence in the directory as a whole. The test should also examine whether inactive resources are properly retained (for historical tracking) versus deleted entirely, as different use cases require different retention policies.

## Endpoint URL Format Validity

This test validates that all endpoint URLs are well-formed, use expected protocols (primarily https for FHIR endpoints), and follow proper syntax for URIs. Electronic connectivity is the foundation of modern healthcare interoperability, and malformed endpoint URLs represent complete blockers to automated data exchange. Even a small percentage of invalid URLs undermines trust in the directory's technical accuracy and suggests insufficient validation at data entry or ingestion time.

The test should specifically check for common problems such as http instead of https (security concern), missing or malformed paths, invalid domain names, and URLs that contain placeholder text rather than actual endpoints. For FHIR endpoints specifically, the test could verify that URLs end with expected patterns (like /metadata or base resource paths). A high rate of malformed URLs indicates either poor source data quality or broken URL normalization logic in the data processing pipeline.

## Geographic Distribution Reasonableness

This test analyzes the geographic distribution of organizations, practitioners, and locations across states and regions, comparing the observed distribution to expected patterns based on population density and known healthcare market characteristics. The purpose is to detect anomalies such as excessive clustering in unexpected locations, complete absence from major population centers, or patterns that suggest data bias or incomplete source coverage. Geographic outliers can indicate either data quality issues or, potentially, fraudulent or suspicious entities.

This test serves as an important trust signal because healthcare directories should reflect the actual geographic distribution of healthcare resources. If a directory shows 80% of pediatricians concentrated in a single small state, or completely lacks providers in major metropolitan areas, it raises serious questions about data completeness and reliability. The test should allow for regional variation (some areas genuinely have more providers) while flagging statistically impossible or highly suspicious distributions that warrant human review.

## Organization Hierarchy Integrity

This test examines the partOf relationships between organizations to ensure that organizational hierarchies are reasonable in depth and structure. It should detect circular references (Organization A is part of B, which is part of A), excessively deep hierarchies (10+ levels of organizational nesting), and orphaned organization fragments that reference non-existent parent organizations. Proper hierarchy representation is essential for understanding corporate structures, ownership relationships, and lines of accountability in healthcare networks.

From a trust perspective, broken or suspicious hierarchies indicate either technical data quality problems or, potentially, attempts to obscure real ownership and control relationships. Healthcare directories are often used for fraud detection and compliance monitoring, so accurate representation of organizational relationships is critical. The test should establish reasonable thresholds (perhaps 5-6 levels maximum) and flag any organization whose partOf chain violates these constraints or contains logical inconsistencies.

## Practitioner Credential Coverage

This test measures the percentage of practitioners who have at least one qualification or credential explicitly listed in their FHIR Practitioner.qualification field. Credentials are fundamental to trust in healthcare - they demonstrate training, certification, and legal authority to practice. A directory where most practitioners lack credential information is of limited value for verification purposes and may not be suitable for use cases that require provider validation.

The test should examine both the presence and the richness of qualification data. Higher-trust directories will not only have qualifications listed but will include issuer information, dates of issuance, and credential identifiers that enable third-party verification. A sudden drop in credential coverage may indicate a data processing change that accidentally stripped qualification information, while consistently low coverage suggests that the directory source data lacks this critical trust dimension.

## Identifier Diversity and Coverage

This test analyzes the diversity and completeness of identifiers across practitioners and organizations, checking for appropriate identifier types such as NPI (National Provider Identifier), state license numbers, DEA numbers, Tax IDs, and other jurisdiction-specific identifiers. Resources with multiple, cross-verifiable identifiers are inherently more trustworthy because they can be validated against multiple authoritative sources. Entities with no identifiers or only a single identifier type are harder to verify and may represent incomplete or suspect data.

This test should measure both identifier presence and identifier type distribution. For example, in the US context, virtually all legitimate practitioners should have an NPI, and many should also have state licenses. Organizations should have NPIs (if eligible) and Tax IDs. The absence of expected identifier types may indicate data quality issues, but could also flag potentially fraudulent entities. The test should also check for identifier format validity to ensure that supposedly authoritative identifiers are actually well-formed and not placeholder values.

## Data Freshness Indicators

This test examines temporal metadata fields such as lastUpdated timestamps, meta.versionId, and any attestation or verification date fields to assess whether directory data is being actively maintained or has become stale. Healthcare information changes frequently - practitioners move, organizations close or reorganize, licenses expire - so data freshness is a critical trust indicator. Directories with widespread stale data can lead to failed care coordination and patient safety issues.

The test should flag resources that have not been updated within a reasonable timeframe (perhaps 6-12 months depending on use case) and should monitor the overall age distribution of the directory. A healthy directory shows continuous update activity across all resource types, while a deteriorating directory will show an aging data population with update activity concentrated in recent entries only. This test also helps detect broken update pipelines where new data stops flowing even though the directory infrastructure remains operational.

## Cross-Reference Integrity

This test validates that reference fields pointing from one resource to another (such as PractitionerRole.practitioner, OrganizationAffiliation.organization, or Endpoint references from various resources) actually point to resources that exist in the directory. Broken references represent a critical data integrity failure that can cause application errors, failed searches, and loss of trust in the directory's reliability. Even a small percentage of broken references indicates serious problems with data loading, update synchronization, or resource lifecycle management.

The test should examine all major reference pathways in the directory and report both broken reference counts and broken reference rates for each pathway. Some broken references may be expected if the directory intentionally includes references to external resources, but within-directory references should have near-perfect integrity. A sudden spike in broken references often indicates a data processing failure where one resource type was updated or reloaded but related resources were not properly synchronized.

## Specialty Taxonomy Breadth

This test measures the diversity of medical specialties and provider types represented in the directory by analyzing the distribution of NUCC taxonomy codes and specialty codings in PractitionerRole and Organization resources. A comprehensive, trustworthy healthcare directory should include a broad spectrum of specialties reflecting the full range of healthcare services in the coverage area. Narrow specialty representation suggests either incomplete source data, geographic limitations, or a directory focused only on a subset of healthcare rather than comprehensive coverage.

The test should not only count distinct specialties but should also validate that the specialty distribution is reasonable for the claimed coverage area. For example, a directory claiming to cover an entire state should include common specialties like family medicine, internal medicine, pediatrics, and obstetrics in substantial numbers, while also including reasonable representation of surgical subspecialties and allied health professions. Absence of entire specialty categories or unusual specialty distributions should trigger alerts about possible data completeness or quality issues.

## Organization Name Duplication Detection

This test identifies patterns of suspicious organization name duplication that might indicate data quality problems, organizational restructuring that was not properly handled, or potentially fraudulent entities. While some name overlap is legitimate (multiple "St. Mary's Hospital" facilities in different locations), excessive exact-match duplication - especially when combined with different identifiers or locations - suggests problems with deduplication logic, entity resolution failures, or data entry errors.

The test should distinguish between legitimate cases (franchise operations, multi-site organizations with similar naming) and problematic cases (identical names with conflicting addresses, identical names with no clear organizational relationship). It should also detect near-duplicates that differ only in punctuation, capitalization, or minor spelling variations, as these often represent the same organization entered multiple times. High duplication rates undermine directory trust because users cannot reliably distinguish between distinct entities and cannot be sure they are connecting with the correct organization.

## Telecom System Diversity

This test validates that organizations have multiple types of contact mechanisms (phone, fax, email, SMS, URL) rather than relying on a single communication channel. Communication redundancy is important for operational reliability - if the primary contact method fails, alternative channels enable continued coordination. Organizations with diverse contact options demonstrate higher operational maturity and are generally more reliable directory participants.

The test should measure both the count and types of telecom systems per organization, flagging organizations that have only one contact method or that rely exclusively on outdated methods (fax only). It should also validate that different system types are actually present - not just multiple instances of the same type (three phone numbers but no email). This test serves as both a data quality indicator and a trust signal about organizational sophistication and accessibility.

## Hours of Operation Coverage

This test measures the percentage of locations and organizations that have hours of operation specified in the availableTime or hoursOfOperation fields. Operating hours are critical for care coordination because they enable appropriate scheduling, help patients avoid unnecessary trips to closed facilities, and support automated appointment booking systems. Lack of hours information significantly reduces the utility of a directory for operational use cases and suggests incomplete data collection.

The test should measure not just presence but also completeness of hours data - ideally specifying hours for each day of the week and handling special cases like 24/7 operations or variable schedules. Locations without hours information cannot support automated scheduling workflows and require manual follow-up, reducing the efficiency gains that healthcare directories are meant to provide. A directory with low hours coverage is less trusted for operational integration because it cannot reliably support time-sensitive care coordination.

## Endpoint Ownership Validation

This test ensures that every endpoint in the directory is actually referenced by at least one active organization, practitioner role, or location - in other words, that there are no orphaned endpoints that exist in isolation without any clear owner or responsible party. Orphaned endpoints represent data integrity failures and security concerns because it is unclear who is responsible for maintaining them, who should be contacted about issues, or whether they should even be trusted for data exchange.

The test should identify endpoints that are not referenced by any other resource and flag them for review or cleanup. It should also examine the reverse relationship - organizations or roles that claim to have endpoints but where those endpoint references are broken. Proper endpoint ownership is fundamental to trust in health information exchange because it establishes accountability and enables validation of endpoint authenticity. High rates of orphaned endpoints suggest problems with data loading sequence, reference integrity, or endpoint lifecycle management.

## Practitioner Name Component Completeness

This test validates that practitioners have complete name information including both family (last) name and given (first) name components, rather than having only partial name data or names that appear to be placeholder values. Complete names are essential for human-readable directories, for disambiguation between similar providers, and for integration with credentialing and verification systems that rely on name matching.

The test should flag practitioners with only single-word names (unless culturally appropriate), names that contain obviously placeholder text ("Test", "Unknown", "Pending"), or names with suspicious patterns like all capitals or special characters. It should also check for minimum name length to catch initials-only entries. From a trust perspective, practitioners without complete, real names cannot be properly verified against external credential sources and may represent test data, data entry errors, or potentially fraudulent entries. A high rate of incomplete names indicates serious data quality issues that undermine directory reliability.
