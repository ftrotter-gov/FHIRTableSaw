"""
Cross-Reference Integrity Validation

Validates that reference fields pointing from one resource to another actually
point to resources that exist in the directory. Broken references represent a
critical data integrity failure that can cause application errors, failed
searches, and loss of trust in the directory's reliability.

This test examines the major reference pathways:
  1. PractitionerRole.practitioner -> Practitioner
  2. PractitionerRole.organization -> Organization
  3. PractitionerRole.endpoint[] -> Endpoint
  4. OrganizationAffiliation.organization -> Organization
  5. OrganizationAffiliation.participatingOrganization -> Organization

For each pathway, it reports broken reference counts and rates.
Within-directory references should have near-perfect integrity.
A sudden spike in broken references often indicates a data processing failure
where one resource type was updated or reloaded but related resources were not
properly synchronized.
"""
from src.utils.inlaw import InLaw


class ValidateCrossReferences(InLaw):
    """Validate that cross-resource references point to resources that actually exist."""

    title = "Cross-reference integrity: references between resources should resolve to existing resources"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate cross-reference integrity across FHIR resource types.

        Checks each major reference pathway separately using simple SQL
        queries, then aggregates results to determine overall pass/fail.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (accepted for InLaw compatibility)

        Returns:
            True if all reference pathways are within acceptable thresholds,
            error message string describing which pathways failed if not
        """
        if config is None:
            return "SKIPPED: No config provided"

        # =============================================================
        # Threshold definitions
        #
        # Maximum acceptable fraction of broken references per pathway.
        # Values are fractions (e.g. 0.01 = 1%). Within-directory
        # references should have near-perfect integrity.
        # =============================================================

        # PractitionerRole.practitioner -> Practitioner
        # Primary link from a role to the practitioner filling it.
        # Broken refs here mean roles with no identifiable provider.
        the_maximum_percent_broken_practitioner_references = 0.01

        # PractitionerRole.organization -> Organization
        # Links role to the org where the practitioner works.
        the_maximum_percent_broken_organization_references = 0.01

        # PractitionerRole.endpoint[] -> Endpoint
        # Slightly higher threshold: endpoints may be managed externally.
        the_maximum_percent_broken_endpoint_references = 0.02

        # OrganizationAffiliation.organization -> Organization (primary)
        the_maximum_percent_broken_affiliation_org_references = 0.01

        # OrganizationAffiliation.participatingOrganization -> Organization
        the_maximum_percent_broken_affiliation_participating_org_references = 0.01

        # Absolute cap on total broken references across all pathways.
        # Even if percentages are low, a large absolute count suggests
        # systemic issues worth investigating.
        the_maximum_total_broken_references = 100

        failures = []
        total_broken_references = 0

        # --- Pathway 1: PractitionerRole.practitioner -> Practitioner ---
        r1 = ValidateCrossReferences._check_practitioner_role_to_practitioner(
            engine=engine,
            maximum_broken_percent=the_maximum_percent_broken_practitioner_references,
        )
        if r1 is not True:
            failures.append(r1)
            if isinstance(r1, tuple):
                total_broken_references += r1[1]

        # --- Pathway 2: PractitionerRole.organization -> Organization ---
        r2 = ValidateCrossReferences._check_practitioner_role_to_organization(
            engine=engine,
            maximum_broken_percent=the_maximum_percent_broken_organization_references,
        )
        if r2 is not True:
            failures.append(r2)
            if isinstance(r2, tuple):
                total_broken_references += r2[1]

        # --- Pathway 3: PractitionerRole.endpoint[] -> Endpoint ---
        r3 = ValidateCrossReferences._check_practitioner_role_to_endpoint(
            engine=engine,
            maximum_broken_percent=the_maximum_percent_broken_endpoint_references,
        )
        if r3 is not True:
            failures.append(r3)
            if isinstance(r3, tuple):
                total_broken_references += r3[1]

        # --- Pathway 4: OrgAffiliation.organization -> Organization ---
        r4 = ValidateCrossReferences._check_org_affiliation_to_organization(
            engine=engine,
            maximum_broken_percent=the_maximum_percent_broken_affiliation_org_references,
        )
        if r4 is not True:
            failures.append(r4)
            if isinstance(r4, tuple):
                total_broken_references += r4[1]

        # --- Pathway 5: OrgAffiliation.participatingOrganization -> Org ---
        r5 = ValidateCrossReferences._check_org_affiliation_to_participating_org(
            engine=engine,
            maximum_broken_percent=the_maximum_percent_broken_affiliation_participating_org_references,
        )
        if r5 is not True:
            failures.append(r5)
            if isinstance(r5, tuple):
                total_broken_references += r5[1]

        # Check absolute total broken references cap
        if total_broken_references > the_maximum_total_broken_references:
            failures.append(
                f"Total broken references ({total_broken_references}) exceeds "
                f"maximum allowed ({the_maximum_total_broken_references})"
            )

        # Collect string failure messages (tuples carry (msg, count))
        failure_messages = []
        for item in failures:
            if isinstance(item, tuple):
                failure_messages.append(item[0])
            elif isinstance(item, str):
                failure_messages.append(item)

        if not failure_messages:
            return True

        return "; ".join(failure_messages)

    # =================================================================
    # Internal helper methods for each reference pathway.
    # Each returns True on success, or (error_message, broken_count)
    # tuple on failure.
    # =================================================================

    @staticmethod
    def _check_practitioner_role_to_practitioner(*, engine, maximum_broken_percent):
        """
        Check PractitionerRole.practitioner references resolve to
        existing Practitioner resources.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            maximum_broken_percent: Max acceptable fraction of broken refs

        Returns:
            True if within threshold, or (error_message, broken_count)
        """
        total_sql = """
            SELECT COUNT(*) AS total_with_practitioner_ref
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(resource, '$.practitioner.reference') IS NOT NULL
        """
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_with_ref = int(total_df['total_with_practitioner_ref'].iloc[0])

        if total_with_ref == 0:
            return True

        broken_sql = """
            SELECT COUNT(*) AS broken_practitioner_ref_count
            FROM fhir_resources AS practitioner_role
            WHERE json_extract_string(practitioner_role.resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(practitioner_role.resource, '$.practitioner.reference') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM fhir_resources AS practitioner
                  WHERE json_extract_string(practitioner.resource, '$.resourceType') = 'Practitioner'
                    AND json_extract_string(practitioner_role.resource, '$.practitioner.reference')
                        LIKE '%' || json_extract_string(practitioner.resource, '$.id')
              )
        """
        broken_gx_df = InLaw.to_gx_dataframe(broken_sql, engine)
        broken_df = broken_gx_df.active_batch.data.dataframe
        broken_count = int(broken_df['broken_practitioner_ref_count'].iloc[0])

        broken_percent = broken_count / total_with_ref
        if broken_percent <= maximum_broken_percent:
            return True

        return (
            f"PractitionerRole->Practitioner: {broken_count}/{total_with_ref} "
            f"broken ({broken_percent:.2%}, threshold {maximum_broken_percent:.2%})",
            broken_count,
        )

    @staticmethod
    def _check_practitioner_role_to_organization(*, engine, maximum_broken_percent):
        """
        Check PractitionerRole.organization references resolve to
        existing Organization resources.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            maximum_broken_percent: Max acceptable fraction of broken refs

        Returns:
            True if within threshold, or (error_message, broken_count)
        """
        total_sql = """
            SELECT COUNT(*) AS total_with_organization_ref
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(resource, '$.organization.reference') IS NOT NULL
        """
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_with_ref = int(total_df['total_with_organization_ref'].iloc[0])

        if total_with_ref == 0:
            return True

        broken_sql = """
            SELECT COUNT(*) AS broken_organization_ref_count
            FROM fhir_resources AS practitioner_role
            WHERE json_extract_string(practitioner_role.resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(practitioner_role.resource, '$.organization.reference') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM fhir_resources AS organization
                  WHERE json_extract_string(organization.resource, '$.resourceType') = 'Organization'
                    AND json_extract_string(practitioner_role.resource, '$.organization.reference')
                        LIKE '%' || json_extract_string(organization.resource, '$.id')
              )
        """
        broken_gx_df = InLaw.to_gx_dataframe(broken_sql, engine)
        broken_df = broken_gx_df.active_batch.data.dataframe
        broken_count = int(broken_df['broken_organization_ref_count'].iloc[0])

        broken_percent = broken_count / total_with_ref
        if broken_percent <= maximum_broken_percent:
            return True

        return (
            f"PractitionerRole->Organization: {broken_count}/{total_with_ref} "
            f"broken ({broken_percent:.2%}, threshold {maximum_broken_percent:.2%})",
            broken_count,
        )

    @staticmethod
    def _check_practitioner_role_to_endpoint(*, engine, maximum_broken_percent):
        """
        Check PractitionerRole.endpoint[] references resolve to
        existing Endpoint resources.

        PractitionerRole.endpoint is an array of references. We unnest
        them and check each one against existing Endpoint resources.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            maximum_broken_percent: Max acceptable fraction of broken refs

        Returns:
            True if within threshold, or (error_message, broken_count)
        """
        total_sql = """
            SELECT COUNT(*) AS total_endpoint_refs
            FROM fhir_resources AS practitioner_role,
                 unnest(json_extract(practitioner_role.resource, '$.endpoint')) AS endpoint_ref
            WHERE json_extract_string(practitioner_role.resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(endpoint_ref, '$.reference') IS NOT NULL
        """
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_endpoint_refs = int(total_df['total_endpoint_refs'].iloc[0])

        if total_endpoint_refs == 0:
            return True

        broken_sql = """
            SELECT COUNT(*) AS broken_endpoint_ref_count
            FROM fhir_resources AS practitioner_role,
                 unnest(json_extract(practitioner_role.resource, '$.endpoint')) AS endpoint_ref
            WHERE json_extract_string(practitioner_role.resource, '$.resourceType') = 'PractitionerRole'
              AND json_extract_string(endpoint_ref, '$.reference') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM fhir_resources AS endpoint
                  WHERE json_extract_string(endpoint.resource, '$.resourceType') = 'Endpoint'
                    AND json_extract_string(endpoint_ref, '$.reference')
                        LIKE '%' || json_extract_string(endpoint.resource, '$.id')
              )
        """
        broken_gx_df = InLaw.to_gx_dataframe(broken_sql, engine)
        broken_df = broken_gx_df.active_batch.data.dataframe
        broken_count = int(broken_df['broken_endpoint_ref_count'].iloc[0])

        broken_percent = broken_count / total_endpoint_refs
        if broken_percent <= maximum_broken_percent:
            return True

        return (
            f"PractitionerRole->Endpoint: {broken_count}/{total_endpoint_refs} "
            f"broken ({broken_percent:.2%}, threshold {maximum_broken_percent:.2%})",
            broken_count,
        )

    @staticmethod
    def _check_org_affiliation_to_organization(*, engine, maximum_broken_percent):
        """
        Check OrganizationAffiliation.organization references resolve to
        existing Organization resources.

        This is the primary organization in the affiliation relationship.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            maximum_broken_percent: Max acceptable fraction of broken refs

        Returns:
            True if within threshold, or (error_message, broken_count)
        """
        total_sql = """
            SELECT COUNT(*) AS total_with_org_ref
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'OrganizationAffiliation'
              AND json_extract_string(resource, '$.organization.reference') IS NOT NULL
        """
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_with_ref = int(total_df['total_with_org_ref'].iloc[0])

        if total_with_ref == 0:
            return True

        broken_sql = """
            SELECT COUNT(*) AS broken_affiliation_org_ref_count
            FROM fhir_resources AS org_affiliation
            WHERE json_extract_string(org_affiliation.resource, '$.resourceType') = 'OrganizationAffiliation'
              AND json_extract_string(org_affiliation.resource, '$.organization.reference') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM fhir_resources AS organization
                  WHERE json_extract_string(organization.resource, '$.resourceType') = 'Organization'
                    AND json_extract_string(org_affiliation.resource, '$.organization.reference')
                        LIKE '%' || json_extract_string(organization.resource, '$.id')
              )
        """
        broken_gx_df = InLaw.to_gx_dataframe(broken_sql, engine)
        broken_df = broken_gx_df.active_batch.data.dataframe
        broken_count = int(broken_df['broken_affiliation_org_ref_count'].iloc[0])

        broken_percent = broken_count / total_with_ref
        if broken_percent <= maximum_broken_percent:
            return True

        return (
            f"OrgAffiliation->Organization: {broken_count}/{total_with_ref} "
            f"broken ({broken_percent:.2%}, threshold {maximum_broken_percent:.2%})",
            broken_count,
        )

    @staticmethod
    def _check_org_affiliation_to_participating_org(*, engine, maximum_broken_percent):
        """
        Check OrganizationAffiliation.participatingOrganization references
        resolve to existing Organization resources.

        This is the participating organization - the entity fulfilling
        the role relative to the primary organization.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            maximum_broken_percent: Max acceptable fraction of broken refs

        Returns:
            True if within threshold, or (error_message, broken_count)
        """
        total_sql = """
            SELECT COUNT(*) AS total_with_participating_org_ref
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'OrganizationAffiliation'
              AND json_extract_string(resource, '$.participatingOrganization.reference') IS NOT NULL
        """
        total_gx_df = InLaw.to_gx_dataframe(total_sql, engine)
        total_df = total_gx_df.active_batch.data.dataframe
        total_with_ref = int(total_df['total_with_participating_org_ref'].iloc[0])

        if total_with_ref == 0:
            return True

        broken_sql = """
            SELECT COUNT(*) AS broken_participating_org_ref_count
            FROM fhir_resources AS org_affiliation
            WHERE json_extract_string(org_affiliation.resource, '$.resourceType') = 'OrganizationAffiliation'
              AND json_extract_string(org_affiliation.resource, '$.participatingOrganization.reference') IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM fhir_resources AS organization
                  WHERE json_extract_string(organization.resource, '$.resourceType') = 'Organization'
                    AND json_extract_string(org_affiliation.resource, '$.participatingOrganization.reference')
                        LIKE '%' || json_extract_string(organization.resource, '$.id')
              )
        """
        broken_gx_df = InLaw.to_gx_dataframe(broken_sql, engine)
        broken_df = broken_gx_df.active_batch.data.dataframe
        broken_count = int(broken_df['broken_participating_org_ref_count'].iloc[0])

        broken_percent = broken_count / total_with_ref
        if broken_percent <= maximum_broken_percent:
            return True

        return (
            f"OrgAffiliation->participatingOrganization: {broken_count}/{total_with_ref} "
            f"broken ({broken_percent:.2%}, threshold {maximum_broken_percent:.2%})",
            broken_count,
        )
