"""
Organization Hierarchy Integrity

Examines partOf relationships between organizations to ensure hierarchies are
reasonable in depth and structure. Detects:
  1. Circular references (Organization A is partOf B, which is partOf A, etc.)
  2. Excessively deep hierarchies (more levels of nesting than expected)
  3. Broken parent references (partOf points to a non-existent organization)

Proper hierarchy representation is essential for understanding corporate structures,
ownership relationships, and lines of accountability in healthcare networks.
Broken or suspicious hierarchies indicate either technical data quality problems
or potentially attempts to obscure real ownership and control relationships.
"""
from src.utils.inlaw import InLaw


class ValidateHierarchyIntegrity(InLaw):
    """Validate organization partOf hierarchy for circular refs, excessive depth, and broken parents."""

    title = (
        "Organization hierarchy should have no circular references, "
        "reasonable depth, and valid parent references"
    )

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate the integrity of the organization partOf hierarchy.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (optional, defaults provided)

        Returns:
            True if all checks pass, error message string otherwise
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ---------------------------------------------------------------
        # Threshold variables with descriptive names
        # ---------------------------------------------------------------

        # Maximum partOf levels considered reasonable for healthcare org
        # hierarchies. Real-world rarely exceeds 5-6 levels (e.g.
        # system -> region -> hospital -> department -> unit -> sub-unit).
        the_maximum_hierarchy_depth_levels = 6

        # Maximum fraction of orgs with partOf pointing to a non-existent
        # parent. Small fraction tolerable (cross-file refs, loading order)
        # but high rate signals data corruption. 0.01 = 1%.
        the_maximum_percent_with_broken_parent_references = 0.01

        # Maximum circular reference chains allowed. Circular refs are
        # always data errors so the default is zero tolerance.
        the_maximum_number_of_circular_references = 0

        # ---------------------------------------------------------------
        # Step 1: Extract all orgs with their id and partOf reference
        # ---------------------------------------------------------------
        sql_org_hierarchy = """
            SELECT
                json_extract_string(resource, '$.id') AS org_id,
                json_extract_string(resource, '$.partOf.reference')
                    AS part_of_reference
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType')
                = 'Organization'
        """
        gx_df = InLaw.to_gx_dataframe(sql_org_hierarchy, engine)
        df = gx_df.active_batch.data.dataframe

        if len(df) == 0:
            return (
                "validate_hierarchy_integrity Error: "
                "No Organization resources found in fhir_resources"
            )

        # ---------------------------------------------------------------
        # Step 2: Normalise parent id from the reference string
        # ---------------------------------------------------------------
        df = df.copy()
        df['parent_id'] = df['part_of_reference'].apply(
            ValidateHierarchyIntegrity._extract_parent_id_from_reference
        )

        all_org_ids = set(df['org_id'].dropna())

        parent_lookup = {}
        for _, row in df.iterrows():
            org_id = row['org_id']
            parent_id = row['parent_id']
            if org_id is not None:
                parent_lookup[org_id] = parent_id

        failures = []

        # -- Check A: Broken parent references --
        broken_count = (
            ValidateHierarchyIntegrity._count_broken_parent_references(
                parent_lookup=parent_lookup,
                all_org_ids=all_org_ids,
            )
        )
        orgs_with_parent_count = sum(
            1 for pid in parent_lookup.values() if pid is not None
        )
        broken_pct = (
            (broken_count / orgs_with_parent_count)
            if orgs_with_parent_count > 0 else 0.0
        )

        if broken_pct > the_maximum_percent_with_broken_parent_references:
            failures.append(
                f"Broken parent references: {broken_count} of "
                f"{orgs_with_parent_count} orgs with partOf "
                f"({broken_pct:.2%}) exceed threshold "
                f"({the_maximum_percent_with_broken_parent_references:.2%})"
            )

        # -- Check B: Circular references --
        circular_org_ids = (
            ValidateHierarchyIntegrity._find_circular_references(
                parent_lookup=parent_lookup,
            )
        )
        if len(circular_org_ids) > the_maximum_number_of_circular_references:
            sample_ids = list(circular_org_ids)[:5]
            failures.append(
                f"Circular references detected: {len(circular_org_ids)} orgs "
                f"in partOf cycles (max allowed: "
                f"{the_maximum_number_of_circular_references}). "
                f"Sample org IDs: {sample_ids}"
            )

        # -- Check C: Excessively deep hierarchies --
        deepest_org_id, max_depth_found = (
            ValidateHierarchyIntegrity._find_maximum_hierarchy_depth(
                parent_lookup=parent_lookup,
                all_org_ids=all_org_ids,
            )
        )
        if max_depth_found > the_maximum_hierarchy_depth_levels:
            failures.append(
                f"Excessive hierarchy depth: deepest chain is "
                f"{max_depth_found} levels (max allowed: "
                f"{the_maximum_hierarchy_depth_levels}). "
                f"Deepest leaf org ID: {deepest_org_id}"
            )

        if not failures:
            return True
        return "; ".join(failures)

    # -------------------------------------------------------------------
    # Private helper static methods
    # -------------------------------------------------------------------

    @staticmethod
    def _extract_parent_id_from_reference(reference_string):
        """
        Extract the bare org id from a FHIR Reference string.

        FHIR partOf.reference values look like "Organization/abc-123".
        This strips the leading resource-type prefix.
        Returns None if input is None, empty, or not a string.
        """
        if reference_string is None:
            return None
        if not isinstance(reference_string, str):
            return None
        reference_string = reference_string.strip()
        if reference_string == '':
            return None
        if '/' in reference_string:
            return reference_string.split('/')[-1]
        return reference_string

    @staticmethod
    def _count_broken_parent_references(*, parent_lookup, all_org_ids):
        """
        Count orgs whose partOf references a parent id that does not
        exist in the set of known organization ids.
        """
        broken_count = 0
        for org_id, parent_id in parent_lookup.items():
            if parent_id is not None and parent_id not in all_org_ids:
                broken_count += 1
        return broken_count

    @staticmethod
    def _find_circular_references(*, parent_lookup):
        """
        Detect organizations that participate in a partOf cycle.

        Walks up the parent chain from each org.  If we revisit a node
        already seen on the current walk, that is a cycle.  Walk length
        is capped to avoid runaway iteration.
        """
        max_walk_steps = 1000
        orgs_in_cycles = set()
        confirmed_acyclic = set()

        for start_org_id in parent_lookup:
            if start_org_id in confirmed_acyclic:
                continue
            if start_org_id in orgs_in_cycles:
                continue

            visited_in_walk = []
            visited_set = set()
            current = start_org_id
            steps = 0

            while current is not None and steps < max_walk_steps:
                if current in confirmed_acyclic:
                    break
                if current in visited_set:
                    cycle_start = visited_in_walk.index(current)
                    cycle_members = visited_in_walk[cycle_start:]
                    orgs_in_cycles.update(cycle_members)
                    break
                visited_in_walk.append(current)
                visited_set.add(current)
                current = parent_lookup.get(current)
                steps += 1

            for node in visited_in_walk:
                if node not in orgs_in_cycles:
                    confirmed_acyclic.add(node)

        return orgs_in_cycles

    @staticmethod
    def _find_maximum_hierarchy_depth(*, parent_lookup, all_org_ids):
        """
        Walk the partOf chain for every org and return the deepest
        depth found plus the org_id at the bottom of that chain.

        Depth 1 = org with no parent.  Depth 2 = org whose parent has
        no parent, etc.  Walks are capped to avoid infinite loops on
        any undetected cycles.

        Returns:
            Tuple of (deepest_org_id, maximum_depth).
            Returns (None, 0) if there are no organizations.
        """
        max_walk_steps = 1000
        depth_cache = {}
        maximum_depth = 0
        deepest_org_id = None

        for org_id in all_org_ids:
            path = []
            current = org_id
            steps = 0

            while (
                current is not None
                and current not in depth_cache
                and current in all_org_ids
                and steps < max_walk_steps
            ):
                path.append(current)
                current = parent_lookup.get(current)
                steps += 1

            if current is not None and current in depth_cache:
                base_depth = depth_cache[current]
            else:
                base_depth = 0

            for i, node in enumerate(reversed(path)):
                depth_cache[node] = base_depth + i + 1

            org_depth = depth_cache.get(org_id, 1)
            if org_depth > maximum_depth:
                maximum_depth = org_depth
                deepest_org_id = org_id

        return deepest_org_id, maximum_depth
