"""
Endpoint URL Format Validity

Validates that all endpoint URLs are well-formed, use expected protocols
(primarily https for FHIR endpoints), and follow proper URI syntax.

Malformed endpoint URLs represent complete blockers to automated data
exchange. This test checks for: http instead of https, missing/malformed
paths, invalid domain names, placeholder text, and empty addresses.
A high rate of malformed URLs indicates poor source data quality or
broken URL normalization logic in the data processing pipeline.

TODO: this should be replaced by the use of the https://github.com/TransparentHealth/inspectorfhir
"""
import re
from urllib.parse import urlparse

from src.utils.inlaw import InLaw


class ValidateEndpointUrlFormat(InLaw):
    """Validate endpoint URLs are well-formed, use https, and are not placeholders."""

    title = "Endpoint URLs should be well-formed, use https, and not contain placeholder text"

    @staticmethod
    def run(engine, config: dict | None = None):
        """
        Validate endpoint URL format across multiple dimensions.

        Checks: (1) null/empty addresses, (2) well-formed URL syntax,
        (3) HTTPS usage for http-scheme URLs, (4) placeholder detection.

        Args:
            engine: SQLAlchemy engine for DuckDB connection
            config: Configuration dictionary (all thresholds have defaults)

        Returns:
            True if all checks pass, error message string if any fail
        """
        if config is None:
            return "SKIPPED: No config provided"

        # ── Threshold variables ──────────────────────────────────
        # Minimum fraction of endpoints with syntactically valid URLs.
        # 95% floor allows for non-URL addresses (mailto:, urn:).
        the_minimum_percent_well_formed_urls = config.get(
            'min_percent_well_formed_urls', 0.95
        )

        # Minimum fraction of http(s)-scheme endpoints using https.
        # FHIR endpoints transmit PHI and should use TLS. 80% allows
        # legacy http while flagging systemic security problems.
        the_minimum_percent_https_urls = config.get(
            'min_percent_https_urls', 0.80
        )

        # Maximum fraction of endpoints with placeholder/test text.
        # Even 1% suggests test data leaked into production.
        the_maximum_percent_placeholder_urls = config.get(
            'max_percent_placeholder_urls', 0.01
        )

        # Maximum fraction of endpoints with NULL/empty address.
        # Every endpoint should have a URL; 2% for rare omissions.
        the_maximum_percent_missing_address = config.get(
            'max_percent_missing_address', 0.02
        )

        # ── Fetch all endpoint addresses ─────────────────────────
        sql_all_addresses = """
            SELECT
                json_extract_string(resource, '$.id') AS endpoint_id,
                json_extract_string(resource, '$.address') AS address
            FROM fhir_resources
            WHERE json_extract_string(resource, '$.resourceType') = 'Endpoint'
        """
        gx_df = InLaw.to_gx_dataframe(sql_all_addresses, engine)
        df = gx_df.active_batch.data.dataframe

        total_endpoints = len(df)
        if total_endpoints == 0:
            return ("validate_url_format Error: "
                    "No Endpoint resources found in fhir_resources")

        failures = []

        # ── Check 1: Null / empty addresses ──────────────────────
        missing_mask = (
            df['address'].isna()
            | (df['address'].str.strip() == '')
        )
        missing_count = int(missing_mask.sum())
        missing_pct = missing_count / total_endpoints

        if missing_pct > the_maximum_percent_missing_address:
            failures.append(
                f"Missing/empty addresses: {missing_count}/{total_endpoints} "
                f"({missing_pct:.2%}) exceeds max "
                f"{the_maximum_percent_missing_address:.2%}"
            )

        df_present = df[~missing_mask].copy()
        present_count = len(df_present)

        if present_count == 0:
            failures.append(
                "validate_url_format Error: "
                "All endpoint addresses are missing/empty"
            )
            return "; ".join(failures)

        # ── Check 2: Well-formed URL syntax ──────────────────────
        df_present['is_well_formed'] = df_present['address'].apply(
            ValidateEndpointUrlFormat._is_well_formed_url
        )
        well_formed_count = int(df_present['is_well_formed'].sum())
        well_formed_pct = well_formed_count / present_count

        if well_formed_pct < the_minimum_percent_well_formed_urls:
            malformed_count = present_count - well_formed_count
            failures.append(
                f"Well-formed URLs: {well_formed_count}/{present_count} "
                f"({well_formed_pct:.2%}) below min "
                f"{the_minimum_percent_well_formed_urls:.2%} "
                f"({malformed_count} malformed)"
            )

        # ── Check 3: HTTPS usage ─────────────────────────────────
        http_mask = df_present['address'].str.lower().str.startswith(
            ('http://', 'https://')
        )
        http_df = df_present[http_mask]
        http_total = len(http_df)

        if http_total > 0:
            https_mask = http_df['address'].str.lower().str.startswith(
                'https://'
            )
            https_count = int(https_mask.sum())
            https_pct = https_count / http_total
            if https_pct < the_minimum_percent_https_urls:
                http_only = http_total - https_count
                failures.append(
                    f"HTTPS usage: {https_count}/{http_total} "
                    f"({https_pct:.2%}) below min "
                    f"{the_minimum_percent_https_urls:.2%} "
                    f"({http_only} use insecure http)"
                )

        # ── Check 4: Placeholder / test URL detection ────────────
        df_present['is_placeholder'] = df_present['address'].apply(
            ValidateEndpointUrlFormat._is_placeholder_url
        )
        placeholder_count = int(df_present['is_placeholder'].sum())
        placeholder_pct = placeholder_count / present_count

        if placeholder_pct > the_maximum_percent_placeholder_urls:
            failures.append(
                f"Placeholder URLs: {placeholder_count}/{present_count} "
                f"({placeholder_pct:.2%}) exceeds max "
                f"{the_maximum_percent_placeholder_urls:.2%}"
            )

        if not failures:
            return True
        return "; ".join(failures)

    # ── Helper methods (underscore prefix = internal use only) ────

    @staticmethod
    def _is_well_formed_url(url_string):
        """
        Check whether a URL string is syntactically well-formed.

        For http/https schemes: requires both scheme and netloc (domain).
        For other schemes (mailto:, urn:): requires scheme and path.

        Args:
            url_string: The URL string to validate

        Returns:
            True if the URL is well-formed, False otherwise
        """
        try:
            parsed = urlparse(str(url_string).strip())
            has_scheme = bool(parsed.scheme)
            has_netloc = bool(parsed.netloc)
            has_path = bool(parsed.path)

            if parsed.scheme in ('http', 'https'):
                return has_scheme and has_netloc
            # For other schemes (mailto:, urn:, etc.)
            return has_scheme and (has_netloc or has_path)
        except Exception:
            return False

    @staticmethod
    def _is_placeholder_url(url_string):
        """
        Detect whether a URL contains obvious placeholder or test text.

        Matches common patterns: example.com, localhost, test domains,
        TODO markers, and other sentinel values.

        Args:
            url_string: The URL string to check

        Returns:
            True if the URL appears to be a placeholder, False otherwise
        """
        url_lower = str(url_string).strip().lower()

        placeholder_patterns = [
            r'example\.com',
            r'example\.org',
            r'example\.net',
            r'localhost',
            r'127\.0\.0\.1',
            r'\btest\b',
            r'\btodo\b',
            r'\bplaceholder\b',
            r'\bfake\b',
            r'\bdummy\b',
            r'\bsample\b',
            r'\.invalid$',
            r'\.test$',
            r'\.example$',
            r'\.localhost$',
            r'your[_\-]?url',
            r'replace[_\-]?me',
            r'changeme',
            r'xxx+',
            r'0\.0\.0\.0',
        ]

        for pattern in placeholder_patterns:
            if re.search(pattern, url_lower):
                return True
        return False
