US_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"

US_CORE_RACE_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
US_CORE_ETHNICITY_URL = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity"

# Placeholder base for project extensions
EXAMPLE_EXT_BASE = "https://example.com/extension_url/"

# NDH
NDH_ENDPOINT_REFERENCE_EXT_URL = (
    "https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-base-ext-endpoint-reference.html"
)

NDH_COMM_PROFICIENCY_EXT_URL = (
    "https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-base-ext-communication-proficiency.html"
)

# --- Clinical Organization placeholders / NDH URLs ---

# NDH Logo extension (real URL provided)
NDH_LOGO_EXT_URL = "https://build.fhir.org/ig/HL7/fhir-us-ndh/StructureDefinition-base-ext-logo.html"

# Placeholder extension URLs (per instruction)
ORG_ALIAS_TYPE_EXT_URL = "https://example.com/extension_url/org-alias-type"
ORG_RATING_EXT_URL = "https://example.com/extension_url/rating"

# --- PractitionerRole placeholders / extension URLs ---

PRACTITIONER_ROLE_ACCEPTING_NEW_PATIENTS_EXT_URL = (
    "https://example.com/extension_url/accepting_new_patients"
)
PRACTITIONER_ROLE_RATING_EXT_URL = "https://example.com/extension_url/practitioner_role_rating"

PRACTITIONER_ROLE_CMS_PECOS_VALIDATED_EXT_URL = (
    "https://example.com/extension_url/practitioner_role_cms_pecos_validated"
)
PRACTITIONER_ROLE_CMS_IAL2_VALIDATED_EXT_URL = (
    "https://example.com/extension_url/practitioner_role_cms_ial2_validated"
)
PRACTITIONER_ROLE_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL = (
    "https://example.com/extension_url/practitioner_role_has_cms_aligned_data_network"
)

ORG_CMS_PECOS_VALIDATED_EXT_URL = "https://example.com/extension_url/cms_pecos_validated"
ORG_CMS_IAL2_VALIDATED_EXT_URL = "https://example.com/extension_url/cms_ial2_validated"
ORG_HAS_CMS_ALIGNED_DATA_NETWORK_EXT_URL = (
    "https://example.com/extension_url/has_cms_aligned_data_network"
)

# FHIR org type coding
HL7_ORG_TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/organization-type"
HL7_ORG_TYPE_PROV_CODE = "prov"

# NDH Organization profile URL (hardcode per instruction)
NDH_ORGANIZATION_PROFILE_URL = "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Organization"


# --- Location / NDH Location ---

NDH_LOCATION_PROFILE_URL = "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Location"

# NDH Location extension URLs (fixedUri on the extension SDs)
NDH_LOCATION_ACCESSIBILITY_EXT_URL = (
    "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-accessibility"
)
NDH_LOCATION_NEWPATIENTS_EXT_URL = (
    "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-newpatients"
)
NDH_LOCATION_VERIFICATION_STATUS_EXT_URL = (
    "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status"
)

# Core FHIR extension used by NDH Location
LOCATION_BOUNDARY_GEOJSON_EXT_URL = "http://hl7.org/fhir/StructureDefinition/location-boundary-geojson"
