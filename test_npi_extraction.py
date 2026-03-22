#!/usr/bin/env python3
"""Test script to verify NPI extraction from the identifier section."""

from fhir_tablesaw_3tier.fhir.practitioner import practitioner_from_fhir_json

# Sample from Practitioner.5.ndjson.pp
sample_practitioner = {
    "resourceType": "Practitioner",
    "id": "1003000753",
    "meta": {"lastUpdated": "2007-09-05T00:00:00Z"},
    "extension": [
        {
            "url": "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-endpoint-reference",
            "valueReference": {"reference": "Endpoint/2519657720674574745"},
        },
        {
            "url": "http://hl7.org/fhir/us/ndh/StructureDefinition/base-ext-verification-status",
            "valueCodeableConcept": {
                "coding": [
                    {
                        "system": "http://hl7.org/fhir/us/ndh/CodeSystem/NdhVerificationStatusCS",
                        "code": "complete",
                        "display": "Complete",
                    }
                ]
            },
        },
    ],
    "identifier": [
        {
            "type": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                        "code": "NPI",
                        "display": "National Provider Identifier",
                    }
                ]
            },
            "system": "http://hl7.org/fhir/sid/us-npi",
            "use": "official",
            "value": "1003000753",
            "period": {"start": "2007-09-05T00:00:00Z", "end": None},
        }
    ],
    "active": True,
    "name": [
        {
            "prefix": [],
            "given": ["JAMES", "F"],
            "family": "DEITLE",
            "suffix": [],
            "use": "official",
            "period": None,
        }
    ],
    "specialty": [
        {
            "coding": [
                {
                    "system": "http://nucc.org/provider-taxonomy",
                    "code": "363A00000X",
                    "display": "Physician Assistant",
                }
            ],
            "text": "Physician Assistant",
        }
    ],
}

print("Testing NPI extraction from identifier section...")
print("-" * 60)

try:
    practitioner, report = practitioner_from_fhir_json(sample_practitioner)

    print("✓ Successfully parsed Practitioner resource")
    print(f"  Resource UUID: {practitioner.resource_uuid}")
    print(f"  NPI: {practitioner.npi}")
    print(f"  Name: {practitioner.first_name} {practitioner.middle_name} {practitioner.last_name}")
    print(f"  Active: {practitioner.active_status}")

    # Verify the NPI matches what's in the identifier section
    expected_npi = "1003000753"
    if practitioner.npi == expected_npi:
        print("\n✓ NPI extraction SUCCESSFUL!")
        print(f"  Expected: {expected_npi}")
        print(f"  Got:      {practitioner.npi}")
        print(
            "\n✓ The identifier section with system 'http://hl7.org/fhir/sid/us-npi' is being used correctly!"
        )
    else:
        print("\n✗ NPI extraction FAILED!")
        print(f"  Expected: {expected_npi}")
        print(f"  Got:      {practitioner.npi}")

    print(f"\nDropped repeats report: {dict(report.dropped_counts)}")

except Exception as e:
    print(f"✗ Error parsing Practitioner: {e}")
    import traceback

    traceback.print_exc()
