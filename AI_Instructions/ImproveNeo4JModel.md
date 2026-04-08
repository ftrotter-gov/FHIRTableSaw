Improve Neo4J Model
======================

Understood. Here is your dictation, rewritten clearly and cleanly.

The Neo4j import model for the FHIR objects needs to be improved based on the structure of the source data. First, the current handling of addresses is incorrect. Addresses should be represented as coherent objects that include lines, city, state, postal code, and related fields. These components should not be modeled as separate objects in Neo4j.

Second, telecom data should be reorganized. Instead of using a generic telecom structure, the data should be separated into specific categories such as email, phone, and fax. Each of these should be represented explicitly rather than grouped under a single telecom abstraction.

Third, identifiers must be treated as pairs of system and value. These two elements are meaningless without each other and must always be kept together. An identifier should therefore be represented as a combined unit consisting of both its system and its value.

Finally, NPIs should be modeled differently depending on the resource type. For organizations, there may be zero or more NPIs, so these should be represented as a list. For practitioners, there should only be a single NPI, so this should be represented as a singular value.

When looking at the "qualifications" block. These should be broken out.

When you see the
        {
            "code": {
                "coding": [
                    {
                        "system": "http://nucc.org/provider-taxonomy",
                        "code": "208M00000X",
                        "display": "Hospitalist Physician"
                    }
                ],
                "text": "Hospitalist Physician"
            }
        },

        This needs to be added a "specialty" array and "Hospitalist Physician" added to it (only once... no matter how many later mentions of this speciality there are)
        the way you tell this is that there is the "system": "http://nucc.org/provider-taxonomy", this always means that the resulting data goes into the "speciality" array

    When you see:
        {
            "code": {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/v2-0360",
                        "code": "MD",
                        "display": "Doctor of Medicine"
                    }
                ]
            }
        },

When you see a block like:

        {
            "code": {
                "coding": [
                    {
                        "system": "http://nucc.org/provider-taxonomy",
                        "code": "207R00000X",
                        "display": "Internal Medicine Physician"
                    }
                ]
            },
            "issuer": {
                "reference": "Organization/Organization-State-PA"
            },
            "identifier": [
                {
                    "use": "official",
                    "type": {
                        "coding": [
                            {
                                "system": "http://terminology.hl7.org/CodeSystem/v2-0203",
                                "code": "MD",
                                "display": "Medical license number"
                            }
                        ]
                    },
                    "value": "MT207604"
                }
            ]
        },

        This is two things. First it is a speciality block.
        Then it is also a medical license block. Generally, this means that there a state code, and a license number. The pair of these together is what it means to have a single entry in the license array.

When you see:
        {
            "coding": [
                {
                    "system": "urn:ietf:bcp:47",
                    "code": "en",
                    "display": "English"
                }
            ],
            "text": "English"
        }

        nobody cares. English is not relevant information for a clinician in the US... but when you see:


    "communication": [
        {
            "coding": [
                {
                    "system": "urn:ietf:bcp:47",
                    "code": "es",
                    "display": "Spanish"
                }
            ],
            "text": "Spanish"
        },

        Any non english language should be added to a "languages_spoken" array on the practicioner.
