Below is a **cleaned and structured version** of your description that you could use directly in an architecture brief. I preserved all your decisions but organized them into a clearer model aligned with **FHIR Organization + NDH extensions + your three-tier architecture rules**.

---

# Clinical Organization Model (FHIR Organization)

## Overview

In the CMS three-tier architecture, the FHIR **Organization** resource is treated as an overloaded type in the base specification. To simplify implementation and storage, organizations will be **materialized as separate logical objects based on organization type**.

Each organization type corresponds to a code from the **HL7 Organization Type CodeSystem**. For clinical provider organizations, the type code will be:

```
prov
```

Meaning:

```
Healthcare Provider Organization
```

Additional organization types (e.g., payer, network, government, etc.) may be implemented later as separate objects following the same pattern.

---

# Core Organization Structure

## Resource Type

```
Organization
```

## Meta

The resource identifier will align with the internal UUID.

Example:

```json
"meta": {
  "profile": [
    "http://hl7.org/fhir/us/ndh/StructureDefinition/ndh-Organization"
  ]
}
```

The FHIR resource `id` will match the system UUID.

---

# Organization Type

Clinical organizations use:

```json
"type": [
  {
    "coding": [
      {
        "system": "http://terminology.hl7.org/CodeSystem/organization-type",
        "code": "prov",
        "display": "Healthcare Provider"
      }
    ]
  }
]
```

This distinguishes the resource from other organization types such as payer, network, government, etc.

---

# Core Attributes

## Active

Boolean indicating whether the organization is active.

```
active : boolean
```

---

## Legal Name

The legal business name of the organization.

Stored in:

```
Organization.name
```

Example:

```json
"name": "Example Health Clinic LLC"
```

---

## Alias

Alternative names (e.g., DBA).

Stored as:

```
Organization.alias[]
```

An extension will identify the alias type:

```
org-alias-type
```

Examples of alias types:

* doing-business-as
* former-name
* common-name

---

## Description

Free-text description of the organization.

```
Organization.description
```

Cardinality:

```
0..1
```

---

## Logo

A single URL referencing the organization's logo.

```
Organization.extension (logo-url)
```

Cardinality:

```
0..1
```

---

## Rating

Although the NDH extension allows multiple ratings, CMS will store:

```
exactly one rating
```

Using the NDH rating extension.

```
extension: rating
```

---

# Verification Status

CMS validation will be tracked using the existing **verification-status extension**, consistent with the Practitioner model.

Possible CMS values include:

```
cms_pecos_validated
cms_ial2_validated
has_cms_aligned_data_network
```

These represent CMS validation states and participation signals.

---

# Identifiers

Clinical organizations will have:

```
exactly one NPI
```

Stored as:

```
Organization.identifier
```

Example:

```json
{
  "system": "http://hl7.org/fhir/sid/us-npi",
  "value": "1234567890"
}
```

If multiple NPIs are needed for sub-units, they should be modeled as **child organizations**.

---

# Telecom

Telecommunications information includes:

* phone
* fax

Stored in:

```
Organization.telecom[]
```

Other telecom types are not used.

---

# Address

Addresses are stored separately in the data model and linked via a **many-to-many bridge table**.

FHIR representation:

```
Organization.address[]
```

---

# Parent Organization

Organizations may reference a parent organization using:

```
Organization.partOf
```

Example:

```json
"partOf": {
  "reference": "Organization/parent-org"
}
```

This allows hierarchical structures such as:

```
Health System
   └ Hospital
       └ Department
```

---

# Contact

Each organization will have **one primary contact**.

FHIR representation:

```
Organization.contact
```

Includes:

* name
* telecom
* address

Example:

```json
"contact": [
  {
    "name": {
      "family": "Johnson",
      "given": ["Emily"]
    },
    "telecom": [
      {
        "system": "phone",
        "value": "7035551212"
      }
    ]
  }
]
```

Cardinality:

```
exactly one
```

---

# Endpoints

Organizations may reference **multiple endpoints**.

```
Organization.endpoint[]
```

These endpoints represent:

* APIs
* Direct addresses
* FHIR servers
* other connectivity mechanisms

---

# Credential vs Clinical Type

Instead of using the NDH **qualification extension**, the architecture separates:

* **clinical organization type**
* **credential information**

into separate objects in the database layer.

This simplifies modeling and avoids overloading the FHIR qualification pattern.

---

# Summary of Key Constraints

| Field               | CMS Rule                           |
| ------------------- | ---------------------------------- |
| Organization.type   | `prov` for clinical organizations  |
| meta.id             | same as UUID                       |
| rating              | exactly one                        |
| description         | 0..1                               |
| logo URL            | 0..1                               |
| active              | boolean                            |
| name                | legal business name                |
| alias               | multiple with alias-type extension |
| telecom             | phone and fax only                 |
| address             | many via bridge table              |
| partOf              | optional parent organization       |
| contact             | exactly one                        |
| endpoints           | multiple                           |
| identifier          | exactly one NPI                    |
| verification-status | used for CMS validation            |

---

If you'd like, I can also show you the **minimal JSON example of a Clinical Organization that matches this entire model**, which is helpful for implementers because Organization in NDH has **a lot of optional fields and extensions**.
